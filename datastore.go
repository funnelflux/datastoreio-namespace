// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package datastoreio forks the Apache Beam datastore reader to add namespace
// awareness needed by the ledger migration pipeline.
package datastoreio

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type clientType interface {
	Run(context.Context, *datastore.Query) *datastore.Iterator
	Close() error
}

// newClientFuncType is the signature of the function datastore.NewClient
type newClientFuncType func(ctx context.Context, projectID string, opts ...option.ClientOption) (clientType, error)

const (
	scatterPropertyName = "__scatter__"
)

// FilterValueType controls how Filter.Value is interpreted before building the datastore query.
type FilterValueType string

const (
	FilterValueTypeString    FilterValueType = "string"
	FilterValueTypeInt       FilterValueType = "int"
	FilterValueTypeFloat     FilterValueType = "float"
	FilterValueTypeBool      FilterValueType = "bool"
	FilterValueTypeTimestamp FilterValueType = "timestamp"
)

// Filter represents a datastore property filter that is applied to each shard query.
type Filter struct {
	Field    string          `json:"field"`
	Operator string          `json:"operator"`
	Value    string          `json:"value"`
	Type     FilterValueType `json:"type"`
}

type readConfig struct {
	filters []Filter
}

// ReadOption customizes the Read helper.
type ReadOption func(*readConfig)

func newReadConfig(opts ...ReadOption) readConfig {
	cfg := readConfig{}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(&cfg)
	}
	return cfg
}

// WithFilters attaches one or more property filters to the datastore query.
func WithFilters(filters ...Filter) ReadOption {
	copied := append([]Filter(nil), filters...)
	return func(cfg *readConfig) {
		cfg.filters = append(cfg.filters, copied...)
	}
}

func init() {
	beam.RegisterType(reflect.TypeOf((*queryFn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*splitQueryFn)(nil)).Elem())
}

// Read reads all rows from the given kind within the provided namespace.
// The kind must have a schema compatible with the given type, t.
// Optional ReadOptions can be supplied to narrow the query.
func Read(s beam.Scope, project, namespace, kind string, shards int, t reflect.Type, typeKey string, opts ...ReadOption) beam.PCollection {
	s = s.Scope("ledger.datastore.Read")
	cfg := newReadConfig(opts...)
	// for portable runner consideration, set newClient to nil for now
	// which will be initialized in DoFn's Setup() method
	return query(s, project, namespace, kind, shards, t, typeKey, cfg, nil)
}

func datastoreNewClient(ctx context.Context, projectID string, opts ...option.ClientOption) (clientType, error) {
	return datastore.NewClient(ctx, projectID, opts...)
}

func query(s beam.Scope, project, namespace, kind string, shards int, t reflect.Type, typeKey string, cfg readConfig, newClient newClientFuncType) beam.PCollection {
	imp := beam.Impulse(s)
	ex := beam.ParDo(s, &splitQueryFn{
		Project:       project,
		Namespace:     namespace,
		Kind:          kind,
		Shards:        shards,
		newClientFunc: newClient,
	}, imp)
	g := beam.GroupByKey(s, ex)
	return beam.ParDo(s, &queryFn{
		Project:       project,
		Namespace:     namespace,
		Kind:          kind,
		Type:          typeKey,
		Filters:       cfg.filters,
		newClientFunc: newClient,
	}, g, beam.TypeDefinition{Var: beam.XType, T: t})
}

type splitQueryFn struct {
	Project       string `json:"project"`
	Namespace     string `json:"namespace"`
	Kind          string `json:"kind"`
	Shards        int    `json:"shards"`
	newClientFunc newClientFuncType
}

// BoundedQuery represents a datastore Query with a bounded key range between [Start, End)
type BoundedQuery struct {
	Start *datastore.Key `json:"start"`
	End   *datastore.Key `json:"end"`
}

func (s *splitQueryFn) Setup() error {
	if nil == s.newClientFunc {
		// setup default newClientFunc for DoFns
		s.newClientFunc = datastoreNewClient
	}
	return nil
}

func (s *splitQueryFn) ProcessElement(ctx context.Context, _ []byte, emit func(k string, val string)) error {
	// Short circuit a single shard
	if s.Shards <= 1 {
		q := BoundedQuery{}
		b, err := json.Marshal(q)
		if err != nil {
			return err
		}
		emit(strconv.Itoa(1), string(b))
		return nil
	}

	client, err := s.newClientFunc(ctx, s.Project)
	if err != nil {
		return err
	}
	defer client.Close()

	splits := []*datastore.Key{}
	query := datastore.NewQuery(s.Kind).Namespace(s.Namespace).Order(scatterPropertyName).Limit((s.Shards - 1) * 32).KeysOnly()
	iter := client.Run(ctx, query)
	for {
		k, err := iter.Next(nil)
		if err != nil {
			if err == iterator.Done {
				break
			}
			return err
		}
		splits = append(splits, k)
	}
	sort.Slice(splits, func(i, j int) bool {
		return keyLessThan(splits[i], splits[j])
	})

	splitKeys := getSplits(splits, s.Shards-1)

	queries := make([]*BoundedQuery, len(splitKeys))
	var lastKey *datastore.Key
	for n, k := range splitKeys {
		q := BoundedQuery{End: k}
		if lastKey != nil {
			q.Start = lastKey
		}
		queries[n] = &q
		lastKey = k
	}
	queries = append(queries, &BoundedQuery{Start: lastKey})

	log.Debugf(ctx, "Datastore: Splitting into %d shards", len(queries))

	for n, q := range queries {
		b, err := json.Marshal(q)
		if err != nil {
			return err
		}
		log.Debugf(ctx, "Datastore: Emitting Bounded Query Shard `%d` Start: `%s` End:`%s`", n, q.Start.String(), q.End.String())
		emit(strconv.Itoa(n), string(b))
	}
	return nil
}

func keyLessThan(a *datastore.Key, b *datastore.Key) bool {
	af, bf := flatten(a), flatten(b)
	for n, k1 := range af {
		if n >= len(bf) {
			return true
		}
		k2 := bf[n]
		r := strings.Compare(k1.Name, k2.Name)
		if r == -1 {
			return true
		} else if r == 1 {
			return false
		}
	}
	return false
}

func flatten(k *datastore.Key) []*datastore.Key {
	pieces := []*datastore.Key{}
	if k.Parent != nil {
		pieces = append(pieces, flatten(k.Parent)...)
	}
	pieces = append(pieces, k)
	return pieces
}

func getSplits(keys []*datastore.Key, numSplits int) []*datastore.Key {
	if len(keys) == 0 || (len(keys) < (numSplits - 1)) {
		return keys
	}

	numKeysPerSplit := math.Max(1.0, float64(len(keys))) / float64((numSplits))

	splitKeys := make([]*datastore.Key, numSplits)
	for n := 1; n <= len(splitKeys); n++ {
		i := int(math.Round(float64(n) * float64(numKeysPerSplit)))
		splitKeys[n-1] = keys[i-1]
	}
	return splitKeys

}

type queryFn struct {
	// Project is the project
	Project string `json:"project"`
	// Namespace is the datastore namespace
	Namespace string `json:"namespace"`
	// Kind is the datastore kind
	Kind string `json:"kind"`
	// Type is the name of the global schema type
	Type string `json:"type"`
	// Filters are optional datastore property filters
	Filters       []Filter `json:"filters"`
	newClientFunc newClientFuncType
}

func (s *queryFn) Setup() error {
	if nil == s.newClientFunc {
		// setup default newClientFunc for DoFns
		s.newClientFunc = datastoreNewClient
	}
	return nil
}

func (s *queryFn) ProcessElement(ctx context.Context, _ string, v func(*string) bool, emit func(beam.X)) error {
	client, err := s.newClientFunc(ctx, s.Project)
	if err != nil {
		return err
	}
	defer client.Close()

	// deserialize Query
	var k string
	v(&k)
	q := BoundedQuery{}
	err = json.Unmarshal([]byte(k), &q)
	if err != nil {
		return err
	}

	// lookup type
	t, ok := runtime.LookupType(s.Type)
	if !ok {
		return fmt.Errorf("no type registered %s", s.Type)
	}

	// Translate BoundedQuery to datastore.Query
	dq := datastore.NewQuery(s.Kind).Namespace(s.Namespace)
	if q.Start != nil {
		dq = dq.Filter("__key__ >=", q.Start)
	}
	if q.End != nil {
		dq = dq.Filter("__key__ <", q.End)
	}

	if len(s.Filters) > 0 {
		var err error
		dq, err = applyFilters(dq, s.Filters)
		if err != nil {
			return err
		}
	}

	// Run Query
	iter := client.Run(ctx, dq)
	for {
		val := reflect.New(t).Interface() // val : *T
		key, err := iter.Next(val)
		if err != nil {
			if err == iterator.Done {
				break
			}
			return err
		}
		if setter, ok := val.(interface {
			SetDatastoreKey(*datastore.Key) error
		}); ok {
			if err := setter.SetDatastoreKey(key); err != nil {
				return err
			}
		}
		emit(reflect.ValueOf(val).Elem().Interface()) // emit(*val)
	}
	return nil
}

var validOperators = map[string]struct{}{
	"=":  {},
	">":  {},
	"<":  {},
	">=": {},
	"<=": {},
}

var timestampLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05",
	"2006-01-02",
}

func applyFilters(q *datastore.Query, filters []Filter) (*datastore.Query, error) {
	dq := q
	for _, filter := range filters {
		expr, value, err := filter.toDatastoreFilter()
		if err != nil {
			return nil, err
		}
		dq = dq.Filter(expr, value)
	}
	return dq, nil
}

func (f Filter) toDatastoreFilter() (string, interface{}, error) {
	field := strings.TrimSpace(f.Field)
	if field == "" {
		return "", nil, fmt.Errorf("datastoreio: filter requires a field name")
	}

	op := strings.TrimSpace(f.Operator)
	if op == "" {
		op = "="
	}
	if _, ok := validOperators[op]; !ok {
		return "", nil, fmt.Errorf("datastoreio: unsupported operator %q for field %q", op, field)
	}

	value, err := f.parseValue()
	if err != nil {
		return "", nil, fmt.Errorf("datastoreio: field %q: %w", field, err)
	}

	return fmt.Sprintf("%s %s", field, op), value, nil
}

func (f Filter) parseValue() (interface{}, error) {
	value := strings.TrimSpace(f.Value)
	switch f.Type {
	case "", FilterValueTypeString:
		return value, nil
	case FilterValueTypeInt:
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse int: %w", err)
		}
		return v, nil
	case FilterValueTypeFloat:
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, fmt.Errorf("parse float: %w", err)
		}
		return v, nil
	case FilterValueTypeBool:
		v, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("parse bool: %w", err)
		}
		return v, nil
	case FilterValueTypeTimestamp:
		ts, err := parseTimestamp(value)
		if err != nil {
			return nil, err
		}
		return ts, nil
	default:
		return nil, fmt.Errorf("unsupported type %q", f.Type)
	}
}

func parseTimestamp(value string) (time.Time, error) {
	for _, layout := range timestampLayouts {
		if ts, err := time.Parse(layout, value); err == nil {
			return ts, nil
		}
	}
	return time.Time{}, fmt.Errorf("parse timestamp %q: unsupported format", value)
}
