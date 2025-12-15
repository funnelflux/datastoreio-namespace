# datastoreio-namespace

Namespace-aware Go fork of Apache Beam's `datastoreio.Read` helper. The stock Beam
SDK hard-codes the default Datastore namespace; this module keeps Beam's original
sharding and bounded-query logic but lets you supply a namespace for both the
scatter pass and the per-shard query, so Dataflow jobs can migrate data from any
tenant bucket without rewriting the pipeline.

## Features

- Drop-in `Read` function that mirrors Beam's API and wiring.
- Works with the standard Beam sharding strategy (`__scatter__` ordering).
- Supports structs that implement `SetDatastoreKey(*datastore.Key)` so you can
  recover the Datastore key even when the runner omits `__key__` from the payload.
- Accepts optional property filters (strings, numbers, bools, timestamps).
- Licensed under Apache 2.0 to stay compatible with upstream Beam.

## Usage

```go
package main

import (
	"reflect"
	"time"

	nsdatastore "github.com/funnelflux/datastoreio-namespace"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

var rawEntityTypeKey = beam.RegisterType(reflect.TypeOf((*RawEntity)(nil)).Elem())

func build(scope beam.Scope) {
	entities := nsdatastore.Read(
		scope,
		"project-id",
		"ledger",            // namespace
		"page",              // kind
		256,                 // shard hint
		reflect.TypeOf(RawEntity{}),
		rawEntityTypeKey,
		nsdatastore.WithFilters(nsdatastore.Filter{
			Field:    "updated_at",
			Operator: ">=",
			Value:    "2025-01-01",
			Type:     nsdatastore.FilterValueTypeTimestamp,
		}),
	)

	// Continue with ParDo transforms...
	_ = entities
}
```

Any structs passed to Beam should either capture the key themselves or implement:

```go
type datastoreKeySetter interface {
	SetDatastoreKey(*datastore.Key) error
}
```

`Read` will call the method when the runner returns the key separately from the
property list (common for Dataflow namespace queries).

### Filter details

`FilterValueTypeTimestamp` accepts any of the following layouts: RFC3339 (with or
without sub-second precision), `2006-01-02 15:04:05`, or `2006-01-02`. The value
is parsed on the worker before applying the Datastore filter.

## Development

```bash
go test ./...
gofmt -w .
```

This repository intentionally stays minimal so it can be vendored or published
as-is. Update the module path in `go.mod` once it lives in its permanent home.
