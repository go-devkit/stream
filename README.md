# go-devkit/stream

Lazy, chainable stream library for Go, built on Go 1.23's
`iter.Seq2[T, error]`. Errors flow with the elements; no shared mutable
state. Streams are re-runnable.

## Requirements

Go **1.27** (uses generic methods). `mise.toml` currently pins
`go = "1.27rc1"`; bump to the final tag once 1.27 ships.

```
mise trust
mise install
task test
```

## Quick taste

```go
import (
    "fmt"
    "github.com/go-devkit/stream"
)

type user struct{ ID int; Name string }

func main() {
    users := []user{{1, "alice"}, {2, "bob"}, {3, "carol"}, {4, "dave"}}

    // top 2 even IDs, doubled
    out, err := stream.FromSlice(users).
        Filter(func(u user) (bool, error) { return u.ID%2 == 0, nil }).
        Map(func(u user) (int, error) { return u.ID * 2, nil }).
        Sort(stream.Ascending).
        Limit(2).
        ToSlice()
    fmt.Println(out, err) // [4 8] <nil>

    // group by even/odd
    groups, _ := stream.FromSlice(users).
        Group(func(u user) (string, error) {
            if u.ID%2 == 0 { return "even", nil }
            return "odd", nil
        })

    // user with smallest ID
    smallest, _ := stream.FromSlice(users).
        Reduce(stream.MinOf(func(u user) int { return u.ID }))

    _, _ = groups, smallest
}
```

## API

**Sources:** `FromSlice`, `FromSeq`, `FromSeq2`, `Of`, `Range`, `FromMap`, `Concat`.

**Transforms (lazy):** `Map`, `Filter`, `FlatMap`, `Limit`, `Skip`, `TakeWhile`,
`DropWhile`, `Peek`, `Zip`, `Distinct(keyFn)`, `Sort(cmpFn)`.

**Terminals:** `ToSlice`, `Count`, `First`, `Reduce`, `Fold`, `ForEach`,
`AnyMatch`, `AllMatch`, `NoneMatch`, `Average`, `Group`, `CollectToMap`,
`Partition`.

**Callback helpers:**
- `Self` — identity.
- `Sum` / `Min` / `Max` — direct reducers (numeric / ordered T).
- `MinOf(keyFn)` / `MaxOf(keyFn)` — reducer factories.
- `SumOf(projectFn)` — Fold-shaped factory.
- `Ascending` / `Descending` / `Reverse(cmpFn)` / `By(keyFn)` — comparator
  helpers for `Sort`.

## Design notes

- **Lazy.** Operators compose without iterating. Terminals drive the pipeline,
  fusing all stages over a single pass of the source. `Limit`, `First`,
  `AnyMatch` short-circuit.
- **Error-aware.** Backed by `iter.Seq2[T, error]` — every yielded element
  carries its own error. Operators forward errors with the same yield protocol;
  no shared err cell, no sticky state across re-runs.
- **Re-runnable.** Each terminal triggers a fresh iteration. Stateful operators
  (`Distinct`, `Sort`) reset per run.
- **No parallel pipeline.** Deliberately omitted for now — adding it would
  require fan-out/fan-in plus associativity contracts. Use goroutines around
  the whole stream for embarrassingly parallel cases.

## When (not) to use this

Chainable stream pipelines are **not mainstream Go**. The Go community
leans imperative — explicit `for` loops, explicit error checks per call —
and `Effective Go` and the standard library reflect that. This library is
useful in specific cases, less so in others.

**Use this when:**
- The pipeline has 3+ transforms and intermediate slices would clutter
  the imperative version.
- You want short-circuit + laziness (`Range(0, big).Filter(...).First()`)
  without writing the loop yourself.
- The data flow reads more clearly as a pipeline than as nested loops.
- You're already comfortable with stream / iterator chaining from other
  languages (Rust, Kotlin, Scala, Java).

**Reach for a plain `for` loop when:**
- The pipeline is short (≤ 2 stages). `out := make([]T, 0, len(in))` + `for`
  is clearer.
- You're on a hot path. Closures + `iter.Seq2` pairs add overhead vs a
  direct loop (rough order: 10–50% in microbenchmarks; profile your own).
- The reader is a Go newcomer who hasn't internalized iterator chaining.
- You want stack traces to point at obvious imperative lines, not into
  generic closure trampolines.

This lib is **legitimate but niche.** Treat the chainable API as a tool
for the right job, not a default.

## Status

Pre-1.0. Requires unreleased Go 1.27. API may shift before tagging.
