// Package stream provides a lazy, chainable pipeline over Go 1.23's
// iter.Seq2[T, error]. Transforms compose without iterating; terminals
// drive the pipeline and surface per-element errors through the iterator
// protocol — no shared mutable state, no sticky err cell, streams are
// re-runnable.
//
// The API surface is grouped into:
//
//   - Sources: FromSlice, FromSeq, FromSeq2, Of, Range, FromMap, Concat.
//   - Transforms (lazy): Map, Filter, FlatMap, Limit, Skip, TakeWhile,
//     DropWhile, Peek, Zip, Distinct, Sort, Chunk, Windowed.
//   - Terminals: ToSlice, Count, First, Last, Reduce, Fold, ForEach,
//     AnyMatch, AllMatch, NoneMatch, Average, Group, CollectToMap,
//     Partition, ToSeq.
//   - Interop: AsSeq2 (cheap view over the underlying iter.Seq2).
//   - Callback helpers: Self, Sum, Min, Max, MinOf, MaxOf, SumOf,
//     Ascending, Descending, Reverse, By.
//
// Requires Go 1.27+ for generic methods. See the package examples for
// idiomatic usage.
package stream
