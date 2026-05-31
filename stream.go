package stream

import (
	"cmp"
	"errors"
	"fmt"
	"iter"
	"slices"
)

// Numeric is the constraint accepted by numeric aggregators (Sum, Average).
// Mirrors golang.org/x/exp/constraints minus complex types.
type Numeric interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64
}

// ErrEmpty is returned by terminals that require at least one element when the
// stream is empty (e.g. Reduce).
var ErrEmpty = errors.New("stream: terminal op on empty stream")

// ErrDuplicateKey is returned (wrapped, with the offending key) by
// CollectToMap when keyFn yields the same key for more than one element.
var ErrDuplicateKey = errors.New("stream: duplicate key in CollectToMap")

// Stream is a lazy pipeline of T. Internally backed by iter.Seq2[T, error],
// so per-element errors flow through the iterator protocol — no shared mutable
// state, and operators stay pure.
//
// Re-use: a Stream is a pipeline description, not a one-shot iterator. Every
// terminal triggers a fresh iteration of the entire pipeline; stateful
// operators (DistinctBy, SortedBy) reset their state per run. Calling multiple
// terminals on the same Stream is safe and yields independent results, unlike
// Java's single-use streams.
//
// When err is non-nil on a yielded pair, the value of T is undefined and
// callers must ignore it. Not safe for concurrent use across goroutines from
// the same terminal call.
type Stream[T any] struct {
	seq iter.Seq2[T, error]
}

// --- Sources ---

func FromSlice[T any](src []T) *Stream[T] {
	return &Stream[T]{seq: func(yield func(T, error) bool) {
		for _, elem := range src {
			if !yield(elem, nil) {
				return
			}
		}
	}}
}

// FromSeq wraps a non-fallible iter.Seq. Every yielded element pairs with a
// nil error.
func FromSeq[T any](src iter.Seq[T]) *Stream[T] {
	return &Stream[T]{seq: func(yield func(T, error) bool) {
		for elem := range src {
			if !yield(elem, nil) {
				return
			}
		}
	}}
}

// FromSeq2 adopts a fallible iter.Seq2 directly.
func FromSeq2[T any](src iter.Seq2[T, error]) *Stream[T] {
	return &Stream[T]{seq: src}
}

func Of[T any](elems ...T) *Stream[T] {
	return FromSlice(elems)
}

// Entry is a key/value pair, used by FromMap.
type Entry[K comparable, V any] struct {
	Key   K
	Value V
}

// FromMap streams a map as Entry values. Iteration order is Go's randomized
// map order; chain SortedBy if a stable order is needed.
func FromMap[K comparable, V any](src map[K]V) *Stream[Entry[K, V]] {
	return &Stream[Entry[K, V]]{seq: func(yield func(Entry[K, V], error) bool) {
		for key, value := range src {
			if !yield(Entry[K, V]{Key: key, Value: value}, nil) {
				return
			}
		}
	}}
}

// Concat emits the elements of each input stream in order. Lazy: downstream
// pulls drive consumption.
func Concat[T any](streams ...*Stream[T]) *Stream[T] {
	return &Stream[T]{seq: func(yield func(T, error) bool) {
		for _, inner := range streams {
			for elem, err := range inner.seq {
				if !yield(elem, err) {
					return
				}
				if err != nil {
					return
				}
			}
		}
	}}
}

func Range(from, to int) *Stream[int] {
	return &Stream[int]{seq: func(yield func(int, error) bool) {
		for idx := from; idx < to; idx++ {
			if !yield(idx, nil) {
				return
			}
		}
	}}
}

// --- Transforms (lazy) ---

func (s *Stream[T]) Map[N any](fn func(T) (N, error)) *Stream[N] {
	return &Stream[N]{seq: func(yield func(N, error) bool) {
		var zero N
		for elem, err := range s.seq {
			if err != nil {
				yield(zero, err)
				return
			}
			mapped, err := fn(elem)
			if err != nil {
				yield(zero, err)
				return
			}
			if !yield(mapped, nil) {
				return
			}
		}
	}}
}

func (s *Stream[T]) Filter(fn func(T) (bool, error)) *Stream[T] {
	return &Stream[T]{seq: func(yield func(T, error) bool) {
		var zero T
		for elem, err := range s.seq {
			if err != nil {
				yield(zero, err)
				return
			}
			keep, err := fn(elem)
			if err != nil {
				yield(zero, err)
				return
			}
			if keep && !yield(elem, nil) {
				return
			}
		}
	}}
}

func (s *Stream[T]) FlatMap[N any](fn func(T) (*Stream[N], error)) *Stream[N] {
	return &Stream[N]{seq: func(yield func(N, error) bool) {
		var zero N
		for elem, err := range s.seq {
			if err != nil {
				yield(zero, err)
				return
			}
			inner, err := fn(elem)
			if err != nil {
				yield(zero, err)
				return
			}
			for mapped, err := range inner.seq {
				if !yield(mapped, err) {
					return
				}
				if err != nil {
					return
				}
			}
		}
	}}
}

func (s *Stream[T]) Limit(count int) *Stream[T] {
	return &Stream[T]{seq: func(yield func(T, error) bool) {
		if count <= 0 {
			return
		}
		taken := 0
		for elem, err := range s.seq {
			if !yield(elem, err) {
				return
			}
			if err != nil {
				return
			}
			taken++
			if taken >= count {
				return
			}
		}
	}}
}

// TakeWhile yields elements while predicateFn returns true. The first element
// for which predicateFn returns false is dropped and terminates the stream;
// downstream sees no further elements.
func (s *Stream[T]) TakeWhile(predicateFn func(T) (bool, error)) *Stream[T] {
	return &Stream[T]{seq: func(yield func(T, error) bool) {
		var zero T
		for elem, err := range s.seq {
			if err != nil {
				yield(zero, err)
				return
			}
			keep, err := predicateFn(elem)
			if err != nil {
				yield(zero, err)
				return
			}
			if !keep {
				return
			}
			if !yield(elem, nil) {
				return
			}
		}
	}}
}

// DropWhile skips elements while predicateFn returns true. Once predicateFn
// returns false for an element, that element and all subsequent elements are
// yielded unconditionally (predicateFn is not consulted again).
func (s *Stream[T]) DropWhile(predicateFn func(T) (bool, error)) *Stream[T] {
	return &Stream[T]{seq: func(yield func(T, error) bool) {
		var zero T
		dropping := true
		for elem, err := range s.seq {
			if err != nil {
				yield(zero, err)
				return
			}
			if dropping {
				keep, err := predicateFn(elem)
				if err != nil {
					yield(zero, err)
					return
				}
				if keep {
					continue
				}
				dropping = false
			}
			if !yield(elem, nil) {
				return
			}
		}
	}}
}

func (s *Stream[T]) Skip(count int) *Stream[T] {
	return &Stream[T]{seq: func(yield func(T, error) bool) {
		skipped := 0
		for elem, err := range s.seq {
			if err != nil {
				yield(elem, err)
				return
			}
			if skipped < count {
				skipped++
				continue
			}
			if !yield(elem, nil) {
				return
			}
		}
	}}
}

// Zip pairs elements of this stream with elements of other, combining each
// pair via combinerFn. Stops as soon as either source exhausts; surplus
// elements in the longer source are not consumed.
func (s *Stream[T]) Zip[U, R any](other *Stream[U], combinerFn func(T, U) (R, error)) *Stream[R] {
	return &Stream[R]{seq: func(yield func(R, error) bool) {
		var zero R
		nextOther, stopOther := iter.Pull2(other.seq)
		defer stopOther()
		for elem, err := range s.seq {
			if err != nil {
				yield(zero, err)
				return
			}
			otherElem, otherErr, hasMore := nextOther()
			if !hasMore {
				return
			}
			if otherErr != nil {
				yield(zero, otherErr)
				return
			}
			combined, err := combinerFn(elem, otherElem)
			if err != nil {
				yield(zero, err)
				return
			}
			if !yield(combined, nil) {
				return
			}
		}
	}}
}

// DistinctBy yields each element whose key (as returned by keyFn) has not been
// seen before, preserving first-occurrence order.
func (s *Stream[T]) DistinctBy[K comparable](keyFn func(T) (K, error)) *Stream[T] {
	return &Stream[T]{seq: func(yield func(T, error) bool) {
		var zero T
		seen := make(map[K]struct{})
		for elem, err := range s.seq {
			if err != nil {
				yield(zero, err)
				return
			}
			key, err := keyFn(elem)
			if err != nil {
				yield(zero, err)
				return
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			if !yield(elem, nil) {
				return
			}
		}
	}}
}

// Distinct yields each distinct element once, preserving first-occurrence
// order. Free function because Go does not permit narrowing the receiver's
// type constraint (Stream is declared Stream[T any]). For non-comparable T
// or custom equality, use DistinctBy with a key extractor.
func Distinct[T comparable](s *Stream[T]) *Stream[T] {
	return s.DistinctBy(func(elem T) (T, error) { return elem, nil })
}

// SortedBy materializes the upstream into a buffer, sorts it per cmpFn, then
// yields elements in order. Sorting cannot be performed lazily — this stage
// blocks until the full upstream is consumed; downstream remains lazy.
func (s *Stream[T]) SortedBy(cmpFn func(a, b T) int) *Stream[T] {
	return &Stream[T]{seq: func(yield func(T, error) bool) {
		var zero T
		var buffer []T
		for elem, err := range s.seq {
			if err != nil {
				yield(zero, err)
				return
			}
			buffer = append(buffer, elem)
		}
		slices.SortFunc(buffer, cmpFn)
		for _, elem := range buffer {
			if !yield(elem, nil) {
				return
			}
		}
	}}
}

// Sort yields the elements of an Ordered stream in ascending order. See the
// laziness caveat on SortedBy.
func Sort[T cmp.Ordered](s *Stream[T]) *Stream[T] {
	return s.SortedBy(cmp.Compare[T])
}

// --- Comparator helpers (for SortedBy / MinBy / MaxBy) ---

// Ascending compares two Ordered values in natural order. Equivalent to
// cmp.Compare[T] but reads naturally as `s.SortedBy(stream.Ascending)`.
func Ascending[T cmp.Ordered](a, b T) int {
	return cmp.Compare(a, b)
}

// Descending compares two Ordered values in reverse order.
func Descending[T cmp.Ordered](a, b T) int {
	return cmp.Compare(b, a)
}

// Reverse wraps cmpFn to produce the opposite ordering. Useful for inverting a
// custom cmp built by By, or any caller-supplied comparator.
// Note: stream.Reverse(stream.Ascending) does not compile due to Go inference
// limits across nested generic calls — use stream.Descending instead for
// Ordered values.
func Reverse[T any](cmpFn func(a, b T) int) func(a, b T) int {
	return func(a, b T) int { return cmpFn(b, a) }
}

// By builds a comparator that orders T values by an Ordered key extracted via
// keyFn. Example: stream.By(func(u user) int { return u.ID }).
func By[T any, K cmp.Ordered](keyFn func(T) K) func(a, b T) int {
	return func(a, b T) int { return cmp.Compare(keyFn(a), keyFn(b)) }
}

// Peek invokes fn for every element passing through this stage without altering
// the stream. fn is not invoked for error-bearing pairs. Useful for logging or
// debugging inside a pipeline.
func (s *Stream[T]) Peek(fn func(T)) *Stream[T] {
	return &Stream[T]{seq: func(yield func(T, error) bool) {
		for elem, err := range s.seq {
			if err == nil {
				fn(elem)
			}
			if !yield(elem, err) {
				return
			}
			if err != nil {
				return
			}
		}
	}}
}

// --- Terminals ---

// ToSlice collects every element into a new slice. Empty input returns a nil
// slice (not []T{}); both have len() == 0 and range cleanly, so the
// distinction matters only for direct nil comparisons.
func (s *Stream[T]) ToSlice() ([]T, error) {
	var out []T
	for elem, err := range s.seq {
		if err != nil {
			return nil, err
		}
		out = append(out, elem)
	}
	return out, nil
}

func (s *Stream[T]) Count() (int, error) {
	count := 0
	for _, err := range s.seq {
		if err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

// First returns the first element and ok=true; ok=false if the stream is
// empty. Pulls a single element and stops the pipeline.
func (s *Stream[T]) First() (T, bool, error) {
	var zero T
	for elem, err := range s.seq {
		if err != nil {
			return zero, false, err
		}
		return elem, true, nil
	}
	return zero, false, nil
}

// Reduce combines elements using the first as the seed; the callback is not
// invoked for a single-element stream. Returns ErrEmpty on empty input. Use
// Fold if an init value is needed or the accumulator type differs from T.
func (s *Stream[T]) Reduce(fn func(T, T) (T, error)) (T, error) {
	var zero, result T
	hasResult := false
	for elem, err := range s.seq {
		if err != nil {
			return zero, err
		}
		if !hasResult {
			result = elem
			hasResult = true
			continue
		}
		next, err := fn(result, elem)
		if err != nil {
			return zero, err
		}
		result = next
	}
	if !hasResult {
		return zero, ErrEmpty
	}
	return result, nil
}

// Fold folds elements into an accumulator of arbitrary type A starting from
// init. Empty input returns init and a nil error.
func (s *Stream[T]) Fold[A any](init A, fn func(A, T) (A, error)) (A, error) {
	acc := init
	for elem, err := range s.seq {
		if err != nil {
			var zero A
			return zero, err
		}
		next, err := fn(acc, elem)
		if err != nil {
			var zero A
			return zero, err
		}
		acc = next
	}
	return acc, nil
}

func (s *Stream[T]) ForEach(fn func(T) error) error {
	for elem, err := range s.seq {
		if err != nil {
			return err
		}
		if err := fn(elem); err != nil {
			return err
		}
	}
	return nil
}

func (s *Stream[T]) AnyMatch(fn func(T) (bool, error)) (bool, error) {
	for elem, err := range s.seq {
		if err != nil {
			return false, err
		}
		matched, err := fn(elem)
		if err != nil {
			return false, err
		}
		if matched {
			return true, nil
		}
	}
	return false, nil
}

func (s *Stream[T]) AllMatch(fn func(T) (bool, error)) (bool, error) {
	for elem, err := range s.seq {
		if err != nil {
			return false, err
		}
		matched, err := fn(elem)
		if err != nil {
			return false, err
		}
		if !matched {
			return false, nil
		}
	}
	return true, nil
}

func (s *Stream[T]) NoneMatch(fn func(T) (bool, error)) (bool, error) {
	matched, err := s.AnyMatch(fn)
	return !matched, err
}

// MinBy returns the smallest element per cmpFn. Returns ErrEmpty on empty input.
func (s *Stream[T]) MinBy(cmpFn func(a, b T) int) (T, error) {
	var zero, best T
	hasBest := false
	for elem, err := range s.seq {
		if err != nil {
			return zero, err
		}
		if !hasBest {
			best = elem
			hasBest = true
			continue
		}
		if cmpFn(elem, best) < 0 {
			best = elem
		}
	}
	if !hasBest {
		return zero, ErrEmpty
	}
	return best, nil
}

// MaxBy returns the largest element per cmpFn. Returns ErrEmpty on empty input.
func (s *Stream[T]) MaxBy(cmpFn func(a, b T) int) (T, error) {
	var zero, best T
	hasBest := false
	for elem, err := range s.seq {
		if err != nil {
			return zero, err
		}
		if !hasBest {
			best = elem
			hasBest = true
			continue
		}
		if cmpFn(elem, best) > 0 {
			best = elem
		}
	}
	if !hasBest {
		return zero, ErrEmpty
	}
	return best, nil
}

// SumBy projects each element to a numeric N via projectFn and sums the
// projections. Empty input returns the zero value and a nil error. Overflow
// wraps silently (matches Go arithmetic semantics).
func (s *Stream[T]) SumBy[N Numeric](projectFn func(T) (N, error)) (N, error) {
	var sum N
	for elem, err := range s.seq {
		if err != nil {
			var zero N
			return zero, err
		}
		projected, err := projectFn(elem)
		if err != nil {
			var zero N
			return zero, err
		}
		sum += projected
	}
	return sum, nil
}

// AverageBy projects each element to a numeric N via projectFn and returns the
// arithmetic mean as float64. Returns ErrEmpty on empty input.
//
// Precision: integer projections larger than 2^53 lose precision when
// converted to float64 (mantissa is 53 bits). For exact integer averages on
// large values, use Fold to track sum+count as int64/big.Int and divide
// yourself.
func (s *Stream[T]) AverageBy[N Numeric](projectFn func(T) (N, error)) (float64, error) {
	var sum float64
	count := 0
	for elem, err := range s.seq {
		if err != nil {
			return 0, err
		}
		projected, err := projectFn(elem)
		if err != nil {
			return 0, err
		}
		sum += float64(projected)
		count++
	}
	if count == 0 {
		return 0, ErrEmpty
	}
	return sum / float64(count), nil
}

// GroupBy partitions elements into a map by the key returned from keyFn.
// Encounter order within each group is preserved. Terminal — materializes the
// full upstream.
func (s *Stream[T]) GroupBy[K comparable](keyFn func(T) (K, error)) (map[K][]T, error) {
	groups := make(map[K][]T)
	for elem, err := range s.seq {
		if err != nil {
			return nil, err
		}
		key, err := keyFn(elem)
		if err != nil {
			return nil, err
		}
		groups[key] = append(groups[key], elem)
	}
	return groups, nil
}

// CollectToMap projects each element to a (key, value) pair via keyFn and
// valueFn and collects them into a map. Returns an error wrapping
// ErrDuplicateKey if keyFn yields the same key for more than one element.
// Terminal — materializes the full upstream.
func (s *Stream[T]) CollectToMap[K comparable, V any](
	keyFn func(T) (K, error),
	valueFn func(T) (V, error),
) (map[K]V, error) {
	out := make(map[K]V)
	for elem, err := range s.seq {
		if err != nil {
			return nil, err
		}
		key, err := keyFn(elem)
		if err != nil {
			return nil, err
		}
		if _, exists := out[key]; exists {
			return nil, fmt.Errorf("%w: %v", ErrDuplicateKey, key)
		}
		value, err := valueFn(elem)
		if err != nil {
			return nil, err
		}
		out[key] = value
	}
	return out, nil
}

// Min returns the smallest element of an Ordered stream. Free function
// because narrowing the receiver's any constraint is not possible; for
// non-Ordered T, use MinBy.
func Min[T cmp.Ordered](s *Stream[T]) (T, error) {
	return s.MinBy(cmp.Compare[T])
}

// Max returns the largest element of an Ordered stream. See Min note.
func Max[T cmp.Ordered](s *Stream[T]) (T, error) {
	return s.MaxBy(cmp.Compare[T])
}

// Sum returns the sum of a numeric stream. Empty input returns zero, nil.
// Free function because Numeric cannot be applied to a method's receiver T.
func Sum[T Numeric](s *Stream[T]) (T, error) {
	return s.SumBy(func(elem T) (T, error) { return elem, nil })
}

// Average returns the mean of a numeric stream as float64. ErrEmpty on empty.
func Average[T Numeric](s *Stream[T]) (float64, error) {
	return s.AverageBy(func(elem T) (T, error) { return elem, nil })
}
