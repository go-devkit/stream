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

// Stream is a lazy pipeline of T. Transforms compose without iterating;
// iteration is driven by terminal operations. Errors raised by callbacks are
// stored in a pipeline-shared cell and surface through the terminal's error
// return; no transform panics. Not safe for concurrent use.
type Stream[T any] struct {
	seq iter.Seq[T]
	err *error
}

func newStream[T any](err *error, seq iter.Seq[T]) *Stream[T] {
	return &Stream[T]{seq: seq, err: err}
}

// --- Sources ---

func FromSlice[T any](src []T) *Stream[T] {
	var err error
	return newStream(&err, slices.Values(src))
}

func FromSeq[T any](seq iter.Seq[T]) *Stream[T] {
	var err error
	return newStream(&err, seq)
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
	var err error
	return newStream(&err, func(yield func(Entry[K, V]) bool) {
		for key, value := range src {
			if !yield(Entry[K, V]{Key: key, Value: value}) {
				return
			}
		}
	})
}

func Range(from, to int) *Stream[int] {
	var err error
	return newStream(&err, func(yield func(int) bool) {
		for idx := from; idx < to; idx++ {
			if !yield(idx) {
				return
			}
		}
	})
}

// --- Transforms (lazy) ---

func (s *Stream[T]) Map[N any](fn func(T) (N, error)) *Stream[N] {
	return newStream(s.err, func(yield func(N) bool) {
		if *s.err != nil {
			return
		}
		for elem := range s.seq {
			mapped, err := fn(elem)
			if err != nil {
				*s.err = err
				return
			}
			if !yield(mapped) {
				return
			}
		}
	})
}

func (s *Stream[T]) Filter(fn func(T) (bool, error)) *Stream[T] {
	return newStream(s.err, func(yield func(T) bool) {
		if *s.err != nil {
			return
		}
		for elem := range s.seq {
			keep, err := fn(elem)
			if err != nil {
				*s.err = err
				return
			}
			if keep && !yield(elem) {
				return
			}
		}
	})
}

func (s *Stream[T]) FlatMap[N any](fn func(T) (*Stream[N], error)) *Stream[N] {
	return newStream(s.err, func(yield func(N) bool) {
		if *s.err != nil {
			return
		}
		for elem := range s.seq {
			inner, err := fn(elem)
			if err != nil {
				*s.err = err
				return
			}
			for mapped := range inner.seq {
				if *s.err != nil {
					return
				}
				if !yield(mapped) {
					return
				}
			}
			if *inner.err != nil {
				*s.err = *inner.err
				return
			}
		}
	})
}

func (s *Stream[T]) Limit(count int) *Stream[T] {
	return newStream(s.err, func(yield func(T) bool) {
		if *s.err != nil || count <= 0 {
			return
		}
		taken := 0
		for elem := range s.seq {
			taken++
			if !yield(elem) {
				return
			}
			if taken >= count {
				return
			}
		}
	})
}

func (s *Stream[T]) Skip(count int) *Stream[T] {
	return newStream(s.err, func(yield func(T) bool) {
		if *s.err != nil {
			return
		}
		skipped := 0
		for elem := range s.seq {
			if skipped < count {
				skipped++
				continue
			}
			if !yield(elem) {
				return
			}
		}
	})
}

// DistinctBy yields each element whose key (as returned by keyFn) has not been
// seen before, preserving first-occurrence order.
func (s *Stream[T]) DistinctBy[K comparable](keyFn func(T) (K, error)) *Stream[T] {
	return newStream(s.err, func(yield func(T) bool) {
		if *s.err != nil {
			return
		}
		seen := make(map[K]struct{})
		for elem := range s.seq {
			key, err := keyFn(elem)
			if err != nil {
				*s.err = err
				return
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			if !yield(elem) {
				return
			}
		}
	})
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
	return newStream(s.err, func(yield func(T) bool) {
		if *s.err != nil {
			return
		}
		var buffer []T
		for elem := range s.seq {
			buffer = append(buffer, elem)
		}
		if *s.err != nil {
			return
		}
		slices.SortFunc(buffer, cmpFn)
		for _, elem := range buffer {
			if !yield(elem) {
				return
			}
		}
	})
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
// the stream. Useful for logging or debugging inside a pipeline.
func (s *Stream[T]) Peek(fn func(T)) *Stream[T] {
	return newStream(s.err, func(yield func(T) bool) {
		if *s.err != nil {
			return
		}
		for elem := range s.seq {
			fn(elem)
			if !yield(elem) {
				return
			}
		}
	})
}

// --- Terminals ---

func (s *Stream[T]) ToSlice() ([]T, error) {
	if *s.err != nil {
		return nil, *s.err
	}
	var out []T
	for elem := range s.seq {
		out = append(out, elem)
	}
	return out, *s.err
}

func (s *Stream[T]) Count() (int, error) {
	if *s.err != nil {
		return 0, *s.err
	}
	count := 0
	for range s.seq {
		count++
	}
	return count, *s.err
}

// First returns the first element and ok=true; ok=false if the stream is
// empty. Pulls a single element and stops the pipeline.
func (s *Stream[T]) First() (T, bool, error) {
	var zero T
	if *s.err != nil {
		return zero, false, *s.err
	}
	for elem := range s.seq {
		if *s.err != nil {
			return zero, false, *s.err
		}
		return elem, true, nil
	}
	return zero, false, *s.err
}

// Reduce combines elements using the first as the seed; the callback is not
// invoked for a single-element stream. Returns ErrEmpty on empty input. Use
// Fold if an init value is needed or the accumulator type differs from T.
func (s *Stream[T]) Reduce(fn func(T, T) (T, error)) (T, error) {
	var zero T
	if *s.err != nil {
		return zero, *s.err
	}
	var result T
	hasResult := false
	for elem := range s.seq {
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
	if *s.err != nil {
		return zero, *s.err
	}
	if !hasResult {
		return zero, ErrEmpty
	}
	return result, nil
}

// Fold folds elements into an accumulator of arbitrary type A starting from
// init. Empty input returns init and a nil error.
func (s *Stream[T]) Fold[A any](init A, fn func(A, T) (A, error)) (A, error) {
	if *s.err != nil {
		var zero A
		return zero, *s.err
	}
	acc := init
	for elem := range s.seq {
		next, err := fn(acc, elem)
		if err != nil {
			var zero A
			return zero, err
		}
		acc = next
	}
	if *s.err != nil {
		var zero A
		return zero, *s.err
	}
	return acc, nil
}

func (s *Stream[T]) ForEach(fn func(T) error) error {
	if *s.err != nil {
		return *s.err
	}
	for elem := range s.seq {
		if err := fn(elem); err != nil {
			return err
		}
	}
	return *s.err
}

func (s *Stream[T]) AnyMatch(fn func(T) (bool, error)) (bool, error) {
	if *s.err != nil {
		return false, *s.err
	}
	for elem := range s.seq {
		matched, err := fn(elem)
		if err != nil {
			return false, err
		}
		if matched {
			return true, nil
		}
	}
	return false, *s.err
}

func (s *Stream[T]) AllMatch(fn func(T) (bool, error)) (bool, error) {
	if *s.err != nil {
		return false, *s.err
	}
	for elem := range s.seq {
		matched, err := fn(elem)
		if err != nil {
			return false, err
		}
		if !matched {
			return false, nil
		}
	}
	if *s.err != nil {
		return false, *s.err
	}
	return true, nil
}

func (s *Stream[T]) NoneMatch(fn func(T) (bool, error)) (bool, error) {
	matched, err := s.AnyMatch(fn)
	return !matched, err
}

// MinBy returns the smallest element per cmpFn. Returns ErrEmpty on empty input.
func (s *Stream[T]) MinBy(cmpFn func(a, b T) int) (T, error) {
	var zero T
	if *s.err != nil {
		return zero, *s.err
	}
	var best T
	hasBest := false
	for elem := range s.seq {
		if !hasBest {
			best = elem
			hasBest = true
			continue
		}
		if cmpFn(elem, best) < 0 {
			best = elem
		}
	}
	if *s.err != nil {
		return zero, *s.err
	}
	if !hasBest {
		return zero, ErrEmpty
	}
	return best, nil
}

// MaxBy returns the largest element per cmpFn. Returns ErrEmpty on empty input.
func (s *Stream[T]) MaxBy(cmpFn func(a, b T) int) (T, error) {
	var zero T
	if *s.err != nil {
		return zero, *s.err
	}
	var best T
	hasBest := false
	for elem := range s.seq {
		if !hasBest {
			best = elem
			hasBest = true
			continue
		}
		if cmpFn(elem, best) > 0 {
			best = elem
		}
	}
	if *s.err != nil {
		return zero, *s.err
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
	if *s.err != nil {
		return sum, *s.err
	}
	for elem := range s.seq {
		projected, err := projectFn(elem)
		if err != nil {
			var zero N
			return zero, err
		}
		sum += projected
	}
	return sum, *s.err
}

// AverageBy projects each element to a numeric N via projectFn and returns the
// arithmetic mean as float64. Returns ErrEmpty on empty input.
func (s *Stream[T]) AverageBy[N Numeric](projectFn func(T) (N, error)) (float64, error) {
	if *s.err != nil {
		return 0, *s.err
	}
	var sum float64
	count := 0
	for elem := range s.seq {
		projected, err := projectFn(elem)
		if err != nil {
			return 0, err
		}
		sum += float64(projected)
		count++
	}
	if *s.err != nil {
		return 0, *s.err
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
	if *s.err != nil {
		return nil, *s.err
	}
	groups := make(map[K][]T)
	for elem := range s.seq {
		key, err := keyFn(elem)
		if err != nil {
			return nil, err
		}
		groups[key] = append(groups[key], elem)
	}
	if *s.err != nil {
		return nil, *s.err
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
	if *s.err != nil {
		return nil, *s.err
	}
	out := make(map[K]V)
	for elem := range s.seq {
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
	if *s.err != nil {
		return nil, *s.err
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
