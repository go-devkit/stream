package stream

import "errors"

// ErrEmpty is returned by Reduce when called on an empty input. Fold accepts
// empty input and returns the supplied init unchanged.
var ErrEmpty = errors.New("stream: reduce on empty input")

// Slice is a chainable, error-propagating wrapper around []T. Not safe for
// concurrent use; each instance is intended to be built and consumed by a
// single goroutine.
type Slice[T any] struct {
	s   []T
	err error
}

func FromSlice[T any](s []T) *Slice[T] {
	return &Slice[T]{s: s}
}

func (s *Slice[T]) Map[N any](f func(T) (N, error)) *Slice[N] {
	if s.err != nil {
		return &Slice[N]{err: s.err}
	}
	mapped := make([]N, len(s.s))
	for i, v := range s.s {
		n, err := f(v)
		if err != nil {
			return &Slice[N]{err: err}
		}
		mapped[i] = n
	}
	return &Slice[N]{s: mapped}
}

func (s *Slice[T]) Filter(f func(T) (bool, error)) *Slice[T] {
	if s.err != nil {
		return s
	}
	var filtered []T
	for _, v := range s.s {
		ok, err := f(v)
		if err != nil {
			return &Slice[T]{err: err}
		}
		if ok {
			filtered = append(filtered, v)
		}
	}
	return &Slice[T]{s: filtered}
}

// Fold folds elements into an accumulator of arbitrary type A, starting from
// init. Empty input returns init and a nil error.
func (s *Slice[T]) Fold[A any](init A, f func(A, T) (A, error)) (A, error) {
	if s.err != nil {
		var zero A
		return zero, s.err
	}
	acc := init
	for _, v := range s.s {
		next, err := f(acc, v)
		if err != nil {
			var zero A
			return zero, err
		}
		acc = next
	}
	return acc, nil
}

// Reduce combines elements using the first as the seed; the callback is not
// invoked for a single-element slice. Returns ErrEmpty on empty input. Use
// Fold if an init value is needed or the accumulator type differs from T.
func (s *Slice[T]) Reduce(f func(T, T) (T, error)) (T, error) {
	var zero T
	if s.err != nil {
		return zero, s.err
	}
	if len(s.s) == 0 {
		return zero, ErrEmpty
	}
	result := s.s[0]
	for _, v := range s.s[1:] {
		next, err := f(result, v)
		if err != nil {
			return zero, err
		}
		result = next
	}
	return result, nil
}

// ToSlice returns the internal slice directly; callers must not mutate the
// returned slice while the wrapper is still in use.
func (s *Slice[T]) ToSlice() ([]T, error) {
	return s.s, s.err
}
