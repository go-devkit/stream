package stream

// Map is a chainable, error-propagating wrapper around map[K]V. Not safe for
// concurrent use; each instance is intended to be built and consumed by a
// single goroutine.
type Map[K comparable, V any] struct {
	m   map[K]V
	err error
}

func FromMap[K comparable, V any](m map[K]V) *Map[K, V] {
	return &Map[K, V]{m: m}
}

func (m *Map[K, V]) Filter(f func(K, V) (bool, error)) *Map[K, V] {
	if m.err != nil {
		return m
	}
	filtered := make(map[K]V, len(m.m))
	for k, v := range m.m {
		ok, err := f(k, v)
		if err != nil {
			return &Map[K, V]{err: err}
		}
		if ok {
			filtered[k] = v
		}
	}
	return &Map[K, V]{m: filtered}
}

func (m *Map[K, V]) MapValues[V2 any](f func(K, V) (V2, error)) *Map[K, V2] {
	if m.err != nil {
		return &Map[K, V2]{err: m.err}
	}
	mapped := make(map[K]V2, len(m.m))
	for k, v := range m.m {
		n, err := f(k, v)
		if err != nil {
			return &Map[K, V2]{err: err}
		}
		mapped[k] = n
	}
	return &Map[K, V2]{m: mapped}
}

// Fold folds entries in Go's randomized map iteration order. The supplied
// function must be commutative and associative — any fold whose result depends
// on order is a caller bug.
func (m *Map[K, V]) Fold[A any](init A, f func(A, K, V) (A, error)) (A, error) {
	if m.err != nil {
		var zero A
		return zero, m.err
	}
	acc := init
	for k, v := range m.m {
		next, err := f(acc, k, v)
		if err != nil {
			var zero A
			return zero, err
		}
		acc = next
	}
	return acc, nil
}

// Keys returns the map's keys in Go's randomized map iteration order. Sort the
// result if a stable order is required.
func (m *Map[K, V]) Keys() ([]K, error) {
	if m.err != nil {
		return nil, m.err
	}
	keys := make([]K, 0, len(m.m))
	for k := range m.m {
		keys = append(keys, k)
	}
	return keys, nil
}

// Values returns the map's values in Go's randomized map iteration order. Sort
// the result if a stable order is required.
func (m *Map[K, V]) Values() ([]V, error) {
	if m.err != nil {
		return nil, m.err
	}
	values := make([]V, 0, len(m.m))
	for _, v := range m.m {
		values = append(values, v)
	}
	return values, nil
}

// ToMap returns the internal map directly; callers must not mutate the returned
// map while the wrapper is still in use.
func (m *Map[K, V]) ToMap() (map[K]V, error) {
	return m.m, m.err
}
