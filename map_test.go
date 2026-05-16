package stream

import (
	"errors"
	"maps"
	"slices"
	"testing"
)

func TestMap_FromToMap(t *testing.T) {
	in := map[string]int{"a": 1, "b": 2}
	out, err := FromMap(in).ToMap()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !maps.Equal(out, in) {
		t.Fatalf("got %v, want %v", out, in)
	}
}

func TestMap_ToMapEmpty(t *testing.T) {
	out, err := FromMap[string, int](nil).ToMap()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("got %v, want empty", out)
	}
}

func TestMap_Filter(t *testing.T) {
	out, err := FromMap(map[string]int{"a": 1, "b": 2, "c": 3}).
		Filter(func(_ string, v int) (bool, error) { return v%2 == 1, nil }).
		ToMap()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	want := map[string]int{"a": 1, "c": 3}
	if !maps.Equal(out, want) {
		t.Fatalf("got %v, want %v", out, want)
	}
}

func TestMap_FilterErrorPropagates(t *testing.T) {
	_, err := FromMap(map[string]int{"a": 1, "b": 2}).
		Filter(func(_ string, _ int) (bool, error) { return false, errBoom }).
		ToMap()
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}

func TestMap_MapValues(t *testing.T) {
	out, err := FromMap(map[string]int{"a": 1, "b": 2}).
		MapValues(func(k string, v int) (string, error) {
			return k, nil
		}).
		ToMap()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	want := map[string]string{"a": "a", "b": "b"}
	if !maps.Equal(out, want) {
		t.Fatalf("got %v, want %v", out, want)
	}
}

func TestMap_MapValuesErrorPropagates(t *testing.T) {
	_, err := FromMap(map[string]int{"a": 1, "b": 2}).
		MapValues(func(_ string, _ int) (int, error) { return 0, errBoom }).
		ToMap()
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}

func TestMap_Fold(t *testing.T) {
	sum, err := FromMap(map[string]int{"a": 1, "b": 2, "c": 3}).
		Fold(0, func(acc int, _ string, v int) (int, error) {
			return acc + v, nil
		})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if sum != 6 {
		t.Fatalf("got %d, want 6", sum)
	}
}

func TestMap_FoldEmptyReturnsInit(t *testing.T) {
	sum, err := FromMap(map[string]int{}).
		Fold(42, func(acc int, _ string, v int) (int, error) {
			return acc + v, nil
		})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if sum != 42 {
		t.Fatalf("got %d, want 42 (init)", sum)
	}
}

func TestMap_FoldErrorPropagates(t *testing.T) {
	_, err := FromMap(map[string]int{"a": 1, "b": 2}).
		Fold(0, func(_ int, _ string, _ int) (int, error) {
			return 0, errBoom
		})
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}

func TestMap_Keys(t *testing.T) {
	keys, err := FromMap(map[string]int{"a": 1, "b": 2, "c": 3}).Keys()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	slices.Sort(keys)
	want := []string{"a", "b", "c"}
	if !slices.Equal(keys, want) {
		t.Fatalf("got %v, want %v", keys, want)
	}
}

func TestMap_Values(t *testing.T) {
	values, err := FromMap(map[string]int{"a": 1, "b": 2, "c": 3}).Values()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	slices.Sort(values)
	want := []int{1, 2, 3}
	if !slices.Equal(values, want) {
		t.Fatalf("got %v, want %v", values, want)
	}
}

func TestMap_ErrorShortCircuitsChain(t *testing.T) {
	filterCalled := 0
	_, err := FromMap(map[string]int{"a": 1, "b": 2}).
		MapValues(func(_ string, _ int) (int, error) { return 0, errBoom }).
		Filter(func(_ string, _ int) (bool, error) {
			filterCalled++
			return true, nil
		}).
		ToMap()
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
	if filterCalled != 0 {
		t.Fatalf("filter should not run after upstream error, called %d times", filterCalled)
	}
}

func TestMap_KeysPropagatesError(t *testing.T) {
	_, err := FromMap(map[string]int{"a": 1}).
		Filter(func(_ string, _ int) (bool, error) { return false, errBoom }).
		Keys()
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}
