package stream

import (
	"errors"
	"slices"
	"testing"
)

var errBoom = errors.New("boom")

func TestFromSliceToSlice(t *testing.T) {
	in := []int{1, 2, 3}
	out, err := FromSlice(in).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, in) {
		t.Fatalf("got %v, want %v", out, in)
	}
}

func TestToSliceEmpty(t *testing.T) {
	out, err := FromSlice[int](nil).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("got %v, want empty", out)
	}
}

func TestMapSuccess(t *testing.T) {
	out, err := FromSlice([]int{1, 2, 3}).
		Map(func(v int) (string, error) {
			return string(rune('a' + v - 1)), nil
		}).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	want := []string{"a", "b", "c"}
	if !slices.Equal(out, want) {
		t.Fatalf("got %v, want %v", out, want)
	}
}

func TestMapErrorPropagates(t *testing.T) {
	_, err := FromSlice([]int{1, 2, 3}).
		Map(func(v int) (int, error) {
			if v == 2 {
				return 0, errBoom
			}
			return v, nil
		}).
		ToSlice()
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}

func TestFilterSuccess(t *testing.T) {
	out, err := FromSlice([]int{1, 2, 3, 4, 5}).
		Filter(func(v int) (bool, error) { return v%2 == 0, nil }).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	want := []int{2, 4}
	if !slices.Equal(out, want) {
		t.Fatalf("got %v, want %v", out, want)
	}
}

func TestFilterErrorPropagates(t *testing.T) {
	_, err := FromSlice([]int{1, 2, 3}).
		Filter(func(v int) (bool, error) {
			if v == 2 {
				return false, errBoom
			}
			return true, nil
		}).
		ToSlice()
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}

func TestFoldSumWithInit(t *testing.T) {
	sum, err := FromSlice([]int{1, 2, 3, 4}).
		Fold(100, func(acc, v int) (int, error) { return acc + v, nil })
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if sum != 110 {
		t.Fatalf("got %d, want 110", sum)
	}
}

func TestFoldDifferentAccumulatorType(t *testing.T) {
	out, err := FromSlice([]int{1, 2, 3}).
		Fold("", func(acc string, v int) (string, error) {
			return acc + string(rune('0'+v)), nil
		})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out != "123" {
		t.Fatalf("got %q, want %q", out, "123")
	}
}

func TestFoldEmptyReturnsInit(t *testing.T) {
	sum, err := FromSlice([]int{}).
		Fold(42, func(acc, v int) (int, error) { return acc + v, nil })
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if sum != 42 {
		t.Fatalf("got %d, want 42 (init)", sum)
	}
}

func TestFoldErrorPropagates(t *testing.T) {
	_, err := FromSlice([]int{1, 2, 3}).
		Fold(0, func(_, v int) (int, error) {
			if v == 3 {
				return 0, errBoom
			}
			return v, nil
		})
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}

func TestReduceSum(t *testing.T) {
	sum, err := FromSlice([]int{1, 2, 3, 4}).
		Reduce(func(a, b int) (int, error) { return a + b, nil })
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if sum != 10 {
		t.Fatalf("got %d, want 10", sum)
	}
}

func TestReduceEmptyReturnsErrEmpty(t *testing.T) {
	_, err := FromSlice([]int{}).
		Reduce(func(a, b int) (int, error) { return a + b, nil })
	if !errors.Is(err, ErrEmpty) {
		t.Fatalf("got %v, want ErrEmpty", err)
	}
}

func TestReduceSingleElementSkipsCallback(t *testing.T) {
	called := false
	out, err := FromSlice([]int{42}).
		Reduce(func(a, b int) (int, error) {
			called = true
			return a + b, nil
		})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out != 42 {
		t.Fatalf("got %d, want 42", out)
	}
	if called {
		t.Fatal("callback should not be invoked for single-element slice")
	}
}

func TestReduceErrorPropagates(t *testing.T) {
	_, err := FromSlice([]int{1, 2, 3}).
		Reduce(func(a, b int) (int, error) {
			if b == 3 {
				return 0, errBoom
			}
			return a + b, nil
		})
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}

func TestErrorShortCircuitsChain(t *testing.T) {
	filterCalled := 0
	out, err := FromSlice([]int{1, 2, 3}).
		Map(func(v int) (int, error) {
			if v == 2 {
				return 0, errBoom
			}
			return v, nil
		}).
		Filter(func(v int) (bool, error) {
			filterCalled++
			return true, nil
		}).
		ToSlice()
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
	if out != nil {
		t.Fatalf("expected nil slice on error, got %v", out)
	}
	if filterCalled != 0 {
		t.Fatalf("filter callback should not run after upstream error, called %d times", filterCalled)
	}
}

func TestChainedFilterMapReduce(t *testing.T) {
	sum, err := FromSlice([]int{1, 2, 3, 4, 5, 6}).
		Filter(func(v int) (bool, error) { return v%2 == 0, nil }).
		Map(func(v int) (int, error) { return v * v, nil }).
		Reduce(func(acc, v int) (int, error) { return acc + v, nil })
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if sum != 4+16+36 {
		t.Fatalf("got %d, want %d", sum, 4+16+36)
	}
}
