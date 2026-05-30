package stream

import (
	"errors"
	"slices"
	"testing"
)

var errBoom = errors.New("boom")

func TestFromSliceToSlice(t *testing.T) {
	out, err := FromSlice([]int{1, 2, 3}).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{1, 2, 3}) {
		t.Fatalf("got %v, want [1 2 3]", out)
	}
}

func TestOf(t *testing.T) {
	out, err := Of(1, 2, 3).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{1, 2, 3}) {
		t.Fatalf("got %v", out)
	}
}

func TestRange(t *testing.T) {
	out, err := Range(0, 5).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{0, 1, 2, 3, 4}) {
		t.Fatalf("got %v", out)
	}
}

func TestMap(t *testing.T) {
	out, err := Of(1, 2, 3).
		Map(func(v int) (int, error) { return v * v, nil }).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{1, 4, 9}) {
		t.Fatalf("got %v", out)
	}
}

func TestMapErrorPropagates(t *testing.T) {
	_, err := Of(1, 2, 3).
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

func TestFilter(t *testing.T) {
	out, err := Of(1, 2, 3, 4, 5).
		Filter(func(v int) (bool, error) { return v%2 == 0, nil }).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{2, 4}) {
		t.Fatalf("got %v", out)
	}
}

func TestFlatMap(t *testing.T) {
	out, err := Of(1, 2, 3).
		FlatMap(func(v int) (*Stream[int], error) {
			return Of(v, v*10), nil
		}).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{1, 10, 2, 20, 3, 30}) {
		t.Fatalf("got %v", out)
	}
}

func TestLimit(t *testing.T) {
	out, err := Range(0, 1_000_000).Limit(3).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{0, 1, 2}) {
		t.Fatalf("got %v", out)
	}
}

func TestSkip(t *testing.T) {
	out, err := Range(0, 5).Skip(2).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{2, 3, 4}) {
		t.Fatalf("got %v", out)
	}
}

func TestLazinessMapShortCircuitedByLimit(t *testing.T) {
	calls := 0
	out, err := Range(0, 1_000_000).
		Map(func(v int) (int, error) {
			calls++
			return v * 2, nil
		}).
		Limit(5).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{0, 2, 4, 6, 8}) {
		t.Fatalf("got %v", out)
	}
	if calls != 5 {
		t.Fatalf("Map called %d times, want 5 (laziness broken)", calls)
	}
}

func TestPeek(t *testing.T) {
	var seen []int
	out, err := Of(1, 2, 3).
		Peek(func(v int) { seen = append(seen, v) }).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{1, 2, 3}) {
		t.Fatalf("got %v", out)
	}
	if !slices.Equal(seen, []int{1, 2, 3}) {
		t.Fatalf("peek saw %v", seen)
	}
}

func TestCount(t *testing.T) {
	n, err := Range(0, 100).Filter(func(v int) (bool, error) { return v%3 == 0, nil }).Count()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if n != 34 {
		t.Fatalf("got %d, want 34", n)
	}
}

func TestFirstFound(t *testing.T) {
	v, ok, err := Range(0, 100).
		Filter(func(v int) (bool, error) { return v > 10, nil }).
		First()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !ok || v != 11 {
		t.Fatalf("got v=%d ok=%v, want 11 true", v, ok)
	}
}

func TestFirstEmpty(t *testing.T) {
	_, ok, err := FromSlice([]int{}).First()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ok {
		t.Fatal("ok should be false for empty stream")
	}
}

func TestReduce(t *testing.T) {
	sum, err := Of(1, 2, 3, 4).Reduce(func(a, b int) (int, error) { return a + b, nil })
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if sum != 10 {
		t.Fatalf("got %d", sum)
	}
}

func TestReduceEmptyReturnsErrEmpty(t *testing.T) {
	_, err := FromSlice([]int{}).Reduce(func(a, b int) (int, error) { return a + b, nil })
	if !errors.Is(err, ErrEmpty) {
		t.Fatalf("got %v, want ErrEmpty", err)
	}
}

func TestFoldDifferentAccumulatorType(t *testing.T) {
	out, err := Of(1, 2, 3).Fold("", func(acc string, v int) (string, error) {
		return acc + string(rune('0'+v)), nil
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out != "123" {
		t.Fatalf("got %q", out)
	}
}

func TestFoldEmptyReturnsInit(t *testing.T) {
	out, err := FromSlice([]int{}).Fold(42, func(acc, v int) (int, error) { return acc + v, nil })
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out != 42 {
		t.Fatalf("got %d", out)
	}
}

func TestForEach(t *testing.T) {
	var seen []int
	err := Of(1, 2, 3).ForEach(func(v int) error {
		seen = append(seen, v)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(seen, []int{1, 2, 3}) {
		t.Fatalf("got %v", seen)
	}
}

func TestForEachErrorStops(t *testing.T) {
	count := 0
	err := Of(1, 2, 3, 4).ForEach(func(v int) error {
		count++
		if v == 2 {
			return errBoom
		}
		return nil
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
	if count != 2 {
		t.Fatalf("got %d calls, want 2", count)
	}
}

func TestAnyMatch(t *testing.T) {
	got, err := Of(1, 2, 3).AnyMatch(func(v int) (bool, error) { return v == 2, nil })
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !got {
		t.Fatal("expected true")
	}
}

func TestAnyMatchShortCircuits(t *testing.T) {
	calls := 0
	got, err := Range(0, 1_000_000).AnyMatch(func(v int) (bool, error) {
		calls++
		return v == 5, nil
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !got {
		t.Fatal("expected true")
	}
	if calls != 6 {
		t.Fatalf("AnyMatch called %d times, want 6 (short-circuit broken)", calls)
	}
}

func TestAllMatch(t *testing.T) {
	got, err := Of(2, 4, 6).AllMatch(func(v int) (bool, error) { return v%2 == 0, nil })
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !got {
		t.Fatal("expected true")
	}
}

func TestNoneMatch(t *testing.T) {
	got, err := Of(1, 2, 3).NoneMatch(func(v int) (bool, error) { return v > 10, nil })
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !got {
		t.Fatal("expected true")
	}
}

func TestErrorShortCircuitsChain(t *testing.T) {
	filterCalled := 0
	_, err := Of(1, 2, 3).
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
	if filterCalled > 2 {
		t.Fatalf("Filter called %d times after upstream error, want <=2 (pre-error elements only)", filterCalled)
	}
}

func TestDistinct(t *testing.T) {
	out, err := Distinct(Of(1, 2, 2, 3, 1, 4, 3)).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{1, 2, 3, 4}) {
		t.Fatalf("got %v, want [1 2 3 4]", out)
	}
}

func TestDistinctEmpty(t *testing.T) {
	out, err := Distinct(FromSlice([]int{})).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("got %v", out)
	}
}

func TestDistinctLazy(t *testing.T) {
	calls := 0
	out, err := Distinct(
		Range(0, 1_000_000).Map(func(v int) (int, error) {
			calls++
			return v % 3, nil
		}),
	).Limit(3).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{0, 1, 2}) {
		t.Fatalf("got %v", out)
	}
	if calls != 3 {
		t.Fatalf("Map called %d times, want 3 (Distinct laziness broken)", calls)
	}
}

func TestDistinctErrorPropagates(t *testing.T) {
	_, err := Distinct(
		Of(1, 2, 3).Map(func(v int) (int, error) {
			if v == 2 {
				return 0, errBoom
			}
			return v, nil
		}),
	).ToSlice()
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}

type user struct {
	ID   int
	Name string
}

func TestDistinctBy(t *testing.T) {
	users := []user{
		{1, "a"}, {2, "b"}, {1, "c"}, {3, "d"}, {2, "e"},
	}
	out, err := FromSlice(users).
		DistinctBy(func(u user) (int, error) { return u.ID, nil }).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	want := []user{{1, "a"}, {2, "b"}, {3, "d"}}
	if !slices.Equal(out, want) {
		t.Fatalf("got %v, want %v", out, want)
	}
}

func TestDistinctByKeyErrorPropagates(t *testing.T) {
	_, err := Of(1, 2, 3).
		DistinctBy(func(v int) (int, error) {
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

func TestMin(t *testing.T) {
	v, err := Min(Of(3, 1, 4, 1, 5, 9, 2, 6))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if v != 1 {
		t.Fatalf("got %d, want 1", v)
	}
}

func TestMinEmpty(t *testing.T) {
	_, err := Min(FromSlice([]int{}))
	if !errors.Is(err, ErrEmpty) {
		t.Fatalf("got %v, want ErrEmpty", err)
	}
}

func TestMax(t *testing.T) {
	v, err := Max(Of(3, 1, 4, 1, 5, 9, 2, 6))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if v != 9 {
		t.Fatalf("got %d, want 9", v)
	}
}

func TestMinByStruct(t *testing.T) {
	users := []user{{3, "c"}, {1, "a"}, {2, "b"}}
	v, err := FromSlice(users).MinBy(func(a, b user) int { return a.ID - b.ID })
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if v.ID != 1 {
		t.Fatalf("got %v, want ID=1", v)
	}
}

func TestMaxByStruct(t *testing.T) {
	users := []user{{3, "c"}, {1, "a"}, {2, "b"}}
	v, err := FromSlice(users).MaxBy(func(a, b user) int { return a.ID - b.ID })
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if v.ID != 3 {
		t.Fatalf("got %v, want ID=3", v)
	}
}

func TestSum(t *testing.T) {
	v, err := Sum(Of(1, 2, 3, 4))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if v != 10 {
		t.Fatalf("got %d, want 10", v)
	}
}

func TestSumEmpty(t *testing.T) {
	v, err := Sum(FromSlice([]int{}))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if v != 0 {
		t.Fatalf("got %d, want 0", v)
	}
}

func TestSumFloat(t *testing.T) {
	v, err := Sum(Of(1.5, 2.5, 3.0))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if v != 7.0 {
		t.Fatalf("got %v, want 7.0", v)
	}
}

func TestSumByProjection(t *testing.T) {
	users := []user{{1, "a"}, {2, "b"}, {3, "c"}}
	v, err := FromSlice(users).SumBy(func(u user) (int, error) { return u.ID, nil })
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if v != 6 {
		t.Fatalf("got %d, want 6", v)
	}
}

func TestSumByErrorPropagates(t *testing.T) {
	_, err := Of(1, 2, 3).SumBy(func(v int) (int, error) {
		if v == 2 {
			return 0, errBoom
		}
		return v, nil
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}

func TestAverage(t *testing.T) {
	v, err := Average(Of(1, 2, 3, 4))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if v != 2.5 {
		t.Fatalf("got %v, want 2.5", v)
	}
}

func TestAverageEmpty(t *testing.T) {
	_, err := Average(FromSlice([]int{}))
	if !errors.Is(err, ErrEmpty) {
		t.Fatalf("got %v, want ErrEmpty", err)
	}
}

func TestAverageByProjection(t *testing.T) {
	users := []user{{2, "a"}, {4, "b"}, {6, "c"}}
	v, err := FromSlice(users).AverageBy(func(u user) (int, error) { return u.ID, nil })
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if v != 4.0 {
		t.Fatalf("got %v, want 4.0", v)
	}
}

func TestFromMap(t *testing.T) {
	src := map[string]int{"a": 1, "b": 2, "c": 3}
	out, err := FromMap(src).
		SortedBy(By(func(elem Entry[string, int]) string { return elem.Key })).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	want := []Entry[string, int]{
		{Key: "a", Value: 1},
		{Key: "b", Value: 2},
		{Key: "c", Value: 3},
	}
	if !slices.Equal(out, want) {
		t.Fatalf("got %v, want %v", out, want)
	}
}

func TestSort(t *testing.T) {
	out, err := Sort(Of(3, 1, 4, 1, 5, 9, 2, 6)).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{1, 1, 2, 3, 4, 5, 6, 9}) {
		t.Fatalf("got %v", out)
	}
}

func TestSortedByDescending(t *testing.T) {
	out, err := Of(3, 1, 4, 1, 5).
		SortedBy(Descending).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{5, 4, 3, 1, 1}) {
		t.Fatalf("got %v", out)
	}
}

func TestSortedByStructField(t *testing.T) {
	users := []user{{3, "c"}, {1, "a"}, {2, "b"}}
	out, err := FromSlice(users).
		SortedBy(By(func(elem user) int { return elem.ID })).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	want := []user{{1, "a"}, {2, "b"}, {3, "c"}}
	if !slices.Equal(out, want) {
		t.Fatalf("got %v, want %v", out, want)
	}
}

func TestSortedByStructFieldDescending(t *testing.T) {
	users := []user{{3, "c"}, {1, "a"}, {2, "b"}}
	out, err := FromSlice(users).
		SortedBy(Reverse(By(func(elem user) int { return elem.ID }))).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	want := []user{{3, "c"}, {2, "b"}, {1, "a"}}
	if !slices.Equal(out, want) {
		t.Fatalf("got %v, want %v", out, want)
	}
}

func TestSortEmpty(t *testing.T) {
	out, err := Sort(FromSlice([]int{})).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("got %v", out)
	}
}

func TestSortDownstreamLazy(t *testing.T) {
	calls := 0
	out, err := Sort(Of(3, 1, 4, 1, 5, 9, 2, 6)).
		Map(func(elem int) (int, error) {
			calls++
			return elem * 10, nil
		}).
		Limit(3).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{10, 10, 20}) {
		t.Fatalf("got %v", out)
	}
	if calls != 3 {
		t.Fatalf("Map called %d times after Sort, want 3 (downstream laziness broken)", calls)
	}
}

func TestGroupBy(t *testing.T) {
	groups, err := Of(1, 2, 3, 4, 5, 6).
		GroupBy(func(elem int) (string, error) {
			if elem%2 == 0 {
				return "even", nil
			}
			return "odd", nil
		})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(groups["even"], []int{2, 4, 6}) {
		t.Fatalf("even: got %v, want [2 4 6]", groups["even"])
	}
	if !slices.Equal(groups["odd"], []int{1, 3, 5}) {
		t.Fatalf("odd: got %v, want [1 3 5]", groups["odd"])
	}
}

func TestGroupByEmpty(t *testing.T) {
	groups, err := FromSlice([]int{}).GroupBy(func(elem int) (int, error) { return elem, nil })
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(groups) != 0 {
		t.Fatalf("got %v, want empty", groups)
	}
}

func TestGroupByKeyFnErrorPropagates(t *testing.T) {
	_, err := Of(1, 2, 3).GroupBy(func(elem int) (int, error) {
		if elem == 2 {
			return 0, errBoom
		}
		return elem, nil
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}

func TestCollectToMap(t *testing.T) {
	users := []user{{1, "alice"}, {2, "bob"}, {3, "carol"}}
	out, err := FromSlice(users).CollectToMap(
		func(elem user) (int, error) { return elem.ID, nil },
		func(elem user) (string, error) { return elem.Name, nil },
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	want := map[int]string{1: "alice", 2: "bob", 3: "carol"}
	if len(out) != len(want) {
		t.Fatalf("got %v, want %v", out, want)
	}
	for key, value := range want {
		if out[key] != value {
			t.Fatalf("key %v: got %v, want %v", key, out[key], value)
		}
	}
}

func TestCollectToMapDuplicateKey(t *testing.T) {
	users := []user{{1, "a"}, {2, "b"}, {1, "c"}}
	_, err := FromSlice(users).CollectToMap(
		func(elem user) (int, error) { return elem.ID, nil },
		func(elem user) (string, error) { return elem.Name, nil },
	)
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("got %v, want ErrDuplicateKey", err)
	}
}

func TestCollectToMapEmpty(t *testing.T) {
	out, err := FromSlice([]user{}).CollectToMap(
		func(elem user) (int, error) { return elem.ID, nil },
		func(elem user) (string, error) { return elem.Name, nil },
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("got %v, want empty", out)
	}
}

func TestCollectToMapKeyFnErrorPropagates(t *testing.T) {
	_, err := Of(1, 2, 3).CollectToMap(
		func(elem int) (int, error) {
			if elem == 2 {
				return 0, errBoom
			}
			return elem, nil
		},
		func(elem int) (int, error) { return elem * 10, nil },
	)
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}

func TestCollectToMapValueFnErrorPropagates(t *testing.T) {
	_, err := Of(1, 2, 3).CollectToMap(
		func(elem int) (int, error) { return elem, nil },
		func(elem int) (int, error) {
			if elem == 2 {
				return 0, errBoom
			}
			return elem * 10, nil
		},
	)
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}

func TestChained(t *testing.T) {
	sum, err := Range(0, 20).
		Filter(func(v int) (bool, error) { return v%2 == 0, nil }).
		Map(func(v int) (int, error) { return v * v, nil }).
		Limit(5).
		Fold(0, func(acc, v int) (int, error) { return acc + v, nil })
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if sum != 0+4+16+36+64 {
		t.Fatalf("got %d, want %d", sum, 0+4+16+36+64)
	}
}
