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
	out, err := Of(1, 2, 2, 3, 1, 4, 3).Distinct(Self).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{1, 2, 3, 4}) {
		t.Fatalf("got %v, want [1 2 3 4]", out)
	}
}

func TestDistinctEmpty(t *testing.T) {
	out, err := FromSlice([]int{}).Distinct(Self).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("got %v", out)
	}
}

func TestDistinctLazy(t *testing.T) {
	calls := 0
	out, err := Range(0, 1_000_000).
		Map(func(elem int) (int, error) {
			calls++
			return elem % 3, nil
		}).
		Distinct(Self).
		Limit(3).
		ToSlice()
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
	_, err := Of(1, 2, 3).
		Map(func(elem int) (int, error) {
			if elem == 2 {
				return 0, errBoom
			}
			return elem, nil
		}).
		Distinct(Self).
		ToSlice()
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}

type user struct {
	ID   int
	Name string
}

func TestDistinctOnStruct(t *testing.T) {
	users := []user{
		{1, "a"}, {2, "b"}, {1, "c"}, {3, "d"}, {2, "e"},
	}
	out, err := FromSlice(users).
		Distinct(func(u user) (int, error) { return u.ID, nil }).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	want := []user{{1, "a"}, {2, "b"}, {3, "d"}}
	if !slices.Equal(out, want) {
		t.Fatalf("got %v, want %v", out, want)
	}
}

func TestDistinctKeyErrorPropagates(t *testing.T) {
	_, err := Of(1, 2, 3).
		Distinct(func(v int) (int, error) {
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

func TestMinViaReduce(t *testing.T) {
	v, err := Of(3, 1, 4, 1, 5, 9, 2, 6).Reduce(Min)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if v != 1 {
		t.Fatalf("got %d, want 1", v)
	}
}

func TestMinViaReduceEmpty(t *testing.T) {
	_, err := FromSlice([]int{}).Reduce(Min)
	if !errors.Is(err, ErrEmpty) {
		t.Fatalf("got %v, want ErrEmpty", err)
	}
}

func TestMaxViaReduce(t *testing.T) {
	v, err := Of(3, 1, 4, 1, 5, 9, 2, 6).Reduce(Max)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if v != 9 {
		t.Fatalf("got %d, want 9", v)
	}
}

func TestMinOfStruct(t *testing.T) {
	users := []user{{3, "c"}, {1, "a"}, {2, "b"}}
	v, err := FromSlice(users).Reduce(MinOf(func(u user) int { return u.ID }))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if v.ID != 1 {
		t.Fatalf("got %v, want ID=1", v)
	}
}

func TestMaxOfStruct(t *testing.T) {
	users := []user{{3, "c"}, {1, "a"}, {2, "b"}}
	v, err := FromSlice(users).Reduce(MaxOf(func(u user) int { return u.ID }))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if v.ID != 3 {
		t.Fatalf("got %v, want ID=3", v)
	}
}

func TestSumViaReduce(t *testing.T) {
	v, err := Of(1, 2, 3, 4).Reduce(Sum)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if v != 10 {
		t.Fatalf("got %d, want 10", v)
	}
}

func TestSumViaReduceEmpty(t *testing.T) {
	_, err := FromSlice([]int{}).Reduce(Sum)
	if !errors.Is(err, ErrEmpty) {
		t.Fatalf("got %v, want ErrEmpty", err)
	}
}

func TestSumViaReduceFloat(t *testing.T) {
	v, err := Of(1.5, 2.5, 3.0).Reduce(Sum)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if v != 7.0 {
		t.Fatalf("got %v, want 7.0", v)
	}
}

func TestSumOfProjection(t *testing.T) {
	users := []user{{1, "a"}, {2, "b"}, {3, "c"}}
	v, err := FromSlice(users).Fold(0, SumOf(func(u user) int { return u.ID }))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if v != 6 {
		t.Fatalf("got %d, want 6", v)
	}
}

func TestAverageViaSelf(t *testing.T) {
	v, err := Of(1, 2, 3, 4).Average(Self)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if v != 2.5 {
		t.Fatalf("got %v, want 2.5", v)
	}
}

func TestAverageViaSelfEmpty(t *testing.T) {
	_, err := FromSlice([]int{}).Average(Self)
	if !errors.Is(err, ErrEmpty) {
		t.Fatalf("got %v, want ErrEmpty", err)
	}
}

func TestAverageProjection(t *testing.T) {
	users := []user{{2, "a"}, {4, "b"}, {6, "c"}}
	v, err := FromSlice(users).Average(func(u user) (int, error) { return u.ID, nil })
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
		Sort(By(func(elem Entry[string, int]) string { return elem.Key })).
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
	out, err := Of(3, 1, 4, 1, 5, 9, 2, 6).Sort(Ascending).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{1, 1, 2, 3, 4, 5, 6, 9}) {
		t.Fatalf("got %v", out)
	}
}

func TestSortDescending(t *testing.T) {
	out, err := Of(3, 1, 4, 1, 5).
		Sort(Descending).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{5, 4, 3, 1, 1}) {
		t.Fatalf("got %v", out)
	}
}

func TestSortStructField(t *testing.T) {
	users := []user{{3, "c"}, {1, "a"}, {2, "b"}}
	out, err := FromSlice(users).
		Sort(By(func(elem user) int { return elem.ID })).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	want := []user{{1, "a"}, {2, "b"}, {3, "c"}}
	if !slices.Equal(out, want) {
		t.Fatalf("got %v, want %v", out, want)
	}
}

func TestSortStructFieldDescending(t *testing.T) {
	users := []user{{3, "c"}, {1, "a"}, {2, "b"}}
	out, err := FromSlice(users).
		Sort(Reverse(By(func(elem user) int { return elem.ID }))).
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
	out, err := FromSlice([]int{}).Sort(Ascending).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("got %v", out)
	}
}

func TestSortDownstreamLazy(t *testing.T) {
	calls := 0
	out, err := Of(3, 1, 4, 1, 5, 9, 2, 6).
		Sort(Ascending).
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

func TestGroup(t *testing.T) {
	groups, err := Of(1, 2, 3, 4, 5, 6).
		Group(func(elem int) (string, error) {
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

func TestGroupEmpty(t *testing.T) {
	groups, err := FromSlice([]int{}).Group(func(elem int) (int, error) { return elem, nil })
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(groups) != 0 {
		t.Fatalf("got %v, want empty", groups)
	}
}

func TestGroupKeyFnErrorPropagates(t *testing.T) {
	_, err := Of(1, 2, 3).Group(func(elem int) (int, error) {
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

func TestTakeWhile(t *testing.T) {
	out, err := Of(1, 2, 3, 4, 5, 1, 2).
		TakeWhile(func(elem int) (bool, error) { return elem < 4, nil }).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{1, 2, 3}) {
		t.Fatalf("got %v, want [1 2 3]", out)
	}
}

func TestTakeWhileLazy(t *testing.T) {
	calls := 0
	out, err := Range(0, 1_000_000).
		Map(func(elem int) (int, error) {
			calls++
			return elem, nil
		}).
		TakeWhile(func(elem int) (bool, error) { return elem < 5, nil }).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{0, 1, 2, 3, 4}) {
		t.Fatalf("got %v", out)
	}
	if calls != 6 {
		t.Fatalf("Map called %d times, want 6 (5 taken + 1 stopper)", calls)
	}
}

func TestTakeWhileErrorPropagates(t *testing.T) {
	_, err := Of(1, 2, 3).
		TakeWhile(func(elem int) (bool, error) {
			if elem == 2 {
				return false, errBoom
			}
			return true, nil
		}).
		ToSlice()
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}

func TestDropWhile(t *testing.T) {
	out, err := Of(1, 2, 3, 4, 5, 1, 2).
		DropWhile(func(elem int) (bool, error) { return elem < 4, nil }).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{4, 5, 1, 2}) {
		t.Fatalf("got %v, want [4 5 1 2]", out)
	}
}

func TestDropWhileAllDropped(t *testing.T) {
	out, err := Of(1, 2, 3).
		DropWhile(func(elem int) (bool, error) { return true, nil }).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("got %v, want empty", out)
	}
}

func TestDropWhilePredicateStopsBeingCalled(t *testing.T) {
	calls := 0
	out, err := Of(1, 2, 3, 1, 2).
		DropWhile(func(elem int) (bool, error) {
			calls++
			return elem < 3, nil
		}).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{3, 1, 2}) {
		t.Fatalf("got %v", out)
	}
	if calls != 3 {
		t.Fatalf("predicate called %d times, want 3 (stops once false seen)", calls)
	}
}

func TestDropWhileErrorPropagates(t *testing.T) {
	_, err := Of(1, 2, 3).
		DropWhile(func(elem int) (bool, error) {
			if elem == 2 {
				return false, errBoom
			}
			return true, nil
		}).
		ToSlice()
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}

func TestConcat(t *testing.T) {
	out, err := Concat(Of(1, 2), Of(3, 4), Of(5)).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{1, 2, 3, 4, 5}) {
		t.Fatalf("got %v", out)
	}
}

func TestConcatEmpty(t *testing.T) {
	out, err := Concat[int]().ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("got %v", out)
	}
}

func TestConcatMixedEmpty(t *testing.T) {
	out, err := Concat(
		FromSlice([]int{}),
		Of(1, 2),
		FromSlice([]int{}),
		Of(3),
	).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{1, 2, 3}) {
		t.Fatalf("got %v", out)
	}
}

func TestConcatLazy(t *testing.T) {
	calls := 0
	out, err := Concat(
		Range(0, 1_000_000).Map(func(elem int) (int, error) {
			calls++
			return elem, nil
		}),
		Of(-1),
	).Limit(3).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{0, 1, 2}) {
		t.Fatalf("got %v", out)
	}
	if calls != 3 {
		t.Fatalf("Map called %d times, want 3 (Concat laziness broken)", calls)
	}
}

func TestConcatErrorPropagates(t *testing.T) {
	failing := Of(1, 2, 3).Map(func(elem int) (int, error) {
		if elem == 2 {
			return 0, errBoom
		}
		return elem, nil
	})
	_, err := Concat(failing, Of(4, 5)).ToSlice()
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}

func TestZip(t *testing.T) {
	out, err := Of(1, 2, 3).
		Zip(Of("a", "b", "c"), func(num int, letter string) (string, error) {
			return letter + ":" + string(rune('0'+num)), nil
		}).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []string{"a:1", "b:2", "c:3"}) {
		t.Fatalf("got %v", out)
	}
}

func TestZipTruncatesToShorter(t *testing.T) {
	out, err := Of(1, 2, 3, 4, 5).
		Zip(Of("a", "b"), func(num int, letter string) (string, error) {
			return letter, nil
		}).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []string{"a", "b"}) {
		t.Fatalf("got %v", out)
	}
}

func TestZipEmpty(t *testing.T) {
	out, err := FromSlice([]int{}).
		Zip(Of("a"), func(num int, letter string) (string, error) { return letter, nil }).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("got %v", out)
	}
}

func TestZipLazy(t *testing.T) {
	leftCalls := 0
	rightCalls := 0
	out, err := Range(0, 1_000_000).
		Map(func(elem int) (int, error) {
			leftCalls++
			return elem, nil
		}).
		Zip(
			Range(0, 1_000_000).Map(func(elem int) (int, error) {
				rightCalls++
				return elem * 10, nil
			}),
			func(a, b int) (int, error) { return a + b, nil },
		).
		Limit(3).
		ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(out, []int{0, 11, 22}) {
		t.Fatalf("got %v", out)
	}
	if leftCalls != 3 || rightCalls != 3 {
		t.Fatalf("left=%d right=%d, want both 3 (Zip laziness broken)", leftCalls, rightCalls)
	}
}

func TestZipLeftErrorPropagates(t *testing.T) {
	failing := Of(1, 2, 3).Map(func(elem int) (int, error) {
		if elem == 2 {
			return 0, errBoom
		}
		return elem, nil
	})
	_, err := failing.Zip(Of("a", "b", "c"), func(num int, letter string) (string, error) {
		return letter, nil
	}).ToSlice()
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}

func TestZipRightErrorPropagates(t *testing.T) {
	failing := Of("a", "b", "c").Map(func(elem string) (string, error) {
		if elem == "b" {
			return "", errBoom
		}
		return elem, nil
	})
	_, err := Of(1, 2, 3).Zip(failing, func(num int, letter string) (string, error) {
		return letter, nil
	}).ToSlice()
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}

func TestZipCombinerErrorPropagates(t *testing.T) {
	_, err := Of(1, 2, 3).Zip(Of(1, 2, 3), func(a, b int) (int, error) {
		if a == 2 {
			return 0, errBoom
		}
		return a + b, nil
	}).ToSlice()
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}

func TestPartition(t *testing.T) {
	matched, unmatched, err := Of(1, 2, 3, 4, 5).
		Partition(func(elem int) (bool, error) { return elem%2 == 0, nil })
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !slices.Equal(matched, []int{2, 4}) {
		t.Fatalf("matched: got %v, want [2 4]", matched)
	}
	if !slices.Equal(unmatched, []int{1, 3, 5}) {
		t.Fatalf("unmatched: got %v, want [1 3 5]", unmatched)
	}
}

func TestPartitionEmpty(t *testing.T) {
	matched, unmatched, err := FromSlice([]int{}).
		Partition(func(elem int) (bool, error) { return true, nil })
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(matched) != 0 || len(unmatched) != 0 {
		t.Fatalf("got matched=%v unmatched=%v, want both empty", matched, unmatched)
	}
}

func TestPartitionPredicateErrorPropagates(t *testing.T) {
	_, _, err := Of(1, 2, 3).Partition(func(elem int) (bool, error) {
		if elem == 2 {
			return false, errBoom
		}
		return true, nil
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}

func TestPartitionUpstreamErrorPropagates(t *testing.T) {
	failing := Of(1, 2, 3).Map(func(elem int) (int, error) {
		if elem == 2 {
			return 0, errBoom
		}
		return elem, nil
	})
	_, _, err := failing.Partition(func(elem int) (bool, error) { return true, nil })
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}

func TestToSeq(t *testing.T) {
	seq, err := Of(1, 2, 3).Filter(func(elem int) (bool, error) { return elem%2 == 1, nil }).ToSeq()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	var collected []int
	for elem := range seq {
		collected = append(collected, elem)
	}
	if !slices.Equal(collected, []int{1, 3}) {
		t.Fatalf("got %v", collected)
	}
}

func TestToSeqErrorPropagates(t *testing.T) {
	_, err := Of(1, 2, 3).Map(func(elem int) (int, error) {
		if elem == 2 {
			return 0, errBoom
		}
		return elem, nil
	}).ToSeq()
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}

func TestAsSeq2Bridge(t *testing.T) {
	var collected []int
	var seenErr error
	for elem, err := range Of(1, 2, 3).Filter(func(elem int) (bool, error) { return elem%2 == 1, nil }).AsSeq2() {
		if err != nil {
			seenErr = err
			break
		}
		collected = append(collected, elem)
	}
	if seenErr != nil {
		t.Fatalf("unexpected err: %v", seenErr)
	}
	if !slices.Equal(collected, []int{1, 3}) {
		t.Fatalf("got %v, want [1 3]", collected)
	}
}

func TestAsSeq2BridgeErrorPropagates(t *testing.T) {
	var seenErr error
	for _, err := range Of(1, 2, 3).Map(func(elem int) (int, error) {
		if elem == 2 {
			return 0, errBoom
		}
		return elem, nil
	}).AsSeq2() {
		if err != nil {
			seenErr = err
			break
		}
	}
	if !errors.Is(seenErr, errBoom) {
		t.Fatalf("got %v, want %v", seenErr, errBoom)
	}
}

func TestLast(t *testing.T) {
	v, ok, err := Of(1, 2, 3).Last()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !ok || v != 3 {
		t.Fatalf("got v=%d ok=%v, want 3 true", v, ok)
	}
}

func TestLastEmpty(t *testing.T) {
	_, ok, err := FromSlice([]int{}).Last()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ok {
		t.Fatal("ok should be false for empty stream")
	}
}

func TestLastErrorPropagates(t *testing.T) {
	_, _, err := Of(1, 2, 3).Map(func(elem int) (int, error) {
		if elem == 2 {
			return 0, errBoom
		}
		return elem, nil
	}).Last()
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}

func TestChunk(t *testing.T) {
	out, err := Chunk(Of(1, 2, 3, 4, 5), 2).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	want := [][]int{{1, 2}, {3, 4}, {5}}
	if len(out) != len(want) {
		t.Fatalf("got %v, want %v", out, want)
	}
	for idx := range want {
		if !slices.Equal(out[idx], want[idx]) {
			t.Fatalf("chunk %d: got %v, want %v", idx, out[idx], want[idx])
		}
	}
}

func TestChunkExactMultiple(t *testing.T) {
	out, err := Chunk(Of(1, 2, 3, 4), 2).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 2 || !slices.Equal(out[0], []int{1, 2}) || !slices.Equal(out[1], []int{3, 4}) {
		t.Fatalf("got %v", out)
	}
}

func TestChunkEmpty(t *testing.T) {
	out, err := Chunk(FromSlice([]int{}), 3).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("got %v", out)
	}
}

func TestChunkNonPositiveSize(t *testing.T) {
	out, err := Chunk(Of(1, 2, 3), 0).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("got %v, want empty for size=0", out)
	}
}

func TestChunkLazy(t *testing.T) {
	calls := 0
	out, err := Chunk(
		Range(0, 1_000_000).Map(func(elem int) (int, error) {
			calls++
			return elem, nil
		}),
		2,
	).Limit(2).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 2 || !slices.Equal(out[0], []int{0, 1}) || !slices.Equal(out[1], []int{2, 3}) {
		t.Fatalf("got %v", out)
	}
	if calls != 4 {
		t.Fatalf("Map called %d times, want 4 (Chunk laziness broken)", calls)
	}
}

func TestChunkErrorPropagates(t *testing.T) {
	_, err := Chunk(
		Of(1, 2, 3).Map(func(elem int) (int, error) {
			if elem == 2 {
				return 0, errBoom
			}
			return elem, nil
		}),
		2,
	).ToSlice()
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want %v", err, errBoom)
	}
}

func TestWindowed(t *testing.T) {
	out, err := Windowed(Of(1, 2, 3, 4, 5), 3).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	want := [][]int{{1, 2, 3}, {2, 3, 4}, {3, 4, 5}}
	if len(out) != len(want) {
		t.Fatalf("got %v, want %v", out, want)
	}
	for idx := range want {
		if !slices.Equal(out[idx], want[idx]) {
			t.Fatalf("window %d: got %v, want %v", idx, out[idx], want[idx])
		}
	}
}

func TestWindowedShorterThanSize(t *testing.T) {
	out, err := Windowed(Of(1, 2), 3).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("got %v, want empty", out)
	}
}

func TestWindowedExactSize(t *testing.T) {
	out, err := Windowed(Of(1, 2, 3), 3).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 1 || !slices.Equal(out[0], []int{1, 2, 3}) {
		t.Fatalf("got %v", out)
	}
}

func TestWindowedLazy(t *testing.T) {
	calls := 0
	out, err := Windowed(
		Range(0, 1_000_000).Map(func(elem int) (int, error) {
			calls++
			return elem, nil
		}),
		3,
	).Limit(2).ToSlice()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 2 || !slices.Equal(out[0], []int{0, 1, 2}) || !slices.Equal(out[1], []int{1, 2, 3}) {
		t.Fatalf("got %v", out)
	}
	if calls != 4 {
		t.Fatalf("Map called %d times, want 4 (Windowed laziness broken)", calls)
	}
}

func TestWindowedErrorPropagates(t *testing.T) {
	_, err := Windowed(
		Of(1, 2, 3, 4).Map(func(elem int) (int, error) {
			if elem == 3 {
				return 0, errBoom
			}
			return elem, nil
		}),
		2,
	).ToSlice()
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
