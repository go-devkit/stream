package stream_test

import (
	"fmt"

	"github.com/go-devkit/stream"
)

type user struct {
	ID   int
	Name string
}

// A full pipeline: filter, transform, sort, take the top 2.
func Example() {
	users := []user{{1, "alice"}, {2, "bob"}, {3, "carol"}, {4, "dave"}}

	out, err := stream.FromSlice(users).
		Filter(func(u user) (bool, error) { return u.ID%2 == 0, nil }).
		Map(func(u user) (int, error) { return u.ID * 2, nil }).
		Sort(stream.Ascending).
		Limit(2).
		ToSlice()
	fmt.Println(out, err)
	// Output: [4 8] <nil>
}

func ExampleStream_Map() {
	out, _ := stream.Of(1, 2, 3).
		Map(func(elem int) (int, error) { return elem * elem, nil }).
		ToSlice()
	fmt.Println(out)
	// Output: [1 4 9]
}

func ExampleStream_Filter() {
	out, _ := stream.Of(1, 2, 3, 4, 5).
		Filter(func(elem int) (bool, error) { return elem%2 == 0, nil }).
		ToSlice()
	fmt.Println(out)
	// Output: [2 4]
}

// Reduce with the pre-built Sum helper for Numeric streams.
func ExampleStream_Reduce() {
	total, _ := stream.Of(1, 2, 3, 4).Reduce(stream.Sum)
	fmt.Println(total)
	// Output: 10
}

// Reduce with MinOf to pick an element by an Ordered key.
func ExampleMinOf() {
	users := []user{{3, "c"}, {1, "a"}, {2, "b"}}
	pick, _ := stream.FromSlice(users).
		Reduce(stream.MinOf(func(u user) int { return u.ID }))
	fmt.Println(pick)
	// Output: {1 a}
}

// Fold with an accumulator type different from the element type.
func ExampleStream_Fold() {
	joined, _ := stream.Of(1, 2, 3).Fold("", func(acc string, elem int) (string, error) {
		return acc + fmt.Sprint(elem), nil
	})
	fmt.Println(joined)
	// Output: 123
}

// Sort with the Ascending comparator helper.
func ExampleStream_Sort() {
	out, _ := stream.Of(3, 1, 4, 1, 5, 9, 2, 6).
		Sort(stream.Ascending).
		ToSlice()
	fmt.Println(out)
	// Output: [1 1 2 3 4 5 6 9]
}

// Sort a slice of structs by a field using stream.By.
func ExampleBy() {
	users := []user{{3, "c"}, {1, "a"}, {2, "b"}}
	sorted, _ := stream.FromSlice(users).
		Sort(stream.By(func(u user) int { return u.ID })).
		ToSlice()
	fmt.Println(sorted)
	// Output: [{1 a} {2 b} {3 c}]
}

// Distinct with the identity helper for comparable T.
func ExampleStream_Distinct() {
	out, _ := stream.Of(1, 2, 2, 3, 1, 4).
		Distinct(stream.Self).
		ToSlice()
	fmt.Println(out)
	// Output: [1 2 3 4]
}

// Group into a map by a key extractor. Sort the keys first to keep the
// example output deterministic.
func ExampleStream_Group() {
	groups, _ := stream.Of(1, 2, 3, 4, 5).
		Group(func(elem int) (string, error) {
			if elem%2 == 0 {
				return "even", nil
			}
			return "odd", nil
		})
	fmt.Println("even:", groups["even"])
	fmt.Println("odd:", groups["odd"])
	// Output:
	// even: [2 4]
	// odd: [1 3 5]
}

// FromMap yields Entry values in random order — pair with Sort for a
// deterministic pipeline.
func ExampleFromMap() {
	m := map[string]int{"a": 1, "b": 2, "c": 3}
	out, _ := stream.FromMap(m).
		Sort(stream.By(func(e stream.Entry[string, int]) string { return e.Key })).
		ToSlice()
	fmt.Println(out)
	// Output: [{a 1} {b 2} {c 3}]
}

// Concat merges multiple streams in order.
func ExampleConcat() {
	out, _ := stream.Concat(
		stream.Of(1, 2),
		stream.Of(3, 4),
		stream.Of(5),
	).ToSlice()
	fmt.Println(out)
	// Output: [1 2 3 4 5]
}

// TakeWhile stops on the first element that fails the predicate.
func ExampleStream_TakeWhile() {
	out, _ := stream.Range(0, 100).
		TakeWhile(func(elem int) (bool, error) { return elem < 5, nil }).
		ToSlice()
	fmt.Println(out)
	// Output: [0 1 2 3 4]
}

// Chunk batches elements into fixed-size slices; the final chunk may be
// shorter.
func ExampleChunk() {
	out, _ := stream.Chunk(stream.Of(1, 2, 3, 4, 5), 2).ToSlice()
	fmt.Println(out)
	// Output: [[1 2] [3 4] [5]]
}

// Windowed emits sliding windows of fixed size, stepping by 1.
func ExampleWindowed() {
	out, _ := stream.Windowed(stream.Of(1, 2, 3, 4), 2).ToSlice()
	fmt.Println(out)
	// Output: [[1 2] [2 3] [3 4]]
}

// Partition splits a stream into two slices by predicate — matched first.
func ExampleStream_Partition() {
	matched, unmatched, _ := stream.Of(1, 2, 3, 4, 5).
		Partition(func(elem int) (bool, error) { return elem%2 == 0, nil })
	fmt.Println(matched, unmatched)
	// Output: [2 4] [1 3 5]
}
