package concurrency

import "errors"

// =============================================================================
// EXERCISE 1.2: Pipelines with Directional Channels
// =============================================================================
//
// Pipelines are a powerful pattern where data flows through stages connected
// by channels. Each stage:
// 1. Receives values from upstream via inbound channels
// 2. Performs some function on that data
// 3. Sends values downstream via outbound channels
//
// KEY CONCEPTS:
// - chan<- T is a send-only channel (can only send to it)
// - <-chan T is a receive-only channel (can only receive from it)
// - These are COMPILE-TIME safety features
// - The stage that creates values should close the channel when done
// - Closing propagates through the pipeline naturally with range
//
// =============================================================================

// =============================================================================
// PART 1: Basic Three-Stage Pipeline
// =============================================================================

// Generate sends integers from start to end (exclusive) on the returned channel.
// This is the SOURCE stage of a pipeline.
//
// TODO: Implement this function to:
// 1. Create a channel
// 2. Launch a goroutine that sends values start, start+1, ... end-1
// 3. Close the channel when done (inside the goroutine!)
// 4. Return the channel immediately (don't wait for goroutine)
//
// QUESTION: Why do we return <-chan int instead of chan int?
func Generate(start, end int) <-chan int {
	// YOUR CODE HERE
	ch := make(chan int)

	go func(start, end int, ch chan<- int) {
		for i := start; i < end; i++ {
			ch <- i
		}
		close(ch)
	}(start, end, ch)

	return ch
}

// Square receives integers, squares them, and sends results.
// This is a TRANSFORM stage of a pipeline.
//
// TODO: Implement this function to:
// 1. Create an output channel
// 2. Launch a goroutine that:
//   - Ranges over the input channel
//   - Sends the square of each value to output
//   - Closes output when input is exhausted
//
// 3. Return the output channel immediately
//
// QUESTION: What happens if you forget to close the output channel?
func Square(in <-chan int) <-chan int {
	// YOUR CODE HERE

	out := make(chan int)

	go func(in <-chan int, out chan<- int) {
		for val := range in {
			out <- val * val
		}
		close(out)
	}(in, out)

	return out
}

// Sum receives integers and returns their sum.
// This is a SINK stage of a pipeline.
//
// TODO: Implement this function to:
// 1. Range over the input channel
// 2. Accumulate the sum
// 3. Return the total
//
// NOTE: This function blocks until the channel is closed!
func Sum(in <-chan int) int {
	// YOUR CODE HERE

	var sum int = 0
	for val := range in {
		sum += val
	}
	return sum
}

// RunPipeline connects the stages: Generate -> Square -> Sum
//
// TODO: Wire up the pipeline and return the sum of squares from start to end
// Example: RunPipeline(1, 4) should compute 1^2 + 2^2 + 3^2 = 1 + 4 + 9 = 14
func RunPipeline(start, end int) int {
	// YOUR CODE HERE
	in := Generate(start, end)
	squares := Square(in)
	sum := Sum(squares)
	return sum
}

// =============================================================================
// PART 2: Pipeline with Multiple Transforms
// =============================================================================

// Filter returns only values that pass the predicate function.
//
// TODO: Implement a filter stage that:
// 1. Creates an output channel
// 2. Launches a goroutine that only forwards values where pred(v) is true
// 3. Closes output when input is exhausted
func Filter(in <-chan int, pred func(int) bool) <-chan int {
	// YOUR CODE HERE
	return nil
}

// Map applies a function to each value.
//
// TODO: Implement a map stage that:
// 1. Creates an output channel
// 2. Launches a goroutine that sends fn(v) for each v
// 3. Closes output when input is exhausted
func Map(in <-chan int, fn func(int) int) <-chan int {
	// YOUR CODE HERE
	return nil
}

// RunFilterMapPipeline demonstrates chaining multiple transforms.
//
// TODO: Create a pipeline that:
// 1. Generates numbers from 1 to 10
// 2. Filters to keep only even numbers
// 3. Maps each to its square
// 4. Returns the sum
//
// Expected: 2^2 + 4^2 + 6^2 + 8^2 = 4 + 16 + 36 + 64 = 120
func RunFilterMapPipeline() int {
	// YOUR CODE HERE
	return 0
}

// =============================================================================
// PART 3: Pipeline with Error Handling
// =============================================================================

// Result holds either a value or an error
type Result struct {
	Value int
	Err   error
}

// GenerateWithError sends Results, occasionally producing errors.
//
// TODO: Implement this to:
// 1. Create a channel of Result
// 2. For each value from start to end-1:
//   - If the value is divisible by errEvery, send a Result with Err set
//   - Otherwise send a Result with Value set
//
// 3. Close the channel when done
func GenerateWithError(start, end, errEvery int) <-chan Result {
	// YOUR CODE HERE
	ch := make(chan Result)

	for i := start; i < end; i++ {
		if i%errEvery == 0 {
			ch <- Result{Err: errors.New("")}
		} else {
			ch <- Result{Value: i}
		}
	}
	close(ch)
	return ch
}

// ProcessResults demonstrates handling errors in a pipeline.
//
// TODO: Implement this to:
// 1. Range over the input channel
// 2. Skip values that have errors (count them)
// 3. Sum the successful values
// 4. Return (sum, errorCount)
func ProcessResults(in <-chan Result) (sum int, errorCount int) {
	// YOUR CODE HERE

	for val := range in {
		if val.Err != nil {
			errorCount++
		}
		sum += val.Value
	}
	return sum, errorCount
}

// =============================================================================
// CHALLENGE: Implement a pipeline that can be cancelled
// =============================================================================

// GenerateCancellable sends integers but stops if done channel is closed.
//
// TODO: Implement this to:
// 1. Create an output channel
// 2. Launch a goroutine that:
//   - Uses select to either send the next value OR detect done
//   - Stops immediately if done is closed
//   - Closes output when finished (either done or reached end)
//
// 3. Return the output channel
//
// HINT: This is a preview of context.Context cancellation patterns!
func GenerateCancellable(start, end int, done <-chan struct{}) <-chan int {
	// YOUR CODE HERE

	return nil
}

// TransformCancellable applies a transform but respects cancellation.
//
// TODO: Similar to above, but transforms values until done is closed
func TransformCancellable(in <-chan int, fn func(int) int, done <-chan struct{}) <-chan int {
	// YOUR CODE HERE
	return nil
}
