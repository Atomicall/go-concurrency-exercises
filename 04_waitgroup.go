package concurrency

import (
	"sync"
)

// =============================================================================
// EXERCISE 2.1: sync.WaitGroup
// =============================================================================
//
// WaitGroup is used to wait for a collection of goroutines to finish.
//
// KEY CONCEPTS:
// - Add(n) increments the counter by n (call BEFORE launching goroutine)
// - Done() decrements the counter by 1 (typically deferred in goroutine)
// - Wait() blocks until the counter reaches zero
// - Counter must never go negative (panic!)
// - WaitGroup can be reused, but not while Wait() is blocking
//
// =============================================================================

// =============================================================================
// PART 1: Basic WaitGroup Usage
// =============================================================================

// ConcurrentSum computes sum of applying fn to each element concurrently.
//
// TODO: Implement this function to:
// 1. Create a WaitGroup
// 2. For each element in nums, launch a goroutine that:
//   - Calls fn(num)
//   - Safely adds result to a shared sum (you'll need a mutex!)
//   - Calls Done() when finished
//
// 3. Wait for all goroutines to complete
// 4. Return the sum
//
// QUESTION: What happens if you call wg.Add(1) inside the goroutine instead of before?
func ConcurrentSum(nums []int, fn func(int) int) int {
	// YOUR CODE HERE

	var wg sync.WaitGroup
	var sum_mx sync.Mutex
	var sum int

	for _, v := range nums {
		wg.Add(1)
		go func(fn func(int) int, arg int) {
			defer wg.Done()
			res := fn(arg)
			sum_mx.Lock()
			sum += res
			sum_mx.Unlock()
		}(fn, v)
	}

	wg.Wait()
	return sum
}

// FetchAll simulates fetching data from multiple URLs concurrently.
// Returns a map of url -> result.
//
// TODO: Implement this to:
// 1. Launch a goroutine for each URL
// 2. Each goroutine calls fetcher(url) and stores result
// 3. Wait for all fetches to complete
// 4. Return the results map
//
// HINT: Maps are not goroutine-safe, protect with mutex or use sync.Map
func FetchAll(urls []string, fetcher func(string) string) map[string]string {
	// YOUR CODE HERE
	var resSync sync.Mutex
	var result = make(map[string]string)
	var wg sync.WaitGroup

	for _, v := range urls {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			res := fetcher(url)
			resSync.Lock()
			result[url] = res
			resSync.Unlock()
		}(v)
	}

	wg.Wait()
	return result
}

// =============================================================================
// PART 2: WaitGroup Gotchas
// =============================================================================

// ProcessBatches processes items in batches with limited concurrency.
//
// TODO: Implement this to:
// 1. Process items in batches of batchSize
// 2. Each batch runs concurrently (all items in batch run at same time)
// 3. Wait for batch to complete before starting next batch
// 4. Return slice of all results in original order
//
// QUESTION: Why process in batches instead of all at once?
func ProcessBatches(items []int, batchSize int, process func(int) int) []int {
	// YOUR CODE HERE

	var result []int = make([]int, len(items))
	var mt sync.Mutex

	runs := len(items) / batchSize
	var left = 0
	if len(items)%batchSize > 0 {
		left = len(items) % batchSize
	}

	itemProcessor := func(val int, idx int, group *sync.WaitGroup) {
		defer group.Done()
		r := process(val)
		mt.Lock()
		result[idx] = r
		mt.Unlock()
	}

	for i := 0; i < runs; i++ {
		batchItems := items[i*batchSize : i*batchSize+batchSize]

		var wg sync.WaitGroup
		for j, v := range batchItems {
			wg.Add(1)
			itemProcessor(v, i*batchSize+j, &wg)
		}
		wg.Wait()
	}

	if left > 0 {
		latestBatch := items[len(items)-left:]
		var wg sync.WaitGroup
		for j, v := range latestBatch {
			wg.Add(1)
			itemProcessor(v, runs*batchSize+j, &wg)
		}
		wg.Wait()

	}
	return result
}

// WaitGroupReuse demonstrates proper WaitGroup reuse.
//
// TODO: Implement this to:
// 1. Use the SAME WaitGroup for two separate rounds of goroutines
// 2. First round: launch n goroutines, each appending "round1" to results
// 3. Wait for first round to complete
// 4. Second round: launch n goroutines, each appending "round2" to results
// 5. Wait for second round to complete
// 6. Return results
//
// QUESTION: What would happen if you started round 2 before round 1 finished?
func WaitGroupReuse(n int) []string {
	// YOUR CODE HERE
	return nil
}

// =============================================================================
// PART 3: WaitGroup with Errors
// =============================================================================

// FirstError runs functions concurrently and returns the first error encountered.
// Returns nil if all functions succeed.
//
// TODO: Implement this to:
// 1. Run all functions concurrently
// 2. If any function returns an error, capture it
// 3. Return the first error (any error if multiple occur)
// 4. Wait for ALL functions to complete (don't leak goroutines!)
//
// HINT: Use sync.Once to capture only the first error
func FirstError(fns []func() error) error {
	// YOUR CODE HERE
	return nil
}

// AllErrors runs functions concurrently and collects all errors.
//
// TODO: Implement this to:
// 1. Run all functions concurrently
// 2. Collect all errors that occur
// 3. Return slice of errors (empty if all succeeded)
func AllErrors(fns []func() error) []error {
	// YOUR CODE HERE
	return nil
}

// =============================================================================
// CHALLENGE: Implement a parallel map with error handling
// =============================================================================

// ParallelMap applies fn to each element concurrently.
// If any fn returns an error, stop processing and return the error.
// Otherwise return the transformed slice.
//
// TODO: Implement this to:
// 1. Launch goroutines for each element
// 2. If any goroutine errors, signal others to stop (use done channel)
// 3. Preserve order of results
// 4. Return (results, nil) on success, (nil, err) on first error
//
// HINT: This is tricky! You need coordination between goroutines.
func ParallelMap(items []int, fn func(int) (int, error)) ([]int, error) {
	// YOUR CODE HERE
	return nil, nil
}

// Ensure sync import is used
var _ = sync.WaitGroup{}
