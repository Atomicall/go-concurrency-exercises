package concurrency

import (
	"time"
)

// =============================================================================
// EXERCISE 1.3: Select Statement Mastery
// =============================================================================
//
// The select statement is Go's way of handling multiple channel operations
// concurrently. It's similar to switch but for channels.
//
// KEY CONCEPTS:
// - select blocks until ONE case can proceed, then executes that case
// - If multiple cases are ready, one is chosen at RANDOM (fair selection)
// - default case makes select non-blocking
// - Closed channels are always ready to receive (return zero value)
// - time.After returns a channel that receives after a duration
//
// NOTE: Before Go 1.23, time.After created timers that weren't garbage
// collected until they fired, causing leaks in loops. Since Go 1.23,
// unreferenced timers are collected even if they haven't fired.
//
// =============================================================================

// =============================================================================
// PART 1: Basic Select Patterns
// =============================================================================

// FirstResponse returns the first value received from either channel.
// This is the "first wins" pattern used in redundant systems.
//
// TODO: Implement using select to return whichever channel produces a value first
// If both are ready simultaneously, either is acceptable.
func FirstResponse(ch1, ch2 <-chan string) string {
	// YOUR CODE HERE
	select {
	case v := <-ch1:
		{
			return v
		}
	case v := <-ch2:
		{
			return v
		}
	}
}

// MergeChannels combines two channels into one output channel.
// Values from both inputs appear on the output in arrival order.
//
// TODO: Implement this to:
// 1. Create an output channel
// 2. Launch a goroutine that:
//   - Uses select in a loop to receive from either ch1 or ch2
//   - Sends received values to output
//   - Handles closure of both channels properly
//   - Closes output when BOTH inputs are closed
//
// 3. Return the output channel
//
// HINT: You need to track which channels are still open. A nil channel
// in select is never ready - use this to "disable" closed channels.
func MergeChannels(ch1, ch2 <-chan int) <-chan int {

	out := make(chan int)

	go func() {
		for {
			select {
			case val, ok := <-ch1:
				{
					if !ok {
						ch1 = nil
					} else {
						out <- val
					}
				}
			case val, ok := <-ch2:
				{
					if !ok {
						ch2 = nil
					} else {
						out <- val
					}
				}
			}
			if ch1 == nil && ch2 == nil {
				close(out)
				return
			}
		}
	}()

	return out
}

// =============================================================================
// PART 2: Timeouts and Deadlines
// =============================================================================

// ReceiveWithTimeout receives from a channel with a timeout.
// Returns (value, true) if received, (zero, false) if timeout.
//
// TODO: Use select with time.After to implement timeout
func ReceiveWithTimeout(ch <-chan int, timeout time.Duration) (int, bool) {
	select {
	case val := <-ch:
		{
			return val, true
		}
	case <-time.After(timeout):
		{
			return 0, false
		}

	}
}

// ReceiveWithDeadline receives until a specific time.
// Returns all values received before the deadline.
//
// TODO: Implement using time.After calculated from deadline
// HINT: time.Until(deadline) gives remaining duration
func ReceiveWithDeadline(ch <-chan int, deadline time.Time) []int {
	// YOUR CODE HERE

	var res []int
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	break_flg := false
	for break_flg != true {
		select {
		case val, ok := <-ch:
			{
				if !ok {
					break_flg = true
				}
				res = append(res, val)
			}
		case <-timer.C:
			{
				break_flg = true
			}
		}
	}
	return res
}

// PeriodicTask runs a function periodically until done is closed.
//
// TODO: Use select with time.Ticker and done channel
// Call fn() every interval, stop when done is closed
// Return the number of times fn was called
func PeriodicTask(fn func(), interval time.Duration, done <-chan struct{}) int {
	// YOUR CODE HERE

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var counter int
	for {
		select {
		case <-done:
			{
				return counter
			}
		case <-ticker.C:
			fn()
			counter++
		}
	}
}

// =============================================================================
// PART 3: Non-Blocking Operations with Default
// =============================================================================

// TrySend attempts to send without blocking.
// Returns true if send succeeded, false if channel is full/blocked.
//
// TODO: Use select with default to make non-blocking send
func TrySend(ch chan<- int, value int) bool {
	// YOUR CODE HERE

	select {
	case ch <- value:
		{
			return true
		}
	default:
		return false
	}
}

// TryReceive attempts to receive without blocking.
// Returns (value, true) if received, (zero, false) if channel is empty.
//
// TODO: Use select with default to make non-blocking receive
func TryReceive(ch <-chan int) (int, bool) {
	select {
	case value := <-ch:
		{
			return value, true
		}
	default:
		return 0, false
	}
}

// DrainChannel empties a channel without blocking.
// Returns all values that were buffered.
//
// TODO: Loop with TryReceive until channel is empty
func DrainChannel(ch <-chan int) (res []int) {
	// YOUR CODE HERE

	for {
		val, ok := TryReceive(ch)
		if !ok {
			break
		}
		res = append(res, val)
	}

	return
}

// =============================================================================
// PART 4: Priority Select (Trick Question!)
// =============================================================================

// PriorityReceive should receive from highPriority if available,
// otherwise from lowPriority.
//
// QUESTION: Why doesn't this simple implementation work correctly?
//
//	select {
//	case v := <-highPriority:
//	    return v, "high"
//	case v := <-lowPriority:
//	    return v, "low"
//	}
//
// TODO: Implement CORRECT priority selection
// HINT: You need nested selects - first try high priority with default,
// then fall back to blocking select on both
func PriorityReceive(highPriority, lowPriority <-chan int) (int, string) {

	select {
	case v := <-highPriority:
		return v, "high"
	default:
		select {
		case v := <-highPriority:
			return v, "high"
		case v := <-lowPriority:
			return v, "low"
		}
	}
}

// =============================================================================
// PART 5: Select with Send and Receive
// =============================================================================

// Relay forwards values from input to output, with buffering.
// Stops when input is closed AND buffer is empty.
//
// TODO: This is tricky! You need to:
// 1. Use an internal buffer (slice)
// 2. select should try to:
//   - Receive from input (if not closed) -> add to buffer
//   - Send to output (if buffer not empty) -> remove from buffer
//
// 3. Handle input closure and drain buffer before returning
//
// HINT: You can conditionally enable select cases using nil channels
// If buffer is empty, set the "send" channel to nil to disable that case
func Relay(input <-chan int, output chan<- int) {
	// YOUR CODE HERE

	var buffer []int
	var nextVal int
	var active_output chan<- int

	for input != nil || len(buffer) != 0 {
		if len(buffer) > 0 {
			nextVal = buffer[0]
			active_output = output
		} else {
			active_output = nil
		}
		select {
		case val, ok := <-input:
			if !ok {
				input = nil
			} else {
				buffer = append(buffer, val)
			}
		case active_output <- nextVal:
			{
				buffer = buffer[1:]
			}
		}
	}
}

// =============================================================================
// CHALLENGE: Implement a multiplexer
// =============================================================================

// Multiplex routes values from input to one of N output channels.
// The routeFn determines which output (0 to n-1) each value goes to.
// Stops when input is closed (close all outputs).
//
// TODO: Implement multiplexing with select
// HINT: You can't use select with a dynamic number of cases directly.
// One approach: try each output in order using non-blocking sends.
func Multiplex(input <-chan int, outputs []chan<- int, routeFn func(int) int) {
	// YOUR CODE HERE

	//for {
	//	val, ok := <-input
	//	if ok != true {
	//		break
	//	}
	//	out_idx := routeFn(val)
	//	outputs[out_idx] <- val
	//}
	//
	//for _, o := range outputs {
	//	close(o)
	//}

	for val := range input {
		out_idx := routeFn(val)
		outputs[out_idx] <- val
	}

	//for _, o := range outputs {
	//	close(o)
	//}

}

// =============================================================================
// CHALLENGE: Implement fair merge
// =============================================================================

// FairMerge merges N channels with fair scheduling.
// No single channel can starve others even if it's always ready.
//
// TODO: Implement round-robin selection from channels
// Return values in round-robin order (not arrival order)
// Stop when all channels are closed
//
// HINT: Track current index, try each channel in order
func FairMerge(channels []<-chan int) <-chan int {
	// YOUR CODE HERE
	out := make(chan int)

	go func() {
		activeChans := make([]<-chan int, len(channels))
		copy(activeChans, channels)

		activeCount := len(activeChans)

		for activeCount > 0 {
			for i, ch := range activeChans {
				if ch == nil {
					continue
				}
				val, ok := <-ch

				if !ok {
					activeChans[i] = nil
					activeCount--
				} else {
					out <- val
				}
			}
		}
		close(out)
	}()

	return out
}
