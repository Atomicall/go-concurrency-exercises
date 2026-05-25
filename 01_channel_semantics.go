package concurrency

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

// =============================================================================
// EXERCISE 1.1: Channel Semantics
// =============================================================================
//
// This exercise explores the fundamental differences between buffered and
// unbuffered channels, and how channels behave when closed.
//
// KEY CONCEPTS:
// - Unbuffered channels block until both sender AND receiver are ready
// - Buffered channels block only when the buffer is full (send) or empty (receive)
// - Receiving from a closed channel returns the zero value immediately
// - Sending to a closed channel panics
// - You can detect closure with the "comma ok" idiom: val, ok := <-ch
//
// =============================================================================

// UnbufferedDemo demonstrates unbuffered channel behavior.
// An unbuffered channel blocks the sender until a receiver is ready.
//
// TODO: Implement this function to:
// 1. Create an unbuffered channel of int
// 2. Launch a goroutine that sends the value 42 to the channel
// 3. Sleep for 100ms in the main goroutine (simulating work)
// 4. Receive from the channel and return the value
//
// QUESTION: Where does the goroutine block? Before or after the sleep?
func UnbufferedDemo() int {
	// YOUR CODE HERE
	ch := make(chan int)

	go func(c chan int) {
		fmt.Println("before send, expect block")
		c <- 42
		fmt.Println("after send")
	}(ch)

	time.Sleep(100 * time.Millisecond)
	fmt.Println("before res")
	res := <-ch
	fmt.Println("after res")
	fmt.Println(res)

	return res
}

// BufferedDemo demonstrates buffered channel behavior.
// A buffered channel allows sends to complete without blocking (until full).
func BufferedDemo() int {
	ch := make(chan int, 5)

	go func(c chan int, rand_val int32) {

		for i := 0; i < 10; i++ {
			fmt.Println("before send {}", i)
			c <- int(rand_val)
			rand_val--
			fmt.Println("after send {}", i)
		}
		fmt.Println("closing ch")
		close(ch)
	}(ch, rand.Int31())

	time.Sleep(100 * time.Millisecond)
	for {
		res, ok := <-ch
		fmt.Println("res {}", res)
		if !ok {
			fmt.Println("ch !ok")
			break
		}
	}

	fmt.Println("before res, assuming 0 val")
	res := <-ch
	fmt.Println("after res assuming 0 val")
	fmt.Println(res)

	res = 42
	return res
}

// BufferFullDemo demonstrates what happens when a buffer fills up.
//
// TODO: Implement this function to:
// 1. Create a buffered channel of int with capacity 2
// 2. Send values 1, 2 to the channel (fills the buffer)
// 3. Launch a goroutine that will receive one value after 50ms
// 4. Send value 3 (this should block until the goroutine receives!)
// 5. Return true if all sends completed successfully
//
// QUESTION: What would happen if you didn't have the goroutine?
func BufferFullDemo() bool {
	ch := make(chan int, 2)
	flag_ch := make(chan interface{})
	ch <- 1
	ch <- 2

	go func(ch <-chan int, flag_ch chan interface{}) {
		for val := range ch {
			fmt.Println("val received {}", val)
			time.Sleep(50 * time.Millisecond)
		}
		flag_ch <- true
	}(ch, flag_ch)
	ch <- 3
	close(ch)
	<-flag_ch
	close(flag_ch)
	return true
}

// ClosedChannelReceive demonstrates receiving from a closed channel.
//
// TODO: Implement this function to:
// 1. Create a buffered channel of string with capacity 2
// 2. Send "first" and "second" to the channel
// 3. Close the channel
// 4. Receive ALL values (including after close) and return them as a slice
//
// HINT: Use the "comma ok" idiom to detect when channel is exhausted
// QUESTION: How many receives can you do? What do you get after the buffered values?
func ClosedChannelReceive() []string {
	// YOUR CODE HER

	ch := make(chan string, 2)
	ch <- "first"
	ch <- "second"
	close(ch)

	var res_slice []string

	for {
		res, ok := <-ch
		res_slice = append(res_slice, res)
		if !ok {
			break
		}
	}

	fmt.Println(strings.Join(res_slice, ", "))

	return res_slice
}

// RangeOverChannel demonstrates using range to receive until close.
//
// TODO: Implement this function to:
// 1. Create an unbuffered channel of int
// 2. Launch a goroutine that sends values 1, 2, 3 then closes the channel
// 3. Use `for val := range ch` to collect all values
// 4. Return the sum of all values
//
// NOTE: range automatically stops when channel is closed
func RangeOverChannel() int {
	// YOUR CODE HERE
	ch := make(chan int, 0)

	go func(c chan int) {
		c <- 1
		c <- 2
		c <- 3
		close(c)
	}(ch)

	var sum = 0

	for val := range ch {
		sum += val
	}

	fmt.Printf("sum %d\n", sum)

	return sum
}

// NilChannelBehavior demonstrates that nil channels block forever.
//
// TODO: Implement this function to:
// 1. Create a nil channel (var ch chan int, NOT make(chan int))
// 2. Use select with the nil channel and a timeout of 100ms
// 3. Return "timeout" if the select hit the timeout case
// 4. Return "received" if somehow a value was received (should never happen!)
//
// QUESTION: Why would you ever want a nil channel? (Hint: dynamic select cases)
func NilChannelBehavior() string {
	// YOUR CODE HERE

	var ch chan int

	for {
		select {
		case <-ch:
			{
				return "received"
			}
		case <-time.After(time.Millisecond * 100):
			return "timeout"
		}
	}
}

// ChannelDirection demonstrates send-only and receive-only channel types.
// This is a compile-time safety feature.
//
// TODO: Complete these three functions:

// generator creates values and sends them on a send-only channel
func generator(out chan<- int, count int) {
	for i := range count {
		out <- i
	}
	close(out)

}

// squarer receives from one channel, squares, sends to another
func squarer(in <-chan int, out chan<- int) {

	for val := range in {
		out <- val * val
	}
	close(out)
	// TODO: For each value from in, send its square to out
}

// ChannelDirectionDemo ties it together
// TODO: Create channels, wire up generator -> squarer, return sum of squares
func ChannelDirectionDemo(count int) int {
	var in = make(chan int)
	var out = make(chan int)

	var sum int

	go generator(in, count)
	go squarer(in, out)

	for {
		val, ok := <-out
		{
			if !ok {
				break
			}
			sum += val
		}
	}

	return sum
}

// =============================================================================
// CHALLENGE: Implement a timeout pattern without time.After
// =============================================================================

// SendWithTimeout attempts to send a value with a timeout.
// Returns true if send succeeded, false if timeout occurred.
//
// TODO: Implement WITHOUT using time.After (use time.NewTimer instead)
// Note: Go 1.23+ garbage collects unreferenced timers even if they haven't
// fired, but using time.NewTimer with Stop() is still good practice.
//
// HINT: time.NewTimer returns a *Timer with a channel C and method Stop()
func SendWithTimeout(ch chan<- int, value int, timeout time.Duration) bool {
	// YOUR CODE HERE
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case ch <- value:
		{
			return true
		}
	case <-t.C:
		{
			return false
		}
	}
}
