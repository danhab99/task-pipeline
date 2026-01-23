package main

// Chan is an "unbounded channel" wrapper.
// - Send to In()
// - Receive from Out()
// - Close the input when done; output will drain then close.
type Chan[T any] struct {
	in  chan T
	out chan T
}

// New creates a new unbounded channel.
func NewBoundlessChan[T any]() *Chan[T] {
	c := &Chan[T]{
		in:  make(chan T, 1),
		out: make(chan T, 1),
	}
	go c.run()
	return c
}

func (c *Chan[T]) In() chan<- T  { return c.in }
func (c *Chan[T]) Out() <-chan T { return c.out }

func (c *Chan[T]) run() {
	// Queue grows as needed (limited by memory).
	var q []T
	var out chan T
	var next T

	for c.in != nil || len(q) > 0 {
		// Enable output only when we have something queued.
		if len(q) > 0 {
			out = c.out
			next = q[0]
		} else {
			out = nil
		}

		select {
		case v, ok := <-c.in:
			if !ok {
				// Input closed: stop accepting new items, drain queue.
				c.in = nil
				continue
			}
			q = append(q, v)

		case out <- next:
			// Pop front (and avoid holding references longer than needed).
			var zero T
			q[0] = zero
			q = q[1:]
		}
	}

	close(c.out)
}
