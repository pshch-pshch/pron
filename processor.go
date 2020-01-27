package pron

import "sync"

// TaskHandler processes tasks passed to Processor.
type TaskHandler interface {
	HandleTask(interface{})
}

// TaskHandlerFunc type is an adapter to allow the use of ordinary function as TaskHandler.
// If f is a function with the appropriate signature, TaskHandlerFunc(f) is a TaskHandler that calls f.
type TaskHandlerFunc func(interface{})

// HandleTask calls f(task).
func (f TaskHandlerFunc) HandleTask(task interface{}) {
	f(task)
}

// Processor can run and wait for worker goroutines to process tasks concurrently.
type Processor struct {
	wg      sync.WaitGroup
	handler TaskHandler
}

// NewProcessor creates Processor for processing tasks using provided TaskHandler.
func NewProcessor(handler TaskHandler) *Processor {
	return &Processor{handler: handler}
}

// RunN starts worker goroutines for processing tasks from channel until it's closed.
//
// The count determines the number of workers to start:
//   n > 0: run exactly n workers
//   n == 0: run individual goroutine for every task
//   n < 0: do nothing (it can be changed in the future)
func (p *Processor) RunN(tasks <-chan interface{}, n int) {
	if n > 0 {
		p.runLimited(tasks, n)
	} else if n == 0 {
		p.runUnlimited(tasks)
	}
}

// Run starts individual worker goroutine for processing every task from channel until it's closed.
// It is equivalent to RunN with a count of 0.
func (p *Processor) Run(tasks <-chan interface{}) {
	p.runUnlimited(tasks)
}

func (p *Processor) runLimited(tasks <-chan interface{}, n int) {
	p.wg.Add(n)

	for i := 0; i < n; i++ {
		go func() {
			defer p.wg.Done()

			for task := range tasks {
				p.handler.HandleTask(task)
			}
		}()
	}
}

func (p *Processor) runUnlimited(tasks <-chan interface{}) {
	p.wg.Add(1)

	go func() {
		defer p.wg.Done()

		for task := range tasks {
			p.wg.Add(1)

			go func(task interface{}) {
				defer p.wg.Done()

				p.handler.HandleTask(task)
			}(task)
		}
	}()
}

// Wait waits for all started worker goroutines to complete.
func (p *Processor) Wait() {
	p.wg.Wait()
}
