package pron_test

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pshch-pshch/pron"
)

func TestProcessor_RunLimited(t *testing.T) {
	const numWorkers = 10
	processor := pron.NewProcessor(pron.TaskHandlerFunc(
		func(c interface{}) {
			<-c.(chan struct{}) // just hang on worker
		},
	))

	tasks := make(chan interface{})
	stop := make(chan struct{})
	processor.RunN(tasks, numWorkers)

	for i := 0; i < numWorkers; i++ {
		tasks <- stop
	}

	runtime.Gosched()
	select {
	case tasks <- -1:
		t.Fatalf("Can process more than %d tasks concurrently", numWorkers)
	default:
	}

	close(stop)
	close(tasks)
	processor.Wait()
}

func TestProcessor_RunUnlimited(t *testing.T) {
	const numWorkers = 1000
	processor := pron.NewProcessor(pron.TaskHandlerFunc(
		func(c interface{}) {
			<-c.(chan struct{}) // just hang on worker
		},
	))

	tasks := make(chan interface{})
	stop := make(chan struct{})
	processor.Run(tasks)

	for i := 0; i < numWorkers; i++ {
		tasks <- stop
	}

	close(stop)
	close(tasks)
	processor.Wait()
}

func TestProcessor_WaitLimited(t *testing.T) {
	var sum, checksum int32

	const numWorkers = 10
	const numTasks = 100
	processor := pron.NewProcessor(pron.TaskHandlerFunc(
		func(v interface{}) {
			time.Sleep(1 * time.Millisecond)
			atomic.AddInt32(&sum, v.(int32))
		},
	))

	tasks := make(chan interface{})
	processor.RunN(tasks, numWorkers)

	for i := 0; i < numTasks; i++ {
		tasks <- int32(i)
		checksum += int32(i)
	}

	close(tasks)
	processor.Wait()

	if checksum != atomic.LoadInt32(&sum) {
		t.Fatal("Not all tasks are completed")
	}
}

func TestProcessor_WaitUnlimited(t *testing.T) {
	var sum, checksum int32

	const numTasks = 100
	processor := pron.NewProcessor(pron.TaskHandlerFunc(
		func(v interface{}) {
			time.Sleep(1 * time.Millisecond)
			atomic.AddInt32(&sum, v.(int32))
		},
	))

	tasks := make(chan interface{})
	processor.Run(tasks)

	for i := 0; i < numTasks; i++ {
		tasks <- int32(i)
		checksum += int32(i)
	}

	close(tasks)
	processor.Wait()

	if checksum != atomic.LoadInt32(&sum) {
		t.Fatal("Not all tasks are completed")
	}
}
