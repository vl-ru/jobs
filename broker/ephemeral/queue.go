package ephemeral

import (
	"github.com/spiral/jobs"
	"sync"
	"sync/atomic"
	"time"
)

type queue struct {
	active int32
	st     *jobs.Stat

	// job pipeline
	concurPool chan interface{}
	jobs       chan *entry

	// active operations
	muw sync.Mutex
	wg  sync.WaitGroup

	// stop channel
	wait chan interface{}

	// exec handlers
	execPool   chan jobs.Handler
	errHandler jobs.ErrorHandler
}

type entry struct {
	id      string
	job     *jobs.Job
	attempt int
}

// create new queue
func newQueue(concurrency int) *queue {
	q := &queue{st: &jobs.Stat{}, jobs: make(chan *entry)}

	if concurrency != 0 {
		q.concurPool = make(chan interface{}, concurrency)
		for i := 0; i < concurrency; i++ {
			q.concurPool <- nil
		}
	}

	return q
}

// associate queue with new do pool
func (q *queue) configure(execPool chan jobs.Handler, err jobs.ErrorHandler) error {
	q.execPool = execPool
	q.errHandler = err

	return nil
}

// serve consumers
func (q *queue) serve() {
	q.wait = make(chan interface{})
	atomic.StoreInt32(&q.active, 1)

	for {
		e := q.consume()
		if e == nil {
			return
		}

		if q.concurPool != nil {
			<-q.concurPool
		}

		atomic.AddInt64(&q.st.Active, 1)
		h := <-q.execPool
		go func(e *entry) {
			q.do(h, e)
			atomic.AddInt64(&q.st.Active, ^int64(0))

			q.execPool <- h

			if q.concurPool != nil {
				q.concurPool <- nil
			}

			q.wg.Done()
		}(e)
	}
}

// allocate one job entry
func (q *queue) consume() *entry {
	q.muw.Lock()
	defer q.muw.Unlock()

	select {
	case <-q.wait:
		return nil
	case e := <-q.jobs:
		q.wg.Add(1)

		return e
	}
}

// do singe job
func (q *queue) do(h jobs.Handler, e *entry) {
	err := h(e.id, e.job)

	if err == nil {
		atomic.AddInt64(&q.st.Queue, ^int64(0))
		return
	}

	q.errHandler(e.id, e.job, err)

	if !e.job.Options.CanRetry(e.attempt) {
		atomic.AddInt64(&q.st.Queue, ^int64(0))
		return
	}

	q.push(e.id, e.job, e.attempt+1, e.job.Options.RetryDuration())
}

// stop the queue consuming
func (q *queue) stop() {
	if atomic.LoadInt32(&q.active) == 0 {
		return
	}

	atomic.StoreInt32(&q.active, 0)

	q.muw.Lock()
	close(q.wait)
	q.muw.Unlock()

	q.wg.Wait()
}

// add job to the queue
func (q *queue) push(id string, j *jobs.Job, attempt int, delay time.Duration) {
	if delay == 0 {
		atomic.AddInt64(&q.st.Queue, 1)
		go func() {
			q.jobs <- &entry{id: id, job: j, attempt: attempt}
		}()

		return
	}

	atomic.AddInt64(&q.st.Delayed, 1)
	go func() {
		time.Sleep(delay)
		atomic.AddInt64(&q.st.Delayed, ^int64(0))
		atomic.AddInt64(&q.st.Queue, 1)

		q.jobs <- &entry{id: id, job: j, attempt: attempt}
	}()
}

func (q *queue) stat() *jobs.Stat {
	return &jobs.Stat{
		InternalName: ":memory:",
		Queue:        atomic.LoadInt64(&q.st.Queue),
		Active:       atomic.LoadInt64(&q.st.Active),
		Delayed:      atomic.LoadInt64(&q.st.Delayed),
	}
}
