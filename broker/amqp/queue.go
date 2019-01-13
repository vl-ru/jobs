package amqp

import (
	"fmt"
	"github.com/spiral/jobs"
	"github.com/streadway/amqp"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type queue struct {
	active         int32
	pipe           *jobs.Pipeline
	exchange, name string
	consumer       string

	// active consuming channel
	muc sync.Mutex
	cc  *channel

	// queue events
	lsn func(event int, ctx interface{})

	// active operations
	muw sync.RWMutex
	wg  sync.WaitGroup

	// exec handlers
	running  int32
	execPool chan jobs.Handler
	err      jobs.ErrorHandler
}

// newQueue creates new queue wrapper for AMQP.
func newQueue(pipe *jobs.Pipeline, lsn func(event int, ctx interface{})) (*queue, error) {
	if pipe.String("queue", "") == "" {
		return nil, fmt.Errorf("missing `queue` parameter on amqp pipeline")
	}

	return &queue{
		exchange: pipe.String("exchange", "amqp.direct"),
		name:     pipe.String("queue", ""),
		consumer: pipe.String("consumer", fmt.Sprintf("rr-jobs:%s-%v", pipe.Name(), os.Getpid())),
		pipe:     pipe,
		lsn:      lsn,
	}, nil
}

// associate queue with new consume pool
func (q *queue) configure(execPool chan jobs.Handler, err jobs.ErrorHandler) error {
	q.execPool = execPool
	q.err = err

	return nil
}

// serve consumes queue
func (q *queue) serve(publish, consume *chanPool) {
	atomic.StoreInt32(&q.active, 1)

	for {
		<-consume.waitConnected()
		if atomic.LoadInt32(&q.active) == 0 {
			// stopped
			return
		}

		delivery, cc, err := q.consume(publish, consume)
		if err != nil {
			q.report(err)
			continue
		}

		q.muc.Lock()
		q.cc = cc
		q.muc.Unlock()

		for d := range delivery {
			if atomic.LoadInt32(&q.active) == 0 {
				q.muw.Unlock()
				return
			}

			q.muw.Lock()
			q.wg.Add(1)
			q.muw.Unlock()

			h := <-q.execPool

			go func(h jobs.Handler, d amqp.Delivery) {
				err := q.do(publish, h, d)

				atomic.AddInt32(&q.running, ^int32(0))
				q.execPool <- h
				q.wg.Done()

				if err != nil {
					q.report(err)
				}
			}(h, d)
		}
	}
}

func (q *queue) consume(publish, consume *chanPool) (jobs <-chan amqp.Delivery, cc *channel, err error) {
	// allocate channel for the consuming
	if cc, err = consume.channel(q.name); err != nil {
		return nil, nil, err
	}

	if err := cc.ch.Qos(q.pipe.Integer("prefetch", 4), 0, false); err != nil {
		consume.release(cc, err)
		return nil, nil, err
	}

	delivery, err := cc.ch.Consume(q.name, q.consumer, false, false, false, false, nil)
	if err != nil {
		consume.release(cc, err)
		return nil, nil, err
	}

	go func() {
		for {
			select {
			case err := <-cc.signal:
				// channel error, we need new channel
				consume.release(cc, err)
				return
			}
		}
	}()

	return delivery, cc, err
}

func (q *queue) do(cp *chanPool, h jobs.Handler, d amqp.Delivery) error {
	id, attempt, j, err := unpack(d)
	if err != nil {
		q.report(err)
		return d.Nack(false, false)
	}

	err = h(id, j)

	if err == nil {
		return d.Ack(false)
	}

	// failed
	q.err(id, j, err)

	if !j.Options.CanRetry(attempt) {
		return d.Nack(false, false)
	}

	// retry as new j (to accommodate attempt number and new delay)
	if err = q.publish(cp, id, attempt+1, j, j.Options.RetryDuration()); err != nil {
		q.report(err)
		return d.Nack(false, true)
	}

	return d.Ack(false)
}

func (q *queue) stop() {
	if atomic.LoadInt32(&q.active) == 0 {
		return
	}

	atomic.StoreInt32(&q.active, 0)

	q.muc.Lock()
	if q.cc != nil {
		// gracefully stop consuming
		if err := q.cc.ch.Cancel(q.consumer, true); err != nil {
			q.report(err)
		}
	}
	q.muc.Unlock()
	q.muw.Lock()
	q.wg.Wait()
	q.muw.Unlock()
}

// publish message to queue or to delayed queue.
func (q *queue) publish(cp *chanPool, id string, attempt int, j *jobs.Job, delay time.Duration) error {
	c, err := cp.channel(q.name)
	if err != nil {
		return err
	}

	qName := q.name

	if delay != 0 {
		delayMs := int64(delay.Seconds() * 1000)
		qName = fmt.Sprintf("delayed-%d.%s.%s", delayMs, q.exchange, q.name)

		err := q.declare(cp, qName, qName, amqp.Table{
			"x-dead-letter-exchange":    q.exchange,
			"x-dead-letter-routing-key": q.name,
			"x-message-ttl":             delayMs,
			"x-expires":                 delayMs * 2,
		})

		if err != nil {
			return err
		}
	}

	err = c.ch.Publish(
		q.exchange, // exchange
		qName,      // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType:  "application/octet-stream",
			Body:         j.Body(),
			DeliveryMode: amqp.Persistent,
			Headers:      pack(id, attempt, j),
		},
	)

	if err != nil {
		go cp.release(c, err)
	}

	return err
}

// declare queue and binding to it
func (q *queue) declare(cp *chanPool, queue string, key string, args amqp.Table) error {
	c, err := cp.channel(q.name)
	if err != nil {
		return err
	}

	err = c.ch.ExchangeDeclare(q.exchange, "direct", true, false, false, false, nil)
	if err != nil {
		go cp.release(c, err)
		return err
	}

	_, err = c.ch.QueueDeclare(queue, true, false, false, false, args)
	if err != nil {
		go cp.release(c, err)
		return err
	}

	err = c.ch.QueueBind(queue, key, q.exchange, false, nil)
	if err != nil {
		go cp.release(c, err)
	}

	return err
}

// inspect the queue
func (q *queue) inspect(cp *chanPool) (*amqp.Queue, error) {
	c, err := cp.channel("stat")
	if err != nil {
		return nil, err
	}

	queue, err := c.ch.QueueInspect(q.name)
	if err != nil {
		go cp.release(c, err)
	}

	return &queue, err
}

// throw handles service, server and pool events.
func (q *queue) report(err error) {
	q.lsn(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: q.pipe, Caused: err})
}