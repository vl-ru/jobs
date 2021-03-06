package ephemeral

import (
	"github.com/spiral/jobs"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var (
	pipe = &jobs.Pipeline{
		"broker": "local",
		"name":   "default",
	}
)

func TestBroker_Init(t *testing.T) {
	b := &Broker{}
	ok, err := b.Init()
	assert.True(t, ok)
	assert.NoError(t, err)
}

func TestBroker_StopNotStarted(t *testing.T) {
	b := &Broker{}
	b.Init()

	b.Stop()
}

func TestBroker_Register(t *testing.T) {
	b := &Broker{}
	b.Init()
	assert.NoError(t, b.Register(pipe))
}

func TestBroker_Register_Twice(t *testing.T) {
	b := &Broker{}
	b.Init()
	assert.NoError(t, b.Register(pipe))
	assert.Error(t, b.Register(pipe))
}

func TestBroker_Consume_Nil_BeforeServe(t *testing.T) {
	b := &Broker{}
	b.Init()
	b.Register(pipe)
	assert.NoError(t, b.Consume(pipe, nil, nil))
}

func TestBroker_Consume_Undefined(t *testing.T) {
	b := &Broker{}
	b.Init()

	assert.Error(t, b.Consume(pipe, nil, nil))
}

func TestBroker_Consume_BeforeServe(t *testing.T) {
	b := &Broker{}
	b.Init()
	b.Register(pipe)

	exec := make(chan jobs.Handler)
	err := func(id string, j *jobs.Job, err error) {}

	assert.NoError(t, b.Consume(pipe, exec, err))
}

func TestBroker_Consume_Serve_Nil_Stop(t *testing.T) {
	b := &Broker{}
	b.Init()
	b.Register(pipe)

	b.Consume(pipe, nil, nil)

	wait := make(chan interface{})
	go func() {
		assert.NoError(t, b.Serve())
		close(wait)
	}()
	time.Sleep(time.Millisecond * 100)
	b.Stop()

	<-wait
}

func TestBroker_Consume_Serve_Stop(t *testing.T) {
	b := &Broker{}
	b.Init()
	b.Register(pipe)

	exec := make(chan jobs.Handler)
	err := func(id string, j *jobs.Job, err error) {}

	b.Consume(pipe, exec, err)

	wait := make(chan interface{})
	go func() {
		assert.NoError(t, b.Serve())
		close(wait)
	}()
	time.Sleep(time.Millisecond * 100)
	b.Stop()

	<-wait
}

func TestBroker_PushToNotRunning(t *testing.T) {
	b := &Broker{}
	b.Init()
	b.Register(pipe)

	_, err := b.Push(pipe, &jobs.Job{})
	assert.Error(t, err)
}

func TestBroker_StatNotRunning(t *testing.T) {
	b := &Broker{}
	b.Init()
	b.Register(pipe)

	_, err := b.Stat(pipe)
	assert.Error(t, err)
}

func TestBroker_PushToNotRegistered(t *testing.T) {
	b := &Broker{}
	b.Init()

	ready := make(chan interface{})
	b.Listen(func(event int, ctx interface{}) {
		if event == jobs.EventBrokerReady {
			close(ready)
		}
	})

	go func() { assert.NoError(t, b.Serve()) }()
	defer b.Stop()

	<-ready

	_, err := b.Push(pipe, &jobs.Job{})
	assert.Error(t, err)
}

func TestBroker_StatNotRegistered(t *testing.T) {
	b := &Broker{}
	b.Init()

	ready := make(chan interface{})
	b.Listen(func(event int, ctx interface{}) {
		if event == jobs.EventBrokerReady {
			close(ready)
		}
	})

	go func() { assert.NoError(t, b.Serve()) }()
	defer b.Stop()

	<-ready

	_, err := b.Stat(pipe)
	assert.Error(t, err)
}
