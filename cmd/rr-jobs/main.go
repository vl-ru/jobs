package main

import (
	"github.com/spiral/jobs"
	"github.com/spiral/jobs/broker/amqp"
	"github.com/spiral/jobs/broker/beanstalk"
	"github.com/spiral/jobs/broker/ephemeral"
	"github.com/spiral/jobs/broker/sqs"
	rr "github.com/spiral/roadrunner/cmd/rr/cmd"
	"github.com/spiral/roadrunner/service/rpc"

	_ "github.com/spiral/jobs/cmd/rr-jobs/jobs"
)

func main() {
	rr.Container.Register(rpc.ID, &rpc.Service{})
	rr.Container.Register(jobs.ID, &jobs.Service{
		Brokers: map[string]jobs.Broker{
			"amqp":      &amqp.Broker{},
			"ephemeral": &ephemeral.Broker{},
			"beanstalk": &beanstalk.Broker{},
			"sqs":       &sqs.Broker{},
		},
	})

	rr.Execute()
}
