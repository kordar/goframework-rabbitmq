package goframework_rabbitmq

import (
	rabbitmq "github.com/kordar/gorabbitmq"
	"github.com/streadway/amqp"
)

type RabbitmqIns struct {
	name string
	ins  *rabbitmq.RabbitMQ
}

type RabbitmqOption struct {
	ExchangeName       string
	ExchangeType       string
	ExchangeDurable    bool
	ExchangeAutoDelete bool
	ExchangeInternal   bool
	ExchangeNoWait     bool
	ExchangeArgs       amqp.Table
}

func NewRabbitmqIns(name string, opts RabbitmqOption) *RabbitmqIns {
	client := rabbitmq.NewRabbitMQ(
		opts.ExchangeName,
		opts.ExchangeType,
		opts.ExchangeDurable,
		opts.ExchangeAutoDelete,
		opts.ExchangeInternal,
		opts.ExchangeNoWait,
		opts.ExchangeArgs,
	)
	return &RabbitmqIns{name: name, ins: client}
}

func (c RabbitmqIns) GetName() string {
	return c.name
}

func (c RabbitmqIns) GetInstance() interface{} {
	return c.ins
}

func (c RabbitmqIns) Close() error {
	return nil
}
