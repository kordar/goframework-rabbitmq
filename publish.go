package goframework_rabbitmq

import (
	rabbitmq "github.com/kordar/gorabbitmq"
)

type RabbitmqPublishIns struct {
	name string
	ins  *rabbitmq.PublishClient
}

func NewRabbitmqPublishIns(name string, dsn string) *RabbitmqPublishIns {
	publishClient := rabbitmq.NewPublishClientWithDsn(dsn)
	return &RabbitmqPublishIns{name: name, ins: publishClient}
}

func (c RabbitmqPublishIns) GetName() string {
	return c.name
}

func (c RabbitmqPublishIns) GetInstance() interface{} {
	return c.ins
}

func (c RabbitmqPublishIns) Close() error {
	return nil
}
