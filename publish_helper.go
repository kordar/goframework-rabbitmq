package goframework_rabbitmq

import (
	"github.com/kordar/godb"
	rabbitmq "github.com/kordar/gorabbitmq"
	"github.com/streadway/amqp"
)

var (
	publishpool = godb.NewDbPool()
)

func GetPublishClient(db string) *rabbitmq.PublishClient {
	return publishpool.Handle(db).(*rabbitmq.PublishClient)
}

// AddPublishInstances 批量添加Publish句柄
func AddPublishInstances(dbs map[string]map[string]string) {
	for db, cfg := range dbs {
		_ = AddPublishInstance(db, cfg["dsn"])
	}
}

// AddPublishInstance 添加Publish句柄
func AddPublishInstance(db string, dsn string) error {
	ins := NewRabbitmqPublishIns(db, dsn)
	return publishpool.Add(ins)
}

// RemovePublishInstance 移除Publish句柄
func RemovePublishInstance(db string) {
	publishpool.Remove(db)
}

// HasPublishInstance Publish句柄是否存在
func HasPublishInstance(db string) bool {
	return publishpool != nil && publishpool.Has(db)
}

func Publish(db string, name string, body []byte) error {
	publishClient := GetPublishClient(db)
	return publishClient.Publish(name, body)
}

func PublishByMsg(db string, name string, msg amqp.Publishing) error {
	publishClient := GetPublishClient(db)
	return publishClient.PublishByMsg(name, msg)
}

func AddChannelObject(db string, channelObjects ...*rabbitmq.ChannelObject) {
	publishClient := GetPublishClient(db)
	publishClient.AddChannelObject(channelObjects...)
}
