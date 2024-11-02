package goframework_rabbitmq

import (
	"github.com/kordar/godb"
	rabbitmq "github.com/kordar/gorabbitmq"
	"github.com/spf13/cast"
	"time"
)

var (
	rabbitmqpool = godb.NewDbPool()
)

func GetRabbitmqClient(db string) *rabbitmq.RabbitMQ {
	return rabbitmqpool.Handle(db).(*rabbitmq.RabbitMQ)
}

// AddRabbitmqInstancesArgs 批量添加rabbitmq句柄
func AddRabbitmqInstancesArgs(dbs map[string]map[string]string, args map[string]interface{}) {
	for db, cfg := range dbs {
		_ = AddRabbitmqInstanceArgs(db, cfg, args)
	}
}

// AddRabbitmqInstances 批量添加rabbitmq句柄
func AddRabbitmqInstances(dbs map[string]map[string]string, args map[string]interface{}) {
	AddRabbitmqInstancesArgs(dbs, args)
}

// AddRabbitmqInstance 添加rabbitmq句柄
func AddRabbitmqInstance(db string, cfg map[string]string, args map[string]interface{}) error {
	return AddRabbitmqInstanceArgs(db, cfg, args)
}

// AddRabbitmqInstanceArgs 添加mqtt句柄
func AddRabbitmqInstanceArgs(db string, cfg map[string]string, args map[string]interface{}) error {
	options := RabbitmqOption{
		ExchangeName:       cfg["exchange_name"],
		ExchangeType:       cfg["exchange_type"],
		ExchangeDurable:    cast.ToBool(cfg["exchange_durable"]),
		ExchangeAutoDelete: cast.ToBool(cfg["exchange_auto_delete"]),
		ExchangeInternal:   cast.ToBool(cfg["exchange_internal"]),
		ExchangeNoWait:     cast.ToBool(cfg["exchange_no_wait"]),
		ExchangeArgs:       args,
	}
	ins := NewRabbitmqIns(db, options)
	return rabbitmqpool.Add(ins)
}

func AddRabbitmqInstanceWithOptions(db string, option RabbitmqOption) error {
	ins := NewRabbitmqIns(db, option)
	return rabbitmqpool.Add(ins)
}

// RemoveRabbitmqInstance 移除rabbitmq句柄
func RemoveRabbitmqInstance(db string) {
	rabbitmqpool.Remove(db)
}

// HasRabbitmqInstance rabbitmq句柄是否存在
func HasRabbitmqInstance(db string) bool {
	return rabbitmqpool != nil && rabbitmqpool.Has(db)
}

func SubscribeWithDur(db string, dsn string, dur time.Duration, receiver ...rabbitmq.Receiver) {
	client := GetRabbitmqClient(db)
	conn := rabbitmq.NewDefaultConn(dsn)
	for _, r := range receiver {
		client.RegisterReceiver(r)
	}
	client.StartD(conn, dur)
}

func Subscribe(db string, dsn string, receiver ...rabbitmq.Receiver) {
	client := GetRabbitmqClient(db)
	conn := rabbitmq.NewDefaultConn(dsn)
	for _, r := range receiver {
		client.RegisterReceiver(r)
	}
	client.Start(conn)
}
