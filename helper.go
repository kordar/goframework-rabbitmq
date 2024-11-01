package goframework_rabbitmq

import (
	"fmt"
	"github.com/kordar/godb"
	logger "github.com/kordar/gologger"
	rabbitmq "github.com/kordar/gorabbitmq"
	"github.com/kordar/goutil"
	"github.com/spf13/cast"
)

var (
	rabbitmqpool = godb.NewDbPool()
)

func GetRabbitmqClient(db string) *rabbitmq.RabbitMQ {
	return rabbitmqpool.Handle(db).(*rabbitmq.RabbitMQ)
}

// AddRabbitmqInstancesArgs 批量添加rabbitmq句柄
func AddRabbitmqInstancesArgs(
	dbs map[string]map[string]string,
) {
	for db, cfg := range dbs {
		_ = AddMqttInstanceArgs(db, cfg, credentialsProvider, defaultPublishHandler, onConnect, onConnectionLost, onReconnecting, onConnectAttempt)
	}
}

// AddRabbitmqInstances 批量添加rabbitmq句柄
func AddRabbitmqInstances(dbs map[string]map[string]string) {
	AddRabbitmqInstanceArgs(dbs)
}

// AddRabbitmqInstanceArgs 添加mqtt句柄
func AddRabbitmqInstanceArgs(
	db string,
	cfg map[string]string,
) error {
	options := mqtt.NewClientOptions()
	options.AddBroker(fmt.Sprintf("tcp://%s:%s", cfg["broker"], cfg["port"]))
	if cfg["id"] == "" {
		uuid := goutil.UUID()
		options.SetClientID(uuid)
	} else {
		options.SetClientID(cfg["id"])
	}

	options.SetUsername(cfg["username"])
	options.SetPassword(cfg["password"])
	if credentialsProvider != nil {
		options.CredentialsProvider = credentialsProvider
	}

	if cfg["clean_session"] != "" {
		options.CleanSession = cast.ToBool(cfg["clean_session"])
	}

	if cfg["order"] != "" {
		options.Order = cast.ToBool(cfg["order"])
	}

	if cfg["will_enabled"] != "" {
		options.WillEnabled = cast.ToBool(cfg["will_enabled"])
	}

	if cfg["will_retained"] != "" {
		options.WillRetained = cast.ToBool(cfg["will_retained"])
	}

	if cfg["will_topic"] != "" {
		options.WillTopic = cfg["will_topic"]
	}

	if cfg["will_payload"] != "" {
		options.WillPayload = []byte(cfg["will_payload"])
	}

	if cfg["will_qos"] != "" {
		options.WillQos = byte(cast.ToInt(cfg["will_qos"]))
	}

	if cfg["protocol_version"] != "" {
		options.ProtocolVersion = cast.ToUint(cfg["protocol_version"])
	}

	if cfg["keep_alive"] != "" {
		options.KeepAlive = cast.ToInt64(cfg["keep_alive"])
	}

	if cfg["ping_timeout"] != "" {
		options.PingTimeout = cast.ToDuration(cfg["ping_timeout"])
	}

	if cfg["connect_timeout"] != "" {
		options.ConnectTimeout = cast.ToDuration(cfg["connect_timeout"])
	}

	if cfg["max_reconnect_interval"] != "" {
		options.MaxReconnectInterval = cast.ToDuration(cfg["max_reconnect_interval"])
	}

	if cfg["connect_retry_interval"] != "" {
		options.ConnectRetryInterval = cast.ToDuration(cfg["connect_retry_interval"])
	}

	if cfg["auto_reconnect"] != "" {
		options.AutoReconnect = cast.ToBool(cfg["auto_reconnect"])
	}

	if cfg["connect_retry"] != "" {
		options.ConnectRetry = cast.ToBool(cfg["connect_retry"])
	}

	if cfg["auto_ack_disabled"] != "" {
		options.AutoAckDisabled = cast.ToBool(cfg["auto_ack_disabled"])
	}

	if cfg["write_timeout"] != "" {
		options.WriteTimeout = cast.ToDuration(cfg["write_timeout"])
	}

	if cfg["resume_subs"] != "" {
		options.ResumeSubs = cast.ToBool(cfg["resume_subs"])
	}

	if cfg["message_channel_depth"] != "" {
		options.MessageChannelDepth = cast.ToUint(cfg["message_channel_depth"])
	}

	if cfg["max_resume_pub_in_flight"] != "" {
		options.MaxResumePubInFlight = cast.ToInt(cfg["max_resume_pub_in_flight"])
	}

	// protocolVersionExplicit bool
	// TLSConfig               *tls.Config
	// Store                   Store

	if defaultPublishHandler != nil {
		options.DefaultPublishHandler = defaultPublishHandler
	}

	if onConnect != nil {
		options.OnConnect = onConnect
	}

	if onConnectionLost != nil {
		options.OnConnectionLost = onConnectionLost
	}

	if onReconnecting != nil {
		options.OnReconnecting = onReconnecting
	}

	if onConnectAttempt != nil {
		options.OnConnectAttempt = onConnectAttempt
	}

	// HTTPHeaders             http.Header
	// WebsocketOptions        *WebsocketOptions
	// Dialer                  *net.Dialer
	// CustomOpenConnectionFn  OpenConnectionFunc

	ins := NewMqttIns(db, options)
	return mqttpool.Add(ins)
}

// AddMqttInstance 添加mqtt句柄
func AddMqttInstance(db string, cfg map[string]string) error {
	return AddMqttInstanceArgs(db, cfg, nil, nil, nil, nil, nil, nil)
}

func AddRabbitmqInstanceWithOptions(db string, option *rabbitmq.ClientOptions) error {
	ins := NewRabbitmqIns(db, option)
	return mqttpool.Add(ins)
}

// RemoveRabbitmqInstance 移除rabbitmq句柄
func RemoveRabbitmqInstance(db string) {
	rabbitmqpool.Remove(db)
}

// HasRabbitmqInstance rabbitmq句柄是否存在
func HasRabbitmqInstance(db string) bool {
	return rabbitmqpool != nil && rabbitmqpool.Has(db)
}

func Publish(db string, topic string, message string, qos byte, retained bool) {
	mqttclient := GetMqttClient(db)
	token := mqttclient.Publish(topic, qos, retained, message)
	token.Wait()
	if token.Error() != nil {
		logger.Warnf("[publish mqtt] token err = %v", token.Error())
	}
}

func Subscribe(db string, topic string, qos byte, f func(client mqtt.Client, message mqtt.Message)) {
	mqttclient := GetMqttClient(db)
	token := mqttclient.Subscribe(topic, qos, f)
	token.Wait()
	logger.Infof("[mqtt] Subscribed to topic: %s", topic)
}
