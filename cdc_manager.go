package tidb_cdc

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
)

// ConsumerGroup ...
type ConsumerGroup struct {
	// 反序列化类型信息
	Typ     reflect.Type
	Brokers []string
	Topic   string
	// 实际消费消息方法
	ConsumerHandlers []ConsumerHandler
	NumOfConsumers   int

	Version  string
	Group    string
	Assignor string
	Oldest   bool
	Verbose  bool
}

// Consumer ...
type Consumer struct {
	Typ              reflect.Type
	ConsumerHandlers []ConsumerHandler

	ready chan bool
}

// ConsumerHandler ...
type ConsumerHandler interface {
	Create(after interface{}) error
	Update(before interface{}, after interface{}) error
	Delete(before interface{}) error
}

// NewConsumerGroup ...
func NewConsumerGroup(
	typ reflect.Type,
	brokers []string,
	topic string,
	numOfConsumers int,
	kafkaVersion string,
	group string,
	assignor string,
	oldest bool,
	verbose bool,
) ConsumerGroup {

	if len(brokers) <= 0 {
		panic("brokers can not be empty")
	}
	if len(topic) <= 0 {
		panic("topic can not be empty")
	}
	if numOfConsumers < 1 {
		numOfConsumers = 1
	}
	if kafkaVersion == "" {
		kafkaVersion = "2.1.1"
	}
	if len(group) <= 0 {
		group = topic
	}
	if len(assignor) <= 0 {
		assignor = "range"
	}

	return ConsumerGroup{
		Typ:              typ,
		Brokers:          brokers,
		Topic:            topic,
		ConsumerHandlers: make([]ConsumerHandler, 0),
		NumOfConsumers:   numOfConsumers,

		Version:  kafkaVersion,
		Group:    group,
		Assignor: assignor,
		Oldest:   oldest,
		Verbose:  verbose,
	}
}

// AddHandler ...
func (c *ConsumerGroup) AddHandler(handler ConsumerHandler) {
	c.ConsumerHandlers = append(c.ConsumerHandlers, handler)
}

// Message ...
type Message struct {
	After  *json.RawMessage `json:"after"`
	Before *json.RawMessage `json:"before"`
	Op     string           `json:"op"`
}

// Start ...
func (c *ConsumerGroup) Start() {
	log.Println("Starting a new Sarama consumer")

	if c.Verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	version, err := sarama.ParseKafkaVersion(c.Version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	/**
	 * Construct a new Sarama configuration.
	 * The Kafka cluster version has to be defined before the consumer/producer is initialized.
	 */
	config := sarama.NewConfig()
	config.Version = version

	switch c.Assignor {
	case "sticky":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case "roundrobin":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "range":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	default:
		log.Panicf("Unrecognized consumer group partition assignor: %s", c.Assignor)
	}

	if c.Oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	client, err := sarama.NewConsumerGroup(c.Brokers, c.Group, config)

	ctx, cancel := context.WithCancel(context.Background())
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	consumer := Consumer{
		Typ:              c.Typ,
		ConsumerHandlers: c.ConsumerHandlers,

		ready: make(chan bool),
	}

	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, []string{c.Topic}, &consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()
	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer", " up and running!...")
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *Consumer) Setup(session sarama.ConsumerGroupSession) error {
	close(c.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

// TODO： 改
// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		if message.Value == nil {
			continue
		}

		var she CDCSchema
		err := json.Unmarshal(message.Value, &she)
		if err != nil {
			log.Printf("%+v", err)
			return err
		}
		op := she.Type
		var before interface{}
		var after interface{}
		var afterBack interface{}

		if op == "update" && she.Old != nil {
			before = reflect.New(c.Typ).Interface()
			err := MaxwellUnmarshal(she.Old, before)
			if err != nil {
				log.Printf("%+v", err)
				return err
			}
		}

		if (op == "insert" || op == "update") && she.Data != nil {
			after = reflect.New(c.Typ).Interface()
			err := MaxwellUnmarshal(she.Data, after)
			if err != nil {
				log.Printf("%+v", err)
				return err
			}

			afterBack = reflect.New(c.Typ).Interface()
			err = MaxwellUnmarshal(she.Data, afterBack)
			if err != nil {
				log.Printf("%+v", err)
				return err
			}
		}

		if op == "delete" && she.Data != nil {
			after = reflect.New(c.Typ).Interface()
			err := MaxwellUnmarshal(she.Data, after)
			if err != nil {
				log.Printf("%+v", err)
				return err
			}
		}

		if after == nil && before == nil {
			log.Println("after == nil && before == nil ", she.Type)
			continue
		}

		// 填充before
		//if before != nil {
		//	var kMap map[string]interface{}
		//	err = json.Unmarshal(*she.Old, &kMap)
		//	if err != nil {
		//		log.Printf("%+v\n", errors.WithStack(err))
		//		return errors.WithStack(err)
		//	}
		//
		//	DeserializeMaxwell(before, afterBack, kMap)
		//	before = afterBack
		//}

		// 是否执行软删除，有各自的 handler 自行控制
		for _, handler := range c.ConsumerHandlers {
			switch op {
			case "insert":
				err = handler.Create(after)
				if err != nil {
					log.Printf("Create, %+v", err)
				}
				break
			case "update":
				err = handler.Update(before, after)
				if err != nil {
					log.Printf("Update, %+v", err)
				}
				break
			case "delete":
				err = handler.Delete(after)
				if err != nil {
					log.Printf("Delete, %+v", err)
				}
				break
			}
		}
		session.MarkMessage(message, "")
	}

	return nil
}

// ma

// Connector ...
type Connector struct {
	consumerGroups map[string]*ConsumerGroup
	config         *Configuration
}

// NewConnector ...
func NewConnector(config *Configuration) *Connector {
	if config == nil {
		panic("config can not be nil")
	}
	cgs := make(map[string]*ConsumerGroup)
	return &Connector{
		consumerGroups: cgs,
		config:         config,
	}
}

var connector *Connector

// InitConnectorWithGroupMap ...
func InitConnectorWithGroupMap(config *Configuration, groupMaps ...map[string]RegistrationHelper) {
	if config == nil {
		panic("config can not be nil")
	}
	connector = NewConnector(config)
	if len(groupMaps) <= 0 {
		log.Printf("WARN: groupMap is empty")
		return
	}
	for _, groupMap := range groupMaps {
		connector.RegisterByGroupMap(groupMap)
	}
}

// StartConnector ...
func StartConnector() {
	if connector == nil {
		panic("Please initialize connector with the method [InitConnectorWithGroupMap]")
	}
	connector.Start()
}

// Start ...
func (c *Connector) Start() {
	wait := sync.WaitGroup{}
	for _, cg := range c.consumerGroups {
		for i := 0; i < cg.NumOfConsumers; i++ {
			wait.Add(1)
			go func(cg ConsumerGroup) {
				cg.Start()
				wait.Done()
			}(*cg)
		}
	}
	wait.Wait()
	log.Print("connector closed")
}

// HandlerFunc ...
type HandlerFunc func(before, after interface{}) error

// Register ...
func (c *Connector) Register(groupName string, dbName string, tableName string, model interface{}, handlers []ConsumerHandler) {
	_, ok := c.consumerGroups[groupName]
	if ok {
		panic("group already exists")
	}
	if dbName == "" {
		panic("database name can not be blank")
	}
	if tableName == "" {
		log.Print("WARN: tableName is blank")
	}
	if model == nil {
		panic("model can not be nil")
	}
	if len(handlers) <= 0 {
		log.Printf("WARN: handlers is empty")
	}
	cg := NewConsumerGroup(
		reflect.TypeOf(model),
		c.config.KafkaConfig.Brokers,
		// topic命名规则为 debezium mysql connector 配置文件中配置的 serverName.databaseName.tableName。
		fmt.Sprintf("%s.%s.%s", c.config.ServerName, dbName, tableName),
		c.config.NumberOfConsumers,
		c.config.KafkaVersion,
		groupName,
		c.config.Assignor,
		c.config.Oldest,
		c.config.Verbose,
	)
	for _, h := range handlers {
		cg.AddHandler(h)
	}
	c.consumerGroups[groupName] = &cg
}

// RegistrationHelper ...
type RegistrationHelper struct {
	DBName    string
	TableName string
	Model     interface{}
	Handlers  []ConsumerHandler
}

// RegisterByGroupMap key: GroupName; value: RegistrationHelper
func (c *Connector) RegisterByGroupMap(groupMap map[string]RegistrationHelper) {
	if len(groupMap) <= 0 {
		log.Printf("WARN: groupMap is empty")
		return
	}
	for groupName, helper := range groupMap {
		c.Register(groupName, helper.DBName, helper.TableName, helper.Model, helper.Handlers)
	}
}

// AddConsumerGroup ...
func (c *Connector) AddConsumerGroup(groupName string, cg *ConsumerGroup) {
	_, ok := c.consumerGroups[groupName]
	if ok {
		panic("group already exists")
	}
	if cg == nil {
		log.Print("WARN: consumer group can not be nil")
	}
	c.consumerGroups[groupName] = cg
}
