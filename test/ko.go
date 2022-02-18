package main

import (
	"context"
	"fmt"
	"io/ioutil"

	"github.com/Shopify/sarama"
)

func main() {
	consumer, err := NewConsumer([]string{"192.168.88.202:9092", "192.168.88.203:9092", "192.168.88.204:9092"}, "TEST")
	if err != nil {
		panic(err)
	}

	pb := pb{}

	for {
		consumer.Consume(context.TODO(), []string{"deal_test.deals"}, &pb)
	}
}

func NewConsumer(addr []string, group string) (sarama.ConsumerGroup, error) {
	kafkaConfig := sarama.NewConfig()

	kafkaConfig.Producer.Retry.Max = 5
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Producer.Partitioner = sarama.NewRandomPartitioner
	kafkaConfig.Producer.MaxMessageBytes = 100000000

	kafkaConfig.Consumer.Offsets.AutoCommit.Enable = false
	kafkaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange

	//if cfg.CONFIG.Oldest {
	kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	//}

	client, err := sarama.NewClient(addr, kafkaConfig)
	if err != nil {
		return nil, err
	}

	fromClient, err := sarama.NewConsumerGroupFromClient(group, client)
	if err != nil {
		return nil, err
	}

	return fromClient, nil
}

type pb struct {
}

func (p *pb) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (p *pb) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (p *pb) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("Message topic:%q partition:%d offset:%d  value:%s\n", msg.Topic, msg.Partition, msg.Offset, string(msg.Value))

		fmt.Println(string(msg.Value))
		ioutil.WriteFile("fff.json", msg.Value, 00666)

		// 手动确认消息
		sess.MarkMessage(msg, "")
	}
	return nil
}

type T struct {
	Database string                 `json:"database"`
	Table    string                 `json:"table"`
	Type     string                 `json:"type"` // delete, insert, update
	Ts       int                    `json:"ts"`
	Data     map[string]interface{} `json:"data"`
	Old      map[string]interface{} `json:"old"`
}
