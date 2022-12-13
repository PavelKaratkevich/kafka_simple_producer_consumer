package producer

import (
	"fmt"
	domain "kafka-go-getting-started/Domain"
	util "kafka-go-getting-started/Util"
	"time"

	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func SendMsgToKafka(msg domain.Message) {

	conf := util.ReadConfig("getting-started.properties")

	topic := "messages"

	p, err := kafka.NewProducer(&conf)

	if err != nil {
		fmt.Printf("Failed to create producer: %s", err)
		os.Exit(1)
	}

	// Go-routine to handle message delivery reports and possibly other event types (errors, stats, etc)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Produced event to topic %s: key = %-10s value = %s\n",
						*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
				}
			}
		}
	}()

	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(msg.Name),
		Value:          []byte(msg.Message),
		Timestamp:      time.Now(),
	}, nil)

	// Wait for all messages to be delivered
	p.Flush(15 * 1000)
	p.Close()
}
