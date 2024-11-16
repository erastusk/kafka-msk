package writer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github/erastusk/kafka-msk/utils"

	"github.com/segmentio/kafka-go"
)

func PlainTextWrites(brokers []string, topic, message string) {
	r := utils.JsonMarshal(300, message)
	z := utils.NewDialer()
	n := utils.KafkaDialer(*z)
	conn := utils.KafkaDialLeader(context.Background(), n, brokers[0], topic)
	KafkaWriter(conn, r)
}

func PlainTextTLSWrites(brokers []string, topic, message string) {
	r := utils.JsonMarshal(400, message)
	z := utils.NewDialer(utils.WithTLS)
	n := utils.KafkaDialer(*z)
	c := utils.KafkaDialLeader(context.Background(), n, brokers[3], topic)
	KafkaWriter(c, r)
}

func SASLTLSWrites(brokers []string, topic, message string) {
	r := utils.JsonMarshal(500, message)
	z := utils.NewDialer(utils.WithTLS, utils.WithSASL)
	n := utils.KafkaDialer(*z)
	c := utils.KafkaDialLeader(context.Background(), n, brokers[6], topic)
	KafkaWriter(c, r)
}

func KafkaWriter(c *kafka.Conn, rb []byte) {
	c.SetWriteDeadline(time.Now().Add(10 * time.Second))
	d, err := c.WriteMessages(
		kafka.Message{Value: rb},
	)
	fmt.Printf("wrote %v bytes\n", d)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}
	if err := c.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}
