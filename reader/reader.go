package reader

import (
	"context"
	"log"
	"time"

	"github/erastusk/kafka-msk/utils"

	"github.com/segmentio/kafka-go"
)

func buildDialer() *kafka.Dialer {
	z := utils.NewDialer(utils.WithTLS, utils.WithSASL)
	n := utils.KafkaDialer(*z)
	return n
}

func KafkaReader(done <-chan interface{}, topic string, part int, brokers []string) <-chan interface{} {
	t := make(chan interface{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)

	go func() {
		defer cancel()
		defer close(t)
		// make a new reader that consumes from topic-A, partition 0, at offset 42
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   brokers,
			Topic:     topic,
			Partition: part,
			MaxBytes:  10e6, // 10MB
			Dialer:    buildDialer(),
		})
		log.Printf("Successfully connected, reading messages.....\n")
		r.SetOffset(12)

		for {
			m, err := r.ReadMessage(ctx)
			if err != nil {
				break
			}
			select {
			case <-done:
				return
			case t <- m:
			}
		}
		if err := r.Close(); err != nil {
			log.Fatal("failed to close reader:", err)
		}
	}()
	return t
}

func KafkaFetcher(done <-chan interface{}, topic string, part int, brokers []string) <-chan interface{} {
	t := make(chan interface{})
	go func() {
		defer close(t)
		// make a new reader that consumes from topic-A, partition 0, at offset 42
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   brokers,
			Topic:     topic,
			Partition: part,
			MaxBytes:  10e6, // 10MB
			Dialer:    buildDialer(),
		})
		r.SetOffset(42)

		m, err := r.FetchMessage(context.Background())
		if err != nil {
			log.Println(err)
			return
		}
		t <- m
		if err := r.Close(); err != nil {
			log.Fatal("failed to close reader:", err)
		}
	}()
	return t
}
