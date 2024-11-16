package main

import (
	"log"
	"runtime"
	"strconv"
	"sync"

	"github/erastusk/kafka-msk/reader"
	"github/erastusk/kafka-msk/utils"
	"github/erastusk/kafka-msk/writer"

	"github.com/segmentio/kafka-go"
)

const (
	topic    = "demo-kafka"
	username = "user"
	password = "xxxxxxxxxxxx"
)

func main() {
	done := make(chan interface{})
	defer close(done)
	saslscram := []string{
		"b-1.xxxxxxxxxxxxxxxxxxxx.xxxxxx.c17.kafka.us-east-1.amazonaws.com:9092",
		"b-2.xxxxxxxxxxxxxxxxxxxx.xxxxxx.c17.kafka.us-east-1.amazonaws.com:9093",
		"b-3.xxxxxxxxxxxxxxxxxxxx.xxxxxx.c17.kafka.us-east-1.amazonaws.com:9094",
		"b-1.xxxxxxxxxxxxxxxxxxxx.xxxxxx.c17.kafka.us-east-1.amazonaws.com:9094",
		"b-2.xxxxxxxxxxxxxxxxxxxx.xxxxxx.c17.kafka.us-east-1.amazonaws.com:9094",
		"b-3.xxxxxxxxxxxxxxxxxxxx.xxxxxx.c17.kafka.us-east-1.amazonaws.com:9094",
		"b-1.xxxxxxxxxxxxxxxxxxxx.xxxxxx.c17.kafka.us-east-1.amazonaws.com:9096",
		"b-2.xxxxxxxxxxxxxxxxxxxx.xxxxxx.c17.kafka.us-east-1.amazonaws.com:9096",
		"b-3.xxxxxxxxxxxxxxxxxxxx.xxxxxx.c17.kafka.us-east-1.amazonaws.com:9096",
	}
	// Write Messages
	writer.SASLTLSWrites(saslscram, topic, "SASL with TLS message")
	writer.PlainTextTLSWrites(saslscram, topic, "Plaintext with TLS message")
	writer.PlainTextWrites(saslscram, topic, "Plaintext  message")

	// Read all messages from specified Topic
	messages_chan := reader.KafkaReader(done, topic, 0, saslscram)

	// Marshall topics method that can be multiplexed to Unmarshal topic messages
	marshalTopics := func(done chan interface{}, t <-chan interface{}) <-chan utils.Testkafka {
		mtopics := make(chan utils.Testkafka)
		marshalkey := func(r interface{}) utils.Testkafka {
			var mk utils.Testkafka
			switch m := r.(type) {
			case kafka.Message:
				mk = utils.JsonUnmarshal(m.Value)
			}
			return mk
		}
		go func() {
			defer close(mtopics)
			for ts := range t {
				select {
				case <-done:
					return
				case mtopics <- marshalkey(ts):
				}
			}
		}()
		return mtopics
	}
	nums := runtime.NumCPU()
	getters := make([]<-chan utils.Testkafka, nums)
	for i := 0; i < nums; i++ {
		getters[i] = marshalTopics(done, messages_chan)
	}
	fanin := func(done chan interface{}, ds ...<-chan utils.Testkafka) <-chan utils.Testkafka {
		agg := make(chan utils.Testkafka)
		wg := &sync.WaitGroup{}
		wg.Add(len(ds))
		for d, z := range ds {
			go func(done chan interface{}, a int, wg *sync.WaitGroup, b <-chan utils.Testkafka) {
				defer wg.Done()
				for m := range b {
					m.GoRoutine = strconv.Itoa(a)
					select {
					case <-done:
						return
					case agg <- m:
					}
				}
			}(done, d, wg, z)
		}
		go func() {
			wg.Wait()
			defer close(agg)
		}()
		return agg
	}
	allmessages := fanin(done, getters...)
	for mess := range allmessages {
		log.Println(mess)
	}
}
