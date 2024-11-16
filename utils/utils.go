package utils

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/segmentio/kafka-go"
)

func KafkaDialer(z Dialer) *kafka.Dialer {
	return &kafka.Dialer{
		Timeout:       z.Timeout,
		DualStack:     z.Dualstack,
		TLS:           z.TLS,
		SASLMechanism: z.SASLMechanism,
	}
}

func KafkaDialLeader(ctx context.Context, dialer *kafka.Dialer, broker string, topic string) *kafka.Conn {
	conn, err := dialer.DialLeader(context.Background(), "tcp", broker, topic, 0)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	return conn
}

func JsonMarshal(id int, message string) []byte {
	r := Testkafka{
		Messageid: id,
		Message:   message,
	}
	rb, err := json.Marshal(r)
	if err != nil {
		log.Printf("Unable to marshal struct: %v\n", err)
	}
	return rb
}

func JsonUnmarshal(b []byte) Testkafka {
	v := Testkafka{}
	// vb := Tkafka{}
	err := json.Unmarshal(b, &v)
	if err != nil {
		log.Printf("Unable to marshal struct: %v\n", err)
	}
	return v
}

func CreateTLSConfig() (*tls.Config, error) {
	// Load CA cert from Amazon
	// Or use a Keystore
	caCert, err := os.ReadFile("AmazonRootCa.pem")
	if err != nil {
		return nil, fmt.Errorf("failed to load CA certificate: %v", err)
	}
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to append CA certificate")
	}
	// Create TLS configuration
	tlsConfig := &tls.Config{
		RootCAs:    caCertPool,
		MinVersion: tls.VersionTLS12, // to ensure minimum TLS version
	}

	return tlsConfig, nil
}
