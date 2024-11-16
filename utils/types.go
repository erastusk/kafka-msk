package utils

import (
	"crypto/tls"
	"log"
	"time"

	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/scram"
)

const (
	username = "user"
	password = "xxxxxxxxxx"
)

type Testkafka struct {
	Messageid int    `json:"messageid"`
	Message   string `json:"message"`
	GoRoutine string `json:"goroutine"`
}
type (
	OptFunc func(*Opts)
	Opts    struct {
		Timeout       time.Duration
		Dualstack     bool
		TLS           *tls.Config
		SASLMechanism sasl.Mechanism
	}
)

func WithTLS(opt *Opts) {
	c, err := CreateTLSConfig()
	if err != nil {
		log.Fatalf("Could not create TLS config: %v", err)
	}
	opt.TLS = c
}

func WithSASL(opt *Opts) {
	mechanism, err := scram.Mechanism(scram.SHA512, username, password)
	if err != nil {
		panic(err)
	}
	opt.SASLMechanism = mechanism
}

type Dialer struct {
	Opts
}

func NewDialer(opts ...OptFunc) *Dialer {
	o := defaultOpts()
	for _, fn := range opts {
		fn(&o)
	}
	return &Dialer{
		Opts: o,
	}
}

func defaultOpts() Opts {
	return Opts{
		Dualstack: true,
		Timeout:   time.Second * 5,
	}
}
