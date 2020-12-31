package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	dnstap "github.com/dnstap/golang-dnstap"
	proto "github.com/golang/protobuf/proto"
	"github.com/miekg/dns"
	kafka "github.com/segmentio/kafka-go"
)

const (
	socketPath = "/usr/local/var/dnstap/dnstap.sock"
	// Output channel buffer size value from main dnstap package.
	outputChannelSize = 32
	// Kafka topic and brokers.
	topic          = "dns-message-log"
	broker1Address = "localhost:9091"
	broker2Address = "localhost:9092"
	broker3Address = "localhost:9093"
)

var logger = log.New(os.Stderr, "", log.LstdFlags)

type kafkaOutput struct {
	kafkaWriter   *kafka.Writer
	format        dnstap.TextFormatFunc
	outputChannel chan []byte
	logger        dnstap.Logger
	ctx           context.Context
}

type dnsRR struct {
	Rrname    string
	Rrtype    uint16
	Rdata     string
	Ttl       uint32
	Timestamp uint64
}

func newKafkaOutput(brokers []string, topic string, formatter dnstap.TextFormatFunc) *kafkaOutput {
	kw := kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokers,
		Topic:   topic,
		// RequiredAcks: 0,
		// BatchSize:    10,
		// BatchTimeout: 2 * time.Second,
		Logger: logger,
	})
	return &kafkaOutput{
		kafkaWriter:   kw,
		outputChannel: make(chan []byte, outputChannelSize),
		format:        formatter,
		ctx:           context.Background(),
		logger:        logger,
	}
}

func (o *kafkaOutput) GetOutputChannel() chan []byte {
	return o.outputChannel
}

func (o *kafkaOutput) RunOutputLoop() {
	dt := &dnstap.Dnstap{}
	f := bufio.NewWriter(os.Stdout)
	defer f.Flush()
	for frame := range o.outputChannel {
		if err := proto.Unmarshal(frame, dt); err != nil {
			o.logger.Printf("kafkaOutput: proto.Unmarshal() failed: %s, returning", err)
			break
		}
		// buf, ok := o.format(dt)
		// if !ok {
		// 	o.logger.Printf("kafkaOutput: text format function failed, returning")
		// 	break
		// }
		// s := string(buf)
		// o.logger.Printf("%s", s)
		msg := new(dns.Msg)
		if err := msg.Unpack(dt.Message.ResponseMessage); err != nil {
			o.logger.Printf("parse failed: %v", err)
		}

		//o.logger.Printf("%s", msg.String())
		for _, rr := range msg.Answer {
			drr := &dnsRR{
				Rrname: rr.Header().Name,
				Rrtype: rr.Header().Rrtype,
				Rdata:  "bla",
				Ttl:    rr.Header().Ttl,
			}
			b, err := json.Marshal(drr)
			if err != nil {
				o.logger.Printf("Failed to convert dnsRR to json.")
			} else {
				//o.logger.Printf(rr.String())
				o.logger.Printf(string(b))
			}
		}

		// err := o.kafkaWriter.WriteMessages(o.ctx, kafka.Message{
		// 	Value: buf,
		// })
		// if err != nil {
		// 	panic("could not write message " + err.Error())
		// }

		// log a confirmation once the message is written
		//o.logger.Printf("kafkaOutput: Wrote message.")
	}
}

func (o *kafkaOutput) Close() {
	if err := o.kafkaWriter.Close(); err != nil {
		o.logger.Printf("failed to close writer:", err)
	}
}

func main() {
	o := newKafkaOutput([]string{broker1Address, broker2Address, broker3Address}, topic, dnstap.JSONFormat)
	go o.RunOutputLoop()
	var iwg sync.WaitGroup
	i, err := dnstap.NewFrameStreamSockInputFromPath(socketPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dnstap: Failed to open input socket %s: %v\n", socketPath, err)
		os.Exit(1)
	}
	i.SetTimeout(0)
	i.SetLogger(logger)
	iwg.Add(1)
	go runInput(i, o, &iwg)
	iwg.Wait()
	o.Close()
}

func runInput(i dnstap.Input, o dnstap.Output, wg *sync.WaitGroup) {
	go i.ReadInto(o.GetOutputChannel())
	i.Wait()
	wg.Done()
}
