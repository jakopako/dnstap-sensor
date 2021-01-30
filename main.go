package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"

	dnstap "github.com/dnstap/golang-dnstap"
	proto "github.com/golang/protobuf/proto"
	"github.com/miekg/dns"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	//	socketPath        = "/usr/local/var/dnstap/dnstap.sock"
	outputChannelSize = 32
)

var logger = log.New(os.Stdout, "", log.LstdFlags)

type kafkaOutput struct {
	kafkaWriter   *kafka.Producer
	outputChannel chan []byte
	topic         string
	logger        dnstap.Logger
	logOnly       bool
}

type dnsRR struct {
	Rrname    string
	Rrtype    uint16
	Rdata     string
	Ttl       uint32
	Timestamp *uint64
}

func newKafkaOutput(topic string, brokers string, logOnly bool) *kafkaOutput {
	if logOnly {
		return &kafkaOutput{
			outputChannel: make(chan []byte, outputChannelSize),
			topic:         topic,
			logger:        logger,
			logOnly:       logOnly,
		}
	}
	kw, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		panic(err)
	}
	return &kafkaOutput{
		kafkaWriter:   kw,
		outputChannel: make(chan []byte, outputChannelSize),
		topic:         topic,
		logger:        logger,
		logOnly:       logOnly,
	}
}

func (o *kafkaOutput) GetOutputChannel() chan []byte {
	return o.outputChannel
}

func (o *kafkaOutput) RunOutputLoop() {
	dt := &dnstap.Dnstap{}
	d := newDNSQueryBuffer(10)
	d.start()
	defer d.stop()
	f := bufio.NewWriter(os.Stdout)
	defer f.Flush()
	for frame := range o.outputChannel {
		if err := proto.Unmarshal(frame, dt); err != nil {
			o.logger.Printf("kafkaOutput: proto.Unmarshal() failed: %s, returning", err)
			break
		}
		msg := new(dns.Msg)
		if dt.Message.GetType() == dnstap.Message_RESOLVER_RESPONSE {
			if err := msg.Unpack(dt.Message.ResponseMessage); err != nil {
				o.logger.Printf("parse failed: %v", err)
			}
			qname := strings.ToLower(msg.Question[0].Name)
			if d.isInBuffer(msg.Id, dt.Message.GetQueryPort(), qname, msg.Question[0].Qtype) {
				for _, rr := range msg.Answer {
					drr := &dnsRR{
						Rrname: strings.ToLower(rr.Header().Name),
						Rrtype: rr.Header().Rrtype,
						// a bit ugly but I haven't found a way to generically extract the rdata
						// since every type has its own differently named fields, see
						// https://github.com/miekg/dns/blob/master/types.go
						Rdata:     strings.Split(rr.String(), "\t")[4],
						Ttl:       rr.Header().Ttl,
						Timestamp: dt.Message.ResponseTimeSec,
					}
					if drr.Rrname == qname {
						b, err := json.Marshal(drr)
						if err != nil {
							o.logger.Printf("Failed to convert dnsRR to json.")
							continue
						}
						if o.logOnly {
							o.logger.Printf("Message: %s", b)
						} else {
							o.kafkaWriter.Produce(&kafka.Message{
								TopicPartition: kafka.TopicPartition{Topic: &o.topic, Partition: kafka.PartitionAny},
								Value:          b,
							}, nil)
						}
					} else {
						o.logger.Printf("Name of question %s section did not match name %s of answer section.", msg.Question[0].Name, drr.Rrname)
					}
				}
			} else {
				o.logger.Printf("Did not find %s %d in buffer.", msg.Question[0].Name, msg.Question[0].Qtype)
			}
		} else if dt.Message.GetType() == dnstap.Message_RESOLVER_QUERY {
			if err := msg.Unpack(dt.Message.QueryMessage); err != nil {
				o.logger.Printf("parse failed: %v", err)
			}
			d.addQuery(msg.Id, dt.Message.GetQueryPort(), strings.ToLower(msg.Question[0].Name), msg.Question[0].Qtype)
		}
	}
}

func (o *kafkaOutput) Close() {
	o.kafkaWriter.Close()
}

func main() {
	// Read command line args
	socketPath := flag.String("s", "/var/named/dnstap/dnstap.sock", "The socket where the dnstap data is send.")
	kafkaBootstrapServer := flag.String("b", "localhost", "The bootstrap server for kafka.")
	kafkaTopic := flag.String("t", "dns_message_log", "The topic to which the messages are send.")
	logOnly := flag.Bool("l", false, "A flag that determines whether the queries should only be logged or not.")
	flag.Parse()
	// signal stuff from here: https://fabianlee.org/2017/05/21/golang-running-a-go-binary-as-a-systemd-service-on-ubuntu-16-04/
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs)
	go func() {
		s := <-sigs
		logger.Printf("received signal: %s", s)
		// some cleanup to do?
		os.Exit(1)
	}()
	o := newKafkaOutput(*kafkaTopic, *kafkaBootstrapServer, *logOnly)
	go o.RunOutputLoop()
	var iwg sync.WaitGroup
	i, err := dnstap.NewFrameStreamSockInputFromPath(*socketPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dnstap: Failed to open input socket %s: %v\n", *socketPath, err)
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
