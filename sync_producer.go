package mq

import (
	"sync"

	"github.com/loveason/wabbit"
)

// SyncProducer describes available methods for synchronous producer.
type SyncProducer interface {
	// Produce sends message to broker. Waits for result (ok, error).
	Produce(data []byte) error
	ProduceWithRoutingKey(data []byte, routingKey string) error
}

type syncProducer struct {
	sync.Mutex // Protect channel during posting and reconnect.

	errorChannel chan<- error
	channel      wabbit.Channel
	exchange     string
	options      wabbit.Option
	routingKey   string
}

func newSyncProducer(channel wabbit.Channel, errorChannel chan<- error, config ProducerConfig) *syncProducer {
	return &syncProducer{
		channel:      channel,
		errorChannel: errorChannel,
		exchange:     config.Exchange,
		options:      wabbit.Option(config.Options),
		routingKey:   config.RoutingKey,
	}
}

func (producer *syncProducer) init() {
	// Do nothing. Already inited.
}

// Method safely sets new RMQ channel.
func (producer *syncProducer) setChannel(channel wabbit.Channel) {
	producer.Lock()
	producer.channel = channel
	producer.Unlock()
}

func (producer *syncProducer) Produce(message []byte) error {
	producer.Lock()
	defer producer.Unlock()

	return producer.channel.Publish(producer.exchange, producer.routingKey, message, producer.options)
}

func (producer *syncProducer) ProduceWithRoutingKey(message []byte, routingKey string) error {
	producer.Lock()
	defer producer.Unlock()
	return producer.channel.Publish(producer.exchange, routingKey, message, producer.options)
}

func (producer *syncProducer) Stop() {
	producer.closeChannel()
}

// Close producer's channel.
func (producer *syncProducer) closeChannel() {
	producer.Lock()
	if err := producer.channel.Close(); err != nil {
		producer.errorChannel <- err
	}
	producer.Unlock()
}
