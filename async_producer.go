package mq

import (
	"sync"

	"github.com/loveason/wabbit"
)

// AsyncProducer describes available methods for producer.
// This kind of producer is asynchronous.
// All occurred errors will be accessible with MQ.Error().
type AsyncProducer interface {
	// Produce sends message to broker. Returns immediately.
	Produce(data []byte)
	ProduceWithRoutingKey(data []byte, routingKey string)
}

type messageWithRoutingKey struct {
	message    []byte
	routingKey string
}

type asyncProducer struct {
	sync.Mutex // Protect channel during posting and reconnect.
	workerStatus

	channel         wabbit.Channel
	errorChannel    chan<- error
	exchange        string
	options         wabbit.Option
	publishChannel  chan messageWithRoutingKey
	routingKey      string
	shutdownChannel chan chan struct{}
}

func newAsyncProducer(channel wabbit.Channel, errorChannel chan<- error, config ProducerConfig) *asyncProducer {
	return &asyncProducer{
		channel:         channel,
		errorChannel:    errorChannel,
		exchange:        config.Exchange,
		options:         wabbit.Option(config.Options),
		publishChannel:  make(chan messageWithRoutingKey),
		routingKey:      config.RoutingKey,
		shutdownChannel: make(chan chan struct{}),
	}
}

func (producer *asyncProducer) init() {
	go producer.worker()
}

func (producer *asyncProducer) worker() {
	producer.markAsRunning()

	for {
		select {
		case message := <-producer.publishChannel:
			err := producer.produce(message)
			if err != nil {
				producer.errorChannel <- err
				// TODO Resend message.
			}
		case done := <-producer.shutdownChannel:
			// TODO It is necessary to guarantee the message delivery order.
			producer.closeChannel()
			close(done)

			return
		}
	}
}

// Method safely sets new RMQ channel.
func (producer *asyncProducer) setChannel(channel wabbit.Channel) {
	producer.Lock()
	producer.channel = channel
	producer.Unlock()
}

// Close producer's channel.
func (producer *asyncProducer) closeChannel() {
	producer.Lock()
	if err := producer.channel.Close(); err != nil {
		producer.errorChannel <- err
	}
	producer.Unlock()
}

func (producer *asyncProducer) Produce(message []byte) {
	publishMsg := messageWithRoutingKey{message: message, routingKey: producer.routingKey}
	producer.publishChannel <- publishMsg
}

func (producer *asyncProducer) ProduceWithRoutingKey(message []byte, routingKey string) {
	publishMsg := messageWithRoutingKey{message: message, routingKey: routingKey}
	producer.publishChannel <- publishMsg
}

func (producer *asyncProducer) produce(message messageWithRoutingKey) error {
	producer.Lock()
	defer producer.Unlock()

	return producer.channel.Publish(producer.exchange, message.routingKey, message.message, producer.options)
}

// Stops the worker if it is running.
func (producer *asyncProducer) Stop() {
	if producer.markAsStoppedIfCan() {
		done := make(chan struct{})
		producer.shutdownChannel <- done
		<-done
	}
}
