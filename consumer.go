/*
 * Copyright (c) CERN 2016
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package stomp

import (
	"github.com/gmallard/stompngo"
	"io"
	"net"
	"syscall"
)

type (
	// Consumer wraps several connections to all broker behind an alias
	Consumer struct {
		Brokers []*Broker
	}

	// AckMode is the possible values for the ack
	AckMode string

	// Message contains messages sent from the broker
	Message struct {
		stompngo.Message
		// Store who sent the message so we can ack/nack
		broker *Broker
	}
)

const (
	// AckAuto means messages are automatically considered acknowledged
	AckAuto = AckMode("auto")
	// AckIndividual means the client has to ack each consumed message
	AckIndividual = AckMode("client-individual")
	// AckBulk means the message, and all the previous ones, are acknowledged at once
	AckBulk = AckMode("client")
)

// NewConsumer creates a new consumer, which will subscribe to all hosts
// behind params.Address and expose a simplified interface
func NewConsumer(params ConnectionParameters) (*Consumer, error) {
	host, port, err := net.SplitHostPort(params.Address)
	if err != nil {
		return nil, err
	}

	if params.EnableTLS {
		params.caCertPool = loadRootCAs(params.CaPath, params.CaCert)
		params.clientCerts, err = loadClientCert(params.UserCert, params.UserKey)
		if err != nil {
			return nil, err
		}
	}

	// Get ips behind the alias
	ips, err := net.LookupIP(host)
	if err != nil {
		return nil, err
	}
	c := &Consumer{
		Brokers: make([]*Broker, len(ips)),
	}
	for i, ip := range ips {
		newParams := params
		newParams.Address = net.JoinHostPort(ip.String(), port)
		if c.Brokers[i], err = dial(newParams); err != nil {
			goto newConsumerFailed
		}
	}

	return c, nil

	// Make sure we release existing connections on failure
newConsumerFailed:
	for _, broker := range c.Brokers {
		if broker != nil {
			broker.close()
		}
	}
	return nil, err
}

// Close disconnects and frees resources
func (c *Consumer) Close() error {
	for _, broker := range c.Brokers {
		broker.close()
	}
	return nil
}

// subscribeToBroker is called once per broker connection
func subscribeToBroker(broker *Broker, headers *stompngo.Headers, out chan<- Message) error {
	in, err := broker.stompConnection.Subscribe(*headers)
	if err != nil {
		return err
	}

	// Now, this is the trickier part
	// To handle disconnects, we need to shovel from one channel to the other, and
	// handle re-subscriptions if the connection is gone
	for {
		if frame, ok := <-in; !ok {
			// Remote channel closed
			return nil
		} else if frame.Error == nil {
			// No error, forward
			out <- Message{
				Message: frame.Message,
				broker:  broker,
			}
		} else if frame.Error != io.EOF || broker.params.ConnectionLost == nil {
			// An error we don't know how to deal with, forward and be done
			return frame.Error
		} else {
			err = frame.Error
			// Retry loop
			for err != nil {
				// Disconnected, notify the client
				broker.params.ConnectionLost(broker)
				// If the client had reconnected, we need to resubscribe
				in, err = broker.stompConnection.Subscribe(*headers)
			}
			// If we are here, managed to reconnect and resubscribe!
		}
	}
}

// Subscribe to a remote topic or queue
func (c *Consumer) Subscribe(destination, id string, ack AckMode) (<-chan Message, <-chan error, error) {
	if id == "" {
		return nil, nil, stompngo.EBADSID
	}
	if destination == "" {
		return nil, nil, stompngo.EREQDSTSUB
	}

	headers := &stompngo.Headers{
		"destination", destination,
		"id", id,
		"ack", string(ack),
	}

	// Aggregate output channels
	out := make(chan Message, 100)
	errs := make(chan error, len(c.Brokers))
	done := make(chan bool)

	// Supervisor goroutine, closes channels when all brokers are gone
	go func() {
		nBrokers := len(c.Brokers)
		for nDone := 0; nDone < nBrokers; {
			_ = <-done
			nDone++
		}
		close(out)
		close(errs)
		close(done)
	}()

	// For each connection, spawn a goroutine that will shovel from one connection to the
	// common channel
	for _, broker := range c.Brokers {
		go func(broker *Broker) {
			if err := subscribeToBroker(broker, headers, out); err != nil {
				// Ignore errors of duplicated subscriptions
				// Possibly one of the hosts has more than one IP (i.e. 4 and 6)
				if err != stompngo.EDUPSID {
					errs <- err
				}
			}
			done <- true
		}(broker)
	}

	return out, errs, nil
}

// Unsubscribe from an existing subscription
func (c *Consumer) Unsubscribe(id string) (err error) {
	if id == "" {
		return stompngo.EBADSID
	}

	headers := stompngo.Headers{
		"id", id,
	}
	for _, broker := range c.Brokers {
		for {
			if err = broker.handleReconnectOnSend(
				broker.stompConnection.Unsubscribe(headers),
			); err != syscall.EAGAIN {
				break
			}
		}
	}
	return
}

// Ack acknowledges the message
func (m *Message) Ack() (err error) {
	headers := stompngo.Headers{
		"message-id", m.Message.Headers.Value("message-id"),
		"subscription", m.Message.Headers.Value("subscription"),
	}
	for {
		if err = m.broker.handleReconnectOnSend(m.broker.stompConnection.Ack(headers)); err != syscall.EAGAIN {
			break
		}
	}
	return
}

// Nack tells the broker that the message has not been consumed
func (m *Message) Nack() (err error) {
	headers := stompngo.Headers{
		"message-id", m.Message.Headers.Value("message-id"),
		"subscription", m.Message.Headers.Value("subscription"),
	}
	for {
		if err = m.broker.handleReconnectOnSend(m.broker.stompConnection.Nack(headers)); err != syscall.EAGAIN {
			break
		}
	}
	return
}
