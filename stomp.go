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
	"errors"
	"fmt"
	"github.com/gmallard/stompngo"
	"io"
	"net"
	"os"
	"syscall"
)

type (
	// ConnectionLostCallback is the callback type for lost connections
	ConnectionLostCallback func(c *Connection)

	// Connection wraps the underlying network and stomp connection
	Connection struct {
		host, port      string
		login, passcode string
		netConnection   net.Conn
		stompConnection *stompngo.Connection

		connectionLostCallback ConnectionLostCallback
	}

	// AckMode is the possible values for the ack
	AckMode string
)

const (
	// AckAuto means messages are automatically considered acknowledged
	AckAuto = AckMode("auto")
	// AckIndividual means the client has to ack each consumed message
	AckIndividual = AckMode("client-individual")
	// AckBulk means the message, and all the previous ones, are acknowledged at once
	AckBulk = AckMode("client")
)

// SetConnectionLostCallback sets the function to call when there is a disconnection
// Mind that in case of disconnection, the stomp wrapper will keep trying to reconnect indefinitely,
// so it is up to the client to abort, or sleep between retries...
func (c *Connection) SetConnectionLostCallback(cb ConnectionLostCallback) {
	c.connectionLostCallback = cb
}

// reconnect triggers a new connection
func (c *Connection) Reconnect() error {
	if c.netConnection != nil {
		c.netConnection.Close()
	}

	var err error
	if c.netConnection, err = net.Dial("tcp", net.JoinHostPort(c.host, c.port)); err != nil {
		return err
	}

	headers := stompngo.Headers{
		"accept-version", "1.1", // https://github.com/gmallard/stompngo/issues/13
		"host", c.host,
	}
	if c.login != "" {
		headers = headers.Add("login", c.login)
	}
	if c.passcode != "" {
		headers = headers.Add("passcode", c.passcode)
	}

	if c.stompConnection, err = stompngo.Connect(c.netConnection, headers); err != nil {
		if err == stompngo.ECONERR {
			return errors.New(c.stompConnection.ConnectResponse.BodyString())
		}
		return err
	}
	return nil
}

// Dial connects to a Stomp broker
func Dial(address, login, passcode string) (c *Connection, err error) {
	aux := &Connection{
		login:    login,
		passcode: passcode,
	}
	if aux.host, aux.port, err = net.SplitHostPort(address); err != nil {
		return
	}
	if err = aux.Reconnect(); err != nil {
		return
	}
	c = aux
	return
}

// Close closes the connections and frees the resources
func (c *Connection) Close() error {
	c.stompConnection.Disconnect(stompngo.Headers{})
	return c.netConnection.Close()
}

// Reconnect loop
func (c *Connection) handleReconnectOnSend(err error) error {
	if err == nil {
		// Success
		return nil
	} else if c.connectionLostCallback == nil {
		// No callback, so do not even bother
		return err
	} else if err == stompngo.ECONBAD {
		// Probably a previous reconnect failed
		c.connectionLostCallback(c)
		return syscall.EAGAIN
	} else if netErr, ok := err.(*net.OpError); ok {
		// Network error, let's see if it is a disconnect one
		if sysErr, ok := netErr.Err.(*os.SyscallError); ok {
			if sysErr.Err == syscall.EPIPE {
				c.connectionLostCallback(c)
				return syscall.EAGAIN
			}
		}
	}
	// An error that is not recoverable
	return err

}

// Send a message to the broker
func (c *Connection) Send(destination, contentType, message string) (err error) {
	if destination == "" {
		return stompngo.EREQDSTSND
	}

	if contentType == "" {
		contentType = "text/plain"
	}
	contentLength := fmt.Sprint(len(message))
	headers := stompngo.Headers{
		"destination", destination,
		"content-type", contentType,
		"content-length", contentLength,
	}
	for {
		if err = c.handleReconnectOnSend(c.stompConnection.Send(headers, message)); err != syscall.EAGAIN {
			break
		}
	}
	return
}

// Subscribe to a remote topic or queue
func (c *Connection) Subscribe(destination, id string, ack AckMode) (<-chan stompngo.MessageData, error) {
	if id == "" {
		return nil, stompngo.EBADSID
	}
	if destination == "" {
		return nil, stompngo.EREQDSTSUB
	}

	headers := stompngo.Headers{
		"destination", destination,
		"id", id,
		"ack", string(ack),
	}
	in, err := c.stompConnection.Subscribe(headers)
	if err != nil {
		return nil, err
	}

	// Now, this is the trickier part
	// To handle disconnects, we need to shovel from one channel to the other, and
	// handle re-subscriptions if the connection is gone
	out := make(chan stompngo.MessageData, 100)
	go func() {
		for {
			frame, ok := <-in
			if !ok {
				// Channel closed, so done here
				close(out)
				return
			} else if frame.Error == nil {
				// No error, forward
				out <- frame
			} else if frame.Error != io.EOF || c.connectionLostCallback == nil {
				// An error we don't know how to deal with, forward and be done
				out <- frame
				close(out)
				return
			} else {
				err = frame.Error
				// Retry loop
				for err != nil {
					// Disconnected, notify the client
					c.connectionLostCallback(c)
					// If the client had reconnected, we need to resubscribe
					in, err = c.stompConnection.Subscribe(headers)
				}
				// If we are here, managed to reconnect and resubscribe!
			}
		}
	}()
	return out, nil
}

// Unsubscribe from an existing subscription
func (c *Connection) Unsubscribe(id string) (err error) {
	if id == "" {
		return stompngo.EBADSID
	}

	headers := stompngo.Headers{
		"id", id,
	}
	for {
		if err = c.handleReconnectOnSend(c.stompConnection.Unsubscribe(headers)); err != syscall.EAGAIN {
			break
		}
	}
	return
}

// Ack acknowledges a message
func (c *Connection) Ack(message stompngo.Message) (err error) {
	headers := stompngo.Headers{
		"message-id", message.Headers.Value("message-id"),
		"subscription", message.Headers.Value("subscription"),
	}
	for {
		if err = c.handleReconnectOnSend(c.stompConnection.Ack(headers)); err != syscall.EAGAIN {
			break
		}
	}
	return
}

// Nack tells the broker that the message has not been consumed
func (c *Connection) Nack(message stompngo.Message) (err error) {
	headers := stompngo.Headers{
		"message-id", message.Headers.Value("message-id"),
		"subscription", message.Headers.Value("subscription"),
	}
	for {
		if err = c.handleReconnectOnSend(c.stompConnection.Nack(headers)); err != syscall.EAGAIN {
			break
		}
	}
	return
}
