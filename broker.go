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
	"github.com/gmallard/stompngo"
	"net"
	"os"
	"syscall"
)

type (
	// ConnectionParameters store the configuration for the Stomp connection
	ConnectionParameters struct {
		Address         string
		Login, Passcode string
		ConnectionLost  ConnectionLostCallback
	}

	// Connection wraps the underlying network and stomp connection, so reconnects can be done
	// transparently
	Broker struct {
		params          ConnectionParameters
		netConnection   net.Conn
		stompConnection *stompngo.Connection
		host            string
	}

	// ConnectionLostCallback is the callback type for lost connections
	ConnectionLostCallback func(c *Broker)
)

// RemoteAddr returns the broker network address
func (c *Broker) RemoteAddr() net.Addr {
	return c.netConnection.RemoteAddr()
}

// reconnect triggers a new connection
func (c *Broker) Reconnect() error {
	if c.netConnection != nil {
		c.netConnection.Close()
	}

	var err error
	if c.netConnection, err = net.Dial("tcp", c.params.Address); err != nil {
		return err
	}

	headers := stompngo.Headers{
		"accept-version", "1.1", // https://github.com/gmallard/stompngo/issues/13
		"host", c.host,
	}
	if c.params.Login != "" {
		headers = headers.Add("login", c.params.Login)
	}
	if c.params.Passcode != "" {
		headers = headers.Add("passcode", c.params.Passcode)
	}

	if c.stompConnection, err = stompngo.Connect(c.netConnection, headers); err != nil {
		if err == stompngo.ECONERR {
			return errors.New(c.stompConnection.ConnectResponse.BodyString())
		}
		return err
	}
	return nil
}

// dial connects to a Stomp broker. Internal user.
func dial(params ConnectionParameters) (c *Broker, err error) {
	aux := &Broker{
		params: params,
	}
	if aux.host, _, err = net.SplitHostPort(params.Address); err != nil {
		return
	}
	if err = aux.Reconnect(); err != nil {
		return
	}
	c = aux
	return
}

// close closes the connections and frees the resources
func (c *Broker) close() error {
	c.stompConnection.Disconnect(stompngo.Headers{})
	return c.netConnection.Close()
}

// Reconnect loop
func (c *Broker) handleReconnectOnSend(err error) error {
	if err == nil {
		// Success
		return nil
	} else if c.params.ConnectionLost == nil {
		// No callback, so do not even bother
		return err
	} else if err == stompngo.ECONBAD {
		// Probably a previous reconnect failed
		c.params.ConnectionLost(c)
		return syscall.EAGAIN
	} else if netErr, ok := err.(*net.OpError); ok {
		// Network error, let's see if it is a disconnect one
		if sysErr, ok := netErr.Err.(*os.SyscallError); ok {
			if sysErr.Err == syscall.EPIPE {
				c.params.ConnectionLost(c)
				return syscall.EAGAIN
			}
		}
	}
	// An error that is not recoverable
	return err
}
