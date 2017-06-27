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
	"crypto/tls"
	"net"
)

// Connect to a plain socket
func (c *Broker) connect() (err error) {
	c.netConnection, err = net.Dial("tcp", c.params.Address)
	return
}

// Connect via TLS
func (c *Broker) connectTLS() error {
	rawConnection, err := net.Dial("tcp", c.params.Address)
	if err != nil {
		return err
	}

	config := &tls.Config{
		Certificates:       c.params.clientCerts,
		RootCAs:            c.params.caCertPool,
		ServerName:         c.host,
		InsecureSkipVerify: c.params.Insecure,
	}

	tlsConnection := tls.Client(rawConnection, config)
	if err = tlsConnection.Handshake(); err != nil {
		return err
	}
	c.netConnection = tlsConnection
	return nil
}
