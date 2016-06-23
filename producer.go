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
	"fmt"
	"github.com/gmallard/stompngo"
	"syscall"
)

type (
	// Producer models a message producer. Only needs a single connection.;
	Producer struct {
		broker *Broker
	}

	SendParams struct {
		Persist     bool
		ContentType string
	}
)

// NewProducer instantiates a new producer and initiates the remote connection
func NewProducer(params ConnectionParameters) (*Producer, error) {
	var err error
	p := &Producer{}
	if p.broker, err = dial(params); err != nil {
		return nil, err
	}
	return p, nil
}

// Close finishes the connection and frees resources
func (p *Producer) Close() error {
	return p.broker.close()
}

// Send a message to the broker
func (p *Producer) Send(destination, message string, params SendParams) (err error) {
	if destination == "" {
		return stompngo.EREQDSTSND
	}

	if params.ContentType == "" {
		params.ContentType = "text/plain"
	}
	contentLength := fmt.Sprint(len(message))
	headers := stompngo.Headers{
		"destination", destination,
		"content-type", params.ContentType,
		"content-length", contentLength,
		"persistent", fmt.Sprint(params.Persist),
	}
	for {
		if err = p.broker.handleReconnectOnSend(p.broker.stompConnection.Send(headers, message)); err != syscall.EAGAIN {
			break
		}
	}
	return
}
