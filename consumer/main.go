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

package main

import (
	"flag"
	"github.com/satori/go.uuid"
	"gitlab.cern.ch/flutter/stomp"
	"log"
	"time"
)

func main() {
	addr := flag.String("connect", "localhost:61613", "Stomp host:port")
	login := flag.String("login", "fts", "Stomp login name")
	passcode := flag.String("passcode", "fts", "Stomp passcode")

	flag.Parse()
	if flag.NArg() < 1 {
		log.Fatal("Missing topic or queue to which subscribe")
	}

	conn, err := stomp.Dial(*addr, *login, *passcode)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	conn.SetConnectionLostCallback(func(c *stomp.Connection) {
		log.Print("Connection lost, reconnecting in 1 second...")
		time.Sleep(1 * time.Second)
		if err := c.Reconnect(); err != nil {
			log.Print("Failed to reconnect!")
		}
	})

	channel, err := conn.Subscribe(flag.Arg(0), uuid.NewV4().String(), stomp.AckIndividual)
	if err != nil {
		log.Panic(err)
	}

	log.Print("Subcribed to ", flag.Arg(0))
	for msg := range channel {
		if msg.Error != nil {
			log.Panic(msg.Error)
		}
		log.Print(msg.Message.Headers)
		log.Print(string(msg.Message.Body))
		log.Print("")
		conn.Ack(msg.Message)
	}
}
