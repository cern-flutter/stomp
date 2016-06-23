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
	"bufio"
	"flag"
	"gitlab.cern.ch/flutter/stomp"
	"log"
	"os"
	"reflect"
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

	producer, err := stomp.NewProducer(stomp.ConnectionParameters{
		Address:  *addr,
		Login:    *login,
		Passcode: *passcode,
		ConnectionLost: func(c *stomp.Broker) {
			log.Print("Connection lost, reconnecting in 1 second...")
			time.Sleep(1 * time.Second)
			if err := c.Reconnect(); err != nil {
				log.Print("Failed to reconnect!")
			}
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	log.Print("Connected")
	log.Print("Write the message")
	reader := bufio.NewReader(os.Stdin)
	text, err := reader.ReadString('\n')
	if err != nil {
		log.Panic(err)
	}

	if err = producer.Send(flag.Arg(0), text, stomp.SendParams{Persist: true}); err != nil {
		log.Panic(reflect.TypeOf(err))
	}
	log.Print("Sent")
}
