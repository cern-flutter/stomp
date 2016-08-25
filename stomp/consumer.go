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
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/satori/go.uuid"
	"github.com/spf13/cobra"
	"gitlab.cern.ch/flutter/stomp"
)

var ConsumerCmd = &cobra.Command{
	Use: "consumer <destination>",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			fmt.Println("Missing destination")
			return
		}

		consumer, err := stomp.NewConsumer(params)
		if err != nil {
			log.Fatal(err)
		}
		defer consumer.Close()

		messages, errors, err := consumer.Subscribe(args[0], uuid.NewV4().String(), stomp.AckAuto)
		if err != nil {
			log.Fatal(err)
		}

		log.Info("Subcribed to ", args[0])
		for {
			select {
			case msg := <-messages:
				log.Print(msg.Headers)
				log.Print(string(msg.Body))
				log.Print("")
			case err = <-errors:
				log.Error(err)
			}
		}
	},
}

func init() {
	RootCmd.AddCommand(ConsumerCmd)
}
