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
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
	"gitlab.cern.ch/flutter/stomp"
	"os"
	"reflect"
)

var ProducerCmd = &cobra.Command{
	Use: "producer <destination>",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			fmt.Println("Missing destination")
			return
		}

		producer, err := stomp.NewProducer(params)
		if err != nil {
			log.Fatal(err)
		}
		defer producer.Close()

		log.Info("Connected")
		log.Info("Write the message")
		reader := bufio.NewReader(os.Stdin)
		text, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}

		if err = producer.Send(args[0], text, stomp.SendParams{Persistent: true}); err != nil {
			log.Panic(reflect.TypeOf(err))
		}
		log.Info("Sent")
	},
}

func init() {
	RootCmd.AddCommand(ProducerCmd)
}
