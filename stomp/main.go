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
	log "github.com/Sirupsen/logrus"
	"github.com/satori/go.uuid"
	"github.com/spf13/cobra"
	"gitlab.cern.ch/flutter/stomp"
	"time"
)

var (
	reconnectAttemps int
	params           stomp.ConnectionParameters
)

var RootCmd = &cobra.Command{
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Usage()
	},
}

func init() {
	params.ConnectionLost = func(c *stomp.Broker) {
		log.Warn("Connection lost, reconnecting: ", c.RemoteAddr())
		time.Sleep(1 * time.Second)
		if err := c.Reconnect(); err != nil {
			log.Fatal(err)
		}
		reconnectAttemps++
	}
	params.ClientId = uuid.NewV4().String()

	RootCmd.PersistentFlags().StringVar(&params.Address, "connect", "localhost:61613", "Stomp host:port")
	RootCmd.PersistentFlags().StringVar(&params.Login, "login", "fts", "Stomp login name")
	RootCmd.PersistentFlags().StringVar(&params.Passcode, "passcode", "fts", "Stomp passcode")
}

func main() {
	if err := RootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
