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
	"crypto/x509"
	log "github.com/Sirupsen/logrus"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

func loadCert(pool *x509.CertPool, path string) {
	log.Debug("Loading ", path)
	fd, err := os.Open(path)
	if err != nil {
		log.Warn("Could not open: ", err)
		return
	}
	defer fd.Close()

	pem, err := ioutil.ReadAll(fd)
	if err != nil {
		log.Warn("Could not read: ", err)
		return
	}

	if !pool.AppendCertsFromPEM(pem) {
		log.Debug("Failed to load certificate")
	}
}

// Load Root CA from a directory
func loadRootCAs(dir, bundle string) (pool *x509.CertPool) {
	pool = x509.NewCertPool()
	if bundle != "" {
		loadCert(pool, bundle)
	}
	if dir != "" {

		log.Debug("Loading Root CAs from ", dir)

		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if info.IsDir() {
				return nil
			}
			if !strings.HasSuffix(info.Name(), ".pem") {
				return nil
			}

			loadCert(pool, path)

			return nil
		})
		if err != nil {
			log.Warn(err)
		}
	}
	return
}

func loadClientCert(ucert, ukey string) ([]tls.Certificate, error) {
	if ukey == "" && ucert == "" {
		return nil, nil
	} else if ukey == "" {
		ukey = ucert
	}

	log.Debug("User certificate ", ucert)
	log.Debug("User key ", ukey)
	cert, err := tls.LoadX509KeyPair(ucert, ukey)
	if err != nil {
		return nil, err
	}
	return []tls.Certificate{cert}, nil
}
