// SPDX-FileCopyrightText: 2024 NOI Techpark <digital@noi.bz.it>
//
// SPDX-License-Identifier: MPL-2.0

package dc

import (
	"github.com/noi-techpark/go-opendatahub-ingest/dto"
	"github.com/noi-techpark/go-opendatahub-ingest/mq"
	"github.com/noi-techpark/go-opendatahub-ingest/ms"
)

type Env struct {
	ms.Env
	PROVIDER    string
	MQ_URI      string
	MQ_EXCHANGE string `default:"ingress"`
	MQ_CLIENT   string
}

func PubFromEnv(e Env) (chan<- dto.RawAny, error) {
	return Pub(e.MQ_URI, e.MQ_EXCHANGE, e.MQ_CLIENT)
}

func Pub(uri string, client string, exchange string) (chan<- dto.RawAny, error) {
	c, err := mq.Connect(uri, client)
	if err != nil {
		return nil, err
	}

	rabbitChan := make(chan dto.RawAny)

	go func() {
		for msg := range rabbitChan {
			c.Publish(msg, exchange)
		}
	}()
	return rabbitChan, nil //UwU
}
