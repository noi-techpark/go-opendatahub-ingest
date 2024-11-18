// SPDX-FileCopyrightText: 2024 NOI Techpark <digital@noi.bz.it>
//
// SPDX-License-Identifier: MPL-2.0

package dc

import (
	"opendatahub.com/ingest/dto"
	"opendatahub.com/ingest/mq"
	"opendatahub.com/ingest/ms"
)

type Env struct {
	ms.BaseEnv
	RABBITMQ_URI      string
	RABBITMQ_EXCHANGE string `default:"ingress"`
	RABBITMQ_CLIENT   string
}

func PubFromEnv(e Env) (chan<- dto.RawAny, error) {
	return Pub(e.RABBITMQ_URI, e.RABBITMQ_EXCHANGE, e.RABBITMQ_CLIENT)
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
