// SPDX-FileCopyrightText: 2024 NOI Techpark <digital@noi.bz.it>
//
// SPDX-License-Identifier: MPL-2.0

package mq

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/noi-techpark/go-opendatahub-ingest/dto"
	amqp "github.com/rabbitmq/amqp091-go"
)

type R struct {
	Con *amqp.Connection
	Ch  *amqp.Channel
}

func (r *R) Close() {
	if r.Ch != nil && !r.Ch.IsClosed() {
		_ = r.Ch.Close()
	}
	if r.Con != nil && !r.Con.IsClosed() {
		_ = r.Con.Close()
	}
}

// Overwrite the handler that is called when rabbitmq connection terminates
// Default handler logs and panics
func (r *R) OnClose(handler func(*amqp.Error)) {
	r.Con.NotifyClose(func() chan *amqp.Error {
		notifyClose := make(chan *amqp.Error)
		go func() {
			err := <-notifyClose
			handler(err)
		}()
		return notifyClose
	}())
}

func Connect(uri string, client string) (R, error) {
	r := R{}
	con, err := amqp.DialConfig(uri, amqp.Config{
		Properties: amqp.Table{"connection_name": client},
	})
	if err != nil {
		return r, err
	}

	ch, err := con.Channel()
	if err != nil {
		return r, err
	}

	prefetch := 3
	ch.Qos(prefetch, 0, true)

	r.Ch = ch
	r.Con = con

	r.OnClose(func(err *amqp.Error) {
		slog.Error("rabbitmq connection closed unexpectedly")
		panic(err)
	})

	return r, nil
}

func (r *R) Publish(msg dto.RawAny, exchange string) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("error marshalling message to json: %w", err)
	}

	err = r.Ch.Publish(
		exchange,     // exchange
		msg.Provider, // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        payload,
			Headers:     amqp.Table{"provider": msg.Provider},
		})

	if err != nil {
		return fmt.Errorf("error sending amqp msg: %w", err)
	}
	return nil
}

func (r *R) Consume(exchange string, queue string, routingkey string) (<-chan amqp.Delivery, error) {
	q, err := r.Ch.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue %s: %w", queue, err)
	}
	err = r.Ch.QueueBind(q.Name, routingkey, exchange, false, nil)
	if err != nil {
		return nil, fmt.Errorf("failed binding queue %s: %w", queue, err)
	}
	mq, err := r.Ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("failed registering consumer for queue %s: %w", queue, err)
	}
	return mq, nil
}
