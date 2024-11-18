// SPDX-FileCopyrightText: 2024 NOI Techpark <digital@noi.bz.it>
//
// SPDX-License-Identifier: AGPL-3.0-or-later

package tr

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"opendatahub.com/ingest/dto"
	"opendatahub.com/ingest/mq"
	"opendatahub.com/ingest/ms"
)

type incoming struct {
	Id         string
	Db         string
	Collection string
}

type Env struct {
	ms.Env
	RABBITMQ_URI      string
	RABBITMQ_EXCHANGE string `default:"routed"`
	RABBITMQ_CLIENT   string
	RABBITMQ_QUEUE    string
	RABBITMQ_KEY      string
	MONGO_URI         string
}

func getMongo[Raw any, Meta any](uri string, m incoming) (*dto.Raw[Raw, Meta], error) {
	c, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	defer c.Disconnect(context.TODO())
	id, err := primitive.ObjectIDFromHex(m.Id)
	if err != nil {
		return nil, err
	}
	r := &dto.Raw[Raw, Meta]{}
	if err := c.Database(m.Db).Collection(m.Collection).FindOne(context.TODO(), bson.M{"_id": id}).Decode(r); err != nil {
		return nil, err
	}
	return r, nil
}

func getRawFrame[Raw any, Meta any](uri string, m incoming) (*dto.Raw[Raw, Meta], error) {
	raw, err := getMongo[Raw, Meta](uri, m)
	if err != nil {
		return nil, fmt.Errorf("error getting raw from mongo: %w", err)
	}

	slog.Debug("Dumping raw data", "dto", raw)
	return raw, nil
}

func msgReject(d *amqp091.Delivery) {
	if err := d.Reject(false); err != nil {
		slog.Error("error rejecting already errored message", "err", err)
		panic(err)
	}
}

func ListenFromEnv[Raw any, Meta any](e Env, handler func(*dto.Raw[Raw, Meta]) error) error {
	return Listen(e.RABBITMQ_URI, e.RABBITMQ_CLIENT, e.RABBITMQ_EXCHANGE, e.RABBITMQ_QUEUE, e.RABBITMQ_KEY, e.MONGO_URI, handler)
}

// Default Listen function for typical transformer with one queue
func Listen[Raw any, Meta any](uri string, client string, exchange string, queue string, key string, mongoUri string, handler func(*dto.Raw[Raw, Meta]) error) error {
	r, err := mq.Connect(uri, client)
	if err != nil {
		return err
	}
	mq, err := r.Consume(exchange, queue, key)
	if err != nil {
		return err
	}
	HandleQueue(mq, mongoUri, handler)
	return nil
}

func HandleDelivery[Raw any, Meta any](delivery amqp091.Delivery, mongoUri string, handler func(*dto.Raw[Raw, Meta]) error) error {
	msgBody := incoming{}
	if err := json.Unmarshal(delivery.Body, &msgBody); err != nil {
		msgReject(&delivery)
		return fmt.Errorf("Error unmarshalling mq message: %w", err)
	}

	rawFrame, err := getRawFrame[Raw, Meta](mongoUri, msgBody)
	if err != nil {
		msgReject(&delivery)
		return fmt.Errorf("Cannot get mongo raw data: %w", err)
	}

	err = handler(rawFrame)
	if err != nil {
		msgReject(&delivery)
		return fmt.Errorf("Error during handling of message: %w", err)
	}

	if err := delivery.Ack(false); err != nil {
		slog.Error("Could not ack elaborated message. Aborting", "err", err)
		panic(err)
	}

	return nil
}

func HandleQueue[Raw any, Meta any](mq <-chan amqp091.Delivery, mongoUri string, handler func(*dto.Raw[Raw, Meta]) error) {
	for msg := range mq {
		slog.Debug("Received a message", "body", msg.Body)

		if err := HandleDelivery(msg, mongoUri, handler); err != nil {
			slog.Error("Message handling failed", "err", err)
		}
	}
}
