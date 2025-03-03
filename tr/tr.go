// SPDX-FileCopyrightText: 2024 NOI Techpark <digital@noi.bz.it>
//
// SPDX-License-Identifier: MPL-2.0

package tr

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/noi-techpark/go-opendatahub-ingest/dto"
	"github.com/noi-techpark/go-opendatahub-ingest/mq"
	"github.com/noi-techpark/go-opendatahub-ingest/ms"
	raw_data_bridge "github.com/noi-techpark/go-opendatahub-ingest/raw-data-bridge"
	"github.com/noi-techpark/go-opendatahub-ingest/urn"
	"github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type RdbEnv = raw_data_bridge.Env
type MsEnv = ms.Env
type Env struct {
	RdbEnv
	MsEnv
	MQ_URI      string
	MQ_EXCHANGE string `default:"routed"`
	MQ_CLIENT   string
	MQ_QUEUE    string
	MQ_KEY      string
	// Deprecated: will be removed in favor of REST based raw data bridge
	MONGO_URI string
}

func getMongo[Raw any](uri string, m dto.Notification) (*dto.Raw[Raw], error) {
	// TODO: cache connection somehow, don't open a new one for every single message
	c, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	defer c.Disconnect(context.TODO())
	id, err := primitive.ObjectIDFromHex(m.Id)
	if err != nil {
		return nil, err
	}
	r := &dto.Raw[Raw]{}
	if err := c.Database(m.Db).Collection(m.Collection).FindOne(context.TODO(), bson.M{"_id": id}).Decode(r); err != nil {
		return nil, err
	}
	return r, nil
}

func getRawFrame[Raw any](uri string, m dto.Notification) (*dto.Raw[Raw], error) {
	raw, err := getMongo[Raw](uri, m)
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

// Default configuration for transformers with one queue
func ListenFromEnv[Raw any](e Env, handler func(*dto.Raw[Raw]) error) error {
	return Listen(e.MQ_URI, e.MQ_CLIENT, e.MQ_EXCHANGE, e.MQ_QUEUE, e.MQ_KEY, e.MONGO_URI, handler)
}

// Configurable listen for transformers with multiple queues
func Listen[Raw any](uri string, client string, exchange string, queue string, key string, mongoUri string, handler func(*dto.Raw[Raw]) error) error {
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

func HandleDelivery[Raw any](delivery amqp091.Delivery, mongoUri string, handler func(*dto.Raw[Raw]) error) error {
	msgBody := dto.Notification{}
	if err := json.Unmarshal(delivery.Body, &msgBody); err != nil {
		msgReject(&delivery)
		return fmt.Errorf("error unmarshalling mq message: %w", err)
	}

	rawFrame, err := getRawFrame[Raw](mongoUri, msgBody)
	if err != nil {
		msgReject(&delivery)
		return fmt.Errorf("cannot get mongo raw data: %w", err)
	}

	err = handler(rawFrame)
	if err != nil {
		msgReject(&delivery)
		return fmt.Errorf("error during handling of message: %w", err)
	}

	if err := delivery.Ack(false); err != nil {
		slog.Error("Could not ack elaborated message. Aborting", "err", err)
		panic(err)
	}

	return nil
}

func HandleQueue[Raw any](mq <-chan amqp091.Delivery, mongoUri string, handler func(*dto.Raw[Raw]) error) error {
	for msg := range mq {
		slog.Debug("Received a message", "body", msg.Body)

		if err := HandleDelivery(msg, mongoUri, handler); err != nil {
			slog.Error("Message handling failed", "err", err)
		}
	}
	return fmt.Errorf("unexpected channel close")
}

type Handler[P any] func(context.Context, *dto.Raw[P]) error

type TrStack[P any] struct {
	data_bridge *raw_data_bridge.RDBridge
	config      *Env
}

func NewTrStack[P any](config *Env) *TrStack[P] {
	return &TrStack[P]{
		data_bridge: raw_data_bridge.NewRDBridge(config.RdbEnv),
		config:      config,
	}
}

func (tr *TrStack[P]) Start(ctx context.Context, handler Handler[P]) error {
	return tr.listen(ctx, handler)
}

func (tr *TrStack[P]) listen(ctx context.Context, handler Handler[P]) error {
	r, err := mq.Connect(tr.config.MQ_URI, tr.config.MQ_CLIENT)
	if err != nil {
		return err
	}
	mq, err := r.Consume(tr.config.MQ_EXCHANGE, tr.config.MQ_QUEUE, tr.config.MQ_KEY)
	if err != nil {
		return err
	}
	for msg := range mq {
		slog.Debug("Received a message", "body", msg.Body)

		if err := tr.handleDelivery(ctx, msg, handler); err != nil {
			slog.Error("Message handling failed", "err", err)
		}
	}
	return fmt.Errorf("unexpected channel close")
}

func (tr *TrStack[P]) handleDelivery(ctx context.Context, delivery amqp091.Delivery, handler Handler[P]) error {
	msgBody := dto.Notification{}
	if err := json.Unmarshal(delivery.Body, &msgBody); err != nil {
		msgReject(&delivery)
		return fmt.Errorf("error unmarshalling mq message: %w", err)
	}

	u, ok := urn.Parse(msgBody.Urn)
	if !ok {
		return fmt.Errorf("invalid urn format in mq message: %s", msgBody.Urn)
	}

	rawFrame, err := raw_data_bridge.Get[P](tr.data_bridge, u)
	if err != nil {
		msgReject(&delivery)
		return fmt.Errorf("cannot get raw data: %w", err)
	}

	err = handler(ctx, &rawFrame)
	if err != nil {
		msgReject(&delivery)
		return fmt.Errorf("error during handling of message: %w", err)
	}

	if err := delivery.Ack(false); err != nil {
		slog.Error("Could not ack elaborated message. Aborting", "err", err)
		panic(err)
	}

	return nil
}
