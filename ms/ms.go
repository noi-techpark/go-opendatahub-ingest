// SPDX-FileCopyrightText: 2024 NOI Techpark <digital@noi.bz.it>
//
// SPDX-License-Identifier: MPL-2.0

package ms

import (
	"log/slog"
	"os"

	"github.com/kelseyhightower/envconfig"
)

func FailOnError(err error, msg string) {
	if err != nil {
		slog.Error(msg, "err", err)
		panic(err)
	}
}

func InitLog(lv string) {
	level := &slog.LevelVar{}
	level.UnmarshalText([]byte(lv))
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})))
}

// Default configuration parameters for logging and provider that all data collectors/transformers need
// Embed this into your env struct
type BaseEnv struct {
	PROVIDER string
	LOGLEVEL string `default:"INFO"`
}

func (b *BaseEnv) LogLevel() string {
	return b.LOGLEVEL
}

type Env interface {
	BaseEnv
	LogLevel() string
}

// Convenience one shot setup method
// Create config from env and initialize logging system
func Init[E Env](e *E) {
	envconfig.MustProcess("", e)
	InitLog((*e).LogLevel())
}
