// SPDX-FileCopyrightText: 2024 NOI Techpark <digital@noi.bz.it>
//
// SPDX-License-Identifier: MPL-2.0

package ms

import (
	"log/slog"
	"os"
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
type Env struct {
	LOG_LEVEL string `default:"INFO"`
}
