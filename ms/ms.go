// SPDX-FileCopyrightText: 2024 NOI Techpark <digital@noi.bz.it>
//
// SPDX-License-Identifier: AGPL-3.0-or-later

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

type Env struct {
	PROVIDER string
	LOGLEVEL string `default:"INFO"`
}