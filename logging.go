// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/juju/loggo"
)

var logger = loggo.GetLogger("mgopurge")

const defaultLogConfig = "<root>=DEBUG"

var loggingConfig = defaultLogConfig

func setupLogging() error {
	writer := loggo.NewSimpleWriter(os.Stderr, logFormatter)
	loggo.ReplaceDefaultWriter(writer)
	return loggo.ConfigureLoggers(loggingConfig)
}

func logFormatter(entry loggo.Entry) string {
	ts := entry.Timestamp.In(time.UTC).Format("2006-01-02 15:04:05")
	return fmt.Sprintf("%s %s %s", ts, entry.Level.Short(), entry.Message)

}
