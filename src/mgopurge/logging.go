package main

import (
	"fmt"
	"os"
	"time"

	"github.com/juju/loggo"
)

var logger = loggo.GetLogger("mgopurge")

func setupLogging() error {
	writer := loggo.NewSimpleWriter(os.Stderr, new(logFormatter))
	loggo.ReplaceDefaultWriter(writer)
	return loggo.ConfigureLoggers("<root>=TRACE")
}

type logFormatter struct{}

func (f *logFormatter) Format(_ loggo.Level, _, _ string, _ int, timestamp time.Time, message string) string {
	ts := timestamp.In(time.UTC).Format("2006-01-02 15:04:05")
	return fmt.Sprintf("%s %s", ts, message)

}
