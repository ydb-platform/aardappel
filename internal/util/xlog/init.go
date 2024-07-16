package xlog

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"log"
)

func SetupLogging(s string) *zap.Logger {
	if len(s) == 0 {
		s = "warning"
	}
	level, err := zapcore.ParseLevel(s)
	if err != nil {
		log.Fatalln("Unable to parse log level:", err)
	}
	cfg := zap.NewProductionConfig()
	cfg.Level.SetLevel(level)

	l, err := cfg.Build()
	if err != nil {
		log.Fatalln("Failed to create logger:", err)
	}
	return l
}
