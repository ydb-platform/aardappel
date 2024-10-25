package xlog

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"log"
)

func SetupLogging(s string) *zap.Logger {
	if len(s) == 0 {
		s = "warn"
	}
	level, err := zapcore.ParseLevel(s)
	if err != nil {
		log.Fatalln("Unable to parse log level:", err)
	}
	cfg := zap.NewProductionConfig()
	cfg.Sampling = nil
	cfg.Level.SetLevel(level)
	cfg.EncoderConfig.MessageKey = "message"
	cfg.EncoderConfig.LevelKey = "level"

	l, err := cfg.Build()
	if err != nil {
		log.Fatalln("Failed to create logger:", err)
	}
	return l
}
