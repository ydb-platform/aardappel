package xlog

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"log"
)

func SetupLogging(verbose bool) *zap.Logger {
	var level zapcore.Level = zapcore.WarnLevel
	if verbose {
		level = zapcore.DebugLevel
	}
	cfg := zap.NewProductionConfig()
	cfg.Level.SetLevel(level)

	l, err := cfg.Build()
	if err != nil {
		log.Fatalln("Failed to create logger:", err)
	}
	return l
}
