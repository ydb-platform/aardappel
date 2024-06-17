package xlog

import (
	"context"
	"go.uber.org/zap"
	"sync/atomic"
)

var internalLogger atomic.Value

type loggerKey struct{}

func SetInternalLogger(logger *zap.Logger) {
	internalLogger.Store(logger.WithOptions(zap.AddCallerSkip(1)))
}

func from(ctx context.Context) *zap.Logger {
	if l, ok := ctx.Value(loggerKey{}).(*zap.Logger); ok {
		return l
	}
	if l, ok := internalLogger.Load().(*zap.Logger); ok {
		return l
	}
	// Fallback, so we don't need to manually init logger in every test.
	SetInternalLogger(zap.Must(zap.NewDevelopmentConfig().Build()))
	return from(ctx)
}

func Debug(ctx context.Context, msg string, fields ...zap.Field) {
	from(ctx).Debug(msg, fields...)
}

func Info(ctx context.Context, msg string, fields ...zap.Field) {
	from(ctx).Info(msg, fields...)
}

func Warn(ctx context.Context, msg string, fields ...zap.Field) {
	from(ctx).Warn(msg, fields...)
}

func Error(ctx context.Context, msg string, fields ...zap.Field) {
	from(ctx).Error(msg, fields...)
}

func Fatal(ctx context.Context, msg string, fields ...zap.Field) {
	from(ctx).Fatal(msg, fields...)
}
