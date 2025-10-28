package types

import (
	"aardappel/internal/util/xlog"
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"
)

type Kind string

const (
	Graceful Kind = "graceful"
	Fatal    Kind = "fatal"
)

type Error struct {
	kind    Kind
	message string
	err     error
}

func (e *Error) Error() string {
	if e.err != nil {
		return fmt.Sprintf("[%s] %s: %v", e.kind, e.message, e.err)
	}
	return fmt.Sprintf("[%s] %s", e.kind, e.message)
}

func (e *Error) Unwrap() error { return e.err }
func (e *Error) Kind() Kind    { return e.kind }

func NewError(kind Kind, msg string) *Error {
	return &Error{kind: kind, message: msg}
}

func WrapError(kind Kind, err error, msg string) *Error {
	return &Error{kind: kind, message: msg, err: err}
}

func NewGraceful(msg string) *Error { return NewError(Graceful, msg) }
func NewFatal(msg string) *Error    { return NewError(Fatal, msg) }

func ReturnError(ctx context.Context, err error, msg string, args ...zap.Field) error {
	if err == nil {
		return nil
	}
	var appErr *Error
	if errors.As(err, &appErr) {
		switch appErr.Kind() {
		case Graceful:
			return NewGraceful(fmt.Sprintf("%s: %s", msg, err.Error()))
		case Fatal:
			xlog.Fatal(ctx, fmt.Sprintf("Fatal error: %s", msg), append(args, zap.Error(err))...)
		}
	} else {
		return err
	}
	return nil
}
