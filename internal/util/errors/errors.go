package errors

import "fmt"

type YDBConnectionError struct {
	Operation string
	Err       error
}

func (e *YDBConnectionError) Error() string {
	return fmt.Sprintf("YDB connection error during %s: %v", e.Operation, e.Err)
}

func (e *YDBConnectionError) Unwrap() error {
	return e.Err
}

func NewYDBConnectionError(operation string, err error) *YDBConnectionError {
	return &YDBConnectionError{
		Operation: operation,
		Err:       err,
	}
}

func IsYDBConnectionError(err error) bool {
	_, ok := err.(*YDBConnectionError)
	return ok
}
