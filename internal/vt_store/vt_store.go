package vt_store

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"os"
)

type Store interface {
	Load() string
	Store(context.Context, string) error
	Close() error
}

type FileStore struct {
	file *os.File
	vt   []byte
}

type YdbStore struct {
	client    table.Client
	tableName string
	vt        []byte
}

func NewFileStore(path string) (*FileStore, error) {
	fh, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	var fs FileStore
	if err != nil {
		return &fs, err
	}
	fs.file = fh

	_, err = fs.file.Read(fs.vt)
	if err != nil {
		return &fs, err
	}

	return &fs, nil
}

func (fs *FileStore) Close() error {
	return fs.file.Close()
}

func (fs *FileStore) Load() []byte {
	return fs.vt
}

func (fs *FileStore) Store(ctx context.Context, vt []byte) error {
	_ = ctx
	_, err := fs.file.Seek(0, 0)
	if err != nil {
		return err
	}

	var sz int
	sz, err = fs.file.Write(vt)

	if err != nil {
		return err
	}

	defer func() {
		if err == nil {
			err = fs.file.Sync()
			fs.vt = vt
		}
	}()

	if sz < len(fs.vt) {
		return fs.file.Truncate(int64(sz))
	}

	return nil
}
