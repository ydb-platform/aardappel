package pmon_test

import (
	"aardappel/internal/config"
	"aardappel/internal/pmon"
	"bytes"
	"context"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"
)

func TestMonCount(t *testing.T) {
	ctx := context.Background()
	p := pmon.NewPromMon(ctx, &config.MonServer{"", "", ":8080"})
	defer p.Stop()
	p.ModificationCount(123)

	repeat := 10
	for {
		res, err := http.Get("http://127.0.0.1:8080/metrics")
		if err != nil {
			if repeat > 0 {
				repeat--
				time.Sleep(time.Duration(1) * time.Second)
				continue
			}
		}
		assert.NoError(t, err)

		resBody, err := io.ReadAll(res.Body)
		assert.NoError(t, err)

		pattern := []byte("modifications_count")
		val := 0
		for _, line := range bytes.Split(resBody, []byte("\n")) {
			if len(line) == 0 {
				continue
			}
			if line[0] == byte('#') {
				continue
			}

			i := bytes.Index(line, pattern)
			if i < 0 {
				continue
			}
			i += len(pattern)
			val, _ = strconv.Atoi(string(line[i+1:]))
			break
		}

		assert.Equal(t, val, 123)
		break
	}

}
