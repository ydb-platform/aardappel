package pmon_test

import (
	"aardappel/internal/config"
	"aardappel/internal/pmon"
	"bytes"
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"
)

func findField(resBody []byte, pattern []byte) int {
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
	return val
}

func TestMonCount(t *testing.T) {
	ctx := context.Background()
	p := pmon.NewPromMon(ctx, &config.MonServer{":8080"})
	defer p.Stop()
	p.ModificationCount(123)
	p.ModificationCountFromTopic(456, "tag_1")
	p.TopicWithoutHB(false, "tag_1")
	p.TopicWithoutHB(true, "tag_2")

	repeat := 10

	for {
		res, err := http.Get("http://127.0.0.1:8080/readyz")
		if err != nil {
			if repeat > 0 {
				repeat--
				time.Sleep(time.Duration(1) * time.Second)
				continue
			}
		}
		assert.Equal(t, res.StatusCode, 503)
		break
	}

	p.SetCompleted()

	res, _ := http.Get("http://127.0.0.1:8080/readyz")
	assert.Equal(t, res.StatusCode, 200)

	res, err := http.Get("http://127.0.0.1:8080/metrics")
	assert.NoError(t, err)

	resBody, err := io.ReadAll(res.Body)
	assert.NoError(t, err)
	fmt.Fprintf(os.Stderr, "%v\n", string(resBody))

	pattern_mc := []byte("modifications_count")
	val_mc := findField(resBody, pattern_mc)

	pattern_mc_per_table := []byte("modifications_count_per_table{stream_tag=\"tag_1\"}")
	val_mc_per_table := findField(resBody, pattern_mc_per_table)

	patterm_nh_per_table_1 := []byte("topic_without_hb{stream_tag=\"tag_1\"}")
	val_nh_per_table_1 := findField(resBody, patterm_nh_per_table_1)

	patterm_nh_per_table_2 := []byte("topic_without_hb{stream_tag=\"tag_2\"}")
	val_nh_per_table_2 := findField(resBody, patterm_nh_per_table_2)

	assert.Equal(t, val_mc, 123)
	assert.Equal(t, val_mc_per_table, 456)
	assert.Equal(t, val_nh_per_table_1, 0)
	assert.Equal(t, val_nh_per_table_2, 1)
}
