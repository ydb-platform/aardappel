package config

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"aardappel/internal/types"

	"github.com/stretchr/testify/require"
)

func TestLegacyConfigTransition(t *testing.T) {
	const legacyYAML = `src_connection_string: grpc://source:2136/source
dst_connection_string: grpc://destination:2136/destination
instance_id: replication
streams:
  - src_topic: orders/updates
    dst_table: orders
    consumer: replication-consumer
    problem_strategy: continue
    mon_tag: orders-monitor
src_client_balancer: true
src_oauth2_file: source-oauth.json
src_oauth2_endpoint: https://source.example/token
src_static_token: source-token
dst_client_balancer: true
dst_oauth2_file: destination-oauth.json
dst_oauth2_endpoint: https://destination.example/token
dst_static_token: destination-token
multiple_instances_mode: true
commit_offset_mode: true
max_concurrent_offset_commits: 42
state_table: replication/state
max_expected_heartbeat_interval: 30
log_level: debug
mon_server:
  listen: localhost:8080
cmd_queue:
  path: replication/commands
  consumer: command-consumer
key_filter:
  table_path: replication/keys
dead_letter_queue:
  path: replication/dead-letters
`
	expected := Config{
		Clusters: []Cluster{
			{
				ID: "source", ConnectionString: "grpc://source:2136/source", ClientBalancer: true,
				OAuthFile: "source-oauth.json", OAuthEndpoint: "https://source.example/token", StaticToken: "source-token",
			},
			{
				ID: "destination", ConnectionString: "grpc://destination:2136/destination", ClientBalancer: true,
				OAuthFile: "destination-oauth.json", OAuthEndpoint: "https://destination.example/token", StaticToken: "destination-token",
			},
		},
		WaitForFeed: true,
		LogLevel:    "debug",
		MonServer:   &MonServer{Listen: "localhost:8080"},
		Feeds: []Feed{{
			ID: "replication", InstanceId: "replication", Source: "source", Destination: "destination",
			CommitOffsetMode: true, MaxConcurrentOffsetCommits: 42,
			Streams: []Stream{{
				SrcTopic: "orders/updates", DstTable: "orders", Consumer: "replication-consumer",
				ProblemStrategy: types.ProblemStrategyContinue, MonTag: "orders-monitor",
			}},
			StateTable: "replication/state", MaxExpHbInterval: 30,
			CmdQueue:  &CmdQueue{Path: "replication/commands", Consumer: "command-consumer"},
			KeyFilter: &KeyFilter{Path: "replication/keys"},
			DLQueue:   &DLQueue{Path: "replication/dead-letters"},
		}},
	}

	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(legacyYAML), 0600))
	converted, err := InitConfig(context.Background(), path)
	require.NoError(t, err)
	require.Equal(t, expected, converted)
}
