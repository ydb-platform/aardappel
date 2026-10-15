package config

import (
	"aardappel/internal/types"
	"aardappel/internal/util/xlog"
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type Stream struct {
	SrcTopic        string `yaml:"src_topic"`
	DstTable        string `yaml:"dst_table"`
	Consumer        string `yaml:"consumer"`
	ProblemStrategy string `yaml:"problem_strategy"`
	MonTag          string `yaml:"mon_tag"`
}

type StreamSet struct {
	ID      string   `yaml:"id"`
	Streams []Stream `yaml:"streams"`
}

type StreamReference struct {
	Ref      string `yaml:"ref"`
	Consumer string `yaml:"consumer"`
}

type MonServer struct {
	Listen string `yaml:"listen"`
}

type CmdQueue struct {
	Path     string `yaml:"path"`
	Consumer string `yaml:"consumer"`
}

type DLQueue struct {
	Path string `yaml:"path"`
}

type KeyFilter struct {
	Path string `yaml:"table_path"`
}

type Config struct {
	Clusters    []Cluster   `yaml:"clusters"`
	StreamSets  []StreamSet `yaml:"stream_sets,omitempty"`
	Feeds       []Feed      `yaml:"feeds"`
	WaitForFeed bool        `yaml:"wait_for_feed"`
	LogLevel    string      `yaml:"log_level"`
	MonServer   *MonServer  `yaml:"mon_server"`
}

type Cluster struct {
	ID               string            `yaml:"id"`
	ConnectionString string            `yaml:"connection_string"`
	ClientBalancer   bool              `yaml:"client_balancer"`
	OAuthFile        string            `yaml:"oauth2_file"`
	OAuthEndpoint    string            `yaml:"oauth2_endpoint"`
	StaticToken      string            `yaml:"static_token"`
}

type Feed struct {
	InstanceId       string            `yaml:"instance_id,omitempty"`
	Source           string            `yaml:"source"`
	Destination      string            `yaml:"destination"`
	ID               string            `yaml:"id"`
	CommitOffsetMode bool              `yaml:"commit_offset_mode"`
	Streams          []Stream          `yaml:"streams"`
	StreamReferences []StreamReference `yaml:"-"`
	StateTable       string            `yaml:"state_table"`
	MaxExpHbInterval uint32            `yaml:"max_expected_heartbeat_interval"`
	CmdQueue         *CmdQueue         `yaml:"cmd_queue"`
	KeyFilter        *KeyFilter        `yaml:"key_filter"`
	DLQueue          *DLQueue          `yaml:"dead_letter_queue"`

	MaxConcurrentOffsetCommits int `yaml:"max_concurrent_offset_commits"`
}

func (config *Config) ClusterByID(id string) (Cluster, error) {
	for _, cluster := range config.Clusters {
		if cluster.ID == id {
			return cluster, nil
		}
	}
	return Cluster{}, fmt.Errorf("unknown cluster id %q", id)
}

func verifyStreamProblemStrategy(configStrategy *string) error {
	if *configStrategy == "" {
		*configStrategy = types.ProblemStrategyStop
		return nil
	}
	streamProblemStrategies := []string{types.ProblemStrategyStop, types.ProblemStrategyContinue}
	for _, strategy := range streamProblemStrategies {
		if strings.EqualFold(strategy, *configStrategy) {
			*configStrategy = strategy
			return nil
		}
	}
	return errors.New("unknown stream problem strategy '" + *configStrategy + "'")
}

func (config *Config) ToString() (string, error) {
	data, err := yaml.Marshal(config)
	if err != nil {
		return "", err
	}
	return "version: 2\n" + string(data), nil
}

func (config *Config) verify() error {
	if len(config.Feeds) == 0 {
		return errors.New("at least one feed is required")
	}
	if len(config.Clusters) == 0 {
		return errors.New("at least one cluster is required")
	}
	clusterIDs := make(map[string]struct{})
	for i, cluster := range config.Clusters {
		clusterID := cluster.ID
		if strings.TrimSpace(clusterID) == "" {
			return fmt.Errorf("clusters[%d]: id is required", i)
		}
		if _, exists := clusterIDs[clusterID]; exists {
			return fmt.Errorf("clusters[%d]: duplicate cluster id %q", i, clusterID)
		}
		clusterIDs[clusterID] = struct{}{}
		if strings.TrimSpace(cluster.ConnectionString) == "" {
			return fmt.Errorf("clusters[%d]: connection_string is required", i)
		}
	}
	streamSets := make(map[string][]Stream)
	for i := range config.StreamSets {
		set := &config.StreamSets[i]
		if strings.TrimSpace(set.ID) == "" {
			return fmt.Errorf("stream_sets[%d]: id is required", i)
		}
		if _, exists := streamSets[set.ID]; exists {
			return fmt.Errorf("stream_sets[%d]: duplicate stream set id %q", i, set.ID)
		}
		if len(set.Streams) == 0 {
			return fmt.Errorf("stream_sets[%d]: at least one stream is required", i)
		}
		for j := range set.Streams {
			if set.Streams[j].Consumer != "" {
				return fmt.Errorf("stream_sets[%d].streams[%d]: consumer belongs to the referencing feed", i, j)
			}
			if err := verifyStreamProblemStrategy(&set.Streams[j].ProblemStrategy); err != nil {
				return fmt.Errorf("stream_sets[%d].streams[%d]: %w", i, j, err)
			}
		}
		streamSets[set.ID] = set.Streams
	}
	feedIDs := make(map[string]struct{})
	for i := range config.Feeds {
		feed := &config.Feeds[i]
		if strings.TrimSpace(feed.ID) == "" {
			return fmt.Errorf("feeds[%d]: id is required", i)
		}
		if _, exists := feedIDs[feed.ID]; exists {
			return fmt.Errorf("feeds[%d]: duplicate feed id %q", i, feed.ID)
		}
		feedIDs[feed.ID] = struct{}{}
		if feed.InstanceId == "" {
			feed.InstanceId = feed.ID
		}
		refs := feed.StreamReferences
		if len(refs) > 0 {
			feed.Streams = nil
			for j, ref := range refs {
				streams, exists := streamSets[ref.Ref]
				if !exists {
					return fmt.Errorf("feeds[%d].streams[%d]: unknown stream set id %q", i, j, ref.Ref)
				}
				for _, stream := range streams {
					stream.Consumer = ref.Consumer
					feed.Streams = append(feed.Streams, stream)
				}
			}
		}
		if err := feed.verify(clusterIDs); err != nil {
			return fmt.Errorf("feeds[%d]: %w", i, err)
		}
	}
	return nil
}

func (config *Config) warnSharedInstances(ctx context.Context) {
	type instanceDestination struct {
		instanceID  string
		destination string
	}
	seen := make(map[instanceDestination]string)
	for _, feed := range config.Feeds {
		key := instanceDestination{feed.InstanceId, feed.Destination}
		if firstFeed, exists := seen[key]; exists {
			xlog.Warn(ctx, "Different feeds use the same instance_id and destination; check that this is intended",
				zap.String("feed_id", feed.ID),
				zap.String("other_feed_id", firstFeed),
				zap.String("instance_id", feed.InstanceId),
				zap.String("destination", feed.Destination))
		} else {
			seen[key] = feed.ID
		}
	}
}

func (feed *Feed) UnmarshalYAML(value *yaml.Node) error {
	type plain Feed
	if value.Kind != yaml.MappingNode {
		return errors.New("feed must be a mapping")
	}
	defaults := plain{MaxConcurrentOffsetCommits: 1000}
	var wire struct{ Streams yaml.Node }
	rest := *value
	rest.Content = nil
	for i := 0; i < len(value.Content); i += 2 {
		if value.Content[i].Value == "streams" {
			if wire.Streams.Kind != 0 {
				return errors.New("duplicate streams field")
			}
			wire.Streams = *value.Content[i+1]
			for wire.Streams.Kind == yaml.AliasNode {
				wire.Streams = *wire.Streams.Alias
			}
		} else {
			rest.Content = append(rest.Content, value.Content[i], value.Content[i+1])
		}
	}
	if err := rest.Decode(&defaults); err != nil {
		return err
	}
	switch wire.Streams.Kind {
	case yaml.SequenceNode:
		hasReferences := slices.ContainsFunc(wire.Streams.Content, isStreamReference)
		if hasReferences {
			for i, entry := range wire.Streams.Content {
				ref, err := decodeStreamReference(entry)
				if err != nil {
					return fmt.Errorf("streams[%d]: %w", i, err)
				}
				defaults.StreamReferences = append(defaults.StreamReferences, ref)
			}
		} else if err := wire.Streams.Decode(&defaults.Streams); err != nil {
			return err
		}
	case 0, yaml.ScalarNode:
		if wire.Streams.Kind != 0 && wire.Streams.Tag != "!!null" {
			return errors.New("streams must be a list of inline streams or references")
		}
	default:
		return errors.New("streams must be a list of inline streams or references")
	}
	*feed = Feed(defaults)
	return nil
}

func isStreamReference(node *yaml.Node) bool {
	for node.Kind == yaml.AliasNode {
		node = node.Alias
	}
	if node.Kind != yaml.MappingNode {
		return false
	}
	for i := 0; i < len(node.Content); i += 2 {
		if node.Content[i].Value == "ref" {
			return true
		}
	}
	return false
}

func decodeStreamReference(node *yaml.Node) (StreamReference, error) {
	for node.Kind == yaml.AliasNode {
		node = node.Alias
	}
	var ref StreamReference
	if node.Kind != yaml.MappingNode {
		return ref, errors.New("streams reference must be a mapping")
	}
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i].Value
		if key != "ref" && key != "consumer" {
			return ref, fmt.Errorf("streams reference: unknown field %q", key)
		}
	}
	if err := node.Decode(&ref); err != nil {
		return ref, err
	}
	if strings.TrimSpace(ref.Ref) == "" {
		return ref, errors.New("streams reference: ref is required")
	}
	if strings.TrimSpace(ref.Consumer) == "" {
		return ref, errors.New("streams reference: consumer is required")
	}
	return ref, nil
}

func (feed Feed) MarshalYAML() (any, error) {
	type plain Feed
	var node yaml.Node
	if err := node.Encode(plain(feed)); err != nil {
		return nil, err
	}
	if len(feed.StreamReferences) > 0 {
		for i := 0; i < len(node.Content); i += 2 {
			if node.Content[i].Value == "streams" {
				if err := node.Content[i+1].Encode(feed.StreamReferences); err != nil {
					return nil, err
				}
			}
		}
	}
	return &node, nil
}

func (feed *Feed) verify(knownIDs map[string]struct{}) error {
	if _, known := knownIDs[feed.Source]; !known {
		return fmt.Errorf("unknown source cluster id %q", feed.Source)
	}
	if _, known := knownIDs[feed.Destination]; !known {
		return fmt.Errorf("unknown destination cluster id %q", feed.Destination)
	}
	if feed.CommitOffsetMode && feed.MaxConcurrentOffsetCommits <= 0 {
		return errors.New("max_concurrent_offset_commits must be greater than zero when commit_offset_mode is enabled")
	}
	for i := range feed.Streams {
		err := verifyStreamProblemStrategy(&feed.Streams[i].ProblemStrategy)
		if err != nil {
			return err
		}
	}
	return nil
}

func InitConfig(ctx context.Context, confPath string) (Config, error) {
	if len(confPath) == 0 {
		return Config{}, errors.New("configuration file path is empty")
	}
	confTxt, err := os.ReadFile(confPath)
	if err != nil {
		xlog.Error(ctx, "Unable to read configuration file",
			zap.String("config_path", confPath),
			zap.Error(err))
		return Config{}, fmt.Errorf("unable to read configuration file: %w", err)
	}
	// Only a version marker on the first line selects the new format.
	firstLine, _, _ := strings.Cut(string(confTxt), "\n")
	var header struct {
		Version int `yaml:"version"`
	}
	config := Config{WaitForFeed: true}
	if yaml.Unmarshal([]byte(firstLine), &header) == nil && header.Version == 2 {
		err = yaml.Unmarshal(confTxt, &config)
	} else {
		xlog.Warn(ctx, "Legacy configuration format is in use; migrate to the new format with version: 2 on the first line",
			zap.String("config_path", confPath))
		legacy := legacyConfig{MaxConcurrentOffsetCommits: 1000}
		err = yaml.Unmarshal(confTxt, &legacy)
		config = legacy.toConfig()
	}
	if err != nil {
		xlog.Error(ctx, "Unable to parse configuration file",
			zap.String("config_path", confPath),
			zap.Error(err))
		return Config{}, fmt.Errorf("unable to parse configuration file: %w", err)
	}
	err = config.verify()
	if err != nil {
		xlog.Error(ctx, "Unable to verify configuration file", zap.Error(err))
		return Config{}, fmt.Errorf("unable to verify configuration file: %w", err)
	}
	config.warnSharedInstances(ctx)
	return config, nil
}
