package config

import (
	"aardappel/internal/types"
	"aardappel/internal/util/xlog"
	"context"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
	"os"
	"strings"
)

type Stream struct {
	SrcTopic        string `yaml:"src_topic"`
	DstTable        string `yaml:"dst_table"`
	Consumer        string `yaml:"consumer"`
	ProblemStrategy string `yaml:"problem_strategy"`
	MonTag          string `yaml:"mon_tag"`
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
	SrcConnectionString string     `yaml:"src_connection_string"`
	SrcClientBalancer   bool       `yaml:"src_client_balancer"`
	SrcOAuthFile        string     `yaml:"src_oauth2_file"`
	SrcStaticToken      string     `yaml:"src_static_token"`
	DstConnectionString string     `yaml:"dst_connection_string"`
	DstClientBalancer   bool       `yaml:"dst_client_balancer"`
	DstOAuthFile        string     `yaml:"dst_oauth2_file"`
	DstStaticToken      string     `yaml:"dst_static_token"`
	InstanceId          string     `yaml:"instance_id"`
	Streams             []Stream   `yaml:"streams"`
	StateTable          string     `yaml:"state_table"`
	MaxExpHbInterval    uint32     `yaml:"max_expected_heartbeat_interval"`
	LogLevel            string     `yaml:"log_level"`
	MonServer           *MonServer `yaml:"mon_server"`
	CmdQueue            *CmdQueue  `yaml:"cmd_queue"`
	KeyFilter           *KeyFilter `yaml:"key_filter"`
	DLQueue             *DLQueue   `yaml:"dead_letter_queue"`
}

func verifyStreamProblemStrategy(configStrategy *string) error {
	if *configStrategy == "" {
		*configStrategy = types.ProblemStrategyStop
		return nil
	}
	streamProblemStrategies := []string{types.ProblemStrategyStop, types.ProblemStrategyContinue}
	for _, strategy := range streamProblemStrategies {
		if strings.ToLower(strategy) == strings.ToLower(*configStrategy) {
			*configStrategy = strategy
			return nil
		}
	}
	return errors.New("unknown stream problem strategy '" + *configStrategy + "'")
}

func (config Config) ToString() (string, error) {
	data, err := yaml.Marshal(&config)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (config Config) verify() error {
	for _, stream := range config.Streams {
		err := verifyStreamProblemStrategy(&stream.ProblemStrategy)
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
	var config Config
	err = yaml.Unmarshal(confTxt, &config)
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
	return config, nil
}
