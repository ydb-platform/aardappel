package config

import (
	"aardappel/internal/util/xlog"
	"context"
	"errors"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
	"os"
)

type Stream struct {
	SrcTopic string `yaml:"src_topic"`
	DstTable string `yaml:"dst_table"`
	Consumer string `yaml:"consumer"`
}

type MonServer struct {
	Listen string `yaml:"listen"`
}

type CmdQueue struct {
	Path     string `yaml:"path"`
	Consumer string `yaml:"consumer"`
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
}

func (config Config) ToString() (string, error) {
	data, err := yaml.Marshal(&config)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func InitConfig(ctx context.Context, confPath string) (Config, error) {
	if len(confPath) != 0 {
		confTxt, err := os.ReadFile(confPath)
		if err != nil {
			xlog.Error(ctx, "Unable to read configuration file",
				zap.String("config_path", confPath),
				zap.Error(err))
			return Config{}, err
		}
		var config Config
		err = yaml.Unmarshal(confTxt, &config)
		if err != nil {
			xlog.Error(ctx, "Unable to parse configuration file",
				zap.String("config_path", confPath),
				zap.Error(err))
			return Config{}, err
		}
		return config, nil
	}
	return Config{}, errors.New("configuration file path is empty")
}
