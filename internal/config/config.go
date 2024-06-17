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

type Config struct {
	SrcConnectionString string   `yaml:"src_connection_string"`
	DstConnectionString string   `yaml:"dst_connection_string"`
	Streams             []Stream `yaml:"streams"`
	StateTable          string   `yaml:"state_table"`
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
