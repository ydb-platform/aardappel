package config

type legacyConfig struct {
	SrcConnectionString   string     `yaml:"src_connection_string"`
	SrcClientBalancer     bool       `yaml:"src_client_balancer"`
	SrcOAuthFile          string     `yaml:"src_oauth2_file"`
	SrcOAuthEndpoint      string     `yaml:"src_oauth2_endpoint"`
	SrcStaticToken        string     `yaml:"src_static_token"`
	DstConnectionString   string     `yaml:"dst_connection_string"`
	DstClientBalancer     bool       `yaml:"dst_client_balancer"`
	DstOAuthFile          string     `yaml:"dst_oauth2_file"`
	DstOAuthEndpoint      string     `yaml:"dst_oauth2_endpoint"`
	DstStaticToken        string     `yaml:"dst_static_token"`
	InstanceId            string     `yaml:"instance_id"`
	MultipleInstancesMode bool       `yaml:"multiple_instances_mode"`
	CommitOffsetMode      bool       `yaml:"commit_offset_mode"`
	Streams               []Stream   `yaml:"streams"`
	StateTable            string     `yaml:"state_table"`
	MaxExpHbInterval      uint32     `yaml:"max_expected_heartbeat_interval"`
	LogLevel              string     `yaml:"log_level"`
	MonServer             *MonServer `yaml:"mon_server"`
	CmdQueue              *CmdQueue  `yaml:"cmd_queue"`
	KeyFilter             *KeyFilter `yaml:"key_filter"`
	DLQueue               *DLQueue   `yaml:"dead_letter_queue"`

	MaxConcurrentOffsetCommits int `yaml:"max_concurrent_offset_commits"`
}

func (legacy legacyConfig) toConfig() Config {
	current := Config{LogLevel: legacy.LogLevel, MonServer: legacy.MonServer}
	current.Clusters = []Cluster{
		{ID: "source", ConnectionString: legacy.SrcConnectionString, ClientBalancer: legacy.SrcClientBalancer,
			OAuthFile: legacy.SrcOAuthFile, OAuthEndpoint: legacy.SrcOAuthEndpoint, StaticToken: legacy.SrcStaticToken},
		{ID: "destination", ConnectionString: legacy.DstConnectionString, ClientBalancer: legacy.DstClientBalancer,
			OAuthFile: legacy.DstOAuthFile, OAuthEndpoint: legacy.DstOAuthEndpoint, StaticToken: legacy.DstStaticToken},
	}
	current.Feeds = []Feed{{
		ID: legacy.InstanceId, Source: "source", Destination: "destination",
		CommitOffsetMode: legacy.CommitOffsetMode, MaxConcurrentOffsetCommits: legacy.MaxConcurrentOffsetCommits,
		Streams: legacy.Streams, StateTable: legacy.StateTable, MaxExpHbInterval: legacy.MaxExpHbInterval,
		CmdQueue: legacy.CmdQueue, KeyFilter: legacy.KeyFilter, DLQueue: legacy.DLQueue,
	}}
	current.WaitForFeed = legacy.MultipleInstancesMode
	return current
}
