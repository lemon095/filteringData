package main

import (
	"fmt"
	"io/ioutil"

	"gopkg.in/yaml.v3"
)

// 环境映射表
var envMapping = map[string]string{
	// 完整名称
	"local":   "local",
	"hk-test": "hk-test",
	"br-test": "br-test",
	"br-prod": "br-prod",
	"us-prod": "us-prod",
	"hk-prod": "hk-prod",

	// 简短别名
	"l":  "local",
	"ht": "hk-test",
	"bt": "br-test",
	"bp": "br-prod",
	"up": "us-prod",
	"hp": "hk-prod",
}

// DatabaseConfig 数据库配置结构体
type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	SSLMode  string `yaml:"sslmode"`
	Timezone string `yaml:"timezone"`
}

// GameConfig 单个游戏配置结构体
type GameConfig struct {
	ID   int     `yaml:"id"`   // 游戏ID
	BL   float64 `yaml:"bl"`   // 投注线数
	IsFb bool    `yaml:"isFb"` // 是否启用购买夺宝
}

// Config 配置结构体
type Config struct {
	// 多环境数据库配置
	Environments map[string]DatabaseConfig `yaml:"environments"`
	DefaultEnv   string                    `yaml:"default_env"`

	// 保持向后兼容的单一数据库配置（可选）
	Database struct {
		Postgres DatabaseConfig `yaml:"postgres"`
	} `yaml:"database"`

	Game struct {
		ID   int  `yaml:"id"`
		IsFb bool `yaml:"isFb"`
	} `yaml:"game"`

	// 多游戏配置
	MultiGame struct {
		Enabled bool         `yaml:"enabled"`
		Games   []GameConfig `yaml:"games"`
	} `yaml:"multi_game"`

	Tables struct {
		SourceTablePrefix string `yaml:"source_table_prefix"`
		OutputTablePrefix string `yaml:"output_table_prefix"`
		DataNum           int    `yaml:"data_num"`
		DataTableNum      int    `yaml:"data_table_num"`
		DataTableNum3     int    `yaml:"data_table_num_3"`
		DataNumFb         int    `yaml:"data_num_fb"`
		DataTableNumFb    int    `yaml:"data_table_num_fb"`
	} `yaml:"tables"`

	Bet struct {
		CS float64 `yaml:"cs"` // 投注额基数 (coin size)
		ML float64 `yaml:"ml"` // 投注倍数 (multiplier)
		BL float64 `yaml:"bl"` // 投注线数 (bet lines)
		FB float64 `yaml:"fb"` // 购买夺宝的倍数
	} `yaml:"bet"`

	PrizeRatios struct {
		BigPrize       float64 `yaml:"big_prize"`
		MegaPrize      float64 `yaml:"mega_prize"`
		SuperMegaPrize float64 `yaml:"super_mega_prize"`
	} `yaml:"prize_ratios"`

	GameplayRatios struct {
		NormalGameplay  float64 `yaml:"normal_gameplay"`
		SpecialGameplay float64 `yaml:"special_gameplay"`
	} `yaml:"gameplay_ratios"`

	RTP struct {
		NormalGameplay  float64 `yaml:"normal_gameplay"`
		SpecialGameplay float64 `yaml:"special_gameplay"`
	} `yaml:"rtp"`

	StageRatios struct {
		Stage1MinRatio    float64 `yaml:"stage1_min_ratio"`
		Stage1MaxRatio    float64 `yaml:"stage1_max_ratio"`
		Stage3WinTopRatio float64 `yaml:"stage3_win_top_ratio"`
		UpperDeviation    float64 `yaml:"upper_deviation"`
	} `yaml:"stage_ratios"`

	Settings struct {
		LogLevel  string `yaml:"log_level"`
		BatchSize int    `yaml:"batch_size"`
		Timeout   int    `yaml:"timeout"`
		// S3导入优化配置
		S3Import struct {
			MaxConcurrency int `yaml:"max_concurrency"` // 最大并发数
			BatchSize      int `yaml:"batch_size"`      // 批处理大小
			BufferSize     int `yaml:"buffer_size"`     // 缓冲区大小
		} `yaml:"s3_import"`
		// 数据库连接池配置
		Database struct {
			MaxOpenConns    int `yaml:"max_open_conns"`     // 最大打开连接数
			MaxIdleConns    int `yaml:"max_idle_conns"`     // 最大空闲连接数
			ConnMaxLifetime int `yaml:"conn_max_lifetime"`  // 连接最大生存时间(分钟)
			ConnMaxIdleTime int `yaml:"conn_max_idle_time"` // 连接最大空闲时间(分钟)
			PingInterval    int `yaml:"ping_interval"`      // 连接健康检查间隔(分钟)
		} `yaml:"database"`
	} `yaml:"settings"`

	// S3配置
	S3 struct {
		Enabled         bool   `yaml:"enabled"`
		Bucket          string `yaml:"bucket"`
		Region          string `yaml:"region"`
		AccessKeyID     string `yaml:"access_key_id"`
		SecretAccessKey string `yaml:"secret_access_key"`
		NormalPrefix    string `yaml:"normal_prefix"` // 普通模式路径前缀
		FbPrefix        string `yaml:"fb_prefix"`     // 购买夺宝模式路径前缀
	} `yaml:"s3"`
}

// ResolveEnv 解析环境参数
func ResolveEnv(envArg string) string {
	if fullEnv, exists := envMapping[envArg]; exists {
		return fullEnv
	}
	return envArg // 如果不在映射表中，直接返回原值
}

// IsEnv 检查参数是否为环境代码
func IsEnv(arg string) bool {
	_, exists := envMapping[arg]
	return exists
}

// GetDatabaseConfig 根据环境获取数据库配置
func (c *Config) GetDatabaseConfig(env string) (*DatabaseConfig, error) {
	if env == "" {
		env = c.DefaultEnv
	}

	// 首先尝试从environments中获取
	if dbConfig, exists := c.Environments[env]; exists {
		return &dbConfig, nil
	}

	// 向后兼容：如果environments为空，使用传统的database配置
	if len(c.Environments) == 0 {
		return &c.Database.Postgres, nil
	}

	return nil, fmt.Errorf("环境 '%s' 不存在", env)
}

// LoadConfig 加载配置文件
func LoadConfig(filename string) (*Config, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %v", err)
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %v", err)
	}

	// 如果没有设置默认环境，设置为local
	if config.DefaultEnv == "" {
		config.DefaultEnv = "local"
	}

	return &config, nil
}
