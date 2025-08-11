package main

import (
	"fmt"
	"io/ioutil"

	"gopkg.in/yaml.v3"
)


// Config 配置结构体
type Config struct {
	Database struct {
		Postgres struct {
			Host     string `yaml:"host"`
			Port     int    `yaml:"port"`
			Username string `yaml:"username"`
			Password string `yaml:"password"`
			Database string `yaml:"database"`
			SSLMode  string `yaml:"sslmode"`
			Timezone string `yaml:"timezone"`
		} `yaml:"postgres"`
	} `yaml:"database"`

	Game struct {
		ID int `yaml:"id"`
	} `yaml:"game"`

	Tables struct {
		SourceTablePrefix string `yaml:"source_table_prefix"`
		OutputTablePrefix string `yaml:"output_table_prefix"`
		DataNum           int    `yaml:"data_num"`
	} `yaml:"tables"`

	Bet struct {
		CS float64 `yaml:"cs"` // 投注额基数 (coin size)
		ML float64 `yaml:"ml"` // 投注倍数 (multiplier)
		BL float64 `yaml:"bl"` // 投注线数 (bet lines)
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

	Settings struct {
		LogLevel  string `yaml:"log_level"`
		BatchSize int    `yaml:"batch_size"`
		Timeout   int    `yaml:"timeout"`
	} `yaml:"settings"`
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

	return &config, nil
}
