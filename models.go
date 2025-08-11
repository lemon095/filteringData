package main

import (
	"database/sql/driver"
	"encoding/json"
	"time"
)

// GameResultData 游戏结果数据结构
type GameResultData struct {
	ID        int       `json:"id" db:"id"`
	TB        int       `json:"tb" db:"tb"`   // 投注额
	AW        float64   `json:"aw" db:"aw"`   // 盈利额
	GWT       int       `json:"gwt" db:"gwt"` // 奖励类型 (2=大奖, 3=巨奖, 4=超巨奖)
	SP        bool      `json:"sp" db:"sp"`   // 是否特殊玩法
	FB        int       `json:"fb" db:"fb"`   // 是否为购买
	GD        JsonData  `json:"gd" db:"gd"`   // 原数据
	CreatedAt time.Time `json:"createdAt" db:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt" db:"updatedAt"`
}


type GameResult struct {
	RtpLevel float64 //rtp等级
	SrNumber int // 第几次
	SrId     int
	Bet      float64
	Win      float64
	Detail   json.RawMessage
}

// JsonData 用于处理 JSONB 类型
type JsonData struct {
	Data interface{}
}

// Value 实现 driver.Valuer 接口
func (j JsonData) Value() (driver.Value, error) {
	if j.Data == nil {
		return nil, nil
	}
	return json.Marshal(j.Data)
}

// Scan 实现 sql.Scanner 接口
func (j *JsonData) Scan(value interface{}) error {
	if value == nil {
		j.Data = nil
		return nil
	}
	
	bytes, ok := value.([]byte)
	if !ok {
		return nil
	}
	
	return json.Unmarshal(bytes, &j.Data)
}

// MarshalJSON 实现 json.Marshaler 接口
func (j JsonData) MarshalJSON() ([]byte, error) {
	return json.Marshal(j.Data)
}

// UnmarshalJSON 实现 json.Unmarshaler 接口
func (j *JsonData) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &j.Data)
}

// FilterCriteria 筛选条件
type FilterCriteria struct {
	MaxAWRatio float64 // aw/tb 的最大比例 (100)
	TotalCount int     // 总数量

	// 目标比例（可调整）
	BigPrizeRatio        float64 // 大奖比例
	MegaPrizeRatio       float64 // 巨奖比例
	SuperMegaRatio       float64 // 超巨奖比例
	NormalGameplayRatio  float64 // 普通玩法比例
	SpecialGameplayRatio float64 // 特殊玩法比例

	// RTP约束
	TargetNormalRTP  float64 // 目标普通玩法RTP
	TargetSpecialRTP float64 // 目标特殊玩法RTP
	RTPTolerance     float64 // RTP允许偏差
}

// FilterResult 筛选结果
type FilterResult struct {
	Data          []GameResultData `json:"data"`
	TotalCount    int              `json:"total_count"`
	FilteredCount int              `json:"filtered_count"`
	Statistics    FilterStats      `json:"statistics"`
}

// FilterStats 筛选统计
type FilterStats struct {
	BigPrizeCount  int `json:"big_prize_count"`
	MegaPrizeCount int `json:"mega_prize_count"`
	SuperMegaCount int `json:"super_mega_count"`
	NormalCount    int `json:"normal_count"`
	SpecialCount   int `json:"special_count"`

	// RTP统计
	NormalGameplayRTP  float64 `json:"normal_gameplay_rtp"`
	SpecialGameplayRTP float64 `json:"special_gameplay_rtp"`
	OverallRTP         float64 `json:"overall_rtp"`

	// 投注和盈利统计
	TotalBet           float64 `json:"total_bet"`
	TotalWin           float64 `json:"total_win"`
	NormalGameplayBet  float64 `json:"normal_gameplay_bet"`
	NormalGameplayWin  float64 `json:"normal_gameplay_win"`
	SpecialGameplayBet float64 `json:"special_gameplay_bet"`
	SpecialGameplayWin float64 `json:"special_gameplay_win"`

	// RTP偏差
	NormalRTPDeviation  float64 `json:"normal_rtp_deviation"`
	SpecialRTPDeviation float64 `json:"special_rtp_deviation"`
}

// CategorizedData 分类数据结构
type CategorizedData struct {
	NormalGameplay  []GameResultData // 普通玩法
	SpecialGameplay []GameResultData // 特殊玩法
	BigPrize        []GameResultData // 大奖 (gwt=2)
	MegaPrize       []GameResultData // 巨奖 (gwt=3)
	SuperMegaPrize  []GameResultData // 超巨奖 (gwt=4)
	Regular         []GameResultData // 普通奖励
}
