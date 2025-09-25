package main

import (
	"fmt"
	"io/ioutil"
	"math"
	"sort"

	"gopkg.in/yaml.v3"
)

// RtpMultiplierDistribution RTP档位倍率分布配置
type RtpMultiplierDistribution struct {
	RtpNo         int     `yaml:"rtp_no"`
	RtpValue      float64 `yaml:"rtp_value"`
	RtpComponents struct {
		NormalRtp  float64 `yaml:"normal_rtp"`
		SpecialRtp float64 `yaml:"special_rtp"`
	} `yaml:"rtp_components"`
	MultiplierDistribution struct {
		ZeroWin             float64 `yaml:"zero_win"`              // 0倍 (不中奖)
		LowMultiplier       float64 `yaml:"low_multiplier"`        // 0-1倍
		MediumMultiplier    float64 `yaml:"medium_multiplier"`     // 1-5倍
		HighMultiplier      float64 `yaml:"high_multiplier"`       // 5-10倍
		VeryHighMultiplier  float64 `yaml:"very_high_multiplier"`  // 10-20倍
		MegaMultiplier      float64 `yaml:"mega_multiplier"`       // 20-50倍
		SuperMegaMultiplier float64 `yaml:"super_mega_multiplier"` // 50-100倍
		UltraMegaMultiplier float64 `yaml:"ultra_mega_multiplier"` // 100-500倍
	} `yaml:"multiplier_distribution"`
}

// RtpMultiplierConfig RTP倍率分布配置
type RtpMultiplierConfig struct {
	RtpMultiplierDistribution map[string]RtpMultiplierDistribution `yaml:"rtp_multiplier_distribution"`
}

// MultiplierRange 倍率区间定义
type MultiplierRange struct {
	Min        float64
	Max        float64
	Count      int
	Percentage float64
	Data       []GameResultData
}

// LoadRtpMultiplierConfig 加载RTP倍率分布配置
func LoadRtpMultiplierConfig(filename string) (*RtpMultiplierConfig, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %v", err)
	}

	var config RtpMultiplierConfig
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %v", err)
	}

	return &config, nil
}

// GetRtpDistribution 获取指定RTP档位的分布配置
func (c *RtpMultiplierConfig) GetRtpDistribution(rtpNo int) (*RtpMultiplierDistribution, error) {
	key := fmt.Sprintf("rtp_%d", rtpNo)
	distribution, exists := c.RtpMultiplierDistribution[key]
	if !exists {
		return nil, fmt.Errorf("未找到RTP档位 %d 的配置", rtpNo)
	}
	return &distribution, nil
}

// ClassifyDataByMultiplier 按倍率区间分类数据
func ClassifyDataByMultiplier(data []GameResultData, betAmount float64) map[string]MultiplierRange {
	ranges := make(map[string]MultiplierRange)

	// 初始化各个区间
	ranges["zero_win"] = MultiplierRange{Min: 0, Max: 0, Count: 0, Percentage: 0, Data: []GameResultData{}}
	ranges["low_multiplier"] = MultiplierRange{Min: 0, Max: 1, Count: 0, Percentage: 0, Data: []GameResultData{}}
	ranges["medium_multiplier"] = MultiplierRange{Min: 1, Max: 5, Count: 0, Percentage: 0, Data: []GameResultData{}}
	ranges["high_multiplier"] = MultiplierRange{Min: 5, Max: 10, Count: 0, Percentage: 0, Data: []GameResultData{}}
	ranges["very_high_multiplier"] = MultiplierRange{Min: 10, Max: 20, Count: 0, Percentage: 0, Data: []GameResultData{}}
	ranges["mega_multiplier"] = MultiplierRange{Min: 20, Max: 50, Count: 0, Percentage: 0, Data: []GameResultData{}}
	ranges["super_mega_multiplier"] = MultiplierRange{Min: 50, Max: 100, Count: 0, Percentage: 0, Data: []GameResultData{}}
	ranges["ultra_mega_multiplier"] = MultiplierRange{Min: 100, Max: 500, Count: 0, Percentage: 0, Data: []GameResultData{}}

	// 分类数据
	for _, item := range data {
		multiplier := item.AW / betAmount

		if multiplier == 0 {
			rangeData := ranges["zero_win"]
			rangeData.Data = append(rangeData.Data, item)
			rangeData.Count++
			ranges["zero_win"] = rangeData
		} else if multiplier > 0 && multiplier <= 1 {
			rangeData := ranges["low_multiplier"]
			rangeData.Data = append(rangeData.Data, item)
			rangeData.Count++
			ranges["low_multiplier"] = rangeData
		} else if multiplier > 1 && multiplier <= 5 {
			rangeData := ranges["medium_multiplier"]
			rangeData.Data = append(rangeData.Data, item)
			rangeData.Count++
			ranges["medium_multiplier"] = rangeData
		} else if multiplier > 5 && multiplier <= 10 {
			rangeData := ranges["high_multiplier"]
			rangeData.Data = append(rangeData.Data, item)
			rangeData.Count++
			ranges["high_multiplier"] = rangeData
		} else if multiplier > 10 && multiplier <= 20 {
			rangeData := ranges["very_high_multiplier"]
			rangeData.Data = append(rangeData.Data, item)
			rangeData.Count++
			ranges["very_high_multiplier"] = rangeData
		} else if multiplier > 20 && multiplier <= 50 {
			rangeData := ranges["mega_multiplier"]
			rangeData.Data = append(rangeData.Data, item)
			rangeData.Count++
			ranges["mega_multiplier"] = rangeData
		} else if multiplier > 50 && multiplier <= 100 {
			rangeData := ranges["super_mega_multiplier"]
			rangeData.Data = append(rangeData.Data, item)
			rangeData.Count++
			ranges["super_mega_multiplier"] = rangeData
		} else if multiplier > 100 && multiplier <= 500 {
			rangeData := ranges["ultra_mega_multiplier"]
			rangeData.Data = append(rangeData.Data, item)
			rangeData.Count++
			ranges["ultra_mega_multiplier"] = rangeData
		}
	}

	// 计算百分比
	totalCount := len(data)
	for key, rangeData := range ranges {
		if totalCount > 0 {
			rangeData.Percentage = float64(rangeData.Count) / float64(totalCount)
			ranges[key] = rangeData
		}
	}

	return ranges
}

// GenerateDataByDistribution 根据分布配置生成数据
func GenerateDataByDistribution(distribution *RtpMultiplierDistribution, totalCount int, dataRanges map[string]MultiplierRange) ([]GameResultData, error) {
	var result []GameResultData

	// 计算每个区间应该分配的数量
	allocations := make(map[string]int)
	allocations["zero_win"] = int(float64(totalCount) * distribution.MultiplierDistribution.ZeroWin)
	allocations["low_multiplier"] = int(float64(totalCount) * distribution.MultiplierDistribution.LowMultiplier)
	allocations["medium_multiplier"] = int(float64(totalCount) * distribution.MultiplierDistribution.MediumMultiplier)
	allocations["high_multiplier"] = int(float64(totalCount) * distribution.MultiplierDistribution.HighMultiplier)
	allocations["very_high_multiplier"] = int(float64(totalCount) * distribution.MultiplierDistribution.VeryHighMultiplier)
	allocations["mega_multiplier"] = int(float64(totalCount) * distribution.MultiplierDistribution.MegaMultiplier)
	allocations["super_mega_multiplier"] = int(float64(totalCount) * distribution.MultiplierDistribution.SuperMegaMultiplier)
	allocations["ultra_mega_multiplier"] = int(float64(totalCount) * distribution.MultiplierDistribution.UltraMegaMultiplier)

	// 按区间分配数据，允许不中奖率有2%偏差
	for rangeName, allocation := range allocations {
		if allocation > 0 && len(dataRanges[rangeName].Data) > 0 {
			// 如果可用数据不足，使用所有可用数据
			actualCount := allocation
			if actualCount > len(dataRanges[rangeName].Data) {
				actualCount = len(dataRanges[rangeName].Data)
			}

			// 随机选择数据
			selectedData := dataRanges[rangeName].Data[:actualCount]
			result = append(result, selectedData...)
		}
	}

	return result, nil
}

// CalculateRTP 计算当前数据的RTP
func CalculateRTP(data []GameResultData, totalBet float64) float64 {
	var totalWin float64
	for _, item := range data {
		totalWin += item.AW
	}
	return totalWin / totalBet
}

// calculateWinRate 计算中奖率（中奖数据占总数据的比例）
func calculateWinRate(data []GameResultData) float64 {
	if len(data) == 0 {
		return 0
	}

	winCount := 0
	for _, item := range data {
		if item.AW > 0 {
			winCount++
		}
	}

	return float64(winCount) / float64(len(data))
}

// AdjustRTPByReplacement 通过替换数据调整RTP
func AdjustRTPByReplacement(data []GameResultData, targetRTP float64, totalBet float64, dataRanges map[string]MultiplierRange, rtpLevel int, config *RTPConfig) ([]GameResultData, error) {
	currentRTP := CalculateRTP(data, totalBet)
	currentWinRate := calculateWinRate(data)

	// 从配置文件中动态获取目标不中奖率
	targetNoWinRate := GetTargetNoWinRateByRtpLevel(config, rtpLevel)

	// 计算目标中奖率
	targetWinRate := 1.0 - targetNoWinRate
	winRateDeviation := math.Abs(currentWinRate - targetWinRate)

	// 允许中奖率有2%的偏差（即不中奖率可以在±2%范围内调整）
	// 例如：配置不中奖率0.77，允许范围0.75-0.79，对应中奖率0.21-0.25
	if winRateDeviation <= 0.02 {
		// 中奖率偏差在可接受范围内，检查RTP是否也满足要求
		// RTP必须满足最低值（配置值），上浮只允许0.005内的偏差
		// 例如：目标0.97，允许范围0.97-0.975
		if currentRTP >= targetRTP && currentRTP <= targetRTP+0.005 {
			return data, nil
		}
	} else {
		// 中奖率偏差超出2%，需要调整不中奖率
		fmt.Printf("📊 中奖率偏差超出2%范围，当前偏差: %.4f\n", winRateDeviation)
	}

	// 如果RTP超出目标，优先替换大倍率区间的数据
	if currentRTP > targetRTP {
		return adjustRTPDownFlexible(data, targetRTP, totalBet, dataRanges)
	}

	// 如果RTP不足，使用灵活的调整策略
	if currentRTP < targetRTP {
		fmt.Printf("📈 RTP过低，需要提升...\n")
		// 优先在1-5倍和5-10倍区间内调整
		return adjustRTPUpFlexible(data, targetRTP, totalBet, dataRanges)
	}

	return data, nil
}

// adjustRTPDown 降低RTP（替换大倍率数据）
func adjustRTPDown(data []GameResultData, targetRTP float64, totalBet float64, dataRanges map[string]MultiplierRange) ([]GameResultData, error) {
	// 按倍率从高到低排序的区间
	rangeOrder := []string{
		"ultra_mega_multiplier",
		"super_mega_multiplier",
		"mega_multiplier",
		"very_high_multiplier",
		"high_multiplier",
		"medium_multiplier",
		"low_multiplier",
	}

	result := make([]GameResultData, len(data))
	copy(result, data)

	for _, rangeName := range rangeOrder {
		if len(dataRanges[rangeName].Data) == 0 {
			continue
		}

		// 找到该区间在结果中的数据，按金额从大到小排序
		var itemsInRange []struct {
			index int
			item  GameResultData
		}

		for i, item := range result {
			if item.AW == 0 {
				continue
			}
			multiplier := item.AW / totalBet
			if isInRange(multiplier, dataRanges[rangeName].Min, dataRanges[rangeName].Max) {
				itemsInRange = append(itemsInRange, struct {
					index int
					item  GameResultData
				}{i, item})
			}
		}

		// 按金额从大到小排序
		sort.Slice(itemsInRange, func(i, j int) bool {
			return itemsInRange[i].item.AW > itemsInRange[j].item.AW
		})

		// 尝试用该区间内金额较小的数据替换金额较大的数据
		for i := 0; i < len(itemsInRange); i++ {
			for j := i + 1; j < len(itemsInRange); j++ {
				if itemsInRange[j].item.AW < itemsInRange[i].item.AW {
					// 替换数据
					result[itemsInRange[i].index] = itemsInRange[j].item

					// 检查RTP是否满足要求
					newRTP := CalculateRTP(result, totalBet)
					if newRTP <= targetRTP {
						return result, nil
					}

					// 如果RTP还是太高，继续替换
					break
				}
			}
		}
	}

	return result, nil
}

// adjustRTPUp 提升RTP（从小倍率区间开始替换）
func adjustRTPUp(data []GameResultData, targetRTP float64, totalBet float64, dataRanges map[string]MultiplierRange) ([]GameResultData, error) {
	// 按倍率从低到高排序的区间
	rangeOrder := []string{
		"zero_win",
		"low_multiplier",
		"medium_multiplier",
		"high_multiplier",
		"very_high_multiplier",
		"mega_multiplier",
		"super_mega_multiplier",
		"ultra_mega_multiplier",
	}

	result := make([]GameResultData, len(data))
	copy(result, data)

	for _, rangeName := range rangeOrder {
		if len(dataRanges[rangeName].Data) == 0 {
			continue
		}

		// 找到该区间在结果中的数据，按金额从小到大排序
		var itemsInRange []struct {
			index int
			item  GameResultData
		}

		for i, item := range result {
			multiplier := item.AW / totalBet
			if isInRange(multiplier, dataRanges[rangeName].Min, dataRanges[rangeName].Max) {
				itemsInRange = append(itemsInRange, struct {
					index int
					item  GameResultData
				}{i, item})
			}
		}

		// 按金额从小到大排序
		sort.Slice(itemsInRange, func(i, j int) bool {
			return itemsInRange[i].item.AW < itemsInRange[j].item.AW
		})

		// 尝试用该区间内金额较大的数据替换金额较小的数据
		for i := 0; i < len(itemsInRange); i++ {
			for j := i + 1; j < len(itemsInRange); j++ {
				if itemsInRange[j].item.AW > itemsInRange[i].item.AW {
					// 替换数据
					result[itemsInRange[i].index] = itemsInRange[j].item

					// 检查RTP是否满足要求
					newRTP := CalculateRTP(result, totalBet)
					if newRTP >= targetRTP {
						return result, nil
					}

					// 如果RTP还是太低，继续替换
					break
				}
			}
		}
	}

	return result, nil
}

// adjustRTPUpAggressive 激进的RTP提升策略（跨区间调整）
func adjustRTPUpAggressive(data []GameResultData, targetRTP float64, totalBet float64, dataRanges map[string]MultiplierRange) ([]GameResultData, error) {
	result := make([]GameResultData, len(data))
	copy(result, data)

	// 收集所有可用的高倍率数据
	var highMultiplierData []GameResultData
	for _, rangeName := range []string{"high_multiplier", "very_high_multiplier", "mega_multiplier", "super_mega_multiplier", "ultra_mega_multiplier"} {
		if len(dataRanges[rangeName].Data) > 0 {
			highMultiplierData = append(highMultiplierData, dataRanges[rangeName].Data...)
		}
	}

	// 按金额从大到小排序高倍率数据
	sort.Slice(highMultiplierData, func(i, j int) bool {
		return highMultiplierData[i].AW > highMultiplierData[j].AW
	})

	// 找到低倍率数据并替换
	for i, item := range result {
		if item.AW == 0 {
			continue
		}

		multiplier := item.AW / totalBet
		// 只替换低倍率数据
		if multiplier <= 5 {
			// 寻找合适的高倍率数据替换
			for _, highData := range highMultiplierData {
				if highData.AW > item.AW {
					result[i] = highData

					// 检查RTP是否满足要求
					newRTP := CalculateRTP(result, totalBet)
					if newRTP >= targetRTP {
						return result, nil
					}
					break
				}
			}
		}
	}

	return result, nil
}

// adjustRTPDownFlexible 灵活的RTP降低策略（跨区间替换大金额为不中奖）
func adjustRTPDownFlexible(data []GameResultData, targetRTP float64, totalBet float64, dataRanges map[string]MultiplierRange) ([]GameResultData, error) {
	result := make([]GameResultData, len(data))
	copy(result, data)

	// 获取不中奖数据
	zeroWinData := dataRanges["zero_win"].Data
	if len(zeroWinData) == 0 {
		return result, fmt.Errorf("没有不中奖数据可供替换")
	}

	// 按倍率从高到低排序的区间，跨区间选择
	rangeOrder := []string{
		"ultra_mega_multiplier",
		"super_mega_multiplier",
		"mega_multiplier",
		"very_high_multiplier",
		"high_multiplier",
		"medium_multiplier",
		"low_multiplier",
	}

	// 收集所有有中奖的数据，按金额从大到小排序
	var allWinItems []struct {
		index     int
		item      GameResultData
		rangeName string
	}

	for i, item := range result {
		if item.AW == 0 {
			continue
		}
		multiplier := item.AW / totalBet

		// 判断属于哪个区间
		var rangeName string
		if multiplier > 500 {
			rangeName = "ultra_mega_multiplier"
		} else if multiplier > 100 && multiplier <= 500 {
			rangeName = "super_mega_multiplier"
		} else if multiplier > 50 && multiplier <= 100 {
			rangeName = "mega_multiplier"
		} else if multiplier > 20 && multiplier <= 50 {
			rangeName = "very_high_multiplier"
		} else if multiplier > 5 && multiplier <= 20 {
			rangeName = "high_multiplier"
		} else if multiplier > 1 && multiplier <= 5 {
			rangeName = "medium_multiplier"
		} else if multiplier > 0 && multiplier <= 1 {
			rangeName = "low_multiplier"
		}

		allWinItems = append(allWinItems, struct {
			index     int
			item      GameResultData
			rangeName string
		}{i, item, rangeName})
	}

	// 按金额从大到小排序
	sort.Slice(allWinItems, func(i, j int) bool {
		return allWinItems[i].item.AW > allWinItems[j].item.AW
	})

	// 按区间优先级和金额大小进行替换
	for _, rangeName := range rangeOrder {
		// 找到该区间的数据
		var itemsInRange []struct {
			index int
			item  GameResultData
		}

		for _, itemInfo := range allWinItems {
			if itemInfo.rangeName == rangeName {
				itemsInRange = append(itemsInRange, struct {
					index int
					item  GameResultData
				}{itemInfo.index, itemInfo.item})
			}
		}

		// 按金额从大到小排序
		sort.Slice(itemsInRange, func(i, j int) bool {
			return itemsInRange[i].item.AW > itemsInRange[j].item.AW
		})

		// 尝试用不中奖数据替换该区间的大金额数据
		for _, itemInfo := range itemsInRange {
			// 随机选择一个不中奖数据，直接替换整个数据
			zeroWinItem := zeroWinData[0] // 使用第一个不中奖数据
			result[itemInfo.index] = zeroWinItem

			// 检查RTP是否满足要求（必须满足最低值，上浮只允许0.005）
			newRTP := CalculateRTP(result, totalBet)
			if newRTP >= targetRTP && newRTP <= targetRTP+0.005 {
				return result, nil
			}
		}
	}

	return result, nil
}

// adjustRTPUpFlexible 灵活的RTP提升策略（优先在1-5倍和5-10倍区间调整）
func adjustRTPUpFlexible(data []GameResultData, targetRTP float64, totalBet float64, dataRanges map[string]MultiplierRange) ([]GameResultData, error) {
	result := make([]GameResultData, len(data))
	copy(result, data)

	// 优先调整的区间：1-5倍和5-10倍
	priorityRanges := []string{"medium_multiplier", "high_multiplier"}

	// 收集这些区间的可用数据
	var availableData []GameResultData
	for _, rangeName := range priorityRanges {
		if len(dataRanges[rangeName].Data) > 0 {
			availableData = append(availableData, dataRanges[rangeName].Data...)
		}
	}

	// 按金额从大到小排序
	sort.Slice(availableData, func(i, j int) bool {
		return availableData[i].AW > availableData[j].AW
	})

	// 找到低倍率数据并替换
	for i, item := range result {
		if item.AW == 0 {
			continue
		}

		multiplier := item.AW / totalBet
		// 只替换低倍率数据（0-1倍区间）
		if multiplier > 0 && multiplier <= 1 {
			// 寻找合适的高倍率数据替换
			for _, highData := range availableData {
				if highData.AW > item.AW {
					result[i] = highData

					// 检查RTP是否满足要求（必须满足最低值，上浮只允许0.005）
					newRTP := CalculateRTP(result, totalBet)
					if newRTP >= targetRTP && newRTP <= targetRTP+0.005 {
						return result, nil
					}
					break
				}
			}
		}
	}

	// 如果1-5倍和5-10倍区间调整不够，再考虑其他区间
	otherRanges := []string{"very_high_multiplier", "mega_multiplier", "super_mega_multiplier", "ultra_mega_multiplier"}
	var otherData []GameResultData
	for _, rangeName := range otherRanges {
		if len(dataRanges[rangeName].Data) > 0 {
			otherData = append(otherData, dataRanges[rangeName].Data...)
		}
	}

	// 按金额从大到小排序
	sort.Slice(otherData, func(i, j int) bool {
		return otherData[i].AW > otherData[j].AW
	})

	// 继续替换低倍率数据
	for i, item := range result {
		if item.AW == 0 {
			continue
		}

		multiplier := item.AW / totalBet
		// 替换1-5倍区间的数据
		if multiplier > 1 && multiplier <= 5 {
			for _, highData := range otherData {
				if highData.AW > item.AW {
					result[i] = highData

					// 检查RTP是否满足要求（必须满足最低值，上浮只允许0.005）
					newRTP := CalculateRTP(result, totalBet)
					if newRTP >= targetRTP && newRTP <= targetRTP+0.005 {
						return result, nil
					}
					break
				}
			}
		}
	}

	return result, nil
}

// isInRange 检查倍率是否在指定范围内
func isInRange(multiplier, min, max float64) bool {
	return multiplier > min && multiplier <= max
}

// findSmallerMultiplierReplacement 找到更小倍率的替换数据
func findSmallerMultiplierReplacement(item GameResultData, dataRanges map[string]MultiplierRange, totalBet float64) *GameResultData {
	currentMultiplier := item.AW / totalBet

	// 按倍率从低到高查找替换数据
	rangeOrder := []string{
		"zero_win",
		"low_multiplier",
		"medium_multiplier",
		"high_multiplier",
		"very_high_multiplier",
		"mega_multiplier",
		"super_mega_multiplier",
	}

	for _, rangeName := range rangeOrder {
		if len(dataRanges[rangeName].Data) == 0 {
			continue
		}

		// 找到该区间内倍率小于当前倍率的数据
		for _, candidate := range dataRanges[rangeName].Data {
			candidateMultiplier := candidate.AW / totalBet
			if candidateMultiplier < currentMultiplier {
				return &candidate
			}
		}
	}

	return nil
}

// findLargerMultiplierReplacement 找到更大倍率的替换数据
func findLargerMultiplierReplacement(item GameResultData, dataRanges map[string]MultiplierRange, totalBet float64) *GameResultData {
	currentMultiplier := item.AW / totalBet

	// 按倍率从高到低查找替换数据
	rangeOrder := []string{
		"ultra_mega_multiplier",
		"super_mega_multiplier",
		"mega_multiplier",
		"very_high_multiplier",
		"high_multiplier",
		"medium_multiplier",
		"low_multiplier",
	}

	for _, rangeName := range rangeOrder {
		if len(dataRanges[rangeName].Data) == 0 {
			continue
		}

		// 找到该区间内倍率大于当前倍率的数据
		for _, candidate := range dataRanges[rangeName].Data {
			candidateMultiplier := candidate.AW / totalBet
			if candidateMultiplier > currentMultiplier {
				return &candidate
			}
		}
	}

	return nil
}

// SortDataByMultiplier 按倍率排序数据
func SortDataByMultiplier(data []GameResultData, totalBet float64, ascending bool) {
	sort.Slice(data, func(i, j int) bool {
		multiplierI := data[i].AW / totalBet
		multiplierJ := data[j].AW / totalBet

		if ascending {
			return multiplierI < multiplierJ
		}
		return multiplierI > multiplierJ
	})
}

// RTPConfig 配置结构体
type RTPConfig struct {
	RtpMultiplierDistribution map[string]RTPLevel `yaml:"rtp_multiplier_distribution"`
}

type RTPLevel struct {
	RtpNo                  int                    `yaml:"rtp_no"`
	RtpValue               float64                `yaml:"rtp_value"`
	RtpComponents          RtpComponents          `yaml:"rtp_components"`
	MultiplierDistribution MultiplierDistribution `yaml:"multiplier_distribution"`
}

type RtpComponents struct {
	NormalRtp  float64 `yaml:"normal_rtp"`
	SpecialRtp float64 `yaml:"special_rtp"`
}

type MultiplierDistribution struct {
	ZeroWin             float64 `yaml:"zero_win"`
	LowMultiplier       float64 `yaml:"low_multiplier"`
	MediumMultiplier    float64 `yaml:"medium_multiplier"`
	HighMultiplier      float64 `yaml:"high_multiplier"`
	VeryHighMultiplier  float64 `yaml:"very_high_multiplier"`
	MegaMultiplier      float64 `yaml:"mega_multiplier"`
	SuperMegaMultiplier float64 `yaml:"super_mega_multiplier"`
	UltraMegaMultiplier float64 `yaml:"ultra_mega_multiplier"`
}

// LoadRTPConfig 加载RTP配置文件
func LoadRTPConfig(configPath string) (*RTPConfig, error) {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %v", err)
	}

	var config RTPConfig
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %v", err)
	}

	return &config, nil
}

// GetTargetNoWinRateByRtpLevel 根据RTP档位获取目标不中奖率
func GetTargetNoWinRateByRtpLevel(config *RTPConfig, rtpLevel int) float64 {
	key := fmt.Sprintf("rtp_%d", rtpLevel)
	if level, exists := config.RtpMultiplierDistribution[key]; exists {
		return level.MultiplierDistribution.ZeroWin
	}
	return 0.77 // 默认值
}

// GetMultiplierDistributionByRtpLevel 根据RTP档位获取倍率分布配置
func GetMultiplierDistributionByRtpLevel(config *RTPConfig, rtpLevel int) MultiplierDistribution {
	key := fmt.Sprintf("rtp_%d", rtpLevel)
	if level, exists := config.RtpMultiplierDistribution[key]; exists {
		return level.MultiplierDistribution
	}
	// 返回默认配置
	return MultiplierDistribution{
		ZeroWin:             0.77,
		LowMultiplier:       0.11,
		MediumMultiplier:    0.08,
		HighMultiplier:      0.02,
		VeryHighMultiplier:  0.01,
		MegaMultiplier:      0.0,
		SuperMegaMultiplier: 0.0,
		UltraMegaMultiplier: 0.0,
	}
}
