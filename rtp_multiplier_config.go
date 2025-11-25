package main

import (
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"sort"
	"time"

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
func GenerateDataByDistribution(distribution *RtpMultiplierDistribution, totalCount int, dataRanges map[string]MultiplierRange, rtpLevel int) ([]GameResultData, error) {
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

	// 创建随机数生成器
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// 针对高档位（300/500）允许有限重复，以缓解高倍率样本不足的问题
	// 对于高倍率区间（very_high_multiplier及以上），只有300/500档位才允许向下找次高倍率数据
	allowHighMultiplierFallback := rtpLevel == 300 || rtpLevel == 500

	// 对于所有中奖区间，数据不足时都应该允许重复补齐，以保持中奖率
	// 只有不中奖区间（zero_win）数据不足时，才需要用其他数据补齐

	// 按区间分配数据
	for rangeName, allocation := range allocations {
		availableData := dataRanges[rangeName].Data

		// 如果分配数量为0，跳过
		if allocation <= 0 {
			continue
		}

		// 如果该区间没有可用数据
		if len(availableData) == 0 {
			// 对于高倍率区间（very_high_multiplier及以上），如果数据为空且分配数量>0
			highMultiplierRanges := []string{"very_high_multiplier", "mega_multiplier", "super_mega_multiplier", "ultra_mega_multiplier"}
			isHighMultiplier := false
			for _, hr := range highMultiplierRanges {
				if rangeName == hr {
					isHighMultiplier = true
					break
				}
			}

			if isHighMultiplier {
				// 对于300/500档位，如果高倍率区间没有数据，向下找次高倍率数据复制补齐
				if allowHighMultiplierFallback {
					// 按优先级向下查找可用的次高倍率数据源（从高到低）
					// 根据当前区间，优先使用更接近的倍率区间
					var fallbackRanges []string
					switch rangeName {
					case "ultra_mega_multiplier":
						fallbackRanges = []string{"super_mega_multiplier", "mega_multiplier", "very_high_multiplier", "high_multiplier", "medium_multiplier"}
					case "super_mega_multiplier":
						fallbackRanges = []string{"mega_multiplier", "very_high_multiplier", "high_multiplier", "medium_multiplier"}
					case "mega_multiplier":
						fallbackRanges = []string{"very_high_multiplier", "high_multiplier", "medium_multiplier"}
					case "very_high_multiplier":
						fallbackRanges = []string{"high_multiplier", "medium_multiplier"}
					default:
						fallbackRanges = []string{"high_multiplier", "medium_multiplier"}
					}

					var fallbackData []GameResultData
					fallbackRangeName := ""

					// 查找第一个有数据的次高倍率区间
					for _, fbRange := range fallbackRanges {
						if len(dataRanges[fbRange].Data) > 0 {
							fallbackData = dataRanges[fbRange].Data
							fallbackRangeName = fbRange
							break
						}
					}

					if len(fallbackData) > 0 {
						// 使用次高倍率数据复制补齐
						fmt.Printf("⚠️ %s 区间需要 %d 条数据，但可用数据为 0 条，将使用 %s 区间的数据复制补齐\n", rangeName, allocation, fallbackRangeName)
						for i := 0; i < allocation; i++ {
							selectedData := fallbackData[rng.Intn(len(fallbackData))]
							result = append(result, selectedData)
						}
						continue
					} else {
						// 如果次高倍率也没有数据，给出警告并跳过
						fmt.Printf("⚠️ 警告：%s 区间需要 %d 条数据，但可用数据为 0 条，且无次高倍率数据可替代，将跳过该区间\n", rangeName, allocation)
						continue
					}
				} else {
					// 非高档位，直接跳过
					fmt.Printf("⚠️ 警告：%s 区间需要 %d 条数据，但可用数据为 0 条，将跳过该区间\n", rangeName, allocation)
					continue
				}
			}
			continue
		}

		// 随机选择数据（优先使用不重复数据）
		perm := rng.Perm(len(availableData))
		var selectedData []GameResultData

		if allocation <= len(availableData) {
			// 数据充足，直接选择
			for i := 0; i < allocation; i++ {
				selectedData = append(selectedData, availableData[perm[i]])
			}
		} else {
			// 数据不足时，先把全部可用数据取完
			for i := 0; i < len(availableData); i++ {
				selectedData = append(selectedData, availableData[perm[i]])
			}

			// 对于所有中奖区间（非zero_win），数据不足时都允许重复补齐，以保持中奖率
			if rangeName != "zero_win" {
				shortage := allocation - len(selectedData)
				fmt.Printf("⚠️ %s 区间数据不足：需要 %d 条，可用 %d 条，将通过重复补齐 %d 条\n", rangeName, allocation, len(availableData), shortage)
				for len(selectedData) < allocation {
					selectedData = append(selectedData, availableData[rng.Intn(len(availableData))])
				}
			} else {
				// 不中奖区间数据不足时，给出警告，后续会用其他数据补齐
				fmt.Printf("⚠️ 警告：%s 区间数据不足：需要 %d 条，可用 %d 条，实际只使用 %d 条\n", rangeName, allocation, len(availableData), len(selectedData))
			}
		}

		result = append(result, selectedData...)
	}

	// 如果总数据量不足，用不中奖数据补齐
	if len(result) < totalCount {
		shortage := totalCount - len(result)
		zeroWinData := dataRanges["zero_win"].Data
		if len(zeroWinData) > 0 {
			fmt.Printf("📊 总数据量不足：需要 %d 条，实际 %d 条，将用不中奖数据补齐 %d 条\n", totalCount, len(result), shortage)
			// 随机选择不中奖数据补齐
			for i := 0; i < shortage; i++ {
				selectedData := zeroWinData[rng.Intn(len(zeroWinData))]
				result = append(result, selectedData)
			}
		} else {
			// 如果没有不中奖数据，尝试用其他数据补齐
			fmt.Printf("⚠️ 警告：总数据量不足 %d 条，实际 %d 条，且无不中奖数据可补齐\n", totalCount, len(result))
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
func AdjustRTPByReplacement(data []GameResultData, targetRTP float64, totalBet float64, dataRanges map[string]MultiplierRange, rtpLevel int, config *RtpMultiplierConfig) ([]GameResultData, error) {
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
		// 为特定RTP档位设置不同的偏差容忍度
		rtpTolerance := getRTPTolerance(rtpLevel)
		if currentRTP >= targetRTP && currentRTP <= targetRTP+rtpTolerance {
			return data, nil
		}
	} else {
		// 中奖率偏差超出2%，需要调整不中奖率
		fmt.Printf("📊 中奖率偏差超出2%%范围，当前偏差: %.4f\n", winRateDeviation)
	}

	// 如果RTP超出目标，优先替换大倍率区间的数据
	if currentRTP > targetRTP {
		return adjustRTPDownFlexible(data, targetRTP, totalBet, dataRanges, rtpLevel)
	}

	// 如果RTP不足，使用灵活的调整策略
	if currentRTP < targetRTP {
		fmt.Printf("📈 RTP过低，需要提升...\n")
		// 优先在1-5倍和5-10倍区间内调整
		return adjustRTPUpFlexible(data, targetRTP, totalBet, dataRanges, rtpLevel)
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
func adjustRTPDownFlexible(data []GameResultData, targetRTP float64, totalBet float64, dataRanges map[string]MultiplierRange, rtpLevel int) ([]GameResultData, error) {
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

			// 检查RTP是否满足要求（必须满足最低值，上浮允许根据档位调整）
			newRTP := CalculateRTP(result, totalBet)
			rtpTolerance := getRTPTolerance(rtpLevel)
			if newRTP >= targetRTP && newRTP <= targetRTP+rtpTolerance {
				return result, nil
			}
		}
	}

	return result, nil
}

// adjustRTPUpFlexible 智能的RTP提升策略（根据档位选择调整区间）
func adjustRTPUpFlexible(data []GameResultData, targetRTP float64, totalBet float64, dataRanges map[string]MultiplierRange, rtpLevel int) ([]GameResultData, error) {
	// 获取该档位的调整策略
	strategy := getAdjustmentStrategy(rtpLevel)

	// 计算允许的最大5-10倍数量
	maxHighMultiplierCount := int(float64(len(data)) * strategy.MaxHighMultiplierRatio)

	result := make([]GameResultData, len(data))
	copy(result, data)

	// 优先使用主要调整区间
	if strategy.PrimaryRange == "medium_multiplier" {
		// 低档位：主要用1-5倍调整
		return adjustUsingMediumMultiplier(result, targetRTP, totalBet, dataRanges, rtpLevel, maxHighMultiplierCount)
	} else {
		// 高档位：可以用5-10倍调整
		return adjustUsingHighMultiplier(result, targetRTP, totalBet, dataRanges, rtpLevel, maxHighMultiplierCount)
	}
}

// adjustUsingMediumMultiplier 使用1-5倍区间进行RTP调整（适用于低档位）
func adjustUsingMediumMultiplier(data []GameResultData, targetRTP float64, totalBet float64, dataRanges map[string]MultiplierRange, rtpLevel int, maxHighMultiplierCount int) ([]GameResultData, error) {
	// 使用新的平衡替换策略
	return adjustRTPBalanced(data, targetRTP, totalBet, dataRanges, rtpLevel, maxHighMultiplierCount)
}

// adjustUsingMediumMultiplierOld 旧的RTP调整方法（保留作为备用）
func adjustUsingMediumMultiplierOld(data []GameResultData, targetRTP float64, totalBet float64, dataRanges map[string]MultiplierRange, rtpLevel int, maxHighMultiplierCount int) ([]GameResultData, error) {
	result := make([]GameResultData, len(data))
	copy(result, data)

	// 收集1-5倍区间的可用数据
	var mediumData []GameResultData
	if len(dataRanges["medium_multiplier"].Data) > 0 {
		mediumData = append(mediumData, dataRanges["medium_multiplier"].Data...)
	}

	// 按金额从大到小排序
	sort.Slice(mediumData, func(i, j int) bool {
		return mediumData[i].AW > mediumData[j].AW
	})

	// 主要用1-5倍数据替换0-1倍数据
	for i, item := range result {
		if item.AW == 0 {
			continue
		}

		multiplier := item.AW / totalBet
		// 只替换低倍率数据（0-1倍区间）
		if multiplier > 0 && multiplier <= 1 {
			// 寻找合适的1-5倍数据替换
			for _, mediumItem := range mediumData {
				if mediumItem.AW > item.AW {
					result[i] = mediumItem

					// 检查RTP是否满足要求
					newRTP := CalculateRTP(result, totalBet)
					rtpTolerance := getRTPTolerance(rtpLevel)
					if newRTP >= targetRTP && newRTP <= targetRTP+rtpTolerance {
						return result, nil
					}
					break
				}
			}
		}
	}

	// 如果1-5倍调整不够，且5-10倍使用量未超限，则使用5-10倍
	if checkRangeLimit(result, "high_multiplier", maxHighMultiplierCount, totalBet, dataRanges) {
		var highData []GameResultData
		if len(dataRanges["high_multiplier"].Data) > 0 {
			highData = append(highData, dataRanges["high_multiplier"].Data...)
		}

		// 按金额从大到小排序
		sort.Slice(highData, func(i, j int) bool {
			return highData[i].AW > highData[j].AW
		})

		// 用5-10倍数据替换0-1倍数据
		for i, item := range result {
			if item.AW == 0 {
				continue
			}

			multiplier := item.AW / totalBet
			if multiplier > 0 && multiplier <= 1 {
				for _, highItem := range highData {
					if highItem.AW > item.AW {
						result[i] = highItem

						// 检查RTP是否满足要求
						newRTP := CalculateRTP(result, totalBet)
						rtpTolerance := getRTPTolerance(rtpLevel)
						if newRTP >= targetRTP && newRTP <= targetRTP+rtpTolerance {
							return result, nil
						}
						break
					}
				}
			}
		}
	}

	return result, nil
}

// adjustUsingHighMultiplier 使用5-10倍区间进行RTP调整（适用于高档位）
func adjustUsingHighMultiplier(data []GameResultData, targetRTP float64, totalBet float64, dataRanges map[string]MultiplierRange, rtpLevel int, maxHighMultiplierCount int) ([]GameResultData, error) {
	// 使用新的平衡替换策略
	return adjustRTPBalanced(data, targetRTP, totalBet, dataRanges, rtpLevel, maxHighMultiplierCount)
}

// adjustUsingHighMultiplierOld 旧的RTP调整方法（保留作为备用）
func adjustUsingHighMultiplierOld(data []GameResultData, targetRTP float64, totalBet float64, dataRanges map[string]MultiplierRange, rtpLevel int, maxHighMultiplierCount int) ([]GameResultData, error) {
	result := make([]GameResultData, len(data))
	copy(result, data)

	// 收集5-10倍区间的可用数据
	var highData []GameResultData
	if len(dataRanges["high_multiplier"].Data) > 0 {
		highData = append(highData, dataRanges["high_multiplier"].Data...)
	}

	// 按金额从大到小排序
	sort.Slice(highData, func(i, j int) bool {
		return highData[i].AW > highData[j].AW
	})

	// 用5-10倍数据替换低倍率数据
	for i, item := range result {
		if item.AW == 0 {
			continue
		}

		multiplier := item.AW / totalBet
		// 替换低倍率数据（0-1倍和1-5倍区间）
		if multiplier > 0 && multiplier <= 5 {
			// 检查5-10倍使用量是否超限
			if checkRangeLimit(result, "high_multiplier", maxHighMultiplierCount, totalBet, dataRanges) {
				for _, highItem := range highData {
					if highItem.AW > item.AW {
						result[i] = highItem

						// 检查RTP是否满足要求
						newRTP := CalculateRTP(result, totalBet)
						rtpTolerance := getRTPTolerance(rtpLevel)
						if newRTP >= targetRTP && newRTP <= targetRTP+rtpTolerance {
							return result, nil
						}
						break
					}
				}
			}
		}
	}

	return result, nil
}

// AdjustmentStrategy 调整策略结构体
type AdjustmentStrategy struct {
	PrimaryRange           string  // 主要调整区间
	SecondaryRange         string  // 次要调整区间
	MaxHighMultiplierRatio float64 // 5-10倍最大使用比例
}

// getAdjustmentStrategy 根据RTP档位获取调整策略
func getAdjustmentStrategy(rtpLevel int) AdjustmentStrategy {
	// 低档位：主要用1-5倍调整，5-10倍使用很少
	lowLevels := []int{1, 2, 3, 4, 5, 20, 30, 40, 50}
	// 中档位：平衡使用1-5倍和5-10倍
	midLevels := []int{6, 7, 8, 9, 10, 11, 12, 13}
	// 高档位：可以使用更多5-10倍
	highLevels := []int{14, 15, 120, 150, 200, 300, 500}

	for _, level := range lowLevels {
		if rtpLevel == level {
			return AdjustmentStrategy{
				PrimaryRange:           "medium_multiplier", // 主要用1-5倍
				SecondaryRange:         "high_multiplier",   // 次要用5-10倍
				MaxHighMultiplierRatio: 0.05,                // 5-10倍最多不超过5%
			}
		}
	}

	for _, level := range midLevels {
		if rtpLevel == level {
			return AdjustmentStrategy{
				PrimaryRange:           "medium_multiplier",
				SecondaryRange:         "high_multiplier",
				MaxHighMultiplierRatio: 0.1, // 5-10倍最多不超过10%
			}
		}
	}

	for _, level := range highLevels {
		if rtpLevel == level {
			return AdjustmentStrategy{
				PrimaryRange:           "high_multiplier", // 高档位可以主要用5-10倍
				SecondaryRange:         "very_high_multiplier",
				MaxHighMultiplierRatio: 0.3, // 5-10倍最多不超过30%
			}
		}
	}

	// 默认策略
	return AdjustmentStrategy{
		PrimaryRange:           "medium_multiplier",
		SecondaryRange:         "high_multiplier",
		MaxHighMultiplierRatio: 0.1,
	}
}

// calculateRangeCounts 计算当前各区间使用量
func calculateRangeCounts(data []GameResultData, totalBet float64, dataRanges map[string]MultiplierRange) map[string]int {
	counts := make(map[string]int)

	for _, item := range data {
		if item.AW == 0 {
			counts["zero_win"]++
			continue
		}

		multiplier := item.AW / totalBet
		if multiplier > 0 && multiplier <= 1 {
			counts["low_multiplier"]++
		} else if multiplier > 1 && multiplier <= 5 {
			counts["medium_multiplier"]++
		} else if multiplier > 5 && multiplier <= 10 {
			counts["high_multiplier"]++
		} else if multiplier > 10 && multiplier <= 20 {
			counts["very_high_multiplier"]++
		} else if multiplier > 20 && multiplier <= 50 {
			counts["mega_multiplier"]++
		} else if multiplier > 50 && multiplier <= 100 {
			counts["super_mega_multiplier"]++
		} else if multiplier > 100 && multiplier <= 500 {
			counts["ultra_mega_multiplier"]++
		}
	}

	return counts
}

// checkRangeLimit 检查区间使用量是否超过限制
func checkRangeLimit(data []GameResultData, rangeName string, maxCount int, totalBet float64, dataRanges map[string]MultiplierRange) bool {
	count := 0
	for _, item := range data {
		if item.AW == 0 {
			if rangeName == "zero_win" {
				count++
			}
			continue
		}
		multiplier := item.AW / totalBet
		if isInRange(multiplier, dataRanges[rangeName].Min, dataRanges[rangeName].Max) {
			count++
		}
	}
	return count < maxCount
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
func GetTargetNoWinRateByRtpLevel(config *RtpMultiplierConfig, rtpLevel int) float64 {
	key := fmt.Sprintf("rtp_%d", rtpLevel)
	if level, exists := config.RtpMultiplierDistribution[key]; exists {
		return level.MultiplierDistribution.ZeroWin
	}
	return 0.77 // 默认值
}

// getRTPTolerance 根据RTP档位获取RTP偏差容忍度
func getRTPTolerance(rtpLevel int) float64 {
	// 特定RTP档位使用更宽松的偏差容忍度（0.05）
	highToleranceLevels := []int{15, 300, 500, 14, 120, 150, 200}
	for _, level := range highToleranceLevels {
		if rtpLevel == level {
			return 0.05
		}
	}
	// 其他档位使用严格的偏差容忍度（0.005）
	return 0.005
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

// adjustRTPToLowerLimit 调整RTP到下限
func adjustRTPToLowerLimit(data []GameResultData, targetRTP float64, totalBet float64, dataRanges map[string]MultiplierRange) ([]GameResultData, error) {
	result := make([]GameResultData, len(data))
	copy(result, data)

	currentRTP := CalculateRTP(result, totalBet)

	// 如果当前RTP已经达到下限，直接返回
	if currentRTP >= targetRTP {
		return result, nil
	}

	// 找到零中奖数据
	var zeroWinIndices []int
	for i, item := range result {
		if item.AW == 0 {
			zeroWinIndices = append(zeroWinIndices, i)
		}
	}

	if len(zeroWinIndices) == 0 {
		return data, fmt.Errorf("没有零中奖数据可替换")
	}

	// 按倍率从低到高排序的区间（优先使用低倍率数据）
	rangeOrder := []string{
		"low_multiplier",
		"medium_multiplier",
		"high_multiplier",
		"very_high_multiplier",
		"mega_multiplier",
		"super_mega_multiplier",
		"ultra_mega_multiplier",
	}

	// 替换零中奖数据直到达到RTP下限
	replaceCount := 0
	maxReplacements := len(zeroWinIndices) // 允许替换所有零中奖数据以确保RTP下限

	for _, rangeName := range rangeOrder {
		if replaceCount >= maxReplacements {
			break
		}

		rangeData := dataRanges[rangeName].Data
		if len(rangeData) == 0 {
			continue
		}

		// 按倍率从低到高排序
		sort.Slice(rangeData, func(i, j int) bool {
			return rangeData[i].AW/float64(rangeData[i].TB) < rangeData[j].AW/float64(rangeData[j].TB)
		})

		for _, item := range rangeData {
			if replaceCount >= maxReplacements || len(zeroWinIndices) == 0 {
				break
			}

			// 替换零中奖数据
			zeroIndex := zeroWinIndices[0]
			zeroWinIndices = zeroWinIndices[1:]
			result[zeroIndex] = item
			replaceCount++

			// 检查RTP是否达到下限
			newRTP := CalculateRTP(result, totalBet)
			if newRTP >= targetRTP {
				return result, nil
			}
		}
	}

	return result, nil
}

// ShuffleDataWithMultiplierDistribution 智能打乱数据，确保大倍率数据均匀分布在整个序列中
// 方案A：将数据分成三类（不中奖、普通中奖、大倍率），先均匀分布普通中奖数据到各个区间，再插入大倍率数据
func ShuffleDataWithMultiplierDistribution(data []GameResultData, betAmount float64, rng *rand.Rand) {
	totalSize := len(data)
	if totalSize == 0 {
		return
	}

	// 步骤1：按倍率分类，将数据分成三类
	var zeroWinData []GameResultData       // 不中奖数据（multiplier == 0）
	var normalWinData []GameResultData     // 普通中奖数据（0 < multiplier <= 20）
	var bigMultiplierData []GameResultData // 大倍率数据（20 < multiplier <= 100）

	for _, item := range data {
		multiplier := item.AW / betAmount

		if multiplier == 0 {
			zeroWinData = append(zeroWinData, item)
		} else if multiplier > 0 && multiplier <= 20 {
			normalWinData = append(normalWinData, item)
		} else if multiplier > 20 && multiplier <= 100 {
			bigMultiplierData = append(bigMultiplierData, item)
		} else {
			// 超过100倍的数据也归入普通中奖数据
			normalWinData = append(normalWinData, item)
		}
	}

	// 步骤2：计算参数
	// 总中奖率 = (普通中奖数量 + 大倍率数量) / 总数据量
	totalWinCount := len(normalWinData) + len(bigMultiplierData)
	totalWinRate := float64(totalWinCount) / float64(totalSize)

	// 区间数量：如果有大倍率数据，使用大倍率数据数量；否则固定分成10个区间
	segmentCount := len(bigMultiplierData)
	if segmentCount == 0 {
		// 没有大倍率数据时，固定分成10个区间
		segmentCount = 10
	}

	// 每个区间的基础大小（不包含大倍率数据）
	// 基础数据 = 不中奖数据 + 普通中奖数据
	baseDataCount := len(zeroWinData) + len(normalWinData)
	baseSegmentSize := baseDataCount / segmentCount
	if baseSegmentSize == 0 {
		baseSegmentSize = 1 // 确保至少为1
	}

	// 步骤3：打乱数据
	rand.Shuffle(len(zeroWinData), func(i, j int) {
		zeroWinData[i], zeroWinData[j] = zeroWinData[j], zeroWinData[i]
	})
	rand.Shuffle(len(normalWinData), func(i, j int) {
		normalWinData[i], normalWinData[j] = normalWinData[j], normalWinData[i]
	})
	rand.Shuffle(len(bigMultiplierData), func(i, j int) {
		bigMultiplierData[i], bigMultiplierData[j] = bigMultiplierData[j], bigMultiplierData[i]
	})

	// 步骤4：按区间分配数据
	result := make([]GameResultData, 0, totalSize)
	normalWinIdx := 0
	zeroWinIdx := 0

	for seg := 0; seg < segmentCount; seg++ {
		segment := make([]GameResultData, 0)

		// 计算当前区间的实际大小（最后一个区间包含所有剩余数据）
		currentSegmentSize := baseSegmentSize
		if seg == segmentCount-1 {
			// 最后一个区间：包含所有剩余的基础数据
			remainingNormalWin := len(normalWinData) - normalWinIdx
			remainingZeroWin := len(zeroWinData) - zeroWinIdx
			currentSegmentSize = remainingNormalWin + remainingZeroWin
		}

		// 计算当前区间的目标中奖数
		currentTargetWin := int(float64(currentSegmentSize) * totalWinRate)
		if seg == segmentCount-1 {
			// 最后一个区间：使用所有剩余的普通中奖数据
			currentTargetWin = len(normalWinData) - normalWinIdx
		}

		// 分配普通中奖数据到当前区间
		normalWinAdded := 0
		for normalWinAdded < currentTargetWin && normalWinIdx < len(normalWinData) {
			segment = append(segment, normalWinData[normalWinIdx])
			normalWinIdx++
			normalWinAdded++
		}

		// 填充不中奖数据到区间大小
		for len(segment) < currentSegmentSize && zeroWinIdx < len(zeroWinData) {
			segment = append(segment, zeroWinData[zeroWinIdx])
			zeroWinIdx++
		}

		// 打乱区间内的数据（普通中奖和不中奖数据）
		rand.Shuffle(len(segment), func(i, j int) {
			segment[i], segment[j] = segment[j], segment[i]
		})

		// 在区间内随机位置插入大倍率数据
		if seg < len(bigMultiplierData) {
			insertPos := 0
			if len(segment) > 0 {
				insertPos = rng.Intn(len(segment) + 1) // +1 允许插入到末尾
			}

			// 插入大倍率数据
			if insertPos >= len(segment) {
				segment = append(segment, bigMultiplierData[seg])
			} else {
				segment = append(segment, GameResultData{})
				copy(segment[insertPos+1:], segment[insertPos:])
				segment[insertPos] = bigMultiplierData[seg]
			}
		}

		// 将区间数据添加到结果
		result = append(result, segment...)
	}

	// 步骤5：将结果复制回原数组（不改变数据本身，只改变排列顺序）
	copy(data, result)
}
