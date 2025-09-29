package main

import (
	"math"
	"sort"
)

// adjustRTPBalanced 平衡的RTP调整策略，少量替换多个区间以保持倍率分布
func adjustRTPBalanced(data []GameResultData, targetRTP float64, totalBet float64, dataRanges map[string]MultiplierRange, rtpLevel int, maxHighMultiplierCount int) ([]GameResultData, error) {
	result := make([]GameResultData, len(data))
	copy(result, data)

	// 计算当前RTP和目标RTP的差距
	currentRTP := CalculateRTP(result, totalBet)
	rtpGap := targetRTP - currentRTP

	if rtpGap <= 0 {
		return result, nil
	}

	// 计算需要替换的总数据量（基于RTP差距）
	totalDataCount := len(data)
	// 估算需要替换的数据量：RTP差距越大，需要替换的数据越多
	estimatedReplaceCount := int(float64(totalDataCount) * rtpGap * 0.5) // 经验系数
	if estimatedReplaceCount < 10 {
		estimatedReplaceCount = 10 // 最少替换10条数据
	}
	if estimatedReplaceCount > totalDataCount/4 {
		estimatedReplaceCount = totalDataCount / 4 // 最多替换25%的数据
	}

	// 定义替换策略：按配置比例分配替换量
	replaceStrategies := []struct {
		rangeName    string
		configRatio  float64
		replaceRatio float64
		priority     int
	}{
		{"low_multiplier", 0.1214, 0.3, 1},   // 0-1倍：配置12.14%，替换30%的替换量
		{"medium_multiplier", 0.091, 0.4, 2}, // 1-5倍：配置9.1%，替换40%的替换量
		{"high_multiplier", 0.0191, 0.3, 3},  // 5-10倍：配置1.91%，替换30%的替换量
	}

	// 收集可用的高倍率数据
	var availableHighData []GameResultData
	for _, rangeName := range []string{"medium_multiplier", "high_multiplier", "very_high_multiplier"} {
		if len(dataRanges[rangeName].Data) > 0 {
			availableHighData = append(availableHighData, dataRanges[rangeName].Data...)
		}
	}

	// 按金额从大到小排序
	sort.Slice(availableHighData, func(i, j int) bool {
		return availableHighData[i].AW > availableHighData[j].AW
	})

	// 按策略执行替换
	replacedCount := 0
	for _, strategy := range replaceStrategies {
		if replacedCount >= estimatedReplaceCount {
			break
		}

		// 计算该区间需要替换的数量
		rangeReplaceCount := int(float64(estimatedReplaceCount) * strategy.replaceRatio)
		if rangeReplaceCount > estimatedReplaceCount-replacedCount {
			rangeReplaceCount = estimatedReplaceCount - replacedCount
		}

		// 找到该区间的数据
		var itemsInRange []struct {
			index int
			item  GameResultData
		}

		for i, item := range result {
			if item.AW == 0 {
				continue
			}
			multiplier := item.AW / totalBet
			if isInRange(multiplier, dataRanges[strategy.rangeName].Min, dataRanges[strategy.rangeName].Max) {
				itemsInRange = append(itemsInRange, struct {
					index int
					item  GameResultData
				}{i, item})
			}
		}

		// 按金额从小到大排序，优先替换金额较小的数据
		sort.Slice(itemsInRange, func(i, j int) bool {
			return itemsInRange[i].item.AW < itemsInRange[j].item.AW
		})

		// 执行替换
		rangeReplacedCount := 0
		for _, itemInfo := range itemsInRange {
			if rangeReplacedCount >= rangeReplaceCount {
				break
			}

			// 寻找合适的高倍率数据替换
			for _, highItem := range availableHighData {
				if highItem.AW > itemInfo.item.AW {
					result[itemInfo.index] = highItem
					rangeReplacedCount++
					replacedCount++

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

	// 如果还有RTP差距，进行最后的微调
	if CalculateRTP(result, totalBet) < targetRTP {
		return adjustRTPFinalTuning(result, targetRTP, totalBet, dataRanges, rtpLevel)
	}

	return result, nil
}

// adjustRTPFinalTuning 最终RTP微调，确保达到目标RTP
func adjustRTPFinalTuning(data []GameResultData, targetRTP float64, totalBet float64, dataRanges map[string]MultiplierRange, rtpLevel int) ([]GameResultData, error) {
	result := make([]GameResultData, len(data))
	copy(result, data)

	// 收集所有可用的高倍率数据
	var allHighData []GameResultData
	for _, rangeName := range []string{"medium_multiplier", "high_multiplier", "very_high_multiplier", "mega_multiplier"} {
		if len(dataRanges[rangeName].Data) > 0 {
			allHighData = append(allHighData, dataRanges[rangeName].Data...)
		}
	}

	// 按金额从大到小排序
	sort.Slice(allHighData, func(i, j int) bool {
		return allHighData[i].AW > allHighData[j].AW
	})

	// 找到所有低倍率数据，按金额从小到大排序
	var lowMultiplierItems []struct {
		index int
		item  GameResultData
	}

	for i, item := range result {
		if item.AW == 0 {
			continue
		}
		multiplier := item.AW / totalBet
		if multiplier > 0 && multiplier <= 5 { // 替换1-5倍及以下的数据
			lowMultiplierItems = append(lowMultiplierItems, struct {
				index int
				item  GameResultData
			}{i, item})
		}
	}

	// 按金额从小到大排序
	sort.Slice(lowMultiplierItems, func(i, j int) bool {
		return lowMultiplierItems[i].item.AW < lowMultiplierItems[j].item.AW
	})

	// 执行最终替换
	for _, itemInfo := range lowMultiplierItems {
		for _, highItem := range allHighData {
			if highItem.AW > itemInfo.item.AW {
				result[itemInfo.index] = highItem

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

	return result, nil
}

// validateDistributionAccuracy 验证倍率分布准确性
func validateDistributionAccuracy(data []GameResultData, totalBet float64, config *RtpMultiplierDistribution) map[string]float64 {
	// 计算实际分布
	actualDistribution := make(map[string]float64)
	totalCount := len(data)

	// 统计各区间数量
	rangeCounts := make(map[string]int)
	for _, item := range data {
		if item.AW == 0 {
			rangeCounts["zero_win"]++
			continue
		}
		multiplier := item.AW / totalBet
		if multiplier > 0 && multiplier <= 1 {
			rangeCounts["low_multiplier"]++
		} else if multiplier > 1 && multiplier <= 5 {
			rangeCounts["medium_multiplier"]++
		} else if multiplier > 5 && multiplier <= 10 {
			rangeCounts["high_multiplier"]++
		} else if multiplier > 10 && multiplier <= 20 {
			rangeCounts["very_high_multiplier"]++
		} else if multiplier > 20 && multiplier <= 50 {
			rangeCounts["mega_multiplier"]++
		} else if multiplier > 50 && multiplier <= 100 {
			rangeCounts["super_mega_multiplier"]++
		} else if multiplier > 100 && multiplier <= 500 {
			rangeCounts["ultra_mega_multiplier"]++
		}
	}

	// 计算实际占比
	for rangeName, count := range rangeCounts {
		actualDistribution[rangeName] = float64(count) / float64(totalCount)
	}

	// 计算与配置的偏差
	deviation := make(map[string]float64)
	deviation["low_multiplier"] = math.Abs(actualDistribution["low_multiplier"] - config.MultiplierDistribution.LowMultiplier)
	deviation["medium_multiplier"] = math.Abs(actualDistribution["medium_multiplier"] - config.MultiplierDistribution.MediumMultiplier)
	deviation["high_multiplier"] = math.Abs(actualDistribution["high_multiplier"] - config.MultiplierDistribution.HighMultiplier)
	deviation["very_high_multiplier"] = math.Abs(actualDistribution["very_high_multiplier"] - config.MultiplierDistribution.VeryHighMultiplier)
	deviation["mega_multiplier"] = math.Abs(actualDistribution["mega_multiplier"] - config.MultiplierDistribution.MegaMultiplier)
	deviation["super_mega_multiplier"] = math.Abs(actualDistribution["super_mega_multiplier"] - config.MultiplierDistribution.SuperMegaMultiplier)
	deviation["ultra_mega_multiplier"] = math.Abs(actualDistribution["ultra_mega_multiplier"] - config.MultiplierDistribution.UltraMegaMultiplier)

	return deviation
}
