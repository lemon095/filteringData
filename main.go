package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// isGameId 检查参数是否为gameId（对应目录存在）
func isGameId(arg string) bool {
	if gid, err := strconv.Atoi(arg); err == nil {
		gameDir := filepath.Join("output", fmt.Sprintf("%d", gid))
		if st, err2 := os.Stat(gameDir); err2 == nil && st.IsDir() {
			return true
		}
	}
	return false
}

// isGameIdFb 检查参数是否为gameId（对应_fb目录存在）
func isGameIdFb(arg string) bool {
	if gid, err := strconv.Atoi(arg); err == nil {
		gameDir := filepath.Join("output", fmt.Sprintf("%d_fb", gid))
		if st, err2 := os.Stat(gameDir); err2 == nil && st.IsDir() {
			return true
		}
	}
	return false
}

type RtpLevel struct {
	RtpNo float64 `json:"rtpNo"`
	Rtp   float64 `json:"rtp"`
}

var RtpLevels = []RtpLevel{
	{RtpNo: 1, Rtp: 0.7},
	{RtpNo: 2, Rtp: 0.7},
	{RtpNo: 3, Rtp: 0.75},
	{RtpNo: 4, Rtp: 0.8},
	{RtpNo: 5, Rtp: 0.85},
	{RtpNo: 6, Rtp: 0.9},
	{RtpNo: 7, Rtp: 0.91},
	{RtpNo: 8, Rtp: 0.92},
	{RtpNo: 9, Rtp: 0.93},
	{RtpNo: 10, Rtp: 0.94},
	{RtpNo: 11, Rtp: 0.95},
	{RtpNo: 12, Rtp: 0.96},
	{RtpNo: 13, Rtp: 0.97},
	{RtpNo: 14, Rtp: 1.5},
	{RtpNo: 15, Rtp: 2},
}

var FbRtpLevels = []RtpLevel{
	{RtpNo: 1, Rtp: 0.7},
	{RtpNo: 2, Rtp: 0.7},
	{RtpNo: 3, Rtp: 0.75},
	{RtpNo: 4, Rtp: 0.8},
	{RtpNo: 5, Rtp: 0.8},
	{RtpNo: 6, Rtp: 0.8},
	{RtpNo: 7, Rtp: 0.8},
	{RtpNo: 8, Rtp: 0.8},
	{RtpNo: 9, Rtp: 0.8},
	{RtpNo: 10, Rtp: 0.8},
	{RtpNo: 11, Rtp: 0.8},
	{RtpNo: 12, Rtp: 0.8},
	{RtpNo: 13, Rtp: 0.8},
	{RtpNo: 14, Rtp: 0.8},
	{RtpNo: 15, Rtp: 0.8},
}

// 保证并发任务按块输出日志
var outputMu sync.Mutex

// runRtpTest 执行单次RTP测试
func runRtpTest(db *Database, config *Config, rtpLevel float64, rtp float64, testNumber int, totalBet float64, winDataAll []GameResultData, noWinDataAll []GameResultData) error {
	var logBuf bytes.Buffer
	printf := func(format string, a ...interface{}) {
		fmt.Fprintf(&logBuf, format, a...)
	}
	testStartTime := time.Now()
	// 任务头分隔线
	printf("\n========== [TASK BEGIN] RtpNo: %.0f | Test: %d | %s =========\n", rtpLevel, testNumber, time.Now().Format(time.RFC3339))
	//计算允许中的金额
	allowWin := totalBet * rtp

	//从所有中奖数据, 中随机获取, 但是大奖, 巨奖, 超级巨奖不能大于配置的值
	bigNum := int(float64(config.Tables.DataNum) * config.PrizeRatios.BigPrize)
	megaNum := int(float64(config.Tables.DataNum) * config.PrizeRatios.MegaPrize)
	superMegaNum := int(float64(config.Tables.DataNum) * config.PrizeRatios.SuperMegaPrize)

	// 使用共享只读中奖数据
	printf("\n获取到中奖数据: %d条\n", len(winDataAll))
	printf("档位: %.0f, 目标RTP: %.4f, 允许中奖金额: %.2f\n", rtpLevel, rtp, allowWin)

	// 第一步：从中奖数据中填充, 直到达到目标金额或数量限制
	var data []GameResultData
	var totalWin float64 = 0
	bigCount := 0
	megaCount := 0
	superMegaCount := 0

	// 每任务独立随机源与乱序索引（避免共享切片原地打乱）
	seed := time.Now().UnixNano() ^ int64(config.Game.ID)*1_000_003 ^ int64(testNumber)*1_000_033 ^ int64(rtpLevel)*1_000_037
	rng := rand.New(rand.NewSource(seed))
	permWin := rng.Perm(len(winDataAll))

	// 特殊处理RtpNo为15的情况
	isSpecialRtp15 := rtpLevel == 15
	var targetRtpMin, targetRtpMax float64
	if isSpecialRtp15 {
		targetRtpMin = 1.9
		targetRtpMax = 2.0
		fmt.Printf("🎯 RtpNo为%.0f,特殊处理：目标RTP范围 [%.1f, %.1f], 允许偏差 ±0.005\n", rtpLevel, targetRtpMin, targetRtpMax)
	}

	for _, idx := range permWin {
		item := winDataAll[idx]
		// 检查是否已经达到数量限制（RTP 2.0特殊处理）
		if rtp >= 2.0 && len(data) >= config.Tables.DataNum {
			printf("⚠️ RTP %.0f档位：已达到数量限制 %d 条, 停止添加中奖数据\n", rtpLevel, config.Tables.DataNum)
			break
		}

		// 判断本条是什么奖励配额（仅检查, 不计数, 计数在成功加入后再做）
		gwt := item.GWT
		switch gwt {
		case 2: // 大奖
			if bigCount >= bigNum {
				continue
			}
		case 3: // 巨奖
			if megaCount >= megaNum {
				continue
			}
		case 4: // 超级巨奖
			if superMegaCount >= superMegaNum {
				continue
			}
		}

		// 计算加入这条数据后的总中奖金额（先计算, 再决定是否加入）
		newTotalWin := totalWin + item.AW
		if newTotalWin > allowWin*1.005 {
			continue
		}

		// 特殊处理RtpNo为15：检查RTP是否在允许范围内（基于加入后的新值判断）
		if isSpecialRtp15 {
			newRtp := newTotalWin / totalBet
			if newRtp > targetRtpMax {
				continue // 如果RTP超过上限, 跳过这条数据
			}
		}
		totalWin += item.AW
		// 添加数据并更新累计
		data = append(data, item)
		// 成功加入后再更新对应奖励计数
		switch gwt {
		case 2:
			bigCount++
		case 3:
			megaCount++
		case 4:
			superMegaCount++
		}
		//这里应该是计算偏差
		if rtpLevel != 15 && totalWin >= allowWin && totalWin <= allowWin*(1+0.005) {
			printf("达到目标范围中奖金额, 当前中奖总额: %.2f, 目标中奖金额: %.2f\n", totalWin, allowWin)
			break
		}

		// 特殊处理RtpNo为15：如果RTP已经达到下限, 可以继续添加更多数据
		if isSpecialRtp15 {
			currentRtp := totalWin / totalBet
			if currentRtp >= targetRtpMin {
				// 如果RTP已经达到下限, 可以继续添加数据直到达到数量限制
				if len(data) >= config.Tables.DataNum {
					fmt.Printf("🎯 RtpNo为:%.0f,已达到数量限制 %d 条, 当前RTP: %.4f, 目标RTP: %.4f\n", rtpLevel, config.Tables.DataNum, currentRtp, rtp)
					break
				}
			}
		}
	}
	fmt.Printf("⚠️ !!!当前中奖总额 %.2f 目标 %.2f,据...\n", totalWin, allowWin)
	// 检查是否达到目标中奖金额, 如果没有达到则补充数据
	if totalWin < allowWin {
		fmt.Print("当前金额小于目标金额，")
		if rtpLevel != 15 {
			fmt.Printf("⚠️ 当前中奖总额 %.2f 未达到目标 %.2f, 开始补充数据...\n", totalWin, allowWin)

			// 计算需要补充的中奖金额
			remainingWin := (allowWin - totalWin) * 1.005
			fmt.Printf("🔍 需要补充中奖金额: %.2f\n", remainingWin)

			// 收集已使用的数据ID, 用于排除
			usedIds := make([]int, 0, len(data))
			for _, item := range data {
				usedIds = append(usedIds, item.ID)
			}

			// 第一步：尝试找到一条数据就能满足条件的情况（允许0.005偏差）
			bestSingleMatch, err := db.GetBestSingleMatch(remainingWin, usedIds, 0.005)
			if err != nil {
				printf("⚠️ 查询最佳匹配数据失败: %v\n", err)
			} else if bestSingleMatch != nil {
				// 检查这条数据是否超过大奖、巨奖、超级巨奖的数量限制
				canAdd := true
				switch bestSingleMatch.GWT {
				case 2: // 大奖
					if bigCount >= bigNum {
						canAdd = false
						printf("⚠️ 大奖数量已达上限, 跳过: AW=%.2f, GWT=%d\n", bestSingleMatch.AW, bestSingleMatch.GWT)
					}
				case 3: // 巨奖
					if megaCount >= megaNum {
						canAdd = false
						printf("⚠️ 巨奖数量已达上限, 跳过: AW=%.2f, GWT=%d\n", bestSingleMatch.AW, bestSingleMatch.GWT)
					}
				case 4: // 超级巨奖
					if superMegaCount >= superMegaNum {
						canAdd = false
						printf("⚠️ 超级巨奖数量已达上限, 跳过: AW=%.2f, GWT=%d\n", bestSingleMatch.AW, bestSingleMatch.GWT)
					}
				}

				if canAdd {
					// 添加数据并更新计数
					data = append(data, *bestSingleMatch)
					totalWin += bestSingleMatch.AW

					// 更新大奖、巨奖、超级巨奖计数
					switch bestSingleMatch.GWT {
					case 2: // 大奖
						bigCount++
					case 3: // 巨奖
						megaCount++
					case 4: // 超级巨奖
						superMegaCount++
					}

					printf("✅ 找到单条数据满足条件: AW=%.2f, 当前中奖总额: %.2f, 目标: %.2f\n",
						bestSingleMatch.AW, totalWin, allowWin)
				} else {
					// 如果因为数量限制无法添加, 则使用多条数据补充逻辑
					printf("🔍 单条数据因数量限制无法添加, 使用多条数据补充\n")
					bestSingleMatch = nil
				}
			}

			// 第二步：如果没有找到合适的单条数据, 则使用多条数据补充
			if bestSingleMatch == nil {
				printf("🔍 没有单条数据满足条件, 使用多条数据补充\n")

				// 使用数据库查询获取适合的填充数据, 限制100条
				fillData, err := db.GetWinDataForFilling(remainingWin, usedIds, 100)
				if err != nil {
					printf("⚠️ 查询填充数据失败: %v, 回退到原始逻辑\n", err)
					// 回退到原始逻辑
					for _, idx := range permWin {
						item := winDataAll[idx]
						// 跳过精度有问题的数据

						// 检查大奖、巨奖、超级巨奖的数量限制
						switch item.GWT {
						case 2: // 大奖
							if bigCount >= bigNum {
								continue // 大奖数量已达上限, 跳过
							}
						case 3: // 巨奖
							if megaCount >= megaNum {
								continue // 巨奖数量已达上限, 跳过
							}
						case 4: // 超级巨奖
							if superMegaCount >= superMegaNum {
								continue // 超级巨奖数量已达上限, 跳过
							}
						}

						// 如果这条数据的中奖金额小于等于remainingWin, 则添加
						if item.AW <= remainingWin && item.AW > 0 {
							// 添加数据
							data = append(data, item)
							totalWin += item.AW
							remainingWin -= item.AW

							// 更新大奖、巨奖、超级巨奖计数
							switch item.GWT {
							case 2: // 大奖
								bigCount++
							case 3: // 巨奖
								megaCount++
							case 4: // 超级巨奖
								superMegaCount++
							}

							printf("➕ 补充数据: AW=%.2f, GWT=%d, 剩余需要: %.2f\n", item.AW, item.GWT, remainingWin)

							// 如果已经达到或超过目标, 停止补充
							if totalWin >= allowWin {
								printf("✅ 补充完成！当前中奖总额: %.2f, 目标: %.2f\n", totalWin, allowWin)
								break
							}
						}
					}
				} else {
					// 使用数据库查询结果进行填充
					printf("🔍 数据库查询到 %d 条候选填充数据\n", len(fillData))

					filledAny := false
					for _, item := range fillData {
						// 检查大奖、巨奖、超级巨奖的数量限制
						switch item.GWT {
						case 2: // 大奖
							if bigCount >= bigNum {
								continue // 大奖数量已达上限, 跳过
							}
						case 3: // 巨奖
							if megaCount >= megaNum {
								continue // 巨奖数量已达上限, 跳过
							}
						case 4: // 超级巨奖
							if superMegaCount >= superMegaNum {
								continue // 超级巨奖数量已达上限, 跳过
							}
						}

						// 如果这条数据的中奖金额小于等于remainingWin, 则添加
						if item.AW <= remainingWin && item.AW > 0 {
							// 添加数据
							data = append(data, item)
							totalWin += item.AW
							remainingWin -= item.AW

							// 更新大奖、巨奖、超级巨奖计数
							switch item.GWT {
							case 2: // 大奖
								bigCount++
							case 3: // 巨奖
								megaCount++
							case 4: // 超级巨奖
								superMegaCount++
							}

							printf("➕ 补充数据: AW=%.2f, GWT=%d, 剩余需要: %.2f\n", item.AW, item.GWT, remainingWin)

							filledAny = true
							// 如果已经达到或超过目标, 停止补充
							if totalWin >= allowWin {
								printf("✅ 补充完成！当前中奖总额: %.2f, 目标: %.2f\n", totalWin, allowWin)
								break
							}
						}
					}
					if !filledAny {
						printf("⚠️ 本次候选未能补充任何数据, remainingWin=%.2f\n", remainingWin)
					}
				}
			}

			printf("选取中奖数据: %d条, 中奖总额: %.2f\n", len(data), totalWin)
			printf("大奖: %d/%d, 巨奖: %d/%d, 超级巨奖: %d/%d\n",
				bigCount, bigNum, megaCount, megaNum, superMegaCount, superMegaNum)

			// 最终检查
			if totalWin < allowWin {
				printf("⚠️ 即使补充后仍未达到目标, 当前: %.2f, 目标: %.2f\n", totalWin, allowWin)
				printf("⚠️ RTP偏差: %.6f (当前: %.6f, 目标: %.6f)\n",
					math.Abs(totalWin/totalBet-rtp), totalWin/totalBet, rtp)
			} else {
				printf("✅ 补充后达到目标, 当前: %.2f, 目标: %.2f\n", totalWin, allowWin)
				printf("✅ RTP偏差: %.6f (当前: %.6f, 目标: %.6f)\n",
					math.Abs(totalWin/totalBet-rtp), totalWin/totalBet, rtp)
			}

		} else {
			//15档位只需要判断是否达到下限即可，目前看暂时不需要这段逻辑，因为采集数据量可以支撑
			//不符合rtpLevel条件
			printf("⚠️ 特殊15档位rtpLevel条件, rtpLevel: %.0f,totalWin: %.2f, allowWin: %.2f, ...\n", rtpLevel, totalWin, allowWin)
		}
	}

	// 第二步：用不中奖数据补全到1万条
	needNum := config.Tables.DataNum - len(data)
	fmt.Printf("📊 数据量统计: 目标 %d 条, 已有中奖数据 %d 条, 需要补全 %d 条\n",
		config.Tables.DataNum, len(data), needNum)

	if needNum > 0 {
		// 使用共享只读的不中奖数据, 任务内自建乱序索引
		fmt.Printf("获取到不中奖数据: %d条, 需要补全: %d条\n", len(noWinDataAll), needNum)

		if len(noWinDataAll) > 0 {
			// 使用与本任务相同的 rng 生成不中奖数据的乱序索引
			permNo := rng.Perm(len(noWinDataAll))
			// 补全数据, 如果不中奖数据不够则重复使用
			for i := 0; i < needNum; i++ {
				idx := permNo[i%len(permNo)]
				data = append(data, noWinDataAll[idx])
			}
		} else {
			// 如果没有不中奖数据, 用中奖数据重复填充（这种情况很少见）
			fmt.Printf("⚠️ 没有不中奖数据, 使用中奖数据重复填充\n")
			for i := 0; i < needNum; i++ {
				idx := permWin[i%len(permWin)]
				data = append(data, winDataAll[idx])
			}
		}
	}

	// 重新计算最终RTP（包含所有数据）
	var finalTotalWin float64
	for _, item := range data {
		finalTotalWin += item.AW
	}
	finalRTP := finalTotalWin / totalBet

	// 计算RTP偏差
	rtpDeviation := math.Abs(finalRTP - rtp)
	printf("✅ 档位: %.0f,📊 最终统计: 总投注 %.2f, 总中奖 %.2f, 实际RTP %.6f, 目标: %0.6f,实际金额: %.2f,预期金额下限: %.2f,预期金额上限: %.2f, RTP偏差: %.6f \n", rtpLevel, totalBet, finalTotalWin, finalRTP, rtp, finalTotalWin, allowWin, allowWin*(1+0.005), rtpDeviation)

	// 最终验证数据量
	printf("🔍 最终验证: 期望 %d 条, 实际 %d 条\n", config.Tables.DataNum, len(data))
	if len(data) != config.Tables.DataNum {
		return fmt.Errorf("❌ 数据量不匹配：期望 %d 条, 实际 %d 条", config.Tables.DataNum, len(data))
	}
	// 特殊处理RtpNo为15：验证RTP是否在允许范围内
	if isSpecialRtp15 {
		if finalRTP < targetRtpMin || finalRTP > targetRtpMax {
			return fmt.Errorf("❌ RtpNo为15的RTP验证失败: 当前RTP %.4f 不在允许范围 [%.1f, %.1f] 内", finalRTP, targetRtpMin, targetRtpMax)
		}
		fmt.Printf("🎯 RtpNo为15 RTP验证通过: %.4f 在范围 [%.1f, %.1f] 内\n", finalRTP, targetRtpMin, targetRtpMax)
	}

	//这里的随机data顺序呢
	rand.Shuffle(len(data), func(i, j int) {
		data[i], data[j] = data[j], data[i]
	})
	var outputDir string = filepath.Join("output", fmt.Sprintf("%d", config.Game.ID))
	if err := saveToJSON(data, config, rtpLevel, testNumber, outputDir); err != nil {
		return fmt.Errorf("保存CSV文件失败: %v", err)
	}

	// 任务尾分隔线
	printf("========== [TASK END]   RtpNo: %.0f | Test: %d =========\n\n", rtpLevel, testNumber)
	// printf("📊 数据已保存到JSON文件: %s\n", filePath)
	printf("⏱️  RTP等级 %.0f (第%d次生成) 耗时: %v\n", rtpLevel, testNumber, time.Since(testStartTime))
	outputMu.Lock()
	fmt.Print(logBuf.String())
	outputMu.Unlock()
	return nil
}

// runRtpTest2 执行单次RTP测试 - 新的四阶段策略版本
func runRtpTest2(db *Database, config *Config, rtpLevel float64, rtp float64, testNumber int, totalBet float64, winDataAll []GameResultData, noWinDataAll []GameResultData, profitDataAll []GameResultData) error {
	var logBuf bytes.Buffer
	printf := func(format string, a ...interface{}) {
		fmt.Fprintf(&logBuf, format, a...)
	}
	testStartTime := time.Now()

	// 任务头分隔线
	printf("\n========== [TASK BEGIN V2] RtpNo: %.0f | Test: %d | %s =========\n", rtpLevel, testNumber, time.Now().Format(time.RFC3339))

	// 计算允许中奖金额和配置参数
	allowWin := totalBet * rtp
	upperBound := allowWin * (1 + config.StageRatios.UpperDeviation)
	perSpinBet := config.Bet.CS * config.Bet.ML * config.Bet.BL

	// 计算奖项数量限制
	bigNum := int(float64(config.Tables.DataNum) * config.PrizeRatios.BigPrize)
	megaNum := int(float64(config.Tables.DataNum) * config.PrizeRatios.MegaPrize)
	superMegaNum := int(float64(config.Tables.DataNum) * config.PrizeRatios.SuperMegaPrize)

	printf("档位: %.0f, 目标RTP: %.4f, 允许中奖金额: %.2f, 上限: %.2f\n", rtpLevel, rtp, allowWin, upperBound)
	printf("候选数据: win(not-profit)=%d, profit=%d, nowin=%d\n", len(winDataAll), len(profitDataAll), len(noWinDataAll))
	printf("奖项限制: 大奖=%d, 巨奖=%d, 超级巨奖=%d\n", bigNum, megaNum, superMegaNum)

	// 随机源
	seed := time.Now().UnixNano() ^ int64(config.Game.ID)*1_000_003 ^ int64(testNumber)*1_000_033 ^ int64(rtpLevel)*1_000_037
	rng := rand.New(rand.NewSource(seed))

	// 结果容器和计数器
	var data []GameResultData
	var totalWin float64
	targetCount := config.Tables.DataNum
	bigCount := 0
	megaCount := 0
	superMegaCount := 0

	// 特殊处理RtpNo为15的情况
	isSpecialRtp15 := rtpLevel == 15
	var targetRtpMin, targetRtpMax float64
	if isSpecialRtp15 {
		targetRtpMin = 1.9
		targetRtpMax = 2.0
		printf("🎯 RtpNo为%.0f,特殊处理：目标RTP范围 [%.1f, %.1f], 允许偏差 ±0.005\n", rtpLevel, targetRtpMin, targetRtpMax)
	}

	// 已使用ID，避免重复
	used := make(map[int]struct{}, targetCount)

	// 辅助函数：尝试加入一条记录（检查奖项限制、去重、上限）
	tryAppend := func(item GameResultData) bool {
		if _, ok := used[item.ID]; ok {
			return false
		}

		// 检查奖项数量限制
		switch item.GWT {
		case 2: // 大奖
			if bigCount >= bigNum {
				return false
			}
		case 3: // 巨奖
			if megaCount >= megaNum {
				return false
			}
		case 4: // 超级巨奖
			if superMegaCount >= superMegaNum {
				return false
			}
		}

		if item.AW <= 0 {
			return false
		}

		// 检查是否超过上限
		if totalWin+item.AW > upperBound {
			return false
		}

		// 特殊处理RtpNo为15：检查RTP是否在允许范围内
		if isSpecialRtp15 {
			newRtp := (totalWin + item.AW) / totalBet
			if newRtp > targetRtpMax {
				return false
			}
		}

		// 添加数据并更新计数
		data = append(data, item)
		totalWin += item.AW
		used[item.ID] = struct{}{}

		// 更新奖项计数
		switch item.GWT {
		case 2:
			bigCount++
		case 3:
			megaCount++
		case 4:
			superMegaCount++
		}
		return true
	}

	// 随机化阶段1比例
	stage1Ratio := config.StageRatios.Stage1MinRatio + rng.Float64()*(config.StageRatios.Stage1MaxRatio-config.StageRatios.Stage1MinRatio)
	stage1Count := int(math.Round(float64(targetCount) * stage1Ratio))

	// 阶段1：打乱 winDataAll，单轮无放回采样
	if len(winDataAll) > 0 && stage1Count > 0 {
		perm := rng.Perm(len(winDataAll))
		for _, idx := range perm {
			if len(data) >= stage1Count {
				break
			}
			_ = tryAppend(winDataAll[idx])
		}
		printf("阶段1：已加入 %d 条（目标 %.1f%%=%d），累计中奖=%.2f\n", len(data), stage1Ratio*100, stage1Count, totalWin)
	}

	// 阶段2：动态占比（profit vs win），根据缺口/剩余名额决定倾向
	if totalWin < allowWin && len(data) < targetCount && (len(profitDataAll) > 0 || len(winDataAll) > 0) {
		permProfit := rng.Perm(len(profitDataAll))
		permWin2 := rng.Perm(len(winDataAll))
		pi, wi := 0, 0

		// 估算初始倾向
		remainingSlots := targetCount - len(data)
		remainingWin := allowWin - totalWin
		needFactor := 0.0
		if remainingSlots > 0 {
			needFactor = remainingWin / (perSpinBet * float64(remainingSlots))
		}
		basePProfit := needFactor
		if basePProfit < 0.2 {
			basePProfit = 0.2
		}
		if basePProfit > 0.8 {
			basePProfit = 0.8
		}
		printf("阶段2：动态占比起始 pProfit=%.3f (needFactor=%.3f)\n", basePProfit, needFactor)

		maxOuter := len(profitDataAll) + len(winDataAll) + 1024
		for outer := 0; outer < maxOuter; outer++ {
			if totalWin >= allowWin || len(data) >= targetCount {
				break
			}

			// 实时更新占比
			remainingSlots = targetCount - len(data)
			remainingWin = allowWin - totalWin
			if remainingSlots <= 0 || remainingWin <= 0 {
				break
			}
			needFactor = remainingWin / (perSpinBet * float64(remainingSlots))
			pProfit := needFactor
			if pProfit < 0.2 {
				pProfit = 0.2
			}
			if pProfit > 0.8 {
				pProfit = 0.8
			}

			chooseProfit := rng.Float64() < pProfit
			appended := false

			if chooseProfit && pi < len(permProfit) {
				for pi < len(permProfit) {
					cand := profitDataAll[permProfit[pi]]
					pi++
					if tryAppend(cand) {
						appended = true
						break
					}
				}
			}

			// 若未能加入或无可用 profit，则尝试 win
			if !appended && wi < len(permWin2) {
				for wi < len(permWin2) {
					cand := winDataAll[permWin2[wi]]
					wi++
					if tryAppend(cand) {
						appended = true
						break
					}
				}
			}

			// 若先选 win 失败，再尝试 profit 兜底
			if !appended && !chooseProfit && pi < len(permProfit) {
				for pi < len(permProfit) {
					cand := profitDataAll[permProfit[pi]]
					pi++
					if tryAppend(cand) {
						appended = true
						break
					}
				}
			}

			// 两边都无法加入，提前退出
			if !appended {
				break
			}
		}
		printf("阶段2完成：累计中奖=%.2f, 目标=%.2f, 数量=%d/%d\n", totalWin, allowWin, len(data), targetCount)
	}

	// 阶段3：若还需要补充（数量未达标），先用 winDataAll 的大额补充
	if len(data) < targetCount {
		remainingSlots := targetCount - len(data)
		stage3aSlots := int(math.Ceil(float64(remainingSlots) * config.StageRatios.Stage3WinTopRatio))

		if stage3aSlots > 0 && len(winDataAll) > 0 {
			// winDataAll 按 aw DESC
			winDesc := make([]GameResultData, len(winDataAll))
			copy(winDesc, winDataAll)
			sort.Slice(winDesc, func(i, j int) bool { return winDesc[i].AW > winDesc[j].AW })
			for _, it := range winDesc {
				if stage3aSlots == 0 || len(data) >= targetCount {
					break
				}
				if tryAppend(it) {
					stage3aSlots--
				}
			}
		}

		// 阶段3b：剩余名额根据缺口大小，用 profitDataAll 小额或大额补齐
		if len(data) < targetCount {
			remainingSlots = targetCount - len(data)
			remainingWin := allowWin - totalWin
			gapSmallThreshold := math.Max(perSpinBet, allowWin*0.02) // 小缺口阈值

			// 若金额已足或接近上限，则直接跳过到数量兜底
			if remainingWin > 0 && len(profitDataAll) > 0 {
				// 按需选择排序方向
				profit := make([]GameResultData, len(profitDataAll))
				copy(profit, profitDataAll)
				if remainingWin <= gapSmallThreshold {
					sort.Slice(profit, func(i, j int) bool { return profit[i].AW < profit[j].AW }) // 小额优先
				} else {
					sort.Slice(profit, func(i, j int) bool { return profit[i].AW > profit[j].AW }) // 大额优先
				}

				for _, it := range profit {
					if remainingSlots == 0 || len(data) >= targetCount {
						break
					}
					// 若已经达到目标金额，仅在不超过上限时允许继续；核心由上限约束
					if tryAppend(it) {
						remainingSlots--
						remainingWin = allowWin - totalWin
						if remainingWin <= 0 {
							// 金额已达标，后续数量不足交由阶段4处理
							break
						}
					}
				}
			}
		}
		printf("阶段3完成：累计中奖=%.2f, 数量=%d/%d\n", totalWin, len(data), targetCount)
	}

	// 阶段4：数量兜底，优先无放回补不中奖；若仍不足，再允许重复不中奖补满
	if len(data) < targetCount && len(noWinDataAll) > 0 {
		need := targetCount - len(data)
		// 先无放回
		perm := rng.Perm(len(noWinDataAll))
		for _, idx := range perm {
			if need == 0 {
				break
			}
			item := noWinDataAll[idx]
			if _, ok := used[item.ID]; ok {
				continue
			}
			data = append(data, item)
			used[item.ID] = struct{}{}
			need--
		}
		// 再重复补齐（仅对不中奖允许重复，以保证条数）
		if need > 0 {
			for i := 0; i < need; i++ {
				data = append(data, noWinDataAll[i%len(noWinDataAll)])
			}
		}
		printf("阶段4完成：补充不中奖数据，最终数量=%d/%d\n", len(data), targetCount)
	}

	// 重新计算最终RTP（包含所有数据）
	var finalTotalWin float64
	for _, item := range data {
		finalTotalWin += item.AW
	}
	finalRTP := finalTotalWin / totalBet

	// 计算RTP偏差
	rtpDeviation := math.Abs(finalRTP - rtp)
	printf("✅ 档位: %.0f,📊 最终统计: 总投注 %.2f, 总中奖 %.2f, 实际RTP %.6f, 目标: %0.6f, RTP偏差: %.6f\n", rtpLevel, totalBet, finalTotalWin, finalRTP, rtp, rtpDeviation)
	printf("🔍 奖项统计: 大奖: %d/%d, 巨奖: %d/%d, 超级巨奖: %d/%d\n", bigCount, bigNum, megaCount, megaNum, superMegaCount, superMegaNum)

	// 最终验证数据量
	printf("🔍 最终验证: 期望 %d 条, 实际 %d 条\n", targetCount, len(data))
	if len(data) != targetCount {
		return fmt.Errorf("❌ 数据量不匹配：期望 %d 条, 实际 %d 条", targetCount, len(data))
	}

	// 特殊处理RtpNo为15：验证RTP是否在允许范围内
	if isSpecialRtp15 {
		if finalRTP < targetRtpMin || finalRTP > targetRtpMax {
			return fmt.Errorf("❌ RtpNo为15的RTP验证失败: 当前RTP %.4f 不在允许范围 [%.1f, %.1f] 内", finalRTP, targetRtpMin, targetRtpMax)
		}
		printf("🎯 RtpNo为15 RTP验证通过: %.4f 在范围 [%.1f, %.1f] 内\n", finalRTP, targetRtpMin, targetRtpMax)
	}

	// 重复率统计（按 id 去重）
	uniq := make(map[int]int, len(data))
	for _, it := range data {
		uniq[it.ID]++
	}
	dupCount := 0
	for _, c := range uniq {
		if c > 1 {
			dupCount += c - 1
		}
	}
	dupRate := 0.0
	if n := len(data); n > 0 {
		dupRate = float64(dupCount) / float64(n)
	}
	printf("🔎 去重统计: 总数=%d, 唯一=%d, 重复=%d, 重复率=%.4f\n", len(data), len(uniq), dupCount, dupRate)

	// 打乱输出顺序
	rand.Shuffle(len(data), func(i, j int) {
		data[i], data[j] = data[j], data[i]
	})

	var outputDir string = filepath.Join("output", fmt.Sprintf("%d", config.Game.ID))
	if err := saveToJSON(data, config, rtpLevel, testNumber, outputDir); err != nil {
		return fmt.Errorf("保存JSON文件失败: %v", err)
	}

	// 任务尾分隔线
	printf("========== [TASK END V2]   RtpNo: %.0f | Test: %d =========\n\n", rtpLevel, testNumber)
	printf("⏱️  RTP等级 %.0f (第%d次生成V2) 耗时: %v\n", rtpLevel, testNumber, time.Since(testStartTime))

	outputMu.Lock()
	fmt.Print(logBuf.String())
	outputMu.Unlock()
	return nil
}

func saveToJSON(data []GameResultData, config *Config, rtpLevel float64, testNumber int, outputDir string) error {
	// 创建输出目录：按游戏ID分目录，例如 output/93
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("创建输出目录失败: %v", err)
	}

	// 生成文件名：output_table_prefix_RtpNo_第几次.json
	fileName := fmt.Sprintf("%s%.0f_%d.json", config.Tables.OutputTablePrefix, rtpLevel, testNumber)
	filePath := filepath.Join(outputDir, fileName)

	// 准备要保存的数据结构
	type OutputData struct {
		RtpLevel int                      `json:"rtpLevel"`
		SrNumber int                      `json:"srNumber"`
		Data     []map[string]interface{} `json:"data"`
	}

	// 转换数据为字典数组格式
	var jsonData []map[string]interface{}
	for _, item := range data {
		row := map[string]interface{}{
			"tb":  item.TB,
			"aw":  item.AW,
			"gwt": item.GWT,
			"sp":  item.SP,
			"fb":  item.FB,
			"gd":  item.GD.Data,
		}
		jsonData = append(jsonData, row)
	}

	// 构建输出数据
	outputData := OutputData{
		RtpLevel: int(rtpLevel),
		SrNumber: testNumber,
		Data:     jsonData,
	}

	// 将数据转换为压缩的JSON
	jsonBytes, err := json.Marshal(outputData)
	if err != nil {
		return fmt.Errorf("JSON序列化失败: %v", err)
	}

	// 写入文件
	if err := os.WriteFile(filePath, jsonBytes, 0644); err != nil {
		return fmt.Errorf("写入JSON文件失败: %v", err)
	}

	fmt.Printf("📊 数据已保存到JSON文件: %s\n", filePath)
	return nil
}

func main() {
	// 检查命令行参数
	if len(os.Args) < 2 {
		fmt.Println("使用方法:")
		fmt.Println("  ./filteringData generate                    # 生成RTP测试数据并保存到JSON文件")
		fmt.Println("  ./filteringData generate2                   # 生成RTP测试数据V2（四阶段策略）")
		fmt.Println("  ./filteringData import                     # 导入output目录下的所有JSON文件到数据库")
		fmt.Println("  ./filteringData import [fileLevelId]       # 只导入指定fileLevelId的JSON文件")
		fmt.Println("")
		fmt.Println("示例:")
		fmt.Println("  ./filteringData import                     # 导入所有文件")
		fmt.Println("  ./filteringData import 1                   # 只导入GameResults_1_*.json文件")
		fmt.Println("  ./filteringData import 93                  # 只导入GameResults_93_*.json文件")
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "generate":
		runGenerateMode()
	case "generate2":
		runGenerateMode2()
	case "import":
		// 支持多环境导入：
		// 1) ./filteringData import                      → 使用默认环境导入全部
		// 2) ./filteringData import <gameId>             → 使用默认环境导入 output/<gameId>/
		// 3) ./filteringData import <levelId>            → 使用默认环境导入指定level
		// 4) ./filteringData import <gameId> <env>       → 使用指定环境导入 output/<gameId>/
		// 5) ./filteringData import <levelId> <env>      → 使用指定环境导入指定level
		// 6) ./filteringData import <gameId> <level> <env> → 使用指定环境导入指定gameId和level
		if len(os.Args) == 2 {
			// ./filteringData import
			runImportMode("", "")
		} else if len(os.Args) == 3 {
			arg := os.Args[2]
			if isGameId(arg) {
				// ./filteringData import <gameId> - 目录存在，当作gameId处理
				gid, _ := strconv.Atoi(arg)
				runImportModeWithGameId(gid, "", "")
			} else {
				// ./filteringData import <levelId> - 目录不存在，当作levelId处理
				// 将在 output/<config.Game.ID>/ 目录下查找包含该levelId的文件
				runImportMode(arg, "")
			}
		} else if len(os.Args) == 4 {
			arg1, arg2 := os.Args[2], os.Args[3]
			if isGameId(arg1) && IsEnv(arg2) {
				// ./filteringData import <gameId> <env>
				gid, _ := strconv.Atoi(arg1)
				env := ResolveEnv(arg2)
				runImportModeWithGameId(gid, "", env)
			} else if IsEnv(arg2) {
				// ./filteringData import <levelId> <env>
				env := ResolveEnv(arg2)
				runImportMode(arg1, env)
			} else if isGameId(arg1) {
				// ./filteringData import <gameId> <level>
				gid, _ := strconv.Atoi(arg1)
				runImportModeWithGameId(gid, arg2, "")
			} else {
				fmt.Printf("❌ 参数错误: 无法识别参数组合\n")
				os.Exit(1)
			}
		} else if len(os.Args) == 5 {
			// ./filteringData import <gameId> <level> <env>
			gidStr, lvl, envStr := os.Args[2], os.Args[3], os.Args[4]
			gid, err := strconv.Atoi(gidStr)
			if err != nil {
				fmt.Printf("❌ 参数错误: gameId 必须为整数\n")
				os.Exit(1)
			}
			env := ResolveEnv(envStr)
			runImportModeWithGameId(gid, lvl, env)
		} else {
			fmt.Printf("❌ 参数错误: import 命令参数过多\n")
			fmt.Println("用法1: ./filteringData import")
			fmt.Println("用法2: ./filteringData import <gameId>")
			fmt.Println("用法3: ./filteringData import <levelId>")
			fmt.Println("用法4: ./filteringData import <gameId> <env>")
			fmt.Println("用法5: ./filteringData import <levelId> <env>")
			fmt.Println("用法6: ./filteringData import <gameId> <level> <env>")
			fmt.Println("\n环境代码: local/l, hk-test/ht, br-test/bt, br-prod/bp, us-prod/up, hk-prod/hp")
			os.Exit(1)
		}
	case "generateFb":
		runGenerateFbMode()
	case "importFb":
		// 支持多环境购买夺宝导入：
		// 1) ./filteringData importFb                      → 使用默认环境导入全部_fb
		// 2) ./filteringData importFb <gameId>             → 使用默认环境导入 output/<gameId>_fb/
		// 3) ./filteringData importFb <levelId>            → 使用默认环境导入指定level
		// 4) ./filteringData importFb <gameId> <env>       → 使用指定环境导入 output/<gameId>_fb/
		// 5) ./filteringData importFb <levelId> <env>      → 使用指定环境导入指定level
		// 6) ./filteringData importFb <gameId> <level> <env> → 使用指定环境导入指定gameId和level
		if len(os.Args) == 2 {
			// ./filteringData importFb
			runImportFbMode("", "")
		} else if len(os.Args) == 3 {
			arg := os.Args[2]
			if isGameIdFb(arg) {
				// ./filteringData importFb <gameId>
				gid, _ := strconv.Atoi(arg)
				runImportFbModeWithGameId(gid, "", "")
			} else {
				// ./filteringData importFb <levelId>
				runImportFbMode(arg, "")
			}
		} else if len(os.Args) == 4 {
			arg1, arg2 := os.Args[2], os.Args[3]
			if isGameIdFb(arg1) && IsEnv(arg2) {
				// ./filteringData importFb <gameId> <env>
				gid, _ := strconv.Atoi(arg1)
				env := ResolveEnv(arg2)
				runImportFbModeWithGameId(gid, "", env)
			} else if IsEnv(arg2) {
				// ./filteringData importFb <levelId> <env>
				env := ResolveEnv(arg2)
				runImportFbMode(arg1, env)
			} else if isGameIdFb(arg1) {
				// ./filteringData importFb <gameId> <level>
				gid, _ := strconv.Atoi(arg1)
				runImportFbModeWithGameId(gid, arg2, "")
			} else {
				fmt.Printf("❌ 参数错误: 无法识别参数组合\n")
				os.Exit(1)
			}
		} else if len(os.Args) == 5 {
			// ./filteringData importFb <gameId> <level> <env>
			gidStr, lvl, envStr := os.Args[2], os.Args[3], os.Args[4]
			gid, err := strconv.Atoi(gidStr)
			if err != nil {
				fmt.Printf("❌ 参数错误: gameId 必须为整数\n")
				os.Exit(1)
			}
			env := ResolveEnv(envStr)
			runImportFbModeWithGameId(gid, lvl, env)
		} else {
			fmt.Printf("❌ 参数错误: importFb 命令参数过多\n")
			fmt.Println("用法1: ./filteringData importFb")
			fmt.Println("用法2: ./filteringData importFb <gameId>")
			fmt.Println("用法3: ./filteringData importFb <levelId>")
			fmt.Println("用法4: ./filteringData importFb <gameId> <env>")
			fmt.Println("用法5: ./filteringData importFb <levelId> <env>")
			fmt.Println("用法6: ./filteringData importFb <gameId> <level> <env>")
			fmt.Println("\n环境代码: local/l, hk-test/ht, br-test/bt, br-prod/bp, us-prod/up, hk-prod/hp")
			os.Exit(1)
		}
	default:
		fmt.Printf("未知命令: %s\n", command)
		fmt.Println("支持的命令: generate, generate2, import, generateFb, importFb")
		os.Exit(1)
	}
}

// runGenerateMode 运行生成模式
func runGenerateMode() {
	// 记录程序开始时间
	startTime := time.Now()

	// 初始化随机数种子
	rand.Seed(time.Now().UnixNano())

	// 加载配置文件
	config, err := LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("加载配置文件失败: %v", err)
	}
	fmt.Printf("配置加载成功 - 游戏ID: %d, 目标数据量: %d\n", config.Game.ID, config.Tables.DataNum)

	// 连接数据库
	db, err := NewDatabase(config, "")
	if err != nil {
		log.Fatalf("数据库连接失败: %v", err)
	}
	defer db.Close()

	//计算总投注
	totalBet := config.Bet.CS * config.Bet.ML * config.Bet.BL * float64(config.Tables.DataNum)

	// 预取共享只读数据
	winDataAll, err := db.GetWinData()
	if err != nil {
		log.Fatalf("获取中奖数据失败: %v", err)
	}
	noWinDataAll, err := db.GetNoWinData()
	if err != nil {
		log.Fatalf("获取不中奖数据失败: %v", err)
	}

	for rtpNum := 0; rtpNum < len(RtpLevels); rtpNum++ {
		// 并发度：CPU 核数
		worker := runtime.NumCPU()
		sem := make(chan struct{}, worker)
		var wg sync.WaitGroup

		for t := 0; t < config.Tables.DataTableNum; t++ {
			sem <- struct{}{}
			wg.Add(1)

			// 捕获当前循环变量
			rtpNo := RtpLevels[rtpNum].RtpNo
			rtpVal := RtpLevels[rtpNum].Rtp
			testIndex := t + 1

			go func(rtpNo float64, rtpVal float64, testIndex int) {
				defer func() { <-sem; wg.Done() }()

				// 记录单次测试开始时间
				testStartTime := time.Now()
				// 即时输出单次任务开始，便于观察进度
				fmt.Printf("▶️ 开始生成 | RTP等级 %.0f | 第%d次 | %s\n", rtpNo, testIndex, testStartTime.Format(time.RFC3339))

				if err := runRtpTest(db, config, rtpNo, rtpVal, testIndex, totalBet, winDataAll, noWinDataAll); err != nil {
					log.Printf("RTP测试失败: %v", err)
				}

				// 计算并输出单次测试耗时
				testDuration := time.Since(testStartTime)
				fmt.Printf("⏱️  RTP等级 %.0f (第%d次生成) 耗时: %v\n", rtpNo, testIndex, testDuration)
			}(rtpNo, rtpVal, testIndex)
		}

		wg.Wait()
	}

	// 计算并输出整个程序的总耗时
	totalDuration := time.Since(startTime)
	fmt.Printf("\n🎉 RTP数据筛选和保存完成！\n")
	fmt.Printf("⏱️  整个程序总耗时: %v\n", totalDuration)
}

// runGenerateMode2 运行生成模式V2 - 使用新的四阶段策略
func runGenerateMode2() {
	// 记录程序开始时间
	startTime := time.Now()

	// 初始化随机数种子
	rand.Seed(time.Now().UnixNano())

	// 加载配置文件
	config, err := LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("加载配置文件失败: %v", err)
	}
	fmt.Printf("配置加载成功V2 - 游戏ID: %d, 目标数据量: %d\n", config.Game.ID, config.Tables.DataNum)
	fmt.Printf("阶段策略配置: 阶段1比例[%.1f%%-%.1f%%], 阶段3比例%.1f%%, 上偏差%.3f\n",
		config.StageRatios.Stage1MinRatio*100, config.StageRatios.Stage1MaxRatio*100,
		config.StageRatios.Stage3WinTopRatio*100, config.StageRatios.UpperDeviation)

	// 连接数据库
	db, err := NewDatabase(config, "")
	if err != nil {
		log.Fatalf("数据库连接失败: %v", err)
	}
	defer db.Close()

	//计算总投注
	totalBet := config.Bet.CS * config.Bet.ML * config.Bet.BL * float64(config.Tables.DataNum)

	// 预取共享只读数据（使用三种数据源）
	fmt.Println("🔄 正在获取中奖但不盈利数据...")
	winDataAll, err := db.GetWinData()
	if err != nil {
		log.Fatalf("获取中奖但不盈利数据失败: %v", err)
	}
	fmt.Printf("✅ 中奖但不盈利数据条数: %d\n", len(winDataAll))

	fmt.Println("🔄 正在获取中奖且盈利数据...")
	profitDataAll, err := db.GetProfitData()
	if err != nil {
		log.Fatalf("获取中奖且盈利数据失败: %v", err)
	}
	fmt.Printf("✅ 中奖且盈利数据条数: %d\n", len(profitDataAll))

	fmt.Println("🔄 正在获取不中奖数据...")
	noWinDataAll, err := db.GetNoWinData()
	if err != nil {
		log.Fatalf("获取不中奖数据失败: %v", err)
	}
	fmt.Printf("✅ 不中奖数据条数: %d\n", len(noWinDataAll))

	if len(winDataAll) == 0 {
		fmt.Println("⚠️ 未获取到中奖但不盈利数据，无法继续。")
		return
	}
	if len(noWinDataAll) == 0 {
		fmt.Println("⚠️ 未获取到不中奖数据，后续将无法补全至目标条数。")
	}

	for rtpNum := 0; rtpNum < len(RtpLevels); rtpNum++ {
		// 并发度：CPU 核数
		worker := runtime.NumCPU()
		sem := make(chan struct{}, worker)
		var wg sync.WaitGroup

		for t := 0; t < config.Tables.DataTableNum; t++ {
			sem <- struct{}{}
			wg.Add(1)

			// 捕获当前循环变量
			rtpNo := RtpLevels[rtpNum].RtpNo
			rtpVal := RtpLevels[rtpNum].Rtp
			testIndex := t + 1

			go func(rtpNo float64, rtpVal float64, testIndex int) {
				defer func() { <-sem; wg.Done() }()

				// 记录单次测试开始时间
				testStartTime := time.Now()
				// 即时输出单次任务开始，便于观察进度
				fmt.Printf("▶️ 开始生成V2 | RTP等级 %.0f | 第%d次 | %s\n", rtpNo, testIndex, testStartTime.Format(time.RFC3339))

				if err := runRtpTest2(db, config, rtpNo, rtpVal, testIndex, totalBet, winDataAll, noWinDataAll, profitDataAll); err != nil {
					log.Printf("RTP测试V2失败: %v", err)
				}

				// 计算并输出单次测试耗时
				testDuration := time.Since(testStartTime)
				fmt.Printf("⏱️  RTP等级 %.0f (第%d次生成V2) 耗时: %v\n", rtpNo, testIndex, testDuration)
			}(rtpNo, rtpVal, testIndex)
		}

		wg.Wait()
	}

	// 计算并输出整个程序的总耗时
	totalDuration := time.Since(startTime)
	fmt.Printf("\n🎉 RTP数据筛选和保存完成V2！\n")
	fmt.Printf("⏱️  整个程序总耗时V2: %v\n", totalDuration)
}

// runImportMode 运行导入模式
func runImportMode(fileLevelId string, env string) {
	envDisplay := ""
	if env != "" {
		envDisplay = fmt.Sprintf(" [环境: %s]", env)
	}

	if fileLevelId == "" {
		fmt.Printf("🔄 启动导入模式 (导入所有文件)%s...\n", envDisplay)
	} else {
		fmt.Printf("🔄 启动导入模式 (只导入fileLevelId=%s的文件)%s...\n", fileLevelId, envDisplay)
	}

	// 加载配置
	config, err := LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("❌ 加载配置失败: %v", err)
	}

	// 连接数据库
	db, err := NewDatabase(config, env)
	if err != nil {
		log.Fatalf("❌ 连接数据库失败: %v", err)
	}
	defer db.Close()

	// 创建导入器
	importer := NewJSONImporter(db, config)

	// 执行导入
	if err := importer.ImportAllFiles(fileLevelId); err != nil {
		log.Fatalf("❌ 导入失败: %v", err)
	}

	fmt.Println("✅ 导入完成！")
}

// runImportModeWithGameId 导入指定 gameId 目录；可选 levelId 过滤
func runImportModeWithGameId(gameId int, levelId string, env string) {
	envDisplay := ""
	if env != "" {
		envDisplay = fmt.Sprintf(" [环境: %s]", env)
	}

	if levelId == "" {
		fmt.Printf("🔄 启动导入模式 (导入 output/%d 所有文件)%s...\n", gameId, envDisplay)
	} else {
		fmt.Printf("🔄 启动导入模式 (只导入 output/%d 下 levelId=%s 的文件)%s...\n", gameId, levelId, envDisplay)
	}

	config, err := LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("❌ 加载配置失败: %v", err)
	}

	db, err := NewDatabase(config, env)
	if err != nil {
		log.Fatalf("❌ 连接数据库失败: %v", err)
	}
	defer db.Close()

	importer := NewJSONImporter(db, config)
	if err := importer.ImportAllFilesWithGameId(gameId, levelId); err != nil {
		log.Fatalf("❌ 导入失败: %v", err)
	}
	fmt.Println("✅ 导入完成！")
}

// runGenerateFbMode 运行购买夺宝生成模式
// func runGenerateFbMode() {
// 	// 加载配置
// 	config, err := LoadConfig("config.yaml")
// 	if err != nil {
// 		log.Fatalf("加载配置文件失败: %v", err)
// 	}
// 	if !config.Game.IsFb {
// 		fmt.Println("⚠️ 当前游戏未启用购买夺宝 (game.is_fb=false)，退出。")
// 		return
// 	}
// 	fmt.Println("▶️ [generateFb] 购买夺宝生成模式启动")

// 	// 连接数据库
// 	db, err := NewDatabase(config)
// 	if err != nil {
// 		log.Fatalf("数据库连接失败: %v", err)
// 	}
// 	defer db.Close()

// 	// 计算总投注：cs * ml * bl * bet.fb * 数据条数
// 	totalBet := config.Bet.CS * config.Bet.ML * config.Bet.BL * config.Bet.FB * float64(config.Tables.DataNumFb)

// 	// 预取共享只读数据（购买模式）
// 	fmt.Println("🔄 [generateFb] 正在获取购买模式中奖数据...")
// 	winDataAll, err := db.GetWinDataFb()
// 	if err != nil {
// 		log.Fatalf("获取购买模式中奖数据失败: %v", err)
// 	}
// 	if len(winDataAll) == 0 {
// 		return
// 	}
// 	fmt.Printf("✅ [generateFb] 购买模式中奖数据条数: %d\n", len(winDataAll))

// 	fmt.Println("🔄 [generateFb] 正在获取购买模式不中奖数据...")
// 	noWinDataAll, err := db.GetNoWinDataFb()
// 	if err != nil {
// 		log.Fatalf("获取购买模式不中奖数据失败: %v", err)
// 	}
// 	fmt.Printf("✅ [generateFb] 购买模式不中奖数据条数: %d\n", len(noWinDataAll))

// 	if len(winDataAll) == 0 {
// 		fmt.Println("⚠️ [generateFb] 未获取到购买模式中奖数据，无法继续。请检查数据条件 (aw>0, gwt<=1, fb=2, sp=true)。")
// 		return
// 	}
// 	if len(noWinDataAll) == 0 {
// 		fmt.Println("⚠️ [generateFb] 未获取到购买模式不中奖数据，后续将无法补全至目标条数。")
// 	}

// 	// 遍历 RTP 档位，每档位执行多次，并统计耗时
// 	fbStartTime := time.Now()
// 	worker := runtime.NumCPU()
// 	sem := make(chan struct{}, worker)

// 	for rtpNum := 0; rtpNum < len(FbRtpLevels); rtpNum++ {
// 		levelStart := time.Now()
// 		levelNo := FbRtpLevels[rtpNum].RtpNo
// 		levelVal := FbRtpLevels[rtpNum].Rtp

// 		var wgLevel sync.WaitGroup
// 		for t := 0; t < config.Tables.DataTableNumFb; t++ {
// 			sem <- struct{}{}
// 			wgLevel.Add(1)

// 			testIndex := t + 1
// 			rtpNo := levelNo
// 			rtpVal := levelVal

// 			go func(rtpNo float64, rtpVal float64, testIndex int) {
// 				defer func() { <-sem; wgLevel.Done() }()
// 				testStartTime := time.Now()
// 				fmt.Printf("▶️ [generateFb] 开始生成 | RTP等级 %.0f | 第%d次 | %s\n", rtpNo, testIndex, testStartTime.Format(time.RFC3339))
// 				fmt.Printf("🔧 [generateFb] totalBet=%.2f allowWin_base=%.2f\n", totalBet, totalBet*rtpVal)

// 				if err := runRtpFbTest(db, config, rtpNo, rtpVal, testIndex, totalBet, winDataAll, noWinDataAll); err != nil {
// 					log.Printf("[generateFb] RTP测试失败: %v", err)
// 				}

// 				fmt.Printf("⏱️  [generateFb] RTP等级 %.0f (第%d次生成) 耗时: %v\n", rtpNo, testIndex, time.Since(testStartTime))
// 			}(rtpNo, rtpVal, testIndex)
// 		}

// 		wgLevel.Wait()
// 		fmt.Printf("⏱️  [generateFb] RTP等级 %.0f 总耗时: %v\n", levelNo, time.Since(levelStart))
// 	}

// 	fmt.Printf("\n🎉 [generateFb] 全部档位生成完成！\n")
// 	fmt.Printf("⏱️  [generateFb] 整体总耗时: %v\n", time.Since(fbStartTime))
// }

// // runRtpFbTest 生成购买夺宝 RTP 数据
// func runRtpFbTest(db *Database, config *Config, rtpLevel float64, rtp float64, testNumber int, totalBet float64, winDataAll []GameResultData, noWinDataAll []GameResultData) error {
// 	var logBuf bytes.Buffer
// 	printf := func(format string, a ...interface{}) {
// 		fmt.Fprintf(&logBuf, format, a...)
// 	}

// 	// 允许中奖金额，额外乘以 FB 倍数（已在 totalBet 包含 FB，此处再次按要求乘以 FB）
// 	allowWin := totalBet * rtp
// 	printf("[FB] allowWin=%.4f (cs=%.2f ml=%.2f bl=%.2f rtp=%.4f fb=%.2f)\n", allowWin, config.Bet.CS, config.Bet.ML, config.Bet.BL, rtp, config.Bet.FB)

// 	printf("\n========== [FB TASK BEGIN] RtpNo: %.0f | Test: %d | %s =========\n", rtpLevel, testNumber, time.Now().Format(time.RFC3339))
// 	printf("获取到中奖数据: %d条, 不中奖数据: %d条\n", len(winDataAll), len(noWinDataAll))
// 	printf("档位: %.0f, 目标RTP: %.4f, 允许中奖金额: %.2f\n", rtpLevel, rtp, allowWin)

// 	// 其余逻辑与普通模式类似：达标且偏差 <= 0.005；购买模式存在高RTP特殊区间处理
// 	// 首次筛选优先选择“单条中奖金额”接近购买投入的 0.7-1.5 区间
// 	seed := time.Now().UnixNano() ^ int64(config.Game.ID)*1_000_003 ^ int64(testNumber)*1_000_033 ^ int64(rtpLevel)*1_000_037
// 	rng := rand.New(rand.NewSource(seed))
// 	perSpinBet := config.Bet.CS * config.Bet.ML * config.Bet.BL * config.Bet.FB
// 	preferredMin := perSpinBet * 0.7
// 	preferredMax := perSpinBet * 1.5
// 	var preferred, nonPreferred []GameResultData
// 	for _, it := range winDataAll {
// 		if it.AW >= preferredMin && it.AW <= preferredMax {
// 			preferred = append(preferred, it)
// 		} else {
// 			nonPreferred = append(nonPreferred, it)
// 		}
// 	}
// 	printf("[FB] 优先区间: [%.2f, %.2f], 候选: %d, 其他: %d\n", preferredMin, preferredMax, len(preferred), len(nonPreferred))
// 	// 按贪心顺序（aw DESC）遍历索引
// 	permPref := make([]int, len(preferred))
// 	for i := range permPref {
// 		permPref[i] = i
// 	}
// 	sort.Slice(permPref, func(i, j int) bool { return preferred[permPref[i]].AW > preferred[permPref[j]].AW })

// 	permRest := make([]int, len(nonPreferred))
// 	for i := range permRest {
// 		permRest[i] = i
// 	}
// 	sort.Slice(permRest, func(i, j int) bool { return nonPreferred[permRest[i]].AW > nonPreferred[permRest[j]].AW })

// 	var data []GameResultData
// 	var totalWin float64

// 	// 15档位特殊区间：绑定档位编号（rtpLevel == 15），范围改为 [0.8, 0.9]
// 	isSpecialRtp15 := (rtpLevel == 15)
// 	var targetRtpMin, targetRtpMax float64
// 	if isSpecialRtp15 {
// 		targetRtpMin = 0.8
// 		targetRtpMax = 0.9
// 		fmt.Printf("🎯 [FB] 15档位特殊处理: 目标RTP范围 [%.1f, %.1f], 允许偏差 ±0.005\n", targetRtpMin, targetRtpMax)
// 	}

// 	// 先遍历优先区间，再遍历其余
// 	for _, idx := range permPref {
// 		if len(data) >= config.Tables.DataNumFb {
// 			break
// 		}
// 		item := preferred[idx]
// 		// 过滤大奖、巨奖、超级巨奖
// 		switch item.GWT {
// 		case 2:
// 			continue
// 		case 3:
// 			continue
// 		case 4:
// 			continue
// 		}

// 		// 累计并校验上限（允许 0.5% 偏差）
// 		newTotalWin := totalWin + item.AW
// 		currentRtp := newTotalWin / totalBet
// 		if newTotalWin > allowWin*1.005 {
// 			continue
// 		}

// 		// 15档位特殊：不超过上限即可；其他档位：需达到 [allowWin, allowWin*1.005] 目标区间
// 		if isSpecialRtp15 {
// 			// 先加入再看是否达标区间
// 			if currentRtp > targetRtpMax {
// 				continue
// 			}
// 		}
// 		// 若仍未达标，遍历其余数据
// 		if !(isSpecialRtp15 || (totalWin >= allowWin && totalWin <= allowWin*(1+0.005))) {
// 			for _, idx := range permRest {
// 				if len(data) >= config.Tables.DataNumFb {
// 					break
// 				}
// 				item := nonPreferred[idx]
// 				// 过滤大奖、巨奖、超级巨奖
// 				switch item.GWT {
// 				case 2:
// 					continue
// 				case 3:
// 					continue
// 				case 4:
// 					continue
// 				}

// 				newTotalWin := totalWin + item.AW
// 				currentRtp := newTotalWin / totalBet
// 				if newTotalWin > allowWin*1.005 {
// 					continue
// 				}
// 				if isSpecialRtp15 {
// 					if currentRtp > targetRtpMax {
// 						continue
// 					}
// 				}
// 				if len(data) >= config.Tables.DataNumFb {
// 					break
// 				}
// 				totalWin += item.AW
// 				data = append(data, item)
// 				if isSpecialRtp15 {
// 					if currentRtp >= targetRtpMin && len(data) >= config.Tables.DataNumFb {
// 						break
// 					}
// 				}
// 				if !isSpecialRtp15 {
// 					if totalWin >= allowWin && totalWin <= allowWin*(1+0.005) {
// 						break
// 					}
// 				}
// 			}
// 		}
// 		// 加入（受条数上限限制）
// 		if len(data) >= config.Tables.DataNumFb {
// 			break
// 		}
// 		totalWin += item.AW
// 		data = append(data, item)

// 		//先判断15档位是否达到下限
// 		if isSpecialRtp15 {
// 			if currentRtp >= targetRtpMin && len(data) >= config.Tables.DataNumFb {
// 				break
// 			}
// 		}

// 		if !isSpecialRtp15 {
// 			if totalWin >= allowWin && totalWin <= allowWin*(1+0.005) {
// 				break
// 			}
// 		}
// 	}
// 	//判断当前是否达标
// 	if totalWin < allowWin {
// 		//判断是否为普通档位
// 		if !isSpecialRtp15 {
// 			//需要继续补全，优先查询符合的
// 			remainingWin := (allowWin - totalWin) * 1.005
// 			// 优先从数据库中查询满足条件的购买模式候选，限制 100 条
// 			// 购买模式允许数据重复，不排除已使用的ID
// 			fillData, err := db.GetWinDataForFillingFb(remainingWin, nil, 100)
// 			if err != nil {
// 				printf("⚠️ [FB] 查询填充数据失败: %v, 回退到原始逻辑\n", err)
// 			}

// 			if len(fillData) > 0 {
// 				printf("🔍 [FB] 数据库查询到 %d 条候选填充数据\n", len(fillData))
// 				for _, item := range fillData {
// 					if len(data) >= config.Tables.DataNumFb {
// 						break
// 					}
// 					if item.AW <= remainingWin && item.AW > 0 {
// 						data = append(data, item)
// 						totalWin += item.AW
// 						remainingWin -= item.AW
// 						printf("➕ [FB] 补充数据: AW=%.2f, 剩余需要: %.2f\n", item.AW, remainingWin)
// 						if totalWin >= allowWin {
// 							break
// 						}
// 					}
// 				}
// 			} else {
// 				// 回退：从预取中奖数据中挑选（已过滤大奖/巨奖/超巨奖），但需满足 fb=2, sp=true, gwt<=1
// 				for _, item := range winDataAll {
// 					if len(data) >= config.Tables.DataNumFb {
// 						break
// 					}
// 					if !(item.FB == 2 && item.SP && item.GWT <= 1) {
// 						continue
// 					}
// 					if item.AW <= remainingWin && item.AW > 0 {
// 						data = append(data, item)
// 						totalWin += item.AW
// 						remainingWin -= item.AW
// 						printf("➕ [FB] 回退补充数据: AW=%.2f, 剩余需要: %.2f\n", item.AW, remainingWin)
// 						if totalWin >= allowWin {
// 							break
// 						}
// 					}
// 				}
// 			}
// 		}
// 	}

// 	// 局部贪心优化：1↔1 替换以进一步逼近目标金额/范围
// 	if len(data) > 0 {
// 		candidates := make([]GameResultData, 0, len(preferred)+len(nonPreferred))
// 		candidates = append(candidates, preferred...)
// 		candidates = append(candidates, nonPreferred...)

// 		targetSum := allowWin
// 		upperBound := allowWin * (1 + 0.005)
// 		if isSpecialRtp15 {
// 			// 15档位瞄准区间中位值，提高命中概率
// 			targetSum = ((targetRtpMin + targetRtpMax) / 2.0) * totalBet
// 			upperBound = targetRtpMax * totalBet
// 		}

// 		bestDev := math.Abs(totalWin - targetSum)
// 		maxIter := 300
// 		for iter := 0; iter < maxIter; iter++ {
// 			idx := rng.Intn(len(data))
// 			removed := data[idx]
// 			base := totalWin - removed.AW
// 			desired := targetSum - base

// 			var best GameResultData
// 			bestDiff := math.MaxFloat64
// 			found := false
// 			for _, cand := range candidates {
// 				if cand.AW <= 0 {
// 					continue
// 				}
// 				// 购买模式过滤大奖/巨奖/超巨奖
// 				switch cand.GWT {
// 				case 2, 3, 4:
// 					continue
// 				}
// 				newTotal := base + cand.AW
// 				if newTotal > upperBound {
// 					continue
// 				}
// 				if isSpecialRtp15 {
// 					if newTotal/totalBet > targetRtpMax {
// 						continue
// 					}
// 				}
// 				diff := math.Abs(cand.AW - desired)
// 				if diff < bestDiff {
// 					bestDiff = diff
// 					best = cand
// 					found = true
// 				}
// 			}

// 			if !found {
// 				continue
// 			}
// 			newTotal := base + best.AW
// 			newDev := math.Abs(newTotal - targetSum)
// 			if newDev+1e-9 < bestDev {
// 				data[idx] = best
// 				totalWin = newTotal
// 				bestDev = newDev
// 			}
// 		}
// 	}

// 	// 用不中奖数据补全到 DataNumFb
// 	needNum := config.Tables.DataNumFb - len(data)
// 	if needNum > 0 && len(noWinDataAll) > 0 {
// 		permNo := rng.Perm(len(noWinDataAll))
// 		for i := 0; i < needNum; i++ {
// 			data = append(data, noWinDataAll[permNo[i%len(permNo)]])
// 		}
// 	}

// 	// 输出最终统计：数量、目标RTP、当前RTP与偏差
// 	printf("📊 [FB] 最终验证: 期望 %d 条, 实际 %d 条\n", config.Tables.DataNumFb, len(data))
// 	var finalTotalWin float64
// 	for _, it := range data {
// 		finalTotalWin += it.AW
// 	}
// 	finalRTP := finalTotalWin / totalBet
// 	rtpDeviation := math.Abs(finalRTP - rtp)
// 	printf("✅ [FB] 档位: %.0f, 目标RTP: %.6f, 实际RTP: %.6f, 偏差: %.6f\n", rtpLevel, rtp, finalRTP, rtpDeviation)

// 	var outputDir = filepath.Join("output", fmt.Sprintf("%d_fb", config.Game.ID))
// 	// 最终保存：沿用普通保存逻辑，但输出仍落在 output/<gameId>，文件名前缀复用
// 	if err := saveToJSON(data, config, rtpLevel, testNumber, outputDir); err != nil {
// 		return fmt.Errorf("[FB] 保存JSON失败: %v", err)
// 	}

// 	outputMu.Lock()
// 	fmt.Print(logBuf.String())
// 	outputMu.Unlock()
// 	return nil
// }

// runGenerateFbMode 运行购买夺宝生成模式
func runGenerateFbMode() {
	// 加载配置
	config, err := LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("加载配置文件失败: %v", err)
	}
	if !config.Game.IsFb {
		fmt.Println("⚠️ 当前游戏未启用购买夺宝 (game.is_fb=false)，退出。")
		return
	}
	fmt.Println("▶️ [generateFb] 购买夺宝生成模式启动")

	// 连接数据库
	db, err := NewDatabase(config, "")
	if err != nil {
		log.Fatalf("数据库连接失败: %v", err)
	}
	defer db.Close()

	// 计算总投注：cs * ml * bl * bet.fb * 数据条数
	totalBet := config.Bet.CS * config.Bet.ML * config.Bet.BL * config.Bet.FB * float64(config.Tables.DataNumFb)

	// 预取共享只读数据（购买模式）
	fmt.Println("🔄 [generateFb] 正在获取购买模式中奖数据...")
	winDataAll, err := db.GetWinDataFb()
	if err != nil {
		log.Fatalf("获取购买模式中奖数据失败: %v", err)
	}

	fmt.Printf("✅ [generateFb] 购买模式中奖但是不盈利的数据条数: %d\n", len(winDataAll))

	profitDataAll, err := db.GetProfitDataFb()
	if err != nil {
		log.Fatalf("获取购买模式中奖数据失败: %v", err)
	}
	if len(profitDataAll) == 0 {
		return
	}
	fmt.Printf("✅ [generateFb] 购买模式中奖并且盈利的数据条数: %d\n", len(profitDataAll))

	fmt.Println("🔄 [generateFb] 正在获取购买模式不中奖数据...")
	noWinDataAll, err := db.GetNoWinDataFb()
	if err != nil {
		log.Fatalf("获取购买模式不中奖数据失败: %v", err)
	}
	fmt.Printf("✅ [generateFb] 购买模式不中奖数据条数: %d\n", len(noWinDataAll))

	if len(winDataAll) == 0 {
		fmt.Println("⚠️ [generateFb] 未获取到购买模式中奖数据，无法继续。请检查数据条件 (aw>0, gwt<=1, fb=2, sp=true)。")
		return
	}
	if len(noWinDataAll) == 0 {
		fmt.Println("⚠️ [generateFb] 未获取到购买模式不中奖数据，后续将无法补全至目标条数。")
	}

	// 遍历 RTP 档位，每档位执行多次，并统计耗时
	fbStartTime := time.Now()
	worker := runtime.NumCPU()
	sem := make(chan struct{}, worker)

	for rtpNum := 0; rtpNum < len(FbRtpLevels); rtpNum++ {
		levelStart := time.Now()
		levelNo := FbRtpLevels[rtpNum].RtpNo
		levelVal := FbRtpLevels[rtpNum].Rtp

		var wgLevel sync.WaitGroup
		for t := 0; t < config.Tables.DataTableNumFb; t++ {
			sem <- struct{}{}
			wgLevel.Add(1)

			testIndex := t + 1
			rtpNo := levelNo
			rtpVal := levelVal

			go func(rtpNo float64, rtpVal float64, testIndex int) {
				defer func() { <-sem; wgLevel.Done() }()
				testStartTime := time.Now()
				fmt.Printf("▶️ [generateFb] 开始生成 | RTP等级 %.0f | 第%d次 | %s\n", rtpNo, testIndex, testStartTime.Format(time.RFC3339))
				fmt.Printf("🔧 [generateFb] totalBet=%.2f allowWin_base=%.2f\n", totalBet, totalBet*rtpVal)

				if err := runRtpFbTest(db, config, rtpNo, rtpVal, testIndex, totalBet, winDataAll, noWinDataAll, profitDataAll); err != nil {
					log.Printf("[generateFb] RTP测试失败: %v", err)
				}

				fmt.Printf("⏱️  [generateFb] RTP等级 %.0f (第%d次生成) 耗时: %v\n", rtpNo, testIndex, time.Since(testStartTime))
			}(rtpNo, rtpVal, testIndex)
		}

		wgLevel.Wait()
		fmt.Printf("⏱️  [generateFb] RTP等级 %.0f 总耗时: %v\n", levelNo, time.Since(levelStart))
	}

	fmt.Printf("\n🎉 [generateFb] 全部档位生成完成！\n")
	fmt.Printf("⏱️  [generateFb] 整体总耗时: %v\n", time.Since(fbStartTime))
}

// runRtpFbTest 生成购买夺宝 RTP 数据
func runRtpFbTest(db *Database, config *Config, rtpLevel float64, rtp float64, testNumber int, totalBet float64, winDataAll []GameResultData, noWinDataAll []GameResultData, profitDataAll []GameResultData) error {
	var logBuf bytes.Buffer
	printf := func(format string, a ...interface{}) {
		fmt.Fprintf(&logBuf, format, a...)
	}

	//
	const (
		upperDeviation    = 0.005 // 允许上偏差
		stage1MinRatio    = 0.60  // 第一阶段占比下限
		stage1MaxRatio    = 0.80  // 第一阶段占比上限
		stage3WinTopRatio = 0.90  // 第三阶段用 winDataAll 大额补齐比例
	)

	// 目标金额与边界
	allowWin := totalBet * rtp
	upperBound := allowWin * (1 + upperDeviation)
	perSpinBet := config.Bet.CS * config.Bet.ML * config.Bet.BL * config.Bet.FB

	printf("\n========== [FB TASK BEGIN] RtpNo: %.0f | Test: %d | %s =========\n", rtpLevel, testNumber, time.Now().Format(time.RFC3339))
	printf("[FB] allowWin=%.4f (cs=%.2f ml=%.2f bl=%.2f fb=%.2f rtp=%.4f)\n", allowWin, config.Bet.CS, config.Bet.ML, config.Bet.BL, config.Bet.FB, rtp)
	printf("候选: win(not-profit)=%d, profit=%d, nowin=%d\n", len(winDataAll), len(profitDataAll), len(noWinDataAll))

	// 随机源
	seed := time.Now().UnixNano() ^ int64(config.Game.ID)*1_000_003 ^ int64(testNumber)*1_000_033 ^ int64(rtpLevel)*1_000_037
	rng := rand.New(rand.NewSource(seed))

	// 结果容器
	var data []GameResultData
	var totalWin float64
	targetCount := config.Tables.DataNumFb
	// 随机化阶段1比例 [60%, 80%]
	stage1Ratio := stage1MinRatio + rng.Float64()*(stage1MaxRatio-stage1MinRatio)
	stage1Count := int(math.Round(float64(targetCount) * stage1Ratio))

	// 已使用ID，避免单文件内重复
	used := make(map[int]struct{}, targetCount)

	// 辅助函数：尝试加入一条记录（不超过上限，过滤大奖/巨奖/超巨奖，去重）
	tryAppend := func(item GameResultData) bool {
		if _, ok := used[item.ID]; ok {
			return false
		}
		switch item.GWT {
		case 2, 3, 4:
			return false
		}
		if item.AW <= 0 {
			return false
		}
		if totalWin+item.AW > upperBound {
			return false
		}
		data = append(data, item)
		totalWin += item.AW
		used[item.ID] = struct{}{}
		return true
	}

	// 阶段1：打乱 winDataAll，单轮无放回采样至 80%
	if len(winDataAll) > 0 && stage1Count > 0 {
		perm := rng.Perm(len(winDataAll))
		for _, idx := range perm {
			if len(data) >= stage1Count {
				break
			}
			_ = tryAppend(winDataAll[idx])
		}
		printf("[FB] 阶段1：已加入 %d 条（目标 %.0f%%=%d），累计中奖=%.2f\n", len(data), stage1Ratio*100, stage1Count, totalWin)
	}

	// 阶段2：动态占比（profit vs win），根据缺口/剩余名额决定倾向，直到达到 allowWin 或数量上限
	if totalWin < allowWin && len(data) < targetCount && (len(profitDataAll) > 0 || len(winDataAll) > 0) {
		permProfit := rng.Perm(len(profitDataAll))
		permWin2 := rng.Perm(len(winDataAll))
		pi, wi := 0, 0

		// 估算初始倾向
		remainingSlots := targetCount - len(data)
		remainingWin := allowWin - totalWin
		needFactor := 0.0
		if remainingSlots > 0 {
			needFactor = remainingWin / (perSpinBet * float64(remainingSlots))
		}
		basePProfit := needFactor
		if basePProfit < 0.2 {
			basePProfit = 0.2
		}
		if basePProfit > 0.8 {
			basePProfit = 0.8
		}
		printf("[FB] 阶段2：动态占比起始 pProfit=%.3f (needFactor=%.3f)\n", basePProfit, needFactor)

		maxOuter := len(profitDataAll) + len(winDataAll) + 1024
		for outer := 0; outer < maxOuter; outer++ {
			if totalWin >= allowWin || len(data) >= targetCount {
				break
			}
			// 实时更新占比
			remainingSlots = targetCount - len(data)
			remainingWin = allowWin - totalWin
			if remainingSlots <= 0 || remainingWin <= 0 {
				break
			}
			needFactor = remainingWin / (perSpinBet * float64(remainingSlots))
			pProfit := needFactor
			if pProfit < 0.2 {
				pProfit = 0.2
			}
			if pProfit > 0.8 {
				pProfit = 0.8
			}

			chooseProfit := rng.Float64() < pProfit
			appended := false

			if chooseProfit && pi < len(permProfit) {
				for pi < len(permProfit) {
					cand := profitDataAll[permProfit[pi]]
					pi++
					if tryAppend(cand) {
						appended = true
						break
					}
				}
			}
			// 若未能加入或无可用 profit，则尝试 win
			if !appended && wi < len(permWin2) {
				for wi < len(permWin2) {
					cand := winDataAll[permWin2[wi]]
					wi++
					if tryAppend(cand) {
						appended = true
						break
					}
				}
			}
			// 若先选 win 失败，再尝试 profit 兜底
			if !appended && !chooseProfit && pi < len(permProfit) {
				for pi < len(permProfit) {
					cand := profitDataAll[permProfit[pi]]
					pi++
					if tryAppend(cand) {
						appended = true
						break
					}
				}
			}
			// 两边都无法加入，提前退出
			if !appended {
				break
			}
		}
		printf("[FB] 阶段2完成：累计中奖=%.2f, 目标=%.2f, 数量=%d/%d\n", totalWin, allowWin, len(data), targetCount)
	}

	// 阶段3：若还需要补充（数量未达标），先用 winDataAll 的大额补 90% 的剩余名额
	if len(data) < targetCount {
		remainingSlots := targetCount - len(data)
		stage3aSlots := int(math.Ceil(float64(remainingSlots) * stage3WinTopRatio))

		if stage3aSlots > 0 && len(winDataAll) > 0 {
			// winDataAll 按 aw DESC
			winDesc := make([]GameResultData, len(winDataAll))
			copy(winDesc, winDataAll)
			sort.Slice(winDesc, func(i, j int) bool { return winDesc[i].AW > winDesc[j].AW })
			for _, it := range winDesc {
				if stage3aSlots == 0 || len(data) >= targetCount {
					break
				}
				if tryAppend(it) {
					stage3aSlots--
				}
			}
		}

		// 阶段3b：剩余名额根据缺口大小，用 profitDataAll 小额或大额补齐
		if len(data) < targetCount {
			remainingSlots = targetCount - len(data)
			remainingWin := allowWin - totalWin
			gapSmallThreshold := math.Max(perSpinBet, allowWin*0.02) // 小缺口阈值

			// 若金额已足或接近上限，则直接跳过到数量兜底
			if remainingWin > 0 && len(profitDataAll) > 0 {
				// 按需选择排序方向
				profit := make([]GameResultData, len(profitDataAll))
				copy(profit, profitDataAll)
				if remainingWin <= gapSmallThreshold {
					sort.Slice(profit, func(i, j int) bool { return profit[i].AW < profit[j].AW }) // 小额优先
				} else {
					sort.Slice(profit, func(i, j int) bool { return profit[i].AW > profit[j].AW }) // 大额优先
				}

				for _, it := range profit {
					if remainingSlots == 0 || len(data) >= targetCount {
						break
					}
					// 若已经达到目标金额，仅在不超过上限时允许继续；核心由上限约束
					if tryAppend(it) {
						remainingSlots--
						remainingWin = allowWin - totalWin
						if remainingWin <= 0 {
							// 金额已达标，后续数量不足交由阶段4处理
							break
						}
					}
				}
			}
		}
	}

	// 阶段4：数量兜底，优先无放回补不中奖；若仍不足，再允许重复不中奖补满
	if len(data) < targetCount && len(noWinDataAll) > 0 {
		need := targetCount - len(data)
		// 先无放回
		perm := rng.Perm(len(noWinDataAll))
		for _, idx := range perm {
			if need == 0 {
				break
			}
			item := noWinDataAll[idx]
			if _, ok := used[item.ID]; ok {
				continue
			}
			data = append(data, item)
			used[item.ID] = struct{}{}
			need--
		}
		// 再重复补齐（仅对不中奖允许重复，以保证条数）
		if need > 0 {
			for i := 0; i < need; i++ {
				data = append(data, noWinDataAll[i%len(noWinDataAll)])
			}
		}
	}

	// 最终统计与保存
	printf("📊 [FB] 最终验证: 期望 %d 条, 实际 %d 条\n", targetCount, len(data))
	var finalTotalWin float64
	for _, it := range data {
		finalTotalWin += it.AW
	}
	finalRTP := finalTotalWin / totalBet
	printf("✅ [FB] 档位: %.0f, 目标RTP: %.6f, 实际RTP: %.6f, 偏差: %.6f\n", rtpLevel, rtp, finalRTP, math.Abs(finalRTP-rtp))

	// 重复率统计（按 id 去重）
	uniq := make(map[int]int, len(data))
	for _, it := range data {
		uniq[it.ID]++
	}
	dupCount := 0
	for _, c := range uniq {
		if c > 1 {
			dupCount += c - 1
		}
	}
	dupRate := 0.0
	if n := len(data); n > 0 {
		dupRate = float64(dupCount) / float64(n)
	}
	printf("🔎 [FB] 去重统计: 总数=%d, 唯一=%d, 重复=%d, 重复率=%.4f\n", len(data), len(uniq), dupCount, dupRate)

	// 打乱输出顺序并写文件
	rand.Shuffle(len(data), func(i, j int) { data[i], data[j] = data[j], data[i] })
	outDir := filepath.Join("output", fmt.Sprintf("%d_fb", config.Game.ID))
	if err := saveToJSON(data, config, rtpLevel, testNumber, outDir); err != nil {
		return fmt.Errorf("[FB] 保存JSON失败: %v", err)
	}

	outputMu.Lock()
	fmt.Print(logBuf.String())
	outputMu.Unlock()
	return nil
}

// runImportFbMode 运行购买夺宝导入模式
func runImportFbMode(fileLevelId string, env string) {
	// 加载配置
	config, err := LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("❌ 加载配置失败: %v", err)
	}
	if !config.Game.IsFb {
		fmt.Println("⚠️ 当前游戏未启用购买夺宝 (game.is_fb=false)，退出。")
		return
	}

	// 连接数据库
	db, err := NewDatabase(config, env)
	if err != nil {
		log.Fatalf("❌ 连接数据库失败: %v", err)
	}
	defer db.Close()

	// 读取目录：output/<gameId>_fb
	outputDir := filepath.Join("output", fmt.Sprintf("%d_fb", config.Game.ID))
	envDisplay := ""
	if env != "" {
		envDisplay = fmt.Sprintf(" [环境: %s]", env)
	}
	fmt.Printf("📂 [importFb] 导入目录: %s%s\n", outputDir, envDisplay)

	// 构建目标表（与普通导入相同：rtpLevel 为 NUMERIC，表名不带 _fb）
	tableName := fmt.Sprintf("%s%d", config.Tables.OutputTablePrefix, config.Game.ID)
	createTable := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS "%s" (
            "id" SERIAL PRIMARY KEY,
            "rtpLevel" REAL NOT NULL,
            "srNumber" INTEGER NOT NULL,
            "srId" SERIAL NOT NULL,
            "bet" NUMERIC NOT NULL,
            "win" NUMERIC NOT NULL,
            "detail" JSONB,
            "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    `, tableName)
	if _, err := db.DB.Exec(createTable); err != nil {
		log.Fatalf("❌ 创建FB目标表失败: %v", err)
	}
	indexQueries := []string{
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s_rtpLevel_idx" ON "%s" ("rtpLevel")`, tableName, tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s_srNumber_idx" ON "%s" ("srNumber")`, tableName, tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s_srId_idx" ON "%s" ("srId")`, tableName, tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s_rtpLevel_srNumber_idx" ON "%s" ("rtpLevel", "srNumber")`, tableName, tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s_rtpLevel_srNumber_srId_idx" ON "%s" ("rtpLevel", "srNumber", "srId")`, tableName, tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s_detail_gin_idx" ON "%s" USING GIN ("detail")`, tableName, tableName),
	}
	for _, q := range indexQueries {
		if _, err := db.DB.Exec(q); err != nil {
			log.Fatalf("❌ 创建索引失败: %v", err)
		}
	}

	// 收集 JSON 文件列表
	type FileInfo struct {
		Path     string
		Name     string
		RtpLevel int
		TestNum  int
	}
	var files []FileInfo
	err = filepath.WalkDir(outputDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(strings.ToLower(d.Name()), ".json") {
			return nil
		}
		re := regexp.MustCompile(`GameResults_(\d+)_(\d+)\.json`)
		m := re.FindStringSubmatch(d.Name())
		if len(m) != 3 {
			return nil
		}
		rl, _ := strconv.Atoi(m[1])
		tn, _ := strconv.Atoi(m[2])
		if fileLevelId != "" && m[1] != fileLevelId {
			return nil
		}
		files = append(files, FileInfo{Path: path, Name: d.Name(), RtpLevel: rl, TestNum: tn})
		return nil
	})
	if err != nil {
		log.Fatalf("❌ 遍历目录失败: %v", err)
	}
	if len(files) == 0 {
		log.Fatalf("❌ 在 %s 未找到待导入的JSON文件", outputDir)
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].RtpLevel == files[j].RtpLevel {
			return files[i].TestNum < files[j].TestNum
		}
		return files[i].RtpLevel < files[j].RtpLevel
	})

	// 每行常量：FB 模式下注额（包含FB）
	// bet := config.Bet.CS * config.Bet.ML * config.Bet.BL * config.Bet.FB

	// 导入一个文件（流式）
	importOne := func(f FileInfo) error {
		fmt.Printf("\n🔄 [importFb] 正在导入: %s\n", f.Name)
		fh, err := os.Open(f.Path)
		if err != nil {
			return fmt.Errorf("打开文件失败: %w", err)
		}
		defer fh.Close()

		dec := json.NewDecoder(fh)
		// 解析头部：rtpLevel, srNumber, 然后定位到 data 数组
		var rtpLevelInt int
		var srNumber int
		// 简易扫描：读取到第一个 '{'
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		if delim, ok := tok.(json.Delim); !ok || delim != '{' {
			return fmt.Errorf("JSON格式错误: 缺少对象开始")
		}
		for dec.More() {
			t, _ := dec.Token()
			key, _ := t.(string)
			switch key {
			case "rtpLevel":
				var v int
				if err := dec.Decode(&v); err != nil {
					return err
				}
				rtpLevelInt = v
			case "srNumber":
				var v int
				if err := dec.Decode(&v); err != nil {
					return err
				}
				srNumber = v
			case "data":
				// 进入数组
				tok, err := dec.Token()
				if err != nil {
					return err
				}
				if delim, ok := tok.(json.Delim); !ok || delim != '[' {
					return fmt.Errorf("JSON格式错误: data应为数组")
				}

				// 开启事务与 stmt
				tx, err := db.DB.Begin()
				if err != nil {
					return fmt.Errorf("开启事务失败: %w", err)
				}
				stmt, err := tx.Prepare(fmt.Sprintf(`
                    INSERT INTO "%s" ("rtpLevel", "srNumber", "srId", "bet", "win", "detail")
                    VALUES ($1, $2, $3, $4, $5, $6)
                `, tableName))
				if err != nil {
					_ = tx.Rollback()
					return fmt.Errorf("准备语句失败: %w", err)
				}

				// rtpLevel 数值：如 13 -> 13.1（写入相同目标表）
				rtpLevelVal := float64(rtpLevelInt) + 0.1
				srId := 0
				for dec.More() {
					var item map[string]interface{}
					if err := dec.Decode(&item); err != nil {
						_ = stmt.Close()
						_ = tx.Rollback()
						return fmt.Errorf("解析记录失败: %w", err)
					}
					srId++

					// win 精度修正
					var winValue float64
					if aw, ok := item["aw"].(float64); ok {
						winValue = math.Round(aw*100) / 100
					}

					// win 精度修正
					var totalBet float64
					if tb, ok := item["tb"].(float64); ok {
						totalBet = math.Round(tb*100) / 100
					}

					// detail 序列化 gd
					var detailVal interface{}
					if item["gd"] != nil {
						gdJSON, err := json.Marshal(item["gd"])
						if err != nil {
							_ = stmt.Close()
							_ = tx.Rollback()
							return fmt.Errorf("序列化gd失败: %w", err)
						}
						detailVal = string(gdJSON)
					}

					if _, err := stmt.Exec(rtpLevelVal, srNumber, srId, totalBet, winValue, detailVal); err != nil {
						_ = stmt.Close()
						_ = tx.Rollback()
						return fmt.Errorf("插入失败: %w", err)
					}
				}
				// 读取数组结束标记 ']'
				if tok, err = dec.Token(); err != nil {
					_ = stmt.Close()
					_ = tx.Rollback()
					return fmt.Errorf("读取数组结束标记失败: %w", err)
				}
				if delim, ok := tok.(json.Delim); !ok || delim != ']' {
					_ = stmt.Close()
					_ = tx.Rollback()
					return fmt.Errorf("JSON格式错误: 缺少数组结束")
				}
				if err := stmt.Close(); err != nil {
					_ = tx.Rollback()
					return fmt.Errorf("关闭stmt失败: %w", err)
				}
				if err := tx.Commit(); err != nil {
					return fmt.Errorf("提交事务失败: %w", err)
				}

			default:
				// 跳过其他键
				var skip interface{}
				if err := dec.Decode(&skip); err != nil {
					return err
				}
			}
		}
		// 读取对象结束 '}'
		if tok, err = dec.Token(); err != nil {
			return err
		}
		if delim, ok := tok.(json.Delim); !ok || delim != '}' {
			return fmt.Errorf("JSON格式错误: 缺少对象结束")
		}
		fmt.Printf("✅ [importFb] 导入完成: %s\n", f.Name)
		return nil
	}

	for _, f := range files {
		if err := importOne(f); err != nil {
			log.Fatalf("❌ [importFb] 导入文件 %s 失败: %v", f.Name, err)
		}
	}
	fmt.Println("\n🎉 [importFb] 所有文件导入完成！")
}

// runImportFbModeWithGameId 购买夺宝：导入指定 gameId 的 _fb 目录；可选 levelId 过滤
func runImportFbModeWithGameId(gameId int, levelId string, env string) {
	// 加载配置
	config, err := LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("❌ 加载配置失败: %v", err)
	}
	if !config.Game.IsFb {
		fmt.Println("⚠️ 当前游戏未启用购买夺宝 (game.is_fb=false)，退出。")
		return
	}

	// 连接数据库
	db, err := NewDatabase(config, env)
	if err != nil {
		log.Fatalf("❌ 连接数据库失败: %v", err)
	}
	defer db.Close()

	// 读取目录：output/<gameId>_fb
	outputDir := filepath.Join("output", fmt.Sprintf("%d_fb", gameId))
	envDisplay := ""
	if env != "" {
		envDisplay = fmt.Sprintf(" [环境: %s]", env)
	}
	fmt.Printf("📂 [importFb] 导入目录: %s%s\n", outputDir, envDisplay)

	// 目标表仍为不带 _fb 的表名（与现有实现一致）
	tableName := fmt.Sprintf("%s%d", config.Tables.OutputTablePrefix, gameId)
	createTable := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS "%s" (
            "id" SERIAL PRIMARY KEY,
            "rtpLevel" REAL NOT NULL,
            "srNumber" INTEGER NOT NULL,
            "srId" SERIAL NOT NULL,
            "bet" NUMERIC NOT NULL,
            "win" NUMERIC NOT NULL,
            "detail" JSONB,
            "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    `, tableName)
	if _, err := db.DB.Exec(createTable); err != nil {
		log.Fatalf("❌ 创建FB目标表失败: %v", err)
	}
	indexQueries := []string{
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s_rtpLevel_idx" ON "%s" ("rtpLevel")`, tableName, tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s_srNumber_idx" ON "%s" ("srNumber")`, tableName, tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s_srId_idx" ON "%s" ("srId")`, tableName, tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s_rtpLevel_srNumber_idx" ON "%s" ("rtpLevel", "srNumber")`, tableName, tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s_rtpLevel_srNumber_srId_idx" ON "%s" ("rtpLevel", "srNumber", "srId")`, tableName, tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s_detail_gin_idx" ON "%s" USING GIN ("detail")`, tableName, tableName),
	}
	for _, q := range indexQueries {
		if _, err := db.DB.Exec(q); err != nil {
			log.Fatalf("❌ 创建索引失败: %v", err)
		}
	}

	// 收集 JSON 文件列表
	type FileInfo struct {
		Path     string
		Name     string
		RtpLevel int
		TestNum  int
	}
	var files []FileInfo
	err = filepath.WalkDir(outputDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(strings.ToLower(d.Name()), ".json") {
			return nil
		}
		re := regexp.MustCompile(`GameResults_(\d+)_(\d+)\.json`)
		m := re.FindStringSubmatch(d.Name())
		if len(m) != 3 {
			return nil
		}
		if levelId != "" && m[1] != levelId {
			return nil
		}
		rl, _ := strconv.Atoi(m[1])
		tn, _ := strconv.Atoi(m[2])
		files = append(files, FileInfo{Path: path, Name: d.Name(), RtpLevel: rl, TestNum: tn})
		return nil
	})
	if err != nil {
		log.Fatalf("❌ 遍历目录失败: %v", err)
	}
	if len(files) == 0 {
		log.Fatalf("❌ 在 %s 未找到待导入的JSON文件", outputDir)
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].RtpLevel == files[j].RtpLevel {
			return files[i].TestNum < files[j].TestNum
		}
		return files[i].RtpLevel < files[j].RtpLevel
	})

	bet := config.Bet.CS * config.Bet.ML * config.Bet.BL * config.Bet.FB
	importOne := func(f FileInfo) error {
		fmt.Printf("\n🔄 [importFb] 正在导入: %s\n", f.Name)
		fh, err := os.Open(f.Path)
		if err != nil {
			return fmt.Errorf("打开文件失败: %w", err)
		}
		defer fh.Close()
		dec := json.NewDecoder(fh)
		var rtpLevelInt int
		var srNumber int
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		if delim, ok := tok.(json.Delim); !ok || delim != '{' {
			return fmt.Errorf("JSON格式错误: 缺少对象开始")
		}
		for dec.More() {
			t, _ := dec.Token()
			key, _ := t.(string)
			switch key {
			case "rtpLevel":
				var v int
				if err := dec.Decode(&v); err != nil {
					return err
				}
				rtpLevelInt = v
			case "srNumber":
				var v int
				if err := dec.Decode(&v); err != nil {
					return err
				}
				srNumber = v
			case "data":
				tok, err := dec.Token()
				if err != nil {
					return err
				}
				if delim, ok := tok.(json.Delim); !ok || delim != '[' {
					return fmt.Errorf("JSON格式错误: data应为数组")
				}
				tx, err := db.DB.Begin()
				if err != nil {
					return fmt.Errorf("开启事务失败: %w", err)
				}
				stmt, err := tx.Prepare(fmt.Sprintf(`
                    INSERT INTO "%s" ("rtpLevel", "srNumber", "srId", "bet", "win", "detail")
                    VALUES ($1, $2, $3, $4, $5, $6)
                `, tableName))
				if err != nil {
					_ = tx.Rollback()
					return fmt.Errorf("准备语句失败: %w", err)
				}
				rtpLevelVal := float64(rtpLevelInt) + 0.1
				srId := 0
				for dec.More() {
					var item map[string]interface{}
					if err := dec.Decode(&item); err != nil {
						_ = stmt.Close()
						_ = tx.Rollback()
						return fmt.Errorf("解析记录失败: %w", err)
					}
					srId++
					var winValue float64
					if aw, ok := item["aw"].(float64); ok {
						winValue = math.Round(aw*100) / 100
					}
					var detailVal interface{}
					if item["gd"] != nil {
						gdJSON, err := json.Marshal(item["gd"])
						if err != nil {
							_ = stmt.Close()
							_ = tx.Rollback()
							return fmt.Errorf("序列化gd失败: %w", err)
						}
						detailVal = string(gdJSON)
					}
					if _, err := stmt.Exec(rtpLevelVal, srNumber, srId, bet, winValue, detailVal); err != nil {
						_ = stmt.Close()
						_ = tx.Rollback()
						return fmt.Errorf("插入失败: %w", err)
					}
				}
				if tok, err = dec.Token(); err != nil {
					_ = stmt.Close()
					_ = tx.Rollback()
					return fmt.Errorf("读取数组结束标记失败: %w", err)
				}
				if delim, ok := tok.(json.Delim); !ok || delim != ']' {
					_ = stmt.Close()
					_ = tx.Rollback()
					return fmt.Errorf("JSON格式错误: 缺少数组结束")
				}
				if err := stmt.Close(); err != nil {
					_ = tx.Rollback()
					return fmt.Errorf("关闭stmt失败: %w", err)
				}
				if err := tx.Commit(); err != nil {
					return fmt.Errorf("提交事务失败: %w", err)
				}
			default:
				var skip interface{}
				if err := dec.Decode(&skip); err != nil {
					return err
				}
			}
		}
		if tok, err = dec.Token(); err != nil {
			return err
		}
		if delim, ok := tok.(json.Delim); !ok || delim != '}' {
			return fmt.Errorf("JSON格式错误: 缺少对象结束")
		}
		fmt.Printf("✅ [importFb] 导入完成: %s\n", f.Name)
		return nil
	}

	for _, f := range files {
		if err := importOne(f); err != nil {
			log.Fatalf("❌ [importFb] 导入文件 %s 失败: %v", f.Name, err)
		}
	}
	fmt.Println("\n🎉 [importFb] 所有文件导入完成！")
}
