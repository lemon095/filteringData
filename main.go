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

// 保证并发任务按块输出日志
var outputMu sync.Mutex

// printFailureSummary 输出失败统计汇总
func printFailureSummary(mode string, gameID int, failedLevels []float64, failedTests []string) {
	if len(failedLevels) == 0 {
		fmt.Printf("✅ [%s] 游戏 %d 所有档位生成成功！\n", mode, gameID)
		return
	}

	// 统计失败的档位
	levelCount := make(map[float64]int)
	for _, level := range failedLevels {
		levelCount[level]++
	}

	fmt.Printf("\n❌ [%s] 游戏 %d 失败统计:\n", mode, gameID)
	fmt.Printf("   总失败次数: %d\n", len(failedLevels))
	fmt.Printf("   失败档位统计:\n")

	// 按档位排序输出
	var sortedLevels []float64
	for level := range levelCount {
		sortedLevels = append(sortedLevels, level)
	}
	sort.Float64s(sortedLevels)

	for _, level := range sortedLevels {
		fmt.Printf("     RTP%.0f: %d次失败\n", level, levelCount[level])
	}

	// 输出详细失败列表
	if len(failedTests) <= 10 {
		fmt.Printf("   详细失败列表:\n")
		for _, test := range failedTests {
			fmt.Printf("     - %s\n", test)
		}
	} else {
		fmt.Printf("   详细失败列表 (前10个):\n")
		for i := 0; i < 10; i++ {
			fmt.Printf("     - %s\n", failedTests[i])
		}
		fmt.Printf("     ... 还有 %d 个失败\n", len(failedTests)-10)
	}
}

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

		// 判断本条是什么奖励配额（基于aw < tb * 100条件）
		// 根据中奖金额判断奖励类型
		var rewardType int
		if item.AW < float64(item.TB)*100 {
			// 根据中奖金额范围判断奖励类型
			if item.AW >= float64(item.TB)*50 {
				rewardType = 4 // 超级巨奖
			} else if item.AW >= float64(item.TB)*20 {
				rewardType = 3 // 巨奖
			} else {
				rewardType = 2 // 大奖
			}
		} else {
			continue // 不符合aw < tb * 100条件，跳过
		}

		switch rewardType {
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
		switch rewardType {
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
			// 四舍五入避免浮点数精度问题
			roundedRemainingWin := math.Round(remainingWin*100) / 100
			bestSingleMatch, err := db.GetBestSingleMatch(roundedRemainingWin, usedIds, 0.005)
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
				// 四舍五入避免浮点数精度问题
				roundedRemainingWin := math.Round(remainingWin*100) / 100
				fillData, err := db.GetWinDataForFilling(roundedRemainingWin, usedIds, 100)
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

	// 生成文件名：output_table_prefix_mode_RtpNo_第几次.json
	fileName := fmt.Sprintf("%s%d_%.0f_%d.json", config.Tables.OutputTablePrefix, config.Game.Mode, rtpLevel, testNumber)
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
		fmt.Println("  ./filteringData generate4                   # 生成RTP测试数据V4（RTP档位倍率分布策略）")
		fmt.Println("  ./filteringData import                     # 导入output目录下的所有JSON文件到数据库")
		fmt.Println("  ./filteringData import [fileLevelId]       # 只导入指定fileLevelId的JSON文件")
		fmt.Println("  ./filteringData import-s3 <gameIds> [level] [env] # 从S3智能导入（自动检测normal和fb模式）")
		fmt.Println("  ./filteringData import-s3-normal <gameIds> [level] [env] # 从S3导入普通模式文件")
		fmt.Println("  ./filteringData import-s3-fb <gameIds> [level] [env] # 从S3导入购买夺宝模式文件")
		fmt.Println("  ./filteringData sp-stats <gameId>              # 统计指定游戏JSON文件的SP数据")
		fmt.Println("  ./filteringData importFb-s3 <gameIds> [level] [env] # 从S3导入多个游戏的购买夺宝模式文件")
		fmt.Println("     gameIds: 逗号分隔的游戏ID列表，如: 112,103,105")
		fmt.Println("     level: 可选的RTP等级过滤")
		fmt.Println("     env: 可选的数据库环境 (local/l, hk-test/ht, br-test/bt, br-prod/bp, us-prod/up, hk-prod/hp)")
		fmt.Println("")
		fmt.Println("示例:")
		fmt.Println("  ./filteringData import                     # 导入所有文件")
		fmt.Println("  ./filteringData import 1                   # 只导入GameResults_1_*.json文件")
		fmt.Println("  ./filteringData import 93                  # 只导入GameResults_93_*.json文件")
		fmt.Println("  ./filteringData import-s3 112,103,105      # 智能导入游戏112,103,105（自动检测模式）")
		fmt.Println("  ./filteringData import-s3-normal 112,103   # 只导入游戏112,103的普通模式文件")
		fmt.Println("  ./filteringData import-s3-fb 112,103       # 只导入游戏112,103的购买夺宝模式文件")
		fmt.Println("  ./filteringData import-s3 112,103 50       # 智能导入RTP等级50的文件")
		fmt.Println("  ./filteringData import-s3 112,103 50 hp    # 智能导入到生产环境")
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "generate4":
		runGenerateMode4()
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
			if isGameId(arg) {
				// ./filteringData importFb <gameId>
				gid, _ := strconv.Atoi(arg)
				runImportFbModeWithGameId(gid, "", "")
			} else {
				// ./filteringData importFb <levelId>
				runImportFbMode(arg, "")
			}
		} else if len(os.Args) == 4 {
			arg1, arg2 := os.Args[2], os.Args[3]
			if isGameId(arg1) && IsEnv(arg2) {
				// ./filteringData importFb <gameId> <env>
				gid, _ := strconv.Atoi(arg1)
				env := ResolveEnv(arg2)
				runImportFbModeWithGameId(gid, "", env)
			} else if IsEnv(arg2) {
				// ./filteringData importFb <levelId> <env>
				env := ResolveEnv(arg2)
				runImportFbMode(arg1, env)
			} else if isGameId(arg1) {
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
	case "import-s3":
		// S3智能导入命令：./filteringData import-s3 <gameIds> [level] [env]
		// 自动检测游戏ID下的normal和fb模式，先导入normal再导入fb
		handleS3ImportCommand("auto")
	case "import-s3-normal":
		// S3普通模式导入命令：./filteringData import-s3-normal <gameIds> [level] [env]
		// 只导入normal模式文件
		handleS3ImportCommand("normal")
	case "import-s3-fb":
		// S3购买夺宝模式导入命令：./filteringData import-s3-fb <gameIds> [level] [env]
		// 只导入fb模式文件
		handleS3ImportCommand("fb")
	case "sp-stats":
		// SP统计命令：./filteringData sp-stats <gameId>
		runSpStatisticsFromJSON()
	default:
		fmt.Printf("未知命令: %s\n", command)
		fmt.Println("支持的命令: generate4, import, importFb, import-s3, import-s3-normal, import-s3-fb, sp-stats")
		os.Exit(1)
	}
}

// runGenerateMode 运行生成模式

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

// smartFillFbData 智能填充购买夺宝数据（动态平衡数量和RTP）
func smartFillFbData(data []GameResultData, totalWin, totalBet float64, targetCount int,
	targetRTP, rtpLowerLimit, maxAllowWin float64, unusedData []GameResultData,
	usedIds map[int]bool, rng *rand.Rand) ([]GameResultData, float64) {

	printf := func(format string, args ...interface{}) {
		fmt.Printf(format, args...)
	}

	// 第一阶段：智能填充到目标数量
	printf("\n🎯 阶段1：智能填充数据到目标数量\n")
	currentRTP := totalWin / totalBet
	rtpGap := targetRTP - currentRTP
	needCount := targetCount - len(data)

	printf("需要填充: %d 条, 当前RTP: %.6f, 目标RTP: %.6f, RTP差距: %.6f\n",
		needCount, currentRTP, targetRTP, rtpGap)

	// 按金额排序，准备分级数据
	sortedData := make([]GameResultData, len(unusedData))
	copy(sortedData, unusedData)
	sort.Slice(sortedData, func(i, j int) bool {
		return sortedData[i].AW < sortedData[j].AW
	})

	// 将数据分为三档：小金额、中金额、大金额
	smallThreshold := len(sortedData) / 3
	largeThreshold := len(sortedData) * 2 / 3

	var smallAW, mediumAW, largeAW []GameResultData
	for i, item := range sortedData {
		if i < smallThreshold {
			smallAW = append(smallAW, item)
		} else if i < largeThreshold {
			mediumAW = append(mediumAW, item)
		} else {
			largeAW = append(largeAW, item)
		}
	}

	printf("数据分级 - 小金额: %d 条, 中金额: %d 条, 大金额: %d 条\n",
		len(smallAW), len(mediumAW), len(largeAW))

	// 根据RTP差距选择填充策略
	var fillSource []GameResultData
	var strategyName string

	if rtpGap > 0.02 {
		// RTP严重不足（差距>2%），主要使用大金额数据
		strategyName = "大金额为主"
		fillSource = append(fillSource, largeAW...)
		fillSource = append(fillSource, mediumAW...)
		fillSource = append(fillSource, smallAW...)
	} else if rtpGap > 0.005 {
		// RTP略微不足（0.5%-2%），主要使用中金额数据
		strategyName = "中金额为主"
		fillSource = append(fillSource, mediumAW...)
		fillSource = append(fillSource, largeAW...)
		fillSource = append(fillSource, smallAW...)
	} else if rtpGap > -0.005 {
		// RTP接近目标（±0.5%），混合使用
		strategyName = "混合策略"
		fillSource = append(fillSource, mediumAW...)
		fillSource = append(fillSource, smallAW...)
		fillSource = append(fillSource, largeAW...)
	} else {
		// RTP超标（差距<-0.5%），主要使用小金额数据
		strategyName = "小金额为主"
		fillSource = append(fillSource, smallAW...)
		fillSource = append(fillSource, mediumAW...)
		fillSource = append(fillSource, largeAW...)
	}

	printf("选择策略: %s (RTP差距: %.6f)\n", strategyName, rtpGap)

	// 执行智能填充
	filled := 0
	perm := rng.Perm(len(fillSource))
	for i := 0; i < needCount && i < len(perm); i++ {
		idx := perm[i]
		item := fillSource[idx]

		// 检查是否已使用
		if usedIds[item.ID] {
			continue
		}

		// 检查RTP上限
		newTotalWin := totalWin + item.AW
		if newTotalWin <= maxAllowWin {
			data = append(data, item)
			totalWin += item.AW
			usedIds[item.ID] = true
			filled++
		}
	}

	printf("✅ 阶段1完成: 填充 %d 条, 当前数量: %d/%d, 当前RTP: %.6f\n",
		filled, len(data), targetCount, totalWin/totalBet)

	// 预处理：如果RTP超标且金额接近上限，先替换大金额为小金额
	currentRTPBeforeFill := totalWin / totalBet
	if len(data) < targetCount && currentRTPBeforeFill > targetRTP && totalWin >= maxAllowWin*0.99 {
		stillNeed := targetCount - len(data)
		printf("⚠️ RTP超标且金额接近上限，先替换 %d 条大金额为小金额\n", min(stillNeed, len(data)/10))

		// 对现有数据按金额从大到小排序
		sort.Slice(data, func(i, j int) bool {
			return data[i].AW > data[j].AW
		})

		// 准备小金额替换数据
		var smallReplacements []GameResultData
		for _, item := range smallAW {
			if !usedIds[item.ID] {
				smallReplacements = append(smallReplacements, item)
			}
		}
		sort.Slice(smallReplacements, func(i, j int) bool {
			return smallReplacements[i].AW < smallReplacements[j].AW
		})

		// 执行预替换（最多替换10%）
		replaceCount := min(stillNeed, len(data)/10)
		if replaceCount > len(smallReplacements) {
			replaceCount = len(smallReplacements)
		}

		replaced := 0
		for i := 0; i < replaceCount && i < len(data) && i < len(smallReplacements); i++ {
			oldItem := data[i]
			newItem := smallReplacements[i]

			if newItem.AW < oldItem.AW {
				data[i] = newItem
				totalWin = totalWin - oldItem.AW + newItem.AW
				delete(usedIds, oldItem.ID)
				usedIds[newItem.ID] = true
				replaced++
			}
		}
		printf("✅ 预替换完成: %d 条, 新RTP: %.6f, 释放空间: %.2f\n",
			replaced, totalWin/totalBet, maxAllowWin-totalWin)
	}

	// 如果数据量还不够，重复使用
	if len(data) < targetCount {
		stillNeed := targetCount - len(data)
		printf("🔄 数据量仍不足 %d 条，重复使用数据填充...\n", stillNeed)

		// 如果当前RTP已经超标，优先使用最小金额的数据
		currentRTPNow := totalWin / totalBet
		if currentRTPNow > targetRTP {
			printf("⚠️ 当前RTP已超标 (%.6f > %.6f)，使用最小金额数据填充\n", currentRTPNow, targetRTP)
			// 使用smallAW（最小金额数据）
			sort.Slice(fillSource, func(i, j int) bool {
				return fillSource[i].AW < fillSource[j].AW
			})
		}

		// 重复填充，放宽RTP上限检查
		relaxedUpperBound := maxAllowWin * 1.01 // 允许超出1%
		for i := 0; i < stillNeed; i++ {
			idx := i % len(fillSource)
			item := fillSource[idx]

			newTotalWin := totalWin + item.AW
			// 如果数量严重不足且RTP接近目标，放宽限制
			if newTotalWin <= relaxedUpperBound || (stillNeed > 100 && math.Abs(currentRTPNow-targetRTP) < 0.01) {
				data = append(data, item)
				totalWin += item.AW
			} else if newTotalWin <= maxAllowWin {
				data = append(data, item)
				totalWin += item.AW
			}
		}
		printf("✅ 重复填充完成, 当前数量: %d/%d, RTP: %.6f\n", len(data), targetCount, totalWin/totalBet)
	}

	// 第二阶段：数量足够后，进行RTP精确调整
	printf("\n🎯 阶段2：RTP精确调整（数量已达标）\n")
	finalRTP := totalWin / totalBet
	finalRtpGap := targetRTP - finalRTP
	printf("当前RTP: %.6f, 目标RTP: %.6f, RTP差距: %.6f\n", finalRTP, targetRTP, finalRtpGap)

	// 如果RTP偏差超过0.5%，进行智能替换调整
	if math.Abs(finalRtpGap) > 0.005 {
		printf("⚠️ RTP偏差较大，开始智能替换调整...\n")

		// 计算需要调整的次数（最多替换20%的数据）
		maxReplacements := targetCount / 5
		replaced := 0

		if finalRtpGap > 0 {
			// RTP不足，需要替换小金额为大金额
			printf("📈 RTP不足，替换小金额为大金额数据\n")

			// 对当前数据按金额排序
			sort.Slice(data, func(i, j int) bool {
				return data[i].AW < data[j].AW
			})

			// 准备大金额替换数据
			var largeReplacements []GameResultData
			for _, item := range largeAW {
				if !usedIds[item.ID] {
					largeReplacements = append(largeReplacements, item)
				}
			}
			for _, item := range mediumAW {
				if !usedIds[item.ID] {
					largeReplacements = append(largeReplacements, item)
				}
			}

			// 按金额从大到小排序
			sort.Slice(largeReplacements, func(i, j int) bool {
				return largeReplacements[i].AW > largeReplacements[j].AW
			})

			printf("可用的大金额替换数据: %d 条\n", len(largeReplacements))

			// 执行替换
			for i := 0; i < maxReplacements && i < len(data) && replaced < len(largeReplacements); i++ {
				oldItem := data[i]
				newItem := largeReplacements[replaced]

				// 确保新数据的金额确实更大
				if newItem.AW > oldItem.AW {
					newTotalWin := totalWin - oldItem.AW + newItem.AW
					newRTP := newTotalWin / totalBet

					// 检查替换后RTP是否更接近目标，且不超过上限
					if newTotalWin <= maxAllowWin && newRTP <= targetRTP+0.01 {
						data[i] = newItem
						totalWin = newTotalWin
						delete(usedIds, oldItem.ID)
						usedIds[newItem.ID] = true
						replaced++

						// 如果已经接近目标，提前停止
						if math.Abs(targetRTP-newRTP) < 0.003 {
							printf("✅ 已接近目标RTP，提前结束替换\n")
							break
						}
					}
				}
			}

			printf("✅ 替换完成: %d 条小金额→大金额, 新RTP: %.6f, 偏差: %.6f\n",
				replaced, totalWin/totalBet, math.Abs(targetRTP-totalWin/totalBet))

		} else {
			// RTP超标，需要替换大金额为小金额
			printf("📉 RTP超标，替换大金额为小金额数据\n")

			// 对当前数据按金额从大到小排序
			sort.Slice(data, func(i, j int) bool {
				return data[i].AW > data[j].AW
			})

			// 准备小金额替换数据
			var smallReplacements []GameResultData
			for _, item := range smallAW {
				if !usedIds[item.ID] {
					smallReplacements = append(smallReplacements, item)
				}
			}
			for _, item := range mediumAW {
				if !usedIds[item.ID] {
					smallReplacements = append(smallReplacements, item)
				}
			}

			// 按金额从小到大排序
			sort.Slice(smallReplacements, func(i, j int) bool {
				return smallReplacements[i].AW < smallReplacements[j].AW
			})

			printf("可用的小金额替换数据: %d 条\n", len(smallReplacements))

			// 执行替换
			for i := 0; i < maxReplacements && i < len(data) && replaced < len(smallReplacements); i++ {
				oldItem := data[i]
				newItem := smallReplacements[replaced]

				// 确保新数据的金额确实更小
				if newItem.AW < oldItem.AW {
					newTotalWin := totalWin - oldItem.AW + newItem.AW
					newRTP := newTotalWin / totalBet

					// 检查替换后RTP是否更接近目标，且不低于下限
					if newRTP >= rtpLowerLimit && newRTP >= targetRTP-0.01 {
						data[i] = newItem
						totalWin = newTotalWin
						delete(usedIds, oldItem.ID)
						usedIds[newItem.ID] = true
						replaced++

						// 如果已经接近目标，提前停止
						if math.Abs(targetRTP-newRTP) < 0.003 {
							printf("✅ 已接近目标RTP，提前结束替换\n")
							break
						}
					}
				}
			}

			printf("✅ 替换完成: %d 条大金额→小金额, 新RTP: %.6f, 偏差: %.6f\n",
				replaced, totalWin/totalBet, math.Abs(targetRTP-totalWin/totalBet))
		}

	} else {
		printf("✅ RTP已在合理范围内 (偏差: %.6f < 0.005)，无需调整\n", math.Abs(finalRtpGap))
	}

	// 最终状态报告
	printf("\n📊 最终状态：\n")
	printf("  数据量: %d/%d (%.1f%%)\n", len(data), targetCount, float64(len(data))/float64(targetCount)*100)
	printf("  RTP: %.6f (目标: %.6f, 下限: %.6f)\n", totalWin/totalBet, targetRTP, rtpLowerLimit)
	printf("  RTP偏差: %.6f\n", math.Abs(targetRTP-totalWin/totalBet))
	printf("  总中奖金额: %.2f (上限: %.2f)\n", totalWin, maxAllowWin)

	return data, totalWin
}

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

// runGenerateMode4 运行生成模式V4 - 使用RTP档位倍率分布策略
func runGenerateMode4() {
	// 记录程序开始时间
	startTime := time.Now()

	// 初始化随机数种子
	rand.Seed(time.Now().UnixNano())

	// 加载配置文件
	config, err := LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("加载配置文件失败: %v", err)
	}
	fmt.Printf("配置加载成功（V4模式）- 游戏ID: %d, 目标数据量: %d\n", config.Game.ID, config.Tables.DataNum)
	fmt.Printf("🔧 V4策略：RTP档位倍率分布策略\n")
	fmt.Printf("📋 高RTP档位(14,15,120,150,200,300,500)将使用V3配置: data_num_v3=%d, data_table_num_3=%d\n",
		config.Tables.DataNum, config.Tables.DataTableNum)

	// 加载RTP倍率分布配置
	rtpConfig, err := LoadRtpMultiplierConfig("rtp_multiplier_config.yaml")
	if err != nil {
		log.Fatalf("加载RTP倍率分布配置失败: %v", err)
	}

	// 连接数据库
	db, err := NewDatabase(config, "")
	if err != nil {
		log.Fatalf("数据库连接失败: %v", err)
	}
	defer db.Close()

	// 清理 sp=true 且 aw=0 的数据
	if err := db.CleanSpZeroAwData(); err != nil {
		log.Fatalf("清理数据失败: %v", err)
	}

	// 预取共享只读数据
	winDataAll, err := db.GetWinData()
	if err != nil {
		log.Fatalf("获取中奖数据失败: %v", err)
	}
	noWinDataAll, err := db.GetNoWinData()
	if err != nil {
		log.Fatalf("获取不中奖数据失败: %v", err)
	}

	// 合并所有数据
	allData := append(winDataAll, noWinDataAll...)
	fmt.Printf("✅ 总数据量: %d 条（中奖: %d, 不中奖: %d）\n", len(allData), len(winDataAll), len(noWinDataAll))

	// 使用RtpLevels配置
	for rtpNum := 0; rtpNum < len(RtpLevels); rtpNum++ {
		// 并发度：CPU 核数
		worker := runtime.NumCPU()
		sem := make(chan struct{}, worker)
		var wg sync.WaitGroup

		// 捕获当前循环变量
		rtpNo := RtpLevels[rtpNum].RtpNo
		rtpVal := RtpLevels[rtpNum].Rtp

		// 根据RTP档位选择配置
		var dataNum, tableNum int
		if isHighRtpLevel(rtpNo) {
			dataNum = config.Tables.DataNumV3
			tableNum = config.Tables.DataTableNum3
			fmt.Printf("🔧 RTP档位 %.0f 使用V3配置: 数据量=%d, 生表数=%d\n", rtpNo, dataNum, tableNum)
		} else {
			dataNum = config.Tables.DataNum
			tableNum = config.Tables.DataTableNum
		}

		// 计算总投注
		totalBet := config.Bet.CS * config.Bet.ML * config.Bet.BL * config.Bet.FB * float64(dataNum)

		for t := 0; t < tableNum; t++ {
			sem <- struct{}{}
			wg.Add(1)

			testIndex := t + 1

			go func(rtpNo float64, rtpVal float64, testIndex int, dataNum int, totalBet float64) {
				defer func() { <-sem; wg.Done() }()

				// 记录单次测试开始时间
				testStartTime := time.Now()
				// 即时输出单次任务开始，便于观察进度
				fmt.Printf("▶️ 开始生成（V4模式）| RTP等级 %.0f | 第%d次 | 数据量:%d | %s\n", rtpNo, testIndex, dataNum, testStartTime.Format(time.RFC3339))

				if err := runRtpTestV4(db, config, rtpConfig, rtpNo, rtpVal, testIndex, totalBet, allData, dataNum); err != nil {
					log.Printf("RTP测试V4失败: %v", err)
				}

				// 计算并输出单次测试耗时
				testDuration := time.Since(testStartTime)
				fmt.Printf("⏱️  RTP等级 %.0f (第%d次生成-V4模式) 耗时: %v\n", rtpNo, testIndex, testDuration)
			}(rtpNo, rtpVal, testIndex, dataNum, totalBet)
		}

		wg.Wait()
	}

	// 计算并输出整个程序的总耗时
	totalDuration := time.Since(startTime)
	fmt.Printf("\n🎉 RTP数据筛选和保存完成（V4模式）！\n")
	fmt.Printf("⏱️  整个程序总耗时: %v\n", totalDuration)
}

// runRtpTestV4 执行单次RTP测试V4 - 使用RTP档位倍率分布策略
func runRtpTestV4(db *Database, config *Config, rtpConfig *RtpMultiplierConfig, rtpLevel float64, rtp float64, testNumber int, totalBet float64, allData []GameResultData, dataNum int) error {
	var logBuf bytes.Buffer
	printf := func(format string, a ...interface{}) {
		fmt.Fprintf(&logBuf, format, a...)
	}
	testStartTime := time.Now()

	// 任务头分隔线
	printf("\n========== [TASK BEGIN - V4 MULTIPLIER] RtpNo: %.0f | Test: %d | %s =========\n", rtpLevel, testNumber, time.Now().Format(time.RFC3339))

	// 获取RTP档位分布配置
	distribution, err := rtpConfig.GetRtpDistribution(int(rtpLevel))
	if err != nil {
		return fmt.Errorf("获取RTP档位分布配置失败: %v", err)
	}

	printf("🎯 RTP档位: %.0f, 目标RTP: %.4f, 允许中奖金额: %.2f\n", rtpLevel, rtp, totalBet*rtp)
	printf("📊 分布配置: 不中奖=%.1f%%, 低倍率=%.1f%%, 中倍率=%.1f%%, 高倍率=%.1f%%\n",
		distribution.MultiplierDistribution.ZeroWin*100,
		distribution.MultiplierDistribution.LowMultiplier*100,
		distribution.MultiplierDistribution.MediumMultiplier*100,
		distribution.MultiplierDistribution.HighMultiplier*100)

	// 每任务独立随机源
	seed := time.Now().UnixNano() ^ int64(config.Game.ID)*1_000_003 ^ int64(testNumber)*1_000_033 ^ int64(rtpLevel)*1_000_037
	rng := rand.New(rand.NewSource(seed))

	// 计算单次投注金额
	perSpinBet := config.Bet.CS * config.Bet.ML * config.Bet.BL

	// 按倍率区间分类数据
	printf("🔄 正在按倍率区间分类数据...\n")
	dataRanges := ClassifyDataByMultiplier(allData, perSpinBet)

	// 输出各区间数据统计
	for rangeName, rangeData := range dataRanges {
		printf("  %s: %d 条 (%.1f%%)\n", rangeName, rangeData.Count, rangeData.Percentage*100)
	}

	// 根据分布配置生成数据
	printf("🔄 正在根据分布配置生成数据...\n")
	generatedData, err := GenerateDataByDistribution(distribution, dataNum, dataRanges)
	if err != nil {
		return fmt.Errorf("根据分布配置生成数据失败: %v", err)
	}

	printf("✅ 初步生成数据: %d 条\n", len(generatedData))

	// 计算当前RTP
	currentRTP := CalculateRTP(generatedData, totalBet)
	printf("📊 当前RTP: %.6f, 目标RTP: %.6f, 偏差: %.6f\n", currentRTP, rtp, math.Abs(currentRTP-rtp))

	// 调整RTP以满足目标
	printf("🔄 正在调整RTP以满足目标...\n")
	adjustedData, err := AdjustRTPByReplacement(generatedData, rtp, totalBet, dataRanges, int(rtpLevel), rtpConfig)
	if err != nil {
		return fmt.Errorf("调整RTP失败: %v", err)
	}

	// 计算调整后的RTP
	finalRTP := CalculateRTP(adjustedData, totalBet)
	rtpDeviation := math.Abs(finalRTP - rtp)

	// 设置RTP下限（目标值-0.1）
	rtpLowerLimit := rtp - 0.1
	if finalRTP < rtpLowerLimit {
		printf("⚠️ RTP低于下限 (%.6f < %.6f)，开始动态调整RTP\n", finalRTP, rtpLowerLimit)

		// 动态调整RTP到下限
		adjustedData, err = adjustRTPToLowerLimit(adjustedData, rtpLowerLimit, totalBet, dataRanges)
		if err == nil {
			newRTP := CalculateRTP(adjustedData, totalBet)
			// 只有新RTP确实提升了才使用
			if newRTP > finalRTP {
				finalRTP = newRTP
				rtpDeviation = math.Abs(finalRTP - rtp)
				printf("✅ RTP动态调整完成，最终RTP: %.6f (提升: %.6f)\n", finalRTP, newRTP-finalRTP)
			} else {
				printf("⚠️ RTP动态调整失败，保持原RTP: %.6f\n", finalRTP)
			}
		} else {
			printf("⚠️ RTP动态调整出错: %v\n", err)
		}
	}

	printf("✅ 调整后RTP: %.6f, 目标RTP: %.6f, 偏差: %.6f\n", finalRTP, rtp, rtpDeviation)

	// 如果数据量不足，用不中奖数据补充
	if len(adjustedData) < dataNum {
		needMore := dataNum - len(adjustedData)
		printf("🔄 数据量不足，需要补充 %d 条不中奖数据\n", needMore)

		// 从零倍数据中随机选择
		if len(dataRanges["zero_win"].Data) > 0 {
			perm := rng.Perm(len(dataRanges["zero_win"].Data))
			for i := 0; i < needMore && i < len(perm); i++ {
				idx := perm[i]
				adjustedData = append(adjustedData, dataRanges["zero_win"].Data[idx])
			}
		}
	}

	// 如果数据量超出，优先移除低RTP数据
	if len(adjustedData) > dataNum {
		excess := len(adjustedData) - dataNum
		printf("🔄 数据量超出，需要移除 %d 条数据\n", excess)

		// 按RTP从低到高排序，优先移除低RTP数据
		sort.Slice(adjustedData, func(i, j int) bool {
			rtpI := adjustedData[i].AW / perSpinBet
			rtpJ := adjustedData[j].AW / perSpinBet
			return rtpI < rtpJ
		})

		// 移除前excess个低RTP数据
		adjustedData = adjustedData[excess:]
	}

	// 最终统计
	finalRTP = CalculateRTP(adjustedData, totalBet)
	rtpDeviation = math.Abs(finalRTP - rtp)

	printf("📊 最终统计:\n")
	printf("  - 总数据量: %d 条\n", len(adjustedData))
	printf("  - 总投注: %.2f\n", totalBet)
	printf("  - 总中奖: %.2f\n", totalBet*finalRTP)
	printf("  - 实际RTP: %.6f\n", finalRTP)
	printf("  - 目标RTP: %.6f\n", rtp)
	printf("  - RTP偏差: %.6f\n", rtpDeviation)

	// 验证数据量
	if len(adjustedData) != dataNum {
		return fmt.Errorf("❌ 数据量不匹配：期望 %d 条, 实际 %d 条", dataNum, len(adjustedData))
	}

	// 打乱输出顺序
	rand.Shuffle(len(adjustedData), func(i, j int) {
		adjustedData[i], adjustedData[j] = adjustedData[j], adjustedData[i]
	})

	// 保存到JSON文件
	var outputDir string = filepath.Join("output", fmt.Sprintf("%d", config.Game.ID))
	if err := saveToJSON(adjustedData, config, rtpLevel, testNumber, outputDir); err != nil {
		return fmt.Errorf("保存JSON文件失败: %v", err)
	}

	// 任务尾分隔线
	printf("========== [TASK END - V4 MULTIPLIER]   RtpNo: %.0f | Test: %d =========\n\n", rtpLevel, testNumber)
	printf("⏱️  RTP等级 %.0f (第%d次生成-V4策略) 耗时: %v\n", rtpLevel, testNumber, time.Since(testStartTime))

	outputMu.Lock()
	fmt.Print(logBuf.String())
	outputMu.Unlock()
	return nil
}

// parseGameIds 解析游戏ID字符串
func parseGameIds(gameIdsStr string) ([]int, error) {
	var gameIds []int

	// 按逗号分割
	parts := strings.Split(gameIdsStr, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		gameId, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("无效的游戏ID: %s", part)
		}

		gameIds = append(gameIds, gameId)
	}

	if len(gameIds) == 0 {
		return nil, fmt.Errorf("未提供有效的游戏ID")
	}

	return gameIds, nil
}

// handleS3ImportCommand 处理S3导入命令的统一函数
func handleS3ImportCommand(mode string) {
	commandName := "import-s3"

	if len(os.Args) < 3 {
		fmt.Println("❌ 缺少游戏ID参数")
		fmt.Printf("用法: ./filteringData %s <gameIds> [level] [env]\n", commandName)
		fmt.Printf("示例: ./filteringData %s 112,103,105\n", commandName)
		fmt.Printf("示例: ./filteringData %s 112,103 50\n", commandName)
		fmt.Printf("示例: ./filteringData %s 112,103 50 hp\n", commandName)
		fmt.Println("\n💡 智能模式：自动检测游戏ID下的normal和fb模式文件")
		fmt.Println("   - 如果同时存在normal和fb文件，先导入normal再导入fb")
		fmt.Println("   - 如果只存在一种模式，只导入该模式的文件")
		os.Exit(1)
	}

	// 解析游戏ID列表
	gameIdsStr := os.Args[2]
	gameIds, err := parseGameIds(gameIdsStr)
	if err != nil {
		fmt.Printf("❌ 解析游戏ID失败: %v\n", err)
		os.Exit(1)
	}

	// 解析等级过滤参数和环境参数
	levelFilter := ""
	env := "" // 默认环境

	if len(os.Args) > 3 {
		arg3 := os.Args[3]
		// 检查第三个参数是环境还是等级
		if IsEnv(arg3) {
			// 第三个参数是环境
			env = ResolveEnv(arg3)
		} else {
			// 第三个参数是等级
			levelFilter = arg3
			// 检查第四个参数是否是环境
			if len(os.Args) > 4 {
				arg4 := os.Args[4]
				if IsEnv(arg4) {
					env = ResolveEnv(arg4)
				} else {
					fmt.Printf("❌ 无效的环境: %s，支持的环境: local/l, hk-test/ht, br-test/bt, br-prod/bp, us-prod/up, hk-prod/hp\n", arg4)
					os.Exit(1)
				}
			}
		}
	}

	runS3ImportMode(gameIds, mode, levelFilter, env)
}

// runS3ImportMode 运行S3导入模式
func runS3ImportMode(gameIds []int, mode string, levelFilter string, env string) {
	envDisplay := ""
	if env != "" {
		envDisplay = fmt.Sprintf(" [环境: %s]", env)
	}

	modeDisplay := "普通模式"
	if mode == "fb" {
		modeDisplay = "购买夺宝模式"
	} else if mode == "auto" {
		modeDisplay = "智能模式"
	}

	fmt.Printf("🔄 启动S3导入模式 (游戏IDs: %v, 模式: %s", gameIds, modeDisplay)
	if levelFilter != "" {
		fmt.Printf(", 等级过滤: %s", levelFilter)
	}
	fmt.Printf(")%s\n", envDisplay)

	// 加载配置
	config, err := LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("❌ 加载配置失败: %v", err)
	}

	// 检查S3配置
	if !config.S3.Enabled {
		log.Fatalf("❌ S3功能未启用，请在配置文件中设置 s3.enabled: true")
	}

	// 连接数据库
	db, err := NewDatabase(config, env)
	if err != nil {
		log.Fatalf("❌ 连接数据库失败: %v", err)
	}
	defer db.Close()

	// 创建S3导入器
	importer, err := NewS3Importer(db, config)
	if err != nil {
		log.Fatalf("❌ 创建S3导入器失败: %v", err)
	}

	// 执行S3导入
	if err := importer.ImportS3Files(gameIds, mode, levelFilter); err != nil {
		log.Fatalf("❌ S3导入失败: %v", err)
	}

	fmt.Println("✅ S3导入完成！")
}

// isHighRtpLevel 判断是否为高RTP档位，需要使用V3配置
func isHighRtpLevel(rtpNo float64) bool {
	highRtpLevels := []float64{14, 15, 120, 150, 200, 300, 500}
	for _, level := range highRtpLevels {
		if rtpNo == level {
			return true
		}
	}
	return false
}

// runSpStatisticsFromJSON 从JSON文件统计SP数据
func runSpStatisticsFromJSON() {
	if len(os.Args) < 3 {
		fmt.Println("❌ 缺少游戏ID参数")
		fmt.Println("用法: ./filteringData sp-stats <gameId>")
		fmt.Println("示例: ./filteringData sp-stats 93")
		fmt.Println("\n💡 功能说明:")
		fmt.Println("   - 读取 output/<gameId> 目录下的所有 JSON 文件")
		fmt.Println("   - 统计每个档位的每张表中 sp=true 的次数")
		fmt.Println("   - 显示详细的统计报告")
		os.Exit(1)
	}

	// 解析游戏ID
	gameIdStr := os.Args[2]
	gameId, err := strconv.Atoi(gameIdStr)
	if err != nil {
		log.Fatalf("无效的游戏ID: %s", gameIdStr)
	}

	// 构建输出目录路径
	outputDir := filepath.Join("output", fmt.Sprintf("%d", gameId))

	// 检查目录是否存在
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		fmt.Printf("❌ 目录不存在: %s\n", outputDir)
		fmt.Printf("💡 请先运行 generate4 命令生成数据\n")
		os.Exit(1)
	}

	fmt.Printf("🔄 正在读取目录: %s\n", outputDir)

	// 读取目录下的所有JSON文件
	files, err := os.ReadDir(outputDir)
	if err != nil {
		log.Fatalf("读取目录失败: %v", err)
	}

	// 按档位分组统计
	type LevelStats struct {
		RtpLevel     float64
		TableCount   int
		TotalRecords int
		SpTrueCount  int
		SpFalseCount int
		Tables       map[int]struct {
			Records      int
			SpTrueCount  int
			SpFalseCount int
		}
	}

	levelStatsMap := make(map[float64]*LevelStats)

	// 遍历所有JSON文件
	jsonCount := 0
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".json") {
			continue
		}

		jsonCount++
		filePath := filepath.Join(outputDir, file.Name())

		// 读取JSON文件
		fileData, err := os.ReadFile(filePath)
		if err != nil {
			fmt.Printf("⚠️ 读取文件失败 %s: %v\n", file.Name(), err)
			continue
		}

		// 解析JSON
		var jsonData struct {
			RtpLevel int `json:"rtpLevel"`
			SrNumber int `json:"srNumber"`
			Data     []struct {
				SP bool `json:"sp"`
			} `json:"data"`
		}

		err = json.Unmarshal(fileData, &jsonData)
		if err != nil {
			fmt.Printf("⚠️ 解析JSON失败 %s: %v\n", file.Name(), err)
			continue
		}

		rtpLevel := float64(jsonData.RtpLevel)

		// 初始化档位统计
		if levelStatsMap[rtpLevel] == nil {
			levelStatsMap[rtpLevel] = &LevelStats{
				RtpLevel: rtpLevel,
				Tables: make(map[int]struct {
					Records      int
					SpTrueCount  int
					SpFalseCount int
				}),
			}
		}

		stats := levelStatsMap[rtpLevel]

		// 统计当前文件的SP数据
		spTrueCount := 0
		spFalseCount := 0
		for _, item := range jsonData.Data {
			if item.SP {
				spTrueCount++
			} else {
				spFalseCount++
			}
		}

		// 更新表统计
		stats.Tables[jsonData.SrNumber] = struct {
			Records      int
			SpTrueCount  int
			SpFalseCount int
		}{
			Records:      len(jsonData.Data),
			SpTrueCount:  spTrueCount,
			SpFalseCount: spFalseCount,
		}

		// 更新档位总计
		stats.TotalRecords += len(jsonData.Data)
		stats.SpTrueCount += spTrueCount
		stats.SpFalseCount += spFalseCount
	}

	if jsonCount == 0 {
		fmt.Printf("❌ 目录中没有找到JSON文件\n")
		os.Exit(1)
	}

	// 打印统计报告
	fmt.Printf("\n📊 游戏 %d 的 SP 统计报告\n", gameId)
	fmt.Printf("============================================================\n")
	fmt.Printf("📁 目录: %s\n", outputDir)
	fmt.Printf("📄 JSON文件数: %d\n", jsonCount)

	// 按档位排序输出
	var levels []float64
	for level := range levelStatsMap {
		levels = append(levels, level)
	}
	sort.Float64s(levels)

	totalRecords := 0
	totalSpTrue := 0
	totalSpFalse := 0
	totalTables := 0

	for _, level := range levels {
		stats := levelStatsMap[level]
		stats.TableCount = len(stats.Tables)

		fmt.Printf("\n🎯 RTP 档位: %.0f\n", stats.RtpLevel)
		fmt.Printf("   📋 总表数: %d\n", stats.TableCount)
		fmt.Printf("   📊 总记录数: %d\n", stats.TotalRecords)

		if stats.TotalRecords > 0 {
			spTrueRatio := float64(stats.SpTrueCount) / float64(stats.TotalRecords) * 100
			spFalseRatio := float64(stats.SpFalseCount) / float64(stats.TotalRecords) * 100
			fmt.Printf("   🎲 SP=True: %d (%.2f%%)\n", stats.SpTrueCount, spTrueRatio)
			fmt.Printf("   🎮 SP=False: %d (%.2f%%)\n", stats.SpFalseCount, spFalseRatio)
		}

		// 按表编号排序
		var tableNumbers []int
		for tableNum := range stats.Tables {
			tableNumbers = append(tableNumbers, tableNum)
		}
		sort.Ints(tableNumbers)

		fmt.Printf("\n   📝 各表详细统计:\n")
		for _, tableNum := range tableNumbers {
			tableStats := stats.Tables[tableNum]
			if tableStats.Records > 0 {
				ratio := float64(tableStats.SpTrueCount) / float64(tableStats.Records) * 100
				fmt.Printf("     表 %d: 总记录 %d, SP=True %d (%.2f%%)\n",
					tableNum, tableStats.Records, tableStats.SpTrueCount, ratio)
			}
		}

		totalRecords += stats.TotalRecords
		totalSpTrue += stats.SpTrueCount
		totalSpFalse += stats.SpFalseCount
		totalTables += stats.TableCount
	}

	// 总体统计
	fmt.Printf("\n============================================================\n")
	fmt.Printf("📈 总体统计:\n")
	fmt.Printf("   总档位数: %d\n", len(levels))
	fmt.Printf("   总表数: %d\n", totalTables)
	fmt.Printf("   总记录数: %d\n", totalRecords)
	if totalRecords > 0 {
		totalSpRatio := float64(totalSpTrue) / float64(totalRecords) * 100
		fmt.Printf("   总 SP=True: %d (%.2f%%)\n", totalSpTrue, totalSpRatio)
		fmt.Printf("   总 SP=False: %d (%.2f%%)\n", totalSpFalse, 100-totalSpRatio)
	}
	fmt.Printf("============================================================\n")
}
