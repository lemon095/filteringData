package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/xuri/excelize/v2"
)

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

// 保证并发任务按块输出日志
var outputMu sync.Mutex

// createRtpTestTable 创建RTP测试结果表并添加索引
func createRtpTestTable(db *Database, config *Config) error {
	tableName := fmt.Sprintf("%s%d", config.Tables.OutputTablePrefix, config.Game.ID)

	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS "%s" (
			id SERIAL NOT NULL,
			"rtpLevel" REAL NOT NULL,
			"srNumber" INTEGER NOT NULL,
			"srId" INTEGER NOT NULL,
			"bet" INTEGER NOT NULL,
			"win" DECIMAL(65,30) NOT NULL,
			"detail" JSONB,
			"createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
			"updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
			CONSTRAINT "%s_pkey" PRIMARY KEY (id)
		);
	`, tableName, tableName)

	// 执行创建表语句
	_, err := db.DB.Exec(createSQL)
	if err != nil {
		return fmt.Errorf("创建表失败: %v", err)
	}

	// 创建索引
	indexes := []string{
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s_rtpLevel_idx" ON "%s" ("rtpLevel")`, tableName, tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s_srNumber_idx" ON "%s" ("srNumber")`, tableName, tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s_srId_idx" ON "%s" ("srId")`, tableName, tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s_rtpLevel_srNumber_srId_idx" ON "%s" ("rtpLevel", "srNumber", "srId")`, tableName, tableName),
	}

	for _, indexSQL := range indexes {
		_, err := db.DB.Exec(indexSQL)
		if err != nil {
			return fmt.Errorf("创建索引失败: %v", err)
		}
	}

	log.Printf("✅ 成功创建RTP测试表: %s", tableName)
	return nil
}

// convertToGameResults 将GameResultData转换为GameResult
func convertToGameResults(data []GameResultData, rtpLevel float64, testNumber int) []GameResult {
	var results []GameResult
	for _, item := range data {
		// 将JsonData转换为json.RawMessage
		detailBytes, _ := json.Marshal(item.GD.Data)

		result := GameResult{
			RtpLevel: rtpLevel,
			SrNumber: testNumber,
			SrId:     item.ID,
			Bet:      float64(item.TB), // 转换为float64
			Win:      item.AW,
			Detail:   detailBytes, // 转换为json.RawMessage
		}
		results = append(results, result)
	}
	return results
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
		targetRtpMin = 1.8
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

		// 精度检查：过滤掉精度有问题的数据（超过2位小数）
		// aw := item.AW
		// roundedAW := math.Round(aw*100) / 100
		// if math.Abs(aw-roundedAW) > 0.0001 {
		// 	// 跳过精度有问题的数据
		// 	continue
		// }

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
		totalWin += item.AW
		if totalWin > allowWin*1.005 {
			continue
		}

		// 特殊处理RtpNo为15：检查RTP是否在允许范围内（基于加入后的新值判断）
		if isSpecialRtp15 {
			newRtp := totalWin / totalBet
			if newRtp > targetRtpMax {
				continue // 如果RTP超过上限, 跳过这条数据
			}
		}

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

		// 达到目标金额就停止（RTP 2.0以下档位）
		// if rtp < 2.0 && totalWin >= allowWin {
		// 	fmt.Printf("达到目标中奖金额, 当前中奖总额: %.2f\n", totalWin)
		// 	break
		// }
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
						aw := item.AW
						roundedAW := math.Round(aw*100) / 100
						if math.Abs(aw-roundedAW) > 0.0001 {
							continue
						}

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
						// 跳过精度有问题的数据
						// aw := item.AW
						// roundedAW := math.Round(aw*100) / 100
						// if math.Abs(aw-roundedAW) > 0.0001 {
						// 	continue
						// }

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
			//不符合rtpLevel条件
			printf("⚠️ 不符合rtpLevel条件, rtpLevel: %.0f,totalWin: %.2f, allowWin: %.2f, ...\n", rtpLevel, totalWin, allowWin)
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

	// 最终验证数据量
	printf("🔍 最终验证: 期望 %d 条, 实际 %d 条\n", config.Tables.DataNum, len(data))
	if len(data) != config.Tables.DataNum {
		return fmt.Errorf("❌ 数据量不匹配：期望 %d 条, 实际 %d 条", config.Tables.DataNum, len(data))
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

	// 特殊处理RtpNo为15：验证RTP是否在允许范围内
	if isSpecialRtp15 {
		if finalRTP < targetRtpMin || finalRTP > targetRtpMax {
			return fmt.Errorf("❌ RtpNo为15的RTP验证失败: 当前RTP %.4f 不在允许范围 [%.1f, %.1f] 内", finalRTP, targetRtpMin, targetRtpMax)
		}
		fmt.Printf("🎯 RtpNo为15 RTP验证通过: %.4f 在范围 [%.1f, %.1f] 内\n", finalRTP, targetRtpMin, targetRtpMax)
	}

	// 保存数据到Excel文件（暂时注释掉数据库写入）
	// dbWriter := NewDBWriter(db, config)
	// if err := dbWriter.SaveFilteredData(convertToGameResults(data, rtpLevel, testNumber)); err != nil {
	// 	return fmt.Errorf("保存数据失败: %v", err)
	// }

	//这里的随机data顺序呢
	rand.Shuffle(len(data), func(i, j int) {
		data[i], data[j] = data[j], data[i]
	})

	// 先构建将要保存的文件路径, 便于记录日志
	// outputDir := "output"
	// fileName := fmt.Sprintf("%s%.0f_%d.json", config.Tables.OutputTablePrefix, rtpLevel, testNumber)
	// filePath := filepath.Join(outputDir, fileName)

	if err := saveToJSON(data, config, rtpLevel, testNumber); err != nil {
		return fmt.Errorf("保存CSV文件失败: %v", err)
	}

	// 追加保存CSV（顺序与JSON一致，因前面已Shuffle）
	// if err := saveToCSV(data, config, rtpLevel, testNumber); err != nil {
	// 	return fmt.Errorf("保存CSV文件失败: %v", err)
	// }

	// // 追加保存Excel，便于大字段（gd）在表格中查看
	// if err := saveToExcel(data, config, rtpLevel, testNumber); err != nil {
	// 	return fmt.Errorf("保存Excel文件失败: %v", err)
	// }

	// 任务尾分隔线
	printf("========== [TASK END]   RtpNo: %.0f | Test: %d =========\n\n", rtpLevel, testNumber)
	// 一次性按任务输出日志, 避免与其它 goroutine 交错
	// printf("📊 数据已保存到JSON文件: %s\n", filePath)
	printf("⏱️  RTP等级 %.0f (第%d次生成) 耗时: %v\n", rtpLevel, testNumber, time.Since(testStartTime))
	outputMu.Lock()
	fmt.Print(logBuf.String())
	outputMu.Unlock()
	return nil
}

func saveToJSON(data []GameResultData, config *Config, rtpLevel float64, testNumber int) error {
	// 创建输出目录
	outputDir := "output"
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
		// 确保AW字段是2位小数
		// roundedAW := math.Round(item.AW*100) / 100

		row := map[string]interface{}{
			"tb":        item.TB,
			"aw":        item.AW,
			"gwt":       item.GWT,
			"sp":        item.SP,
			"fb":        item.FB,
			"gd":        item.GD.Data,
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

// saveToCSV 将数据保存为CSV到 outcsv 目录
func saveToCSV(data []GameResultData, config *Config, rtpLevel float64, testNumber int) error {
	// 创建输出目录
	outputDir := "outcsv"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("创建CSV输出目录失败: %v", err)
	}

	// 文件名与 JSON 保持一致前缀，扩展名为 .csv
	fileName := fmt.Sprintf("%s%.0f_%d.csv", config.Tables.OutputTablePrefix, rtpLevel, testNumber)
	filePath := filepath.Join(outputDir, fileName)

	// 打开文件
	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("创建CSV文件失败: %v", err)
	}
	defer f.Close()

	// 写入 UTF-8 BOM，便于部分工具识别编码（不写 sep 行）
	_, _ = f.Write([]byte{0xEF, 0xBB, 0xBF}) // BOM

	w := csv.NewWriter(f)
	// 使用 CRLF 以提升在表格工具中的兼容性；分隔符采用标准逗号
	w.UseCRLF = true
	// 1) 统计 gd 顶层键，生成动态列
	gdKeySet := make(map[string]struct{})
	for _, item := range data {
		if item.GD.Data == nil {
			continue
		}
		if m, ok := item.GD.Data.(map[string]interface{}); ok {
			for k := range m {
				gdKeySet[k] = struct{}{}
			}
		}
	}
	gdKeys := make([]string, 0, len(gdKeySet))
	for k := range gdKeySet {
		gdKeys = append(gdKeys, k)
	}
	sort.Strings(gdKeys)

	// 2) 写表头：固定列 + 动态 gd.* 列 + 固定时间列
	header := []string{"tb", "aw", "gwt", "sp", "fb"}
	header = append(header, gdKeys...)
	header = append(header, "createdAt", "updatedAt")
	if err := w.Write(header); err != nil {
		return fmt.Errorf("写入CSV表头失败: %v", err)
	}

	// 3) 写数据行
	for _, item := range data {
		roundedAW := math.Round(item.AW*100) / 100

		row := []string{
			fmt.Sprintf("%d", item.TB),
			fmt.Sprintf("%.2f", roundedAW),
			fmt.Sprintf("%d", item.GWT),
			fmt.Sprintf("%t", item.SP),
			fmt.Sprintf("%d", item.FB),
		}

		var m map[string]interface{}
		if item.GD.Data != nil {
			if mm, ok := item.GD.Data.(map[string]interface{}); ok {
				m = mm
			}
		}
		for _, k := range gdKeys {
			var valStr string
			if m != nil {
				if v, ok := m[k]; ok && v != nil {
					switch vv := v.(type) {
					case string:
						valStr = vv
					case float64, bool, int, int64, float32:
						valStr = fmt.Sprint(vv)
					case map[string]interface{}, []interface{}:
						if b, err := json.Marshal(v); err == nil {
							valStr = string(b)
						}
					default:
						valStr = fmt.Sprint(vv)
					}
				}
			}
			row = append(row, valStr)
		}

		row = append(row,
			item.CreatedAt.Format(time.RFC3339),
			item.UpdatedAt.Format(time.RFC3339),
		)

		if err := w.Write(row); err != nil {
			return fmt.Errorf("写入CSV记录失败: %v", err)
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return fmt.Errorf("刷新CSV写入器失败: %v", err)
	}

	fmt.Printf("📄 CSV 已保存: %s\n", filePath)
	return nil
}

// saveToExcel 将数据保存为Excel到 outexcel 目录
func saveToExcel(data []GameResultData, config *Config, rtpLevel float64, testNumber int) error {
	// 创建输出目录
	outputDir := "outexcel"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("创建Excel输出目录失败: %v", err)
	}

	// 文件名
	fileName := fmt.Sprintf("%s%.0f_%d.xlsx", config.Tables.OutputTablePrefix, rtpLevel, testNumber)
	filePath := filepath.Join(outputDir, fileName)

	f := excelize.NewFile()
	defer func() { _ = f.Close() }()

	sheet := f.GetSheetName(0)
	if sheet == "" {
		sheet = "Sheet1"
	}
	// 统一命名为 Data
	_ = f.SetSheetName(sheet, "Data")
	sheet = "Data"

	// 表头
	headers := []string{"tb", "aw", "gwt", "sp", "fb", "gd", "createdAt", "updatedAt"}
	for i, h := range headers {
		cell, _ := excelize.CoordinatesToCellName(i+1, 1)
		_ = f.SetCellValue(sheet, cell, h)
	}

	// 内容
	for rowIdx, item := range data {
		r := rowIdx + 2
		roundedAW := math.Round(item.AW*100) / 100

		// gd 写为紧凑 JSON 文本，使用 SetCellStr 避免被当作公式或日期
		gdStr := ""
		if item.GD.Data != nil {
			if b, err := json.Marshal(item.GD.Data); err == nil {
				gdStr = string(b)
			}
		}

		// 逐列写入
		_ = f.SetCellValue(sheet, fmt.Sprintf("A%d", r), item.TB)
		_ = f.SetCellValue(sheet, fmt.Sprintf("B%d", r), fmt.Sprintf("%.2f", roundedAW))
		_ = f.SetCellValue(sheet, fmt.Sprintf("C%d", r), item.GWT)
		_ = f.SetCellValue(sheet, fmt.Sprintf("D%d", r), item.SP)
		_ = f.SetCellValue(sheet, fmt.Sprintf("E%d", r), item.FB)
		_ = f.SetCellStr(sheet, fmt.Sprintf("F%d", r), gdStr)
		_ = f.SetCellValue(sheet, fmt.Sprintf("G%d", r), item.CreatedAt.Format(time.RFC3339))
		_ = f.SetCellValue(sheet, fmt.Sprintf("H%d", r), item.UpdatedAt.Format(time.RFC3339))
	}

	// 样式：自动换行、设置列宽
	wrapID, _ := f.NewStyle(&excelize.Style{Alignment: &excelize.Alignment{WrapText: true}})
	_ = f.SetColWidth(sheet, "A", "H", 14)
	_ = f.SetColWidth(sheet, "F", "F", 60)
	_ = f.SetCellStyle(sheet, "A2", fmt.Sprintf("H%d", len(data)+1), wrapID)

	// 冻结表头
	_ = f.SetPanes(sheet, &excelize.Panes{Freeze: true, YSplit: 1})

	if err := f.SaveAs(filePath); err != nil {
		return fmt.Errorf("保存Excel失败: %v", err)
	}

	fmt.Printf("📘 Excel 已保存: %s\n", filePath)
	return nil
}

func main() {
	// 检查命令行参数
	if len(os.Args) < 2 {
		fmt.Println("使用方法:")
		fmt.Println("  ./filteringData generate                    # 生成RTP测试数据并保存到JSON文件")
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
	case "import":
		// 检查是否有可选的fileLevelId参数
		if len(os.Args) == 2 {
			runImportMode("") // 导入所有文件
		} else if len(os.Args) == 3 {
			fileLevelId := os.Args[2]
			runImportMode(fileLevelId) // 导入指定fileLevelId的文件
		} else {
			fmt.Printf("❌ 参数错误: import命令最多接受1个参数\n")

			fmt.Println("使用方法: ./filteringData import [fileLevelId]")
			os.Exit(1)
		}
	default:
		fmt.Printf("未知命令: %s\n", command)
		fmt.Println("支持的命令: generate, import")
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
	db, err := NewDatabase(config)
	if err != nil {
		log.Fatalf("数据库连接失败: %v", err)
	}
	defer db.Close()

	// 创建RTP测试结果表（暂时注释掉, 因为现在只写入Excel）
	// err = createRtpTestTable(db, config)
	// if err != nil {
	// 	log.Fatalf("创建RTP测试表失败: %v", err)
	// }

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

		for t := 0; t < 10; t++ {
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

// runImportMode 运行导入模式
func runImportMode(fileLevelId string) {
	if fileLevelId == "" {
		fmt.Println("🔄 启动导入模式 (导入所有文件)...")
	} else {
		fmt.Printf("🔄 启动导入模式 (只导入fileLevelId=%s的文件)...\n", fileLevelId)
	}

	// 加载配置
	config, err := LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("❌ 加载配置失败: %v", err)
	}

	// 连接数据库
	db, err := NewDatabase(config)
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
