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
	case "generateFb":
		runGenerateFbMode()
	case "importFb":
		if len(os.Args) == 2 {
			runImportFbMode("")
		} else if len(os.Args) == 3 {
			fileLevelId := os.Args[2]
			runImportFbMode(fileLevelId)
		} else {
			fmt.Printf("❌ 参数错误: importFb命令最多接受1个参数\n")
			fmt.Println("使用方法: ./filteringData importFb [fileLevelId]")
			os.Exit(1)
		}
	default:
		fmt.Printf("未知命令: %s\n", command)
		fmt.Println("支持的命令: generate, import, generateFb, importFb")
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
	db, err := NewDatabase(config)
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
	if len(winDataAll) == 0 {
		return
	}
	fmt.Printf("✅ [generateFb] 购买模式中奖数据条数: %d\n", len(winDataAll))

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

				if err := runRtpFbTest(db, config, rtpNo, rtpVal, testIndex, totalBet, winDataAll, noWinDataAll); err != nil {
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
func runRtpFbTest(db *Database, config *Config, rtpLevel float64, rtp float64, testNumber int, totalBet float64, winDataAll []GameResultData, noWinDataAll []GameResultData) error {
	var logBuf bytes.Buffer
	printf := func(format string, a ...interface{}) {
		fmt.Fprintf(&logBuf, format, a...)
	}

	// 允许中奖金额，额外乘以 FB 倍数（已在 totalBet 包含 FB，此处再次按要求乘以 FB）
	allowWin := totalBet * rtp
	printf("[FB] allowWin=%.4f (cs=%.2f ml=%.2f bl=%.2f rtp=%.4f fb=%.2f)\n", allowWin, config.Bet.CS, config.Bet.ML, config.Bet.BL, rtp, config.Bet.FB)

	printf("\n========== [FB TASK BEGIN] RtpNo: %.0f | Test: %d | %s =========\n", rtpLevel, testNumber, time.Now().Format(time.RFC3339))
	printf("获取到中奖数据: %d条, 不中奖数据: %d条\n", len(winDataAll), len(noWinDataAll))
	printf("档位: %.0f, 目标RTP: %.4f, 允许中奖金额: %.2f\n", rtpLevel, rtp, allowWin)

	// 其余逻辑与普通模式类似：达标且偏差 <= 0.005；购买模式存在高RTP特殊区间处理
	// 首次筛选优先选择“单条中奖金额”接近购买投入的 0.7-1.5 区间
	seed := time.Now().UnixNano() ^ int64(config.Game.ID)*1_000_003 ^ int64(testNumber)*1_000_033 ^ int64(rtpLevel)*1_000_037
	rng := rand.New(rand.NewSource(seed))
	perSpinBet := config.Bet.CS * config.Bet.ML * config.Bet.BL * config.Bet.FB
	preferredMin := perSpinBet * 0.7
	preferredMax := perSpinBet * 1.5
	var preferred, nonPreferred []GameResultData
	for _, it := range winDataAll {
		if it.AW >= preferredMin && it.AW <= preferredMax {
			preferred = append(preferred, it)
		} else {
			nonPreferred = append(nonPreferred, it)
		}
	}
	printf("[FB] 优先区间: [%.2f, %.2f], 候选: %d, 其他: %d\n", preferredMin, preferredMax, len(preferred), len(nonPreferred))
	// 按贪心顺序（aw DESC）遍历索引
	permPref := make([]int, len(preferred))
	for i := range permPref {
		permPref[i] = i
	}
	sort.Slice(permPref, func(i, j int) bool { return preferred[permPref[i]].AW > preferred[permPref[j]].AW })

	permRest := make([]int, len(nonPreferred))
	for i := range permRest {
		permRest[i] = i
	}
	sort.Slice(permRest, func(i, j int) bool { return nonPreferred[permRest[i]].AW > nonPreferred[permRest[j]].AW })

	var data []GameResultData
	var totalWin float64

	// 15档位特殊区间：绑定档位编号（rtpLevel == 15），范围改为 [0.8, 0.9]
	isSpecialRtp15 := (rtpLevel == 15)
	var targetRtpMin, targetRtpMax float64
	if isSpecialRtp15 {
		targetRtpMin = 0.8
		targetRtpMax = 0.9
		fmt.Printf("🎯 [FB] 15档位特殊处理: 目标RTP范围 [%.1f, %.1f], 允许偏差 ±0.005\n", targetRtpMin, targetRtpMax)
	}

	// 先遍历优先区间，再遍历其余
	for _, idx := range permPref {
		if len(data) >= config.Tables.DataNumFb {
			break
		}
		item := preferred[idx]
		// 过滤大奖、巨奖、超级巨奖
		switch item.GWT {
		case 2:
			continue
		case 3:
			continue
		case 4:
			continue
		}

		// 累计并校验上限（允许 0.5% 偏差）
		newTotalWin := totalWin + item.AW
		currentRtp := newTotalWin / totalBet
		if newTotalWin > allowWin*1.005 {
			continue
		}

		// 15档位特殊：不超过上限即可；其他档位：需达到 [allowWin, allowWin*1.005] 目标区间
		if isSpecialRtp15 {
			// 先加入再看是否达标区间
			if currentRtp > targetRtpMax {
				continue
			}
		}
		// 若仍未达标，遍历其余数据
		if !(isSpecialRtp15 || (totalWin >= allowWin && totalWin <= allowWin*(1+0.005))) {
			for _, idx := range permRest {
				if len(data) >= config.Tables.DataNumFb {
					break
				}
				item := nonPreferred[idx]
				// 过滤大奖、巨奖、超级巨奖
				switch item.GWT {
				case 2:
					continue
				case 3:
					continue
				case 4:
					continue
				}

				newTotalWin := totalWin + item.AW
				currentRtp := newTotalWin / totalBet
				if newTotalWin > allowWin*1.005 {
					continue
				}
				if isSpecialRtp15 {
					if currentRtp > targetRtpMax {
						continue
					}
				}
				if len(data) >= config.Tables.DataNumFb {
					break
				}
				totalWin += item.AW
				data = append(data, item)
				if isSpecialRtp15 {
					if currentRtp >= targetRtpMin && len(data) >= config.Tables.DataNumFb {
						break
					}
				}
				if !isSpecialRtp15 {
					if totalWin >= allowWin && totalWin <= allowWin*(1+0.005) {
						break
					}
				}
			}
		}
		// 加入（受条数上限限制）
		if len(data) >= config.Tables.DataNumFb {
			break
		}
		totalWin += item.AW
		data = append(data, item)

		//先判断15档位是否达到下限
		if isSpecialRtp15 {
			if currentRtp >= targetRtpMin && len(data) >= config.Tables.DataNumFb {
				break
			}
		}

		if !isSpecialRtp15 {
			if totalWin >= allowWin && totalWin <= allowWin*(1+0.005) {
				break
			}
		}
	}
	//判断当前是否达标
	if totalWin < allowWin {
		//判断是否为普通档位
		if !isSpecialRtp15 {
			//需要继续补全，优先查询符合的
			remainingWin := (allowWin - totalWin) * 1.005
			// 优先从数据库中查询满足条件的购买模式候选，限制 100 条
			// 购买模式允许数据重复，不排除已使用的ID
			fillData, err := db.GetWinDataForFillingFb(remainingWin, nil, 100)
			if err != nil {
				printf("⚠️ [FB] 查询填充数据失败: %v, 回退到原始逻辑\n", err)
			}

			if len(fillData) > 0 {
				printf("🔍 [FB] 数据库查询到 %d 条候选填充数据\n", len(fillData))
				for _, item := range fillData {
					if len(data) >= config.Tables.DataNumFb {
						break
					}
					if item.AW <= remainingWin && item.AW > 0 {
						data = append(data, item)
						totalWin += item.AW
						remainingWin -= item.AW
						printf("➕ [FB] 补充数据: AW=%.2f, 剩余需要: %.2f\n", item.AW, remainingWin)
						if totalWin >= allowWin {
							break
						}
					}
				}
			} else {
				// 回退：从预取中奖数据中挑选（已过滤大奖/巨奖/超巨奖），但需满足 fb=2, sp=true, gwt<=1
				for _, item := range winDataAll {
					if len(data) >= config.Tables.DataNumFb {
						break
					}
					if !(item.FB == 2 && item.SP && item.GWT <= 1) {
						continue
					}
					if item.AW <= remainingWin && item.AW > 0 {
						data = append(data, item)
						totalWin += item.AW
						remainingWin -= item.AW
						printf("➕ [FB] 回退补充数据: AW=%.2f, 剩余需要: %.2f\n", item.AW, remainingWin)
						if totalWin >= allowWin {
							break
						}
					}
				}
			}
		}
	}

	// 局部贪心优化：1↔1 替换以进一步逼近目标金额/范围
	if len(data) > 0 {
		candidates := make([]GameResultData, 0, len(preferred)+len(nonPreferred))
		candidates = append(candidates, preferred...)
		candidates = append(candidates, nonPreferred...)

		targetSum := allowWin
		upperBound := allowWin * (1 + 0.005)
		if isSpecialRtp15 {
			// 15档位瞄准区间中位值，提高命中概率
			targetSum = ((targetRtpMin + targetRtpMax) / 2.0) * totalBet
			upperBound = targetRtpMax * totalBet
		}

		bestDev := math.Abs(totalWin - targetSum)
		maxIter := 300
		for iter := 0; iter < maxIter; iter++ {
			idx := rng.Intn(len(data))
			removed := data[idx]
			base := totalWin - removed.AW
			desired := targetSum - base

			var best GameResultData
			bestDiff := math.MaxFloat64
			found := false
			for _, cand := range candidates {
				if cand.AW <= 0 {
					continue
				}
				// 购买模式过滤大奖/巨奖/超巨奖
				switch cand.GWT {
				case 2, 3, 4:
					continue
				}
				newTotal := base + cand.AW
				if newTotal > upperBound {
					continue
				}
				if isSpecialRtp15 {
					if newTotal/totalBet > targetRtpMax {
						continue
					}
				}
				diff := math.Abs(cand.AW - desired)
				if diff < bestDiff {
					bestDiff = diff
					best = cand
					found = true
				}
			}

			if !found {
				continue
			}
			newTotal := base + best.AW
			newDev := math.Abs(newTotal - targetSum)
			if newDev+1e-9 < bestDev {
				data[idx] = best
				totalWin = newTotal
				bestDev = newDev
			}
		}
	}

	// 用不中奖数据补全到 DataNumFb
	needNum := config.Tables.DataNumFb - len(data)
	if needNum > 0 && len(noWinDataAll) > 0 {
		permNo := rng.Perm(len(noWinDataAll))
		for i := 0; i < needNum; i++ {
			data = append(data, noWinDataAll[permNo[i%len(permNo)]])
		}
	}

	// 输出最终统计：数量、目标RTP、当前RTP与偏差
	printf("📊 [FB] 最终验证: 期望 %d 条, 实际 %d 条\n", config.Tables.DataNumFb, len(data))
	var finalTotalWin float64
	for _, it := range data {
		finalTotalWin += it.AW
	}
	finalRTP := finalTotalWin / totalBet
	rtpDeviation := math.Abs(finalRTP - rtp)
	printf("✅ [FB] 档位: %.0f, 目标RTP: %.6f, 实际RTP: %.6f, 偏差: %.6f\n", rtpLevel, rtp, finalRTP, rtpDeviation)

	var outputDir = filepath.Join("output", fmt.Sprintf("%d_fb", config.Game.ID))
	// 最终保存：沿用普通保存逻辑，但输出仍落在 output/<gameId>，文件名前缀复用
	if err := saveToJSON(data, config, rtpLevel, testNumber, outputDir); err != nil {
		return fmt.Errorf("[FB] 保存JSON失败: %v", err)
	}

	outputMu.Lock()
	fmt.Print(logBuf.String())
	outputMu.Unlock()
	return nil
}

// runImportFbMode 运行购买夺宝导入模式
func runImportFbMode(fileLevelId string) {
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
	db, err := NewDatabase(config)
	if err != nil {
		log.Fatalf("❌ 连接数据库失败: %v", err)
	}
	defer db.Close()

	// 读取目录：output/<gameId>_fb
	outputDir := filepath.Join("output", fmt.Sprintf("%d_fb", config.Game.ID))
	fmt.Printf("📂 [importFb] 导入目录: %s\n", outputDir)

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
	bet := config.Bet.CS * config.Bet.ML * config.Bet.BL * config.Bet.FB

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

					if _, err := stmt.Exec(rtpLevelVal, srNumber, srId, bet, winValue, detailVal); err != nil {
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
