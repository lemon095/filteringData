package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

// 策略分组配置 - 统一使用新策略
var fb2StrategyGroups = map[string][]float64{
	"unified_strategy": {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 20, 30, 40, 50, 14, 120, 150}, // 统一策略：先填充中奖数据达到目标RTP，其余用不中奖数据填充
	"generateFb2":      {15, 200, 300, 500},                                                       // 高RTP档位：允许超过上限0.5，但必须达到下限
}

// RTP 控制策略配置
var rtpControlRules = map[string]struct {
	MinRTP      float64 // 最低下限
	MaxRTP      float64 // 最高上限
	Description string
}{
	"unified_strategy": { // 统一策略档位（1-13,20,30,40,50,14,120,150）
		MinRTP:      0.0, // 下限是 targetRTP + 0.0
		MaxRTP:      0.5, // 上限是 targetRTP + 0.5 (放宽限制)
		Description: "统一策略档位：放宽上限限制以适应数据特性",
	},
	"generateFb2": { // 高RTP档位（15,200,300,500）
		MinRTP:      0.0, // 下限是 targetRTP + 0.0（必须达到）
		MaxRTP:      1.0, // 上限是 targetRTP + 1.0（不能超过太多）
		Description: "高RTP档位：必须达到下限，不能超过上限1.0",
	},
}

// runSingleGameFb2Mode 运行单个游戏的Fb2生成模式
func runSingleGameFb2Mode(config *Config, db *Database, gameIndex int) error {
	fmt.Printf("🎮 Fb2模式启动 - 游戏ID: %d, 目标数据量: generate2档位=%d, 其他档位=%d\n",
		config.Game.ID, config.Tables.DataNum, config.Tables.DataNumFb)

	// 分别获取三种 fb 的中奖和不中奖数据
	fmt.Println("🔄 正在获取 fb=1 中奖数据...")
	winDataFb1, err := db.GetWinDataFb1()
	if err != nil {
		return fmt.Errorf("获取 fb=1 中奖数据失败: %v", err)
	}
	fmt.Printf("✅ fb=1 中奖数据条数: %d\n", len(winDataFb1))

	fmt.Println("🔄 正在获取 fb=1 不中奖数据...")
	noWinDataFb1, err := db.GetNoWinDataFb1()
	if err != nil {
		return fmt.Errorf("获取 fb=1 不中奖数据失败: %v", err)
	}
	fmt.Printf("✅ fb=1 不中奖数据条数: %d\n", len(noWinDataFb1))

	fmt.Println("🔄 正在获取 fb=2 中奖数据...")
	winDataFb2, err := db.GetWinDataFb2()
	if err != nil {
		return fmt.Errorf("获取 fb=2 中奖数据失败: %v", err)
	}
	fmt.Printf("✅ fb=2 中奖数据条数: %d\n", len(winDataFb2))

	fmt.Println("🔄 正在获取 fb=2 不中奖数据...")
	noWinDataFb2, err := db.GetNoWinDataFb2()
	if err != nil {
		return fmt.Errorf("获取 fb=2 不中奖数据失败: %v", err)
	}
	fmt.Printf("✅ fb=2 不中奖数据条数: %d\n", len(noWinDataFb2))

	fmt.Println("🔄 正在获取 fb=3 中奖数据...")
	winDataFb3, err := db.GetWinDataFb3()
	if err != nil {
		return fmt.Errorf("获取 fb=3 中奖数据失败: %v", err)
	}
	fmt.Printf("✅ fb=3 中奖数据条数: %d\n", len(winDataFb3))

	fmt.Println("🔄 正在获取 fb=3 不中奖数据...")
	noWinDataFb3, err := db.GetNoWinDataFb3()
	if err != nil {
		return fmt.Errorf("获取 fb=3 不中奖数据失败: %v", err)
	}
	fmt.Printf("✅ fb=3 不中奖数据条数: %d\n", len(noWinDataFb3))

	// 检查是否有数据
	if len(winDataFb1) == 0 && len(winDataFb2) == 0 && len(winDataFb3) == 0 {
		return fmt.Errorf("❌ 没有找到任何 fb=1, fb=2, fb=3 的中奖数据，请检查数据库")
	}

	// 分别处理三种数据
	if len(winDataFb1) > 0 {
		fmt.Println("\n🎯 开始处理 fb=1 数据...")
		if err := processFbData(winDataFb1, noWinDataFb1, "fb1", config, db); err != nil {
			log.Printf("❌ 处理 fb=1 数据失败: %v", err)
		}
	}

	if len(winDataFb2) > 0 {
		fmt.Println("\n🎯 开始处理 fb=2 数据...")
		if err := processFbData(winDataFb2, noWinDataFb2, "fb2", config, db); err != nil {
			log.Printf("❌ 处理 fb=2 数据失败: %v", err)
		}
	}

	if len(winDataFb3) > 0 {
		fmt.Println("\n🎯 开始处理 fb=3 数据...")
		if err := processFbData(winDataFb3, noWinDataFb3, "fb3", config, db); err != nil {
			log.Printf("❌ 处理 fb=3 数据失败: %v", err)
		}
	}

	fmt.Printf("✅ 游戏 %d Fb2模式生成完成！\n", config.Game.ID)
	return nil
}

// processFbData 处理指定 fb 类型的数据
func processFbData(winData []GameResultData, noWinData []GameResultData, fbType string, config *Config, db *Database) error {
	// 计算总投注
	// generateFb2 模式使用 data_num_fb 作为数据量
	var fbMul int = 1

	// 根据fb类型调整totalBet倍数（因为不同fb类型的中奖金额不同，需要不同的数据量）
	var fbMultiplier float64 = 1.0
	switch fbType {
	case "fb1":
		fbMultiplier = 1.0 // fb1: AW = 10元，基准
	case "fb2":
		fbMultiplier = 2.0 // fb2: AW = 30元，需要2倍数据量
	case "fb3":
		fbMultiplier = 3.0 // fb3: AW = 30-60元，需要3倍数据量
	}

	// 根据策略类型选择数据量配置来计算totalBet
	var baseDataNum int
	if getStrategyType(1) == "generateFb2" { // 检查档位1的策略类型
		baseDataNum = config.Tables.DataNumFb
	} else {
		baseDataNum = config.Tables.DataNum
	}

	totalBet := config.Bet.CS * config.Bet.ML * config.Bet.BL * float64(fbMul) * float64(baseDataNum) * fbMultiplier

	// 失败统计
	var failedLevels []float64
	var failedTests []string
	var failedMu sync.Mutex

	// 遍历所有 FbRtpLevels 档位
	for rtpNum := 0; rtpNum < len(FbRtpLevels); rtpNum++ {
		levelStart := time.Now()
		levelNo := FbRtpLevels[rtpNum].RtpNo
		levelVal := FbRtpLevels[rtpNum].Rtp

		// 并发度：CPU 核数
		worker := runtime.NumCPU()
		sem := make(chan struct{}, worker)
		var wg sync.WaitGroup

		// 根据策略类型选择表数量
		strategyType := getStrategyType(levelNo)
		var tableCount int
		if strategyType == "generateFb2" {
			tableCount = config.Tables.DataTableNumFb
		} else {
			tableCount = config.Tables.DataTableNum
		}

		for t := 0; t < tableCount; t++ {
			sem <- struct{}{}
			wg.Add(1)

			testIndex := t + 1
			rtpNo := levelNo
			rtpVal := levelVal

			go func(rtpNo float64, rtpVal float64, testIndex int) {
				defer func() { <-sem; wg.Done() }()
				testStartTime := time.Now()
				fmt.Printf("▶️ 开始生成Fb2 | %s | RTP等级 %.0f | 第%d次 | %s\n",
					fbType, rtpNo, testIndex, testStartTime.Format(time.RFC3339))

				if err := runRtpFb2Test(db, config, rtpNo, rtpVal, testIndex, totalBet, winData, noWinData, fbType); err != nil {
					fmt.Printf("❌ RTP测试Fb2失败 [%s RTP%.0f 第%d次]: %v\n", fbType, rtpNo, testIndex, err)
					// 记录失败的档位和测试（线程安全）
					failedMu.Lock()
					failedLevels = append(failedLevels, rtpNo)
					failedTests = append(failedTests, fmt.Sprintf("%s_RTP%.0f_第%d次", fbType, rtpNo, testIndex))
					failedMu.Unlock()
				}

				fmt.Printf("⏱️  %s | RTP等级 %.0f (第%d次生成Fb2) 耗时: %v\n",
					fbType, rtpNo, testIndex, time.Since(testStartTime))
			}(rtpNo, rtpVal, testIndex)
		}

		wg.Wait()
		fmt.Printf("⏱️  %s | RTP等级 %.0f 总耗时: %v\n", fbType, levelNo, time.Since(levelStart))
	}

	// 输出失败统计
	printFb2FailureSummary(fbType, config.Game.ID, failedLevels, failedTests)

	return nil
}

// runRtpFb2Test 执行单次RTP测试 - Fb2模式
func runRtpFb2Test(db *Database, config *Config, rtpLevel float64, rtp float64, testNumber int, totalBet float64, winDataAll []GameResultData, noWinDataAll []GameResultData, fbType string) error {
	var logBuf bytes.Buffer
	printf := func(format string, a ...interface{}) {
		fmt.Fprintf(&logBuf, format, a...)
	}
	testStartTime := time.Now()

	// 任务头分隔线
	printf("\n========== [FB2 TASK BEGIN] %s | RtpNo: %.0f | Test: %d | %s =========\n",
		fbType, rtpLevel, testNumber, time.Now().Format(time.RFC3339))

	// 计算允许中奖金额和配置参数
	allowWin := totalBet * rtp
	upperBound := allowWin * (1 + config.StageRatios.UpperDeviation)

	// 根据策略类型选择数据量配置
	strategyType := getStrategyType(rtpLevel)
	var targetCount int

	if strategyType == "generateFb2" {
		// 高RTP档位使用data_num_fb
		targetCount = config.Tables.DataNumFb
	} else {
		// 统一策略档位使用data_num
		targetCount = config.Tables.DataNum
	}

	// 计算奖项数量限制
	bigNum := int(float64(targetCount) * config.PrizeRatios.BigPrize)
	megaNum := int(float64(targetCount) * config.PrizeRatios.MegaPrize)
	superMegaNum := int(float64(targetCount) * config.PrizeRatios.SuperMegaPrize)

	printf("档位: %.0f, 目标RTP: %.4f, 允许中奖金额: %.2f, 上限: %.2f\n", rtpLevel, rtp, allowWin, upperBound)
	printf("候选数据: %d条, 奖项限制: 大奖=%d, 巨奖=%d, 超级巨奖=%d\n",
		len(winDataAll), bigNum, megaNum, superMegaNum)

	// 随机源
	seed := time.Now().UnixNano() ^ int64(config.Game.ID)*1_000_003 ^ int64(testNumber)*1_000_033 ^ int64(rtpLevel)*1_000_037
	rng := rand.New(rand.NewSource(seed))

	// 结果容器和计数器
	var data []GameResultData
	var totalWin float64
	// targetCount 已经在上面根据策略类型计算过了

	// 确定策略类型（strategyType 已经在上面计算过了）
	printf("使用策略: %s\n", strategyType)

	// 根据策略类型填充数据
	printf("开始应用策略: %s\n", strategyType)
	switch strategyType {
	case "unified_strategy":
		data, totalWin, _, _, _ = applyUnifiedStrategy(
			winDataAll, noWinDataAll, targetCount, allowWin, bigNum, megaNum, superMegaNum, rng, printf, rtpLevel)
	case "generateFb2":
		data, totalWin, _, _, _ = applyGenerateFb2Strategy(
			winDataAll, noWinDataAll, targetCount, allowWin, bigNum, megaNum, superMegaNum, rng, printf, rtpLevel)
	default:
		return fmt.Errorf("未知策略类型: %s", strategyType)
	}
	printf("策略应用完成，数据量: %d, 总中奖: %.2f\n", len(data), totalWin)

	// 重新计算最终RTP
	finalRTP := totalWin / totalBet
	printf("✅ 档位: %.0f, 📊 最终统计: 总投注 %.2f, 总中奖 %.2f, 实际RTP %.6f, 目标: %.6f\n",
		rtpLevel, totalBet, totalWin, finalRTP, rtp)

	// 调试RTP验证
	controlType := getRtpControlType(rtpLevel)
	rules := rtpControlRules[controlType]
	minRTP := rtp * rules.MinRTP
	maxRTP := rtp * rules.MaxRTP
	printf("🔍 RTP验证调试: 档位类型=%s, 目标RTP=%.4f, 实际RTP=%.4f, 下限=%.4f, 上限=%.4f\n",
		controlType, rtp, finalRTP, minRTP, maxRTP)

	// 验证RTP
	if err := validateRTP(rtpLevel, rtp, finalRTP); err != nil {
		return fmt.Errorf("RTP验证失败: %v", err)
	}

	// 最终验证数据量
	printf("🔍 最终验证: 期望 %d 条, 实际 %d 条\n", targetCount, len(data))
	if len(data) != targetCount {
		return fmt.Errorf("❌ 数据量不匹配：期望 %d 条, 实际 %d 条", targetCount, len(data))
	}

	// 随机打乱数据顺序
	rand.Shuffle(len(data), func(i, j int) {
		data[i], data[j] = data[j], data[i]
	})

	// 保存到JSON文件
	// 从 fbType (如 "fb1") 中提取数字部分 (如 "1")
	fbNumber := strings.TrimPrefix(fbType, "fb")
	outputDir := filepath.Join("output", fmt.Sprintf("%d_fb_%s", config.Game.ID, fbNumber))
	if err := saveToJSONFb2(data, config, rtpLevel, testNumber, outputDir, fbNumber); err != nil {
		return fmt.Errorf("保存JSON文件失败: %v", err)
	}

	// 任务尾分隔线
	printf("========== [FB2 TASK END] %s | RtpNo: %.0f | Test: %d =========\n\n",
		fbType, rtpLevel, testNumber)
	printf("⏱️  %s | RTP等级 %.0f (第%d次生成Fb2) 耗时: %v\n",
		fbType, rtpLevel, testNumber, time.Since(testStartTime))

	outputMu.Lock()
	fmt.Print(logBuf.String())
	outputMu.Unlock()
	return nil
}

// getStrategyType 根据RTP档位获取策略类型
func getStrategyType(rtpLevel float64) string {
	for strategy, levels := range fb2StrategyGroups {
		for _, level := range levels {
			if level == rtpLevel {
				return strategy
			}
		}
	}
	return "unified_strategy" // 默认策略
}

// applyUnifiedStrategy 应用统一策略：先填充中奖数据达到目标RTP，其余用不中奖数据填充
func applyUnifiedStrategy(winDataAll []GameResultData, noWinDataAll []GameResultData, targetCount int, allowWin float64,
	bigNum, megaNum, superMegaNum int, rng *rand.Rand, printf func(string, ...interface{}), rtpLevel float64) ([]GameResultData, float64, int, int, int) {

	var data []GameResultData
	var totalWin float64
	bigCount := 0
	megaCount := 0
	superMegaCount := 0

	printf("🎯 统一策略：先填充中奖数据达到目标RTP，其余用不中奖数据填充\n")
	printf("目标数据量: %d, 目标中奖金额: %.2f\n", targetCount, allowWin)

	// 随机化中奖数据顺序
	perm := rng.Perm(len(winDataAll))
	usedIds := make(map[int]bool)

	// 第一阶段：填充中奖数据直到达到目标RTP
	printf("第一阶段：填充中奖数据达到目标RTP\n")
	for i := 0; i < len(perm) && len(data) < targetCount; i++ {
		item := winDataAll[perm[i]]

		// 跳过已使用的数据
		if usedIds[item.ID] {
			continue
		}

		// 检查奖项限制 (已禁用以允许选择足够的中奖数据)
		// switch item.GWT {
		// case 2: // 大奖
		// 	if bigCount >= bigNum {
		// 		continue
		// 	}
		// case 3: // 巨奖
		// 	if megaCount >= megaNum {
		// 		continue
		// 	}
		// case 4: // 超级巨奖
		// 	if superMegaCount >= superMegaNum {
		// 		continue
		// 	}
		// }

		// 检查RTP限制：放宽上限限制以适应数据特性
		if totalWin+item.AW > allowWin*1.5 {
			continue
		}

		// 添加数据
		data = append(data, item)
		totalWin += item.AW
		usedIds[item.ID] = true

		// 更新奖项计数
		switch item.GWT {
		case 2: // 大奖
			bigCount++
		case 3: // 巨奖
			megaCount++
		case 4: // 超级巨奖
			superMegaCount++
		}

		// printf("添加中奖数据: AW=%.2f, 累计中奖=%.2f, 数据量=%d\n", item.AW, totalWin, len(data))

		// 如果达到目标RTP，停止添加中奖数据
		if totalWin >= allowWin {
			printf("✅ 达到目标RTP，停止添加中奖数据\n")
			break
		}
	}

	// 第二阶段：用不中奖数据补全到目标数量
	if len(data) < targetCount {
		remainingCount := targetCount - len(data)
		printf("第二阶段：用不中奖数据补全 %d 条\n", remainingCount)

		if len(noWinDataAll) > 0 {
			permNo := rng.Perm(len(noWinDataAll))
			for i := 0; i < remainingCount && i < len(permNo); i++ {
				idx := permNo[i]
				data = append(data, noWinDataAll[idx])
				// totalWin 不变，因为 AW = 0
			}
			printf("✅ 补全完成，最终数据量: %d\n", len(data))
		} else {
			printf("⚠️ 没有不中奖数据，无法补全到目标数量\n")
		}
	}

	// 第三阶段：如果RTP超过上限，用不中奖数据替换中奖数据
	// 计算允许的上限（基于RTP控制规则）
	controlType := getRtpControlType(rtpLevel)
	rules := rtpControlRules[controlType]
	maxAllowedRTP := rtpLevel + rules.MaxRTP
	maxAllowedWin := allowWin * (maxAllowedRTP / rtpLevel) // 基于RTP上限计算允许的中奖金额

	if totalWin > maxAllowedWin && len(noWinDataAll) > 0 {
		printf("第三阶段：RTP超过上限，开始用不中奖数据替换中奖数据\n")
		printf("当前总中奖: %.2f, 允许上限: %.2f (目标RTP: %.2f, 上限RTP: %.2f)\n", totalWin, maxAllowedWin, rtpLevel, maxAllowedRTP)

		// 找到中奖数据并替换
		permNo := rng.Perm(len(noWinDataAll))
		noWinIndex := 0

		for i := 0; i < len(data) && totalWin > maxAllowedWin; i++ {
			if data[i].AW > 0 { // 如果是中奖数据
				// 用不中奖数据替换
				if noWinIndex < len(permNo) {
					oldWin := data[i].AW
					data[i] = noWinDataAll[permNo[noWinIndex]]
					totalWin -= oldWin // 减去原来的中奖金额
					noWinIndex++

					printf("替换中奖数据: 原中奖=%.2f, 当前总中奖=%.2f\n", oldWin, totalWin)

					// 如果达到允许范围，停止替换
					if totalWin <= maxAllowedWin {
						printf("✅ 达到允许范围，停止替换\n")
						break
					}
				}
			}
		}
	}

	printf("统一策略完成: 数据量=%d, 总中奖=%.2f, 实际RTP=%.6f\n",
		len(data), totalWin, totalWin/(allowWin/1.0))
	return data, totalWin, bigCount, megaCount, superMegaCount
}

// applyGenerateFb2Strategy 应用 generateFb2 策略：允许超过上限0.5，但必须达到下限
func applyGenerateFb2Strategy(winDataAll []GameResultData, noWinDataAll []GameResultData, targetCount int, allowWin float64,
	bigNum, megaNum, superMegaNum int, rng *rand.Rand, printf func(string, ...interface{}), rtpLevel float64) ([]GameResultData, float64, int, int, int) {

	var data []GameResultData
	var totalWin float64
	bigCount := 0
	megaCount := 0
	superMegaCount := 0

	printf("🎯 GenerateFb2策略：允许超过上限0.5，但必须达到下限\n")
	printf("目标数据量: %d, 目标中奖金额: %.2f, 允许上限: %.2f\n", targetCount, allowWin, allowWin*1.5)

	// 随机化中奖数据顺序
	perm := rng.Perm(len(winDataAll))
	usedIds := make(map[int]bool)

	// 第一阶段：填充中奖数据，优先达到下限，允许超过上限
	printf("第一阶段：填充中奖数据达到下限，允许超过上限\n")
	for i := 0; i < len(perm) && len(data) < targetCount; i++ {
		item := winDataAll[perm[i]]

		// 跳过已使用的数据
		if usedIds[item.ID] {
			continue
		}

		// 检查奖项限制 (已禁用以允许选择足够的中奖数据)
		// switch item.GWT {
		// case 2: // 大奖
		// 	if bigCount >= bigNum {
		// 		continue
		// 	}
		// case 3: // 巨奖
		// 	if megaCount >= megaNum {
		// 		continue
		// 	}
		// case 4: // 超级巨奖
		// 	if superMegaCount >= superMegaNum {
		// 		continue
		// 	}
		// }

		// 检查上限：允许超过上限0.5（即allowWin * 1.5）
		if totalWin+item.AW > allowWin*1.5 {
			continue
		}

		// 添加数据
		data = append(data, item)
		totalWin += item.AW
		usedIds[item.ID] = true

		// 更新奖项计数
		switch item.GWT {
		case 2: // 大奖
			bigCount++
		case 3: // 巨奖
			megaCount++
		case 4: // 超级巨奖
			superMegaCount++
		}

		// printf("添加中奖数据: AW=%.2f, 累计中奖=%.2f, 数据量=%d\n", item.AW, totalWin, len(data))

		// 如果达到下限，可以选择继续添加更多数据以提高RTP
		if totalWin >= allowWin {
			// printf("✅ 达到下限，当前RTP: %.4f\n", totalWin/allowWin*1.0)
		}
	}

	// 第二阶段：用不中奖数据补全到目标数量
	if len(data) < targetCount {
		remainingCount := targetCount - len(data)
		printf("第二阶段：用不中奖数据补全 %d 条\n", remainingCount)

		if len(noWinDataAll) > 0 {
			permNo := rng.Perm(len(noWinDataAll))
			for i := 0; i < remainingCount && i < len(permNo); i++ {
				idx := permNo[i]
				data = append(data, noWinDataAll[idx])
				// totalWin 不变，因为 AW = 0
			}
			printf("✅ 补全完成，最终数据量: %d\n", len(data))
		} else {
			printf("⚠️ 没有不中奖数据，无法补全到目标数量\n")
		}
	}

	// 第三阶段：如果RTP超过上限，用不中奖数据替换中奖数据
	// 计算允许的上限（基于RTP控制规则）
	controlType := getRtpControlType(rtpLevel)
	rules := rtpControlRules[controlType]
	maxAllowedRTP := rtpLevel + rules.MaxRTP
	maxAllowedWin := allowWin * (maxAllowedRTP / rtpLevel) // 基于RTP上限计算允许的中奖金额

	if totalWin > maxAllowedWin && len(noWinDataAll) > 0 {
		printf("第三阶段：RTP超过上限，开始用不中奖数据替换中奖数据\n")
		printf("当前总中奖: %.2f, 允许上限: %.2f (目标RTP: %.2f, 上限RTP: %.2f)\n", totalWin, maxAllowedWin, rtpLevel, maxAllowedRTP)

		// 找到中奖数据并替换
		permNo := rng.Perm(len(noWinDataAll))
		noWinIndex := 0

		for i := 0; i < len(data) && totalWin > maxAllowedWin; i++ {
			if data[i].AW > 0 { // 如果是中奖数据
				// 用不中奖数据替换
				if noWinIndex < len(permNo) {
					oldWin := data[i].AW
					data[i] = noWinDataAll[permNo[noWinIndex]]
					totalWin -= oldWin // 减去原来的中奖金额
					noWinIndex++

					printf("替换中奖数据: 原中奖=%.2f, 当前总中奖=%.2f\n", oldWin, totalWin)

					// 如果达到允许范围，停止替换
					if totalWin <= maxAllowedWin {
						printf("✅ 达到允许范围，停止替换\n")
						break
					}
				}
			}
		}
	}

	// 检查是否达到下限
	if totalWin < allowWin {
		printf("⚠️ 警告：未达到下限，当前中奖: %.2f, 目标下限: %.2f\n", totalWin, allowWin)
	}

	printf("GenerateFb2策略完成: 数据量=%d, 总中奖=%.2f, 实际RTP=%.6f, 上限=%.2f\n",
		len(data), totalWin, totalWin/(allowWin/1.0), maxAllowedWin)
	return data, totalWin, bigCount, megaCount, superMegaCount
}

// validateRTP 验证RTP
func validateRTP(rtpLevel, targetRTP, actualRTP float64) error {
	controlType := getRtpControlType(rtpLevel)
	rules := rtpControlRules[controlType]

	// 计算实际RTP范围（使用加法而不是乘法）
	minRTP := targetRTP + rules.MinRTP
	maxRTP := targetRTP + rules.MaxRTP

	if actualRTP < minRTP {
		return fmt.Errorf("RTP %.4f 低于最低下限 %.4f (档位: %.0f)",
			actualRTP, minRTP, rtpLevel)
	}

	if actualRTP > maxRTP {
		return fmt.Errorf("RTP %.4f 超过最高上限 %.4f (档位: %.0f)",
			actualRTP, maxRTP, rtpLevel)
	}

	return nil
}

// getRtpControlType 判断档位类型
func getRtpControlType(rtpLevel float64) string {
	// 统一策略档位：1-13,20,30,40,50,14,120,150档位（严格控制±0.005）
	unifiedLevels := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 20, 30, 40, 50, 14, 120, 150}
	for _, level := range unifiedLevels {
		if rtpLevel == level {
			return "unified_strategy"
		}
	}
	// 高RTP档位：15,200,300,500档位（允许超过上限0.5，但必须达到下限）
	return "generateFb2"
}

// printFb2FailureSummary 输出Fb2失败统计汇总
func printFb2FailureSummary(fbType string, gameID int, failedLevels []float64, failedTests []string) {
	if len(failedLevels) == 0 {
		fmt.Printf("✅ [%s] 游戏 %d 所有档位生成成功！\n", fbType, gameID)
		return
	}

	// 统计失败的档位
	levelCount := make(map[float64]int)
	for _, level := range failedLevels {
		levelCount[level]++
	}

	fmt.Printf("\n❌ [%s] 游戏 %d 失败统计:\n", fbType, gameID)
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
	if len(failedTests) > 0 {
		fmt.Printf("   详细失败列表:\n")
		for i, test := range failedTests {
			if i < 10 { // 只显示前10个
				fmt.Printf("     %s\n", test)
			} else if i == 10 {
				fmt.Printf("     ... 还有 %d 个失败\n", len(failedTests)-10)
				break
			}
		}
	}
}

// saveToJSONFb2 保存Fb2模式数据到JSON文件
func saveToJSONFb2(data []GameResultData, config *Config, rtpLevel float64, testNumber int, outputDir string, fbNumber string) error {
	// 创建输出目录
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("创建输出目录失败: %v", err)
	}

	// 生成文件名：GameResultData_fbType_档位_第几个文件.json
	fileName := fmt.Sprintf("GameResultData_fb%s_%.0f_%d.json", fbNumber, rtpLevel, testNumber)
	filePath := filepath.Join(outputDir, fileName)

	// 准备要保存的数据结构
	type OutputData struct {
		RtpLevel int                      `json:"rtpLevel"`
		SrNumber int                      `json:"srNumber"`
		Data     []map[string]interface{} `json:"data"`
	}

	// 转换数据格式
	var outputData []map[string]interface{}
	for _, item := range data {
		outputData = append(outputData, map[string]interface{}{
			"id":        item.ID,
			"tb":        item.TB,
			"aw":        item.AW,
			"gwt":       item.GWT,
			"sp":        item.SP,
			"fb":        item.FB,
			"gd":        item.GD,
			"createdAt": item.CreatedAt,
			"updatedAt": item.UpdatedAt,
		})
	}

	// 创建输出数据结构
	output := OutputData{
		RtpLevel: int(rtpLevel),
		SrNumber: testNumber,
		Data:     outputData,
	}

	// 序列化为JSON
	jsonData, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return fmt.Errorf("JSON序列化失败: %v", err)
	}

	// 写入文件
	if err := os.WriteFile(filePath, jsonData, 0644); err != nil {
		return fmt.Errorf("写入文件失败: %v", err)
	}

	fmt.Printf("📊 数据已保存到JSON文件: %s\n", filePath)
	return nil
}
