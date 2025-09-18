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

// 策略分组配置
var fb2StrategyGroups = map[string][]float64{
	"generate2":   {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 20, 30, 40, 50, 14, 120}, // 使用generate2策略（中奖+不中奖）
	"generate3":   {150},                                                                // 使用generate3策略（中奖+不中奖）
	"generateFb2": {15, 200, 300, 500},                                                  // 全部用中奖数据（高RTP档位）
}

// RTP 控制策略配置
var rtpControlRules = map[string]struct {
	MinRTP      float64 // 最低下限
	MaxRTP      float64 // 最高上限
	Description string
}{
	"strict_levels": { // 1-13档位和20,30,40,50档位
		MinRTP:      0.0,   // 下限是 targetRTP + 0.0
		MaxRTP:      0.005, // 上限是 targetRTP + 0.005
		Description: "严格档位：下限必须满足，上浮最多0.005",
	},
	"loose_levels": { // 其他档位（14,15,120,150,200,300,500）
		MinRTP:      0.0, // 下限是 targetRTP + 0.0
		MaxRTP:      0.5, // 上限是 targetRTP + 0.5
		Description: "宽松档位：下限必须满足，允许上浮0.5",
	},
}

// 新策略填充策略配置
var fb2FillStrategy = map[string]struct {
	Ratio       float64
	DataSource  string // 数据来源
	Condition   func(aw, tb float64) bool
	Description string
}{
	"stage1": {
		Ratio:      0.20,          // 20%
		DataSource: "categorized", // 使用分类数据
		Condition: func(aw, tb float64) bool {
			return aw > 0 && aw <= tb
		},
		Description: "中奖不盈利数据",
	},
	"stage2": {
		Ratio:      0.50,          // 50%
		DataSource: "categorized", // 使用分类数据
		Condition: func(aw, tb float64) bool {
			return aw > tb && aw <= tb*20
		},
		Description: "中奖盈利数据（1-20倍）",
	},
	"stage3": {
		Ratio:      0.15,          // 15%
		DataSource: "categorized", // 使用分类数据
		Condition: func(aw, tb float64) bool {
			return aw > tb*20 && aw <= tb*50
		},
		Description: "中奖盈利较多数据（20-50倍）",
	},
	"stage4": {
		Ratio:       0.15,      // 15%
		DataSource:  "all_win", // 使用所有中奖数据
		Condition:   nil,       // 无限制条件
		Description: "调整替换数据（所有中奖数据）",
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
	if fbType == "fb1" {
		fbMul = 1
	}
	if fbType == "fb2" {
		fbMul = 2
	}
	if fbType == "fb3" {
		fbMul = 3
	}

	totalBet := config.Bet.CS * config.Bet.ML * config.Bet.BL * float64(fbMul) * float64(config.Tables.DataNumFb)

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
		if strategyType == "generate2" {
			tableCount = config.Tables.DataTableNum
		} else {
			tableCount = config.Tables.DataTableNumFb
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

	if strategyType == "generate2" {
		// 1-13档位和20,30,40,50档位使用data_num和data_table_num
		targetCount = config.Tables.DataNum
	} else {
		// 其他档位使用data_num_fb和data_table_num_fb
		targetCount = config.Tables.DataNumFb
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
	case "generate2":
		data, totalWin, _, _, _ = applyGenerate2Strategy(
			winDataAll, noWinDataAll, targetCount, allowWin, bigNum, megaNum, superMegaNum, rng, printf)
	case "generate3":
		data, totalWin, _, _, _ = applyGenerate3Strategy(
			winDataAll, noWinDataAll, targetCount, allowWin, bigNum, megaNum, superMegaNum, rng, printf)
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
	return "generate2" // 默认策略
}

// applyGenerate2Strategy 应用 generate2 策略
func applyGenerate2Strategy(winDataAll []GameResultData, noWinDataAll []GameResultData, targetCount int, allowWin float64,
	bigNum, megaNum, superMegaNum int, rng *rand.Rand, printf func(string, ...interface{})) ([]GameResultData, float64, int, int, int) {

	var data []GameResultData
	var totalWin float64
	bigCount := 0
	megaCount := 0
	superMegaCount := 0

	// 随机选择数据
	perm := rng.Perm(len(winDataAll))
	usedIds := make(map[int]bool)

	for i := 0; i < len(perm) && len(data) < targetCount; i++ {
		item := winDataAll[perm[i]]

		// 跳过已使用的数据
		if usedIds[item.ID] {
			continue
		}

		// 检查奖项限制
		switch item.GWT {
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

		// 检查RTP限制（参考generate2策略）
		if totalWin+item.AW > allowWin*1.005 {
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
	}

	// 如果中奖数据不够，先尝试补充中奖数据
	if totalWin < allowWin {
		remainingWin := allowWin - totalWin
		printf("中奖金额不足，需要补充 %.2f\n", remainingWin)

		// 尝试补充中奖数据
		for i := 0; i < len(perm) && totalWin < allowWin; i++ {
			item := winDataAll[perm[i]]
			if usedIds[item.ID] {
				continue
			}
			if totalWin+item.AW <= allowWin*1.005 {
				data = append(data, item)
				totalWin += item.AW
				usedIds[item.ID] = true
			}
		}
	}

	// 如果数据量不够，用真实的不中奖数据补全
	if len(data) < targetCount {
		remainingCount := targetCount - len(data)
		printf("数据量不足，需要补全 %d 条不中奖数据\n", remainingCount)

		// 使用真实的不中奖数据补全
		if len(noWinDataAll) > 0 {
			permNo := rng.Perm(len(noWinDataAll))
			for i := 0; i < remainingCount && i < len(permNo); i++ {
				idx := permNo[i]
				data = append(data, noWinDataAll[idx])
				// totalWin 不变，因为 AW = 0
			}
		} else {
			printf("⚠️ 没有不中奖数据，无法补全到目标数量\n")
		}
	}

	printf("Generate2策略完成: 数据量=%d, 总中奖=%.2f\n", len(data), totalWin)
	return data, totalWin, bigCount, megaCount, superMegaCount
}

// applyGenerate3Strategy 应用 generate3 策略
func applyGenerate3Strategy(winDataAll []GameResultData, noWinDataAll []GameResultData, targetCount int, allowWin float64,
	bigNum, megaNum, superMegaNum int, rng *rand.Rand, printf func(string, ...interface{})) ([]GameResultData, float64, int, int, int) {

	var data []GameResultData
	var totalWin float64
	bigCount := 0
	megaCount := 0
	superMegaCount := 0

	// 分类数据
	noProfitData := []GameResultData{} // 不盈利
	profitData := []GameResultData{}   // 盈利

	for _, item := range winDataAll {
		if item.AW <= float64(item.TB) {
			noProfitData = append(noProfitData, item)
		} else {
			profitData = append(profitData, item)
		}
	}

	// 按比例填充
	noProfitCount := int(float64(targetCount) * 0.4) // 40%不盈利
	profitCount := int(float64(targetCount) * 0.3)   // 30%盈利
	// 剩余30%用不盈利数据填充

	usedIds := make(map[int]bool)

	// 填充不盈利数据
	perm1 := rng.Perm(len(noProfitData))
	for i := 0; i < noProfitCount && i < len(perm1) && len(data) < targetCount; i++ {
		item := noProfitData[perm1[i]]
		if usedIds[item.ID] {
			continue
		}
		if totalWin+item.AW <= allowWin*1.005 {
			data = append(data, item)
			totalWin += item.AW
			usedIds[item.ID] = true
		}
	}

	// 填充盈利数据
	perm2 := rng.Perm(len(profitData))
	for i := 0; i < profitCount && i < len(perm2) && len(data) < targetCount; i++ {
		item := profitData[perm2[i]]
		if usedIds[item.ID] {
			continue
		}
		if totalWin+item.AW <= allowWin*1.005 {
			data = append(data, item)
			totalWin += item.AW
			usedIds[item.ID] = true
		}
	}

	// 如果数据不够，使用真实的不中奖数据填充
	if len(data) < targetCount {
		remainingCount := targetCount - len(data)
		printf("数据不足，需要填充 %d 条不中奖数据\n", remainingCount)

		// 使用真实的不中奖数据补全
		if len(noWinDataAll) > 0 {
			permNo := rng.Perm(len(noWinDataAll))
			for i := 0; i < remainingCount && i < len(permNo); i++ {
				idx := permNo[i]
				data = append(data, noWinDataAll[idx])
				// totalWin 不变，因为 AW = 0
			}
		} else {
			printf("⚠️ 没有不中奖数据，无法补全到目标数量\n")
		}
	}

	printf("Generate3策略完成: 数据量=%d, 总中奖=%.2f\n", len(data), totalWin)
	return data, totalWin, bigCount, megaCount, superMegaCount
}

// applyGenerateFb2Strategy 应用 generateFb2 新策略
func applyGenerateFb2Strategy(winDataAll []GameResultData, noWinDataAll []GameResultData, targetCount int, allowWin float64,
	bigNum, megaNum, superMegaNum int, rng *rand.Rand, printf func(string, ...interface{}), rtpLevel float64) ([]GameResultData, float64, int, int, int) {

	var data []GameResultData
	var totalWin float64
	bigCount := 0
	megaCount := 0
	superMegaCount := 0

	// 判断是否为高RTP档位（RTP >= 2.0）
	// 需要根据档位号查找对应的RTP值
	var currentRTP float64
	for _, level := range FbRtpLevels {
		if level.RtpNo == rtpLevel {
			currentRTP = level.Rtp
			break
		}
	}
	isHighRTP := currentRTP >= 2.0

	if isHighRTP {
		printf("🎯 检测到高RTP档位（RTP >= 2.0），使用全部中奖数据策略\n")
		return applyHighRTPStrategy(winDataAll, noWinDataAll, targetCount, allowWin, bigNum, megaNum, superMegaNum, rng, printf)
	}

	// 低RTP档位使用原有的分阶段策略
	printf("🎯 低RTP档位，使用分阶段策略\n")

	// 分类数据
	categorized := categorizeFb2Data(winDataAll)
	printf("数据分类: stage1=%d, stage2=%d, stage3=%d, stage4=%d\n",
		len(categorized["stage1"]), len(categorized["stage2"]),
		len(categorized["stage3"]), len(categorized["stage4"]))

	usedIds := make(map[int]bool)

	// 填充前三个阶段（65%）
	for stage, strategy := range fb2FillStrategy {
		if strategy.DataSource == "categorized" {
			count := int(float64(targetCount) * strategy.Ratio)
			stageData := selectDataByConditionWithUsed(categorized[stage], count, strategy.Condition, allowWin, &totalWin, rng, usedIds)
			data = append(data, stageData...)
			printf("阶段%s填充: 目标%d条, 实际%d条, 累计中奖%.2f\n",
				stage, count, len(stageData), totalWin)
		}
	}

	// 第四阶段：优先使用高额中奖数据（stage4 - 所有中奖数据）
	if len(categorized["stage4"]) > 0 {
		stage4Count := int(float64(targetCount) * 0.15) // 15%
		stage4Data := selectDataByConditionWithUsed(categorized["stage4"], stage4Count, nil, allowWin, &totalWin, rng, usedIds)
		data = append(data, stage4Data...)
		printf("阶段4填充: 目标%d条, 实际%d条, 累计中奖%.2f\n",
			stage4Count, len(stage4Data), totalWin)
	}

	// 如果中奖金额不足，尝试补充更多高额中奖数据
	if totalWin < allowWin {
		remainingWin := allowWin - totalWin
		printf("中奖金额不足，需要补充 %.2f，尝试使用高额中奖数据\n", remainingWin)

		// 按中奖金额降序排序所有未使用的中奖数据
		var availableHighWinData []GameResultData
		for _, item := range winDataAll {
			if !usedIds[item.ID] && item.AW > 0 {
				availableHighWinData = append(availableHighWinData, item)
			}
		}

		// 按中奖金额降序排序
		sort.Slice(availableHighWinData, func(i, j int) bool {
			return availableHighWinData[i].AW > availableHighWinData[j].AW
		})

		// 优先选择高额中奖数据
		for _, item := range availableHighWinData {
			if totalWin+item.AW <= allowWin*1.01 { // 允许1%的偏差
				data = append(data, item)
				totalWin += item.AW
				usedIds[item.ID] = true
				printf("补充高额中奖数据: AW=%.2f, 累计中奖%.2f\n", item.AW, totalWin)

				if totalWin >= allowWin {
					printf("✅ 高额数据补充完成！当前中奖总额: %.2f, 目标: %.2f\n", totalWin, allowWin)
					break
				}
			}
		}
	}

	// 如果数据量不够，使用真实的不中奖数据填充
	if len(data) < targetCount {
		remainingCount := targetCount - len(data)
		printf("数据量不足，需要填充 %d 条不中奖数据\n", remainingCount)

		// 使用真实的不中奖数据补全
		if len(noWinDataAll) > 0 {
			permNo := rng.Perm(len(noWinDataAll))
			for i := 0; i < remainingCount && i < len(permNo); i++ {
				idx := permNo[i]
				data = append(data, noWinDataAll[idx])
				// totalWin 不变，因为 AW = 0
			}
		} else {
			printf("⚠️ 没有不中奖数据，无法补全到目标数量\n")
		}
	}

	printf("GenerateFb2策略完成: 数据量=%d, 总中奖=%.2f\n", len(data), totalWin)
	return data, totalWin, bigCount, megaCount, superMegaCount
}

// categorizeFb2Data 分类数据
func categorizeFb2Data(data []GameResultData) map[string][]GameResultData {
	categorized := map[string][]GameResultData{
		"stage1": []GameResultData{}, // 20%
		"stage2": []GameResultData{}, // 50%
		"stage3": []GameResultData{}, // 15%
		"stage4": []GameResultData{}, // 15%
	}

	for _, item := range data {
		aw, tb := item.AW, float64(item.TB)

		if aw > 0 && aw <= tb {
			categorized["stage1"] = append(categorized["stage1"], item)
		} else if aw > tb && aw <= tb*20 {
			categorized["stage2"] = append(categorized["stage2"], item)
		} else if aw > tb*20 && aw <= tb*50 {
			categorized["stage3"] = append(categorized["stage3"], item)
		} else if aw > tb*50 {
			categorized["stage4"] = append(categorized["stage4"], item)
		}
	}

	return categorized
}

// selectDataByCondition 根据条件选择数据
func selectDataByCondition(data []GameResultData, count int, condition func(aw, tb float64) bool,
	allowWin float64, totalWin *float64, rng *rand.Rand) []GameResultData {

	var result []GameResultData
	perm := rng.Perm(len(data))

	for i := 0; i < count && i < len(perm); i++ {
		item := data[perm[i]]

		if condition != nil && !condition(item.AW, float64(item.TB)) {
			continue
		}

		if *totalWin+item.AW <= allowWin*1.01 {
			result = append(result, item)
			*totalWin += item.AW
		}
	}

	return result
}

// selectDataByConditionWithUsed 根据条件选择数据（带已使用检查）
func selectDataByConditionWithUsed(data []GameResultData, count int, condition func(aw, tb float64) bool,
	allowWin float64, totalWin *float64, rng *rand.Rand, usedIds map[int]bool) []GameResultData {

	var result []GameResultData
	perm := rng.Perm(len(data))

	for i := 0; i < count && i < len(perm); i++ {
		item := data[perm[i]]

		if usedIds[item.ID] {
			continue
		}

		if condition != nil && !condition(item.AW, float64(item.TB)) {
			continue
		}

		if *totalWin+item.AW <= allowWin*1.01 {
			result = append(result, item)
			*totalWin += item.AW
			usedIds[item.ID] = true
		}
	}

	return result
}

// applyAdjustmentStage 应用调整替换阶段
func applyAdjustmentStage(data []GameResultData, allWinData []GameResultData, count int,
	currentWin, allowWin float64, rng *rand.Rand) []GameResultData {

	// 计算目标RTP
	targetRTP := allowWin / (allowWin / 1.0) // 简化计算
	currentRTP := currentWin / (allowWin / 1.0)

	if currentRTP < targetRTP {
		// RTP过低，需要增加中奖金额
		return addHighWinData(allWinData, count, rng)
	} else if currentRTP > targetRTP {
		// RTP过高，需要减少中奖金额
		return addLowWinData(allWinData, count, rng)
	} else {
		// RTP刚好，随机选择
		return addRandomWinData(allWinData, count, rng)
	}
}

// addHighWinData 添加高额中奖数据（提升RTP）
func addHighWinData(allWinData []GameResultData, count int, rng *rand.Rand) []GameResultData {
	// 按中奖金额降序排序
	sortedData := make([]GameResultData, len(allWinData))
	copy(sortedData, allWinData)
	sort.Slice(sortedData, func(i, j int) bool {
		return sortedData[i].AW > sortedData[j].AW
	})

	var result []GameResultData
	for i := 0; i < count && i < len(sortedData); i++ {
		result = append(result, sortedData[i])
	}
	return result
}

// addLowWinData 添加低额中奖数据（降低RTP）
func addLowWinData(allWinData []GameResultData, count int, rng *rand.Rand) []GameResultData {
	// 按中奖金额升序排序
	sortedData := make([]GameResultData, len(allWinData))
	copy(sortedData, allWinData)
	sort.Slice(sortedData, func(i, j int) bool {
		return sortedData[i].AW < sortedData[j].AW
	})

	var result []GameResultData
	for i := 0; i < count && i < len(sortedData); i++ {
		result = append(result, sortedData[i])
	}
	return result
}

// addRandomWinData 随机选择中奖数据
func addRandomWinData(allWinData []GameResultData, count int, rng *rand.Rand) []GameResultData {
	// 随机选择count条数据
	perm := rng.Perm(len(allWinData))
	var result []GameResultData
	for i := 0; i < count && i < len(perm); i++ {
		result = append(result, allWinData[perm[i]])
	}
	return result
}

// calculateTotalWin 计算总中奖金额
func calculateTotalWin(data []GameResultData) float64 {
	var total float64
	for _, item := range data {
		total += item.AW
	}
	return total
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
	// 严格档位：1-13档位和20,30,40,50档位（上浮最多0.005）
	strictLevels := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 20, 30, 40, 50}
	for _, level := range strictLevels {
		if rtpLevel == level {
			return "strict_levels"
		}
	}
	// 宽松档位：其他档位（14,15,120,150,200,300,500）（允许上浮0.5）
	return "loose_levels"
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

// applyHighRTPStrategy 高RTP档位策略：全部使用中奖数据，超出RTP时用低金额替换
func applyHighRTPStrategy(winDataAll []GameResultData, noWinDataAll []GameResultData, targetCount int, allowWin float64,
	bigNum, megaNum, superMegaNum int, rng *rand.Rand, printf func(string, ...interface{})) ([]GameResultData, float64, int, int, int) {

	var data []GameResultData
	var totalWin float64
	bigCount := 0
	megaCount := 0
	superMegaCount := 0

	printf("🎯 高RTP档位策略：全部使用中奖数据填充\n")
	printf("目标数据量: %d, 允许中奖金额: %.2f\n", targetCount, allowWin)

	// 按中奖金额降序排序所有中奖数据
	sortedWinData := make([]GameResultData, len(winDataAll))
	copy(sortedWinData, winDataAll)
	sort.Slice(sortedWinData, func(i, j int) bool {
		return sortedWinData[i].AW > sortedWinData[j].AW
	})

	usedIds := make(map[int]bool)

	// 第一阶段：优先选择高额中奖数据，直到接近目标RTP
	printf("第一阶段：选择高额中奖数据\n")
	for _, item := range sortedWinData {
		if len(data) >= targetCount {
			break
		}

		if usedIds[item.ID] {
			continue
		}

		// 检查奖项限制
		switch item.GWT {
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

		// 如果添加这个数据会超出RTP上限，跳过
		if totalWin+item.AW > allowWin*1.01 { // 允许1%的偏差
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

		printf("添加高额数据: AW=%.2f, 累计中奖=%.2f, 数据量=%d\n", item.AW, totalWin, len(data))
	}

	// 第二阶段：如果数据量不够，继续添加中奖数据（可能超出RTP）
	if len(data) < targetCount {
		printf("第二阶段：继续添加中奖数据（可能超出RTP）\n")
		remainingCount := targetCount - len(data)

		for _, item := range sortedWinData {
			if remainingCount <= 0 {
				break
			}

			if usedIds[item.ID] {
				continue
			}

			// 检查奖项限制
			switch item.GWT {
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

			// 添加数据（即使可能超出RTP）
			data = append(data, item)
			totalWin += item.AW
			usedIds[item.ID] = true
			remainingCount--

			// 更新奖项计数
			switch item.GWT {
			case 2: // 大奖
				bigCount++
			case 3: // 巨奖
				megaCount++
			case 4: // 超级巨奖
				superMegaCount++
			}

			printf("添加额外数据: AW=%.2f, 累计中奖=%.2f, 剩余需要=%d\n", item.AW, totalWin, remainingCount)
		}
	}

	// 第三阶段：如果RTP超出，用低金额数据替换高金额数据
	if totalWin > allowWin*1.01 {
		printf("第三阶段：RTP超出，开始替换策略\n")
		printf("当前RTP: %.4f, 目标RTP: %.4f, 超出: %.4f\n", totalWin/allowWin*1.0, 1.0, (totalWin-allowWin)/allowWin*1.0)

		// 按中奖金额升序排序，准备替换数据
		lowWinData := make([]GameResultData, 0)
		for _, item := range winDataAll {
			if !usedIds[item.ID] && item.AW > 0 {
				lowWinData = append(lowWinData, item)
			}
		}
		sort.Slice(lowWinData, func(i, j int) bool {
			return lowWinData[i].AW < lowWinData[j].AW
		})

		// 按中奖金额降序排序当前数据，准备被替换
		sort.Slice(data, func(i, j int) bool {
			return data[i].AW > data[j].AW
		})

		// 替换策略：用低金额数据替换高金额数据
		replacedCount := 0
		for i := 0; i < len(data) && totalWin > allowWin*1.01; i++ {
			currentItem := data[i]

			// 寻找合适的低金额替换数据
			for j := 0; j < len(lowWinData); j++ {
				replacementItem := lowWinData[j]

				if usedIds[replacementItem.ID] {
					continue
				}

				// 计算替换后的总中奖金额
				newTotalWin := totalWin - currentItem.AW + replacementItem.AW

				// 如果替换后更接近目标RTP，则进行替换
				if newTotalWin <= allowWin*1.01 {
					// 执行替换
					totalWin = newTotalWin
					data[i] = replacementItem
					usedIds[replacementItem.ID] = true
					usedIds[currentItem.ID] = false
					replacedCount++

					printf("替换数据: 原AW=%.2f -> 新AW=%.2f, 新总中奖=%.2f\n",
						currentItem.AW, replacementItem.AW, totalWin)
					break
				}
			}
		}

		printf("替换完成: 替换了%d条数据, 最终总中奖=%.2f\n", replacedCount, totalWin)
	}

	// 第四阶段：如果数据量仍然不够，用不中奖数据补全
	if len(data) < targetCount {
		remainingCount := targetCount - len(data)
		printf("第四阶段：用不中奖数据补全 %d 条\n", remainingCount)

		if len(noWinDataAll) > 0 {
			permNo := rng.Perm(len(noWinDataAll))
			for i := 0; i < remainingCount && i < len(permNo); i++ {
				idx := permNo[i]
				data = append(data, noWinDataAll[idx])
				// totalWin 不变，因为 AW = 0
			}
		} else {
			printf("⚠️ 没有不中奖数据，无法补全到目标数量\n")
		}
	}

	printf("高RTP策略完成: 数据量=%d, 总中奖=%.2f, 实际RTP=%.4f\n",
		len(data), totalWin, totalWin/allowWin*1.0)
	return data, totalWin, bigCount, megaCount, superMegaCount
}
