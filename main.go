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

// runMultiGameMode 运行多游戏生成模式
func runMultiGameMode(mode string) {
	// 记录程序开始时间
	startTime := time.Now()

	// 初始化随机数种子
	rand.Seed(time.Now().UnixNano())

	// 加载配置文件
	config, err := LoadConfig("buconfig.yaml")
	if err != nil {
		log.Fatalf("加载配置文件失败: %v", err)
	}

	// 验证生成模式
	validModes := map[string]bool{
		"generate":   true,
		"generate2":  true,
		"generate3":  true,
		"generateFb": true,
	}
	if !validModes[mode] {
		fmt.Printf("❌ 无效的生成模式: %s\n", mode)
		fmt.Println("支持的模式: generate, generate2, generate3, generateFb")
		return
	}

	// 检查是否启用多游戏模式
	if !config.MultiGame.Enabled {
		fmt.Println("⚠️ 多游戏模式未启用，请设置 multi_game.enabled: true")
		return
	}

	if len(config.MultiGame.Games) == 0 {
		fmt.Println("⚠️ 未配置任何游戏，请检查 multi_game.games 配置")
		return
	}

	fmt.Printf("🎮 多游戏模式启动，生成模式: %s，共配置 %d 个游戏\n", mode, len(config.MultiGame.Games))
	for i, game := range config.MultiGame.Games {
		fmt.Printf("  游戏 %d: ID=%d, BL=%.0f, IsFb=%t\n", i+1, game.ID, game.BL, game.IsFb)
	}

	// 连接数据库
	db, err := NewDatabase(config, "")
	if err != nil {
		log.Fatalf("数据库连接失败: %v", err)
	}
	defer db.Close()

	// 为每个游戏生成数据
	for gameIndex, gameConfig := range config.MultiGame.Games {
		gameStartTime := time.Now()
		fmt.Printf("\n🎯 开始处理游戏 %d/%d: ID=%d, BL=%.0f\n",
			gameIndex+1, len(config.MultiGame.Games), gameConfig.ID, gameConfig.BL)

		// 检查连接健康状态
		if err := db.EnsureConnection(); err != nil {
			fmt.Printf("⚠️ 连接健康检查失败: %v\n", err)
		}

		// 创建游戏特定的配置
		gameConfigCopy := *config
		gameConfigCopy.Game.ID = gameConfig.ID
		gameConfigCopy.Game.IsFb = gameConfig.IsFb
		gameConfigCopy.Bet.BL = gameConfig.BL

		// 根据指定的生成模式选择对应的函数
		fmt.Printf("🔄 游戏 %d 使用 %s 模式\n", gameConfig.ID, mode)
		switch mode {
		case "generate":
			err = runSingleGameMode(&gameConfigCopy, db, gameIndex+1)
		case "generate2":
			err = runSingleGameMode2(&gameConfigCopy, db, gameIndex+1)
		case "generate3":
			err = runSingleGameMode3(&gameConfigCopy, db, gameIndex+1)
		case "generateFb":
			err = runSingleGameFbMode(&gameConfigCopy, db, gameIndex+1)
		default:
			err = fmt.Errorf("不支持的生成模式: %s", mode)
		}

		if err != nil {
			log.Printf("❌ 游戏 %d 生成失败: %v", gameConfig.ID, err)
			continue
		}

		gameDuration := time.Since(gameStartTime)
		fmt.Printf("✅ 游戏 %d 生成完成，耗时: %v\n", gameConfig.ID, gameDuration)

		// 游戏间连接健康检查
		if gameIndex < len(config.MultiGame.Games)-1 {
			fmt.Printf("🔍 检查连接健康状态...\n")
			if err := db.EnsureConnection(); err != nil {
				fmt.Printf("⚠️ 连接健康检查失败: %v\n", err)
			}
		}
	}

	totalDuration := time.Since(startTime)
	fmt.Printf("\n🎉 所有游戏生成完成！总耗时: %v\n", totalDuration)
}

// runSingleGameMode 运行单个游戏的标准生成模式
func runSingleGameMode(config *Config, db *Database, gameIndex int) error {
	fmt.Printf("配置加载成功 - 游戏ID: %d, 目标数据量: %d\n", config.Game.ID, config.Tables.DataNum)

	// 计算总投注
	totalBet := config.Bet.CS * config.Bet.ML * config.Bet.BL * float64(config.Tables.DataNum)

	// 失败统计
	var failedLevels []float64
	var failedTests []string

	// 预取共享只读数据
	winDataAll, err := db.GetWinData()
	if err != nil {
		return fmt.Errorf("获取中奖数据失败: %v", err)
	}
	noWinDataAll, err := db.GetNoWinData()
	if err != nil {
		return fmt.Errorf("获取不中奖数据失败: %v", err)
	}

	// 遍历 RTP 档位
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
				testStartTime := time.Now()
				fmt.Printf("▶️ 开始生成 | 游戏%d | RTP等级 %.0f | 第%d次 | %s\n",
					config.Game.ID, rtpNo, testIndex, testStartTime.Format(time.RFC3339))
				fmt.Printf("🔧 totalBet=%.2f allowWin_base=%.2f\n", totalBet, totalBet*rtpVal)

				if err := runRtpTest(db, config, rtpNo, rtpVal, testIndex, totalBet, winDataAll, noWinDataAll); err != nil {
					log.Printf("RTP测试失败: %v", err)
					// 记录失败的档位和测试
					failedLevels = append(failedLevels, rtpNo)
					failedTests = append(failedTests, fmt.Sprintf("RTP%.0f_第%d次", rtpNo, testIndex))
				}

				fmt.Printf("⏱️  游戏%d | RTP等级 %.0f (第%d次生成) 耗时: %v\n",
					config.Game.ID, rtpNo, testIndex, time.Since(testStartTime))
			}(rtpNo, rtpVal, testIndex)
		}

		wg.Wait()
		fmt.Printf("⏱️  游戏%d | RTP等级 %.0f 总耗时: %v\n", config.Game.ID, RtpLevels[rtpNum].RtpNo, time.Since(time.Now()))
	}

	// 输出失败统计
	printFailureSummary("generate", config.Game.ID, failedLevels, failedTests)

	fmt.Printf("✅ 游戏 %d 导入完成！\n", config.Game.ID)
	return nil
}

// runSingleGameFbMode 运行单个游戏的购买夺宝生成模式
func runSingleGameFbMode(config *Config, db *Database, gameIndex int) error {
	fmt.Printf("配置加载成功 - 游戏ID: %d, 目标数据量: %d (购买夺宝模式)\n", config.Game.ID, config.Tables.DataNumFb)

	// 计算总投注：cs * ml * bl * bet.fb * 数据条数
	totalBet := config.Bet.CS * config.Bet.ML * config.Bet.BL * config.Bet.FB * float64(config.Tables.DataNumFb)

	// 预取共享只读数据（购买模式）
	fmt.Println("🔄 正在获取购买模式中奖数据...")
	winDataAll, err := db.GetWinDataFb()
	if err != nil {
		return fmt.Errorf("获取购买模式中奖数据失败: %v", err)
	}
	if len(winDataAll) == 0 {
		return fmt.Errorf("未获取到购买模式中奖数据，无法继续")
	}
	fmt.Printf("✅ 购买模式中奖数据条数: %d\n", len(winDataAll))

	fmt.Println("🔄 正在获取购买模式不中奖数据...")
	noWinDataAll, err := db.GetNoWinDataFb()
	if err != nil {
		return fmt.Errorf("获取购买模式不中奖数据失败: %v", err)
	}
	fmt.Printf("✅ 购买模式不中奖数据条数: %d\n", len(noWinDataAll))

	// 遍历 RTP 档位，每档位执行多次
	for rtpNum := 0; rtpNum < len(FbRtpLevels); rtpNum++ {
		levelStart := time.Now()
		levelNo := FbRtpLevels[rtpNum].RtpNo
		levelVal := FbRtpLevels[rtpNum].Rtp

		var wgLevel sync.WaitGroup
		worker := runtime.NumCPU()
		sem := make(chan struct{}, worker)

		for t := 0; t < config.Tables.DataTableNumFb; t++ {
			sem <- struct{}{}
			wgLevel.Add(1)

			testIndex := t + 1
			rtpNo := levelNo
			rtpVal := levelVal

			go func(rtpNo float64, rtpVal float64, testIndex int) {
				defer func() { <-sem; wgLevel.Done() }()
				testStartTime := time.Now()
				fmt.Printf("▶️ 开始生成 | 游戏%d | RTP等级 %.0f | 第%d次 | %s\n",
					config.Game.ID, rtpNo, testIndex, testStartTime.Format(time.RFC3339))
				fmt.Printf("🔧 totalBet=%.2f allowWin_base=%.2f\n", totalBet, totalBet*rtpVal)

				if err := runRtpFbTest(db, config, rtpNo, rtpVal, testIndex, totalBet, winDataAll, noWinDataAll, []GameResultData{}); err != nil {
					log.Printf("RTP测试失败: %v", err)
				}

				fmt.Printf("⏱️  游戏%d | RTP等级 %.0f (第%d次生成) 耗时: %v\n",
					config.Game.ID, rtpNo, testIndex, time.Since(testStartTime))
			}(rtpNo, rtpVal, testIndex)
		}

		wgLevel.Wait()
		fmt.Printf("⏱️  游戏%d | RTP等级 %.0f 总耗时: %v\n", config.Game.ID, levelNo, time.Since(levelStart))
	}

	fmt.Printf("✅ 游戏 %d 导入完成！\n", config.Game.ID)
	return nil
}

// runSingleGameMode2 运行单个游戏的V2生成模式（四阶段策略）
func runSingleGameMode2(config *Config, db *Database, gameIndex int) error {
	fmt.Printf("配置加载成功V2 - 游戏ID: %d, 目标数据量: %d\n", config.Game.ID, config.Tables.DataNum)
	fmt.Printf("阶段策略配置: 阶段1比例[%.1f%%-%.1f%%], 阶段3比例%.1f%%, 上偏差%.3f\n",
		config.StageRatios.Stage1MinRatio*100, config.StageRatios.Stage1MaxRatio*100,
		config.StageRatios.Stage3WinTopRatio*100, config.StageRatios.UpperDeviation)

	// 计算总投注
	totalBet := config.Bet.CS * config.Bet.ML * config.Bet.BL * float64(config.Tables.DataNum)

	// 失败统计
	var failedLevels []float64
	var failedTests []string

	// 预取共享只读数据（使用三种数据源）
	fmt.Println("🔄 正在获取中奖但不盈利数据...")
	winDataAll, err := db.GetWinData()
	if err != nil {
		return fmt.Errorf("获取中奖但不盈利数据失败: %v", err)
	}
	fmt.Printf("✅ 中奖但不盈利数据条数: %d\n", len(winDataAll))

	fmt.Println("🔄 正在获取中奖且盈利数据...")
	profitDataAll, err := db.GetProfitData()
	if err != nil {
		return fmt.Errorf("获取中奖且盈利数据失败: %v", err)
	}
	fmt.Printf("✅ 中奖且盈利数据条数: %d\n", len(profitDataAll))

	fmt.Println("🔄 正在获取不中奖数据...")
	noWinDataAll, err := db.GetNoWinData()
	if err != nil {
		return fmt.Errorf("获取不中奖数据失败: %v", err)
	}
	fmt.Printf("✅ 不中奖数据条数: %d\n", len(noWinDataAll))

	if len(winDataAll) == 0 {
		return fmt.Errorf("未获取到中奖但不盈利数据，无法继续")
	}
	if len(noWinDataAll) == 0 {
		fmt.Println("⚠️ 未获取到不中奖数据，后续将无法补全至目标条数。")
	}

	// 遍历 RTP 档位
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
				testStartTime := time.Now()
				fmt.Printf("▶️ 开始生成V2 | 游戏%d | RTP等级 %.0f | 第%d次 | %s\n",
					config.Game.ID, rtpNo, testIndex, testStartTime.Format(time.RFC3339))

				if err := runRtpTest2(db, config, rtpNo, rtpVal, testIndex, totalBet, winDataAll, noWinDataAll, profitDataAll); err != nil {
					log.Printf("RTP测试V2失败: %v", err)
					// 记录失败的档位和测试
					failedLevels = append(failedLevels, rtpNo)
					failedTests = append(failedTests, fmt.Sprintf("RTP%.0f_第%d次", rtpNo, testIndex))
				}

				fmt.Printf("⏱️  游戏%d | RTP等级 %.0f (第%d次生成V2) 耗时: %v\n",
					config.Game.ID, rtpNo, testIndex, time.Since(testStartTime))
			}(rtpNo, rtpVal, testIndex)
		}

		wg.Wait()
	}

	// 输出失败统计
	printFailureSummary("generate2", config.Game.ID, failedLevels, failedTests)

	fmt.Printf("✅ 游戏 %d 导入完成！\n", config.Game.ID)
	return nil
}

// runSingleGameMode3 运行单个游戏的V3生成模式（10%不中奖+40%不盈利+30%盈利策略）
func runSingleGameMode3(config *Config, db *Database, gameIndex int) error {
	fmt.Printf("配置加载成功（V3模式）- 游戏ID: %d, 目标数据量: %d\n", config.Game.ID, config.Tables.DataNum)
	fmt.Printf("🔧 V3策略：10%%不中奖 + 40%%不盈利 + 30%%盈利数据\n")

	// 计算总投注
	totalBet := config.Bet.CS * config.Bet.ML * config.Bet.BL * float64(config.Tables.DataNum)

	// 失败统计
	var failedLevels []float64
	var failedTests []string

	// 预取共享只读数据
	winDataAll, err := db.GetWinData()
	if err != nil {
		return fmt.Errorf("获取中奖数据失败: %v", err)
	}
	noWinDataAll, err := db.GetNoWinData()
	if err != nil {
		return fmt.Errorf("获取不中奖数据失败: %v", err)
	}

	// 使用RtpLevelsTest配置
	for rtpNum := 0; rtpNum < len(RtpLevelsTest); rtpNum++ {
		// 并发度：CPU 核数
		worker := runtime.NumCPU()
		sem := make(chan struct{}, worker)
		var wg sync.WaitGroup

		for t := 0; t < config.Tables.DataTableNum3; t++ {
			sem <- struct{}{}
			wg.Add(1)

			// 捕获当前循环变量
			rtpNo := RtpLevelsTest[rtpNum].RtpNo
			rtpVal := RtpLevelsTest[rtpNum].Rtp
			testIndex := t + 1

			go func(rtpNo float64, rtpVal float64, testIndex int) {
				defer func() { <-sem; wg.Done() }()
				testStartTime := time.Now()
				fmt.Printf("▶️ 开始生成V3 | 游戏%d | RTP等级 %.0f | 第%d次 | %s\n",
					config.Game.ID, rtpNo, testIndex, testStartTime.Format(time.RFC3339))

				if err := runRtpTestV3(db, config, rtpNo, rtpVal, testIndex, totalBet, winDataAll, noWinDataAll); err != nil {
					log.Printf("RTP测试V3失败: %v", err)
					// 记录失败的档位和测试
					failedLevels = append(failedLevels, rtpNo)
					failedTests = append(failedTests, fmt.Sprintf("RTP%.0f_第%d次", rtpNo, testIndex))
				}

				fmt.Printf("⏱️  游戏%d | RTP等级 %.0f (第%d次生成V3) 耗时: %v\n",
					config.Game.ID, rtpNo, testIndex, time.Since(testStartTime))
			}(rtpNo, rtpVal, testIndex)
		}

		wg.Wait()
	}

	// 输出失败统计
	printFailureSummary("generate3", config.Game.ID, failedLevels, failedTests)

	fmt.Printf("✅ 游戏 %d 导入完成！\n", config.Game.ID)
	return nil
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
		fmt.Println("  ./filteringData generate3                   # 生成RTP测试数据V3（10%不中奖+40%不盈利+30%盈利策略）")
		fmt.Println("  ./filteringData multi-game [mode]           # 多游戏顺序生成模式")
		fmt.Println("     mode: generate/generate2/generate3/generateFb")
		fmt.Println("  ./filteringData import                     # 导入output目录下的所有JSON文件到数据库")
		fmt.Println("  ./filteringData import [fileLevelId]       # 只导入指定fileLevelId的JSON文件")
		fmt.Println("  ./filteringData import-s3 <gameIds> [level] [env] # 从S3智能导入（自动检测normal和fb模式）")
		fmt.Println("  ./filteringData import-s3-normal <gameIds> [level] [env] # 从S3导入普通模式文件")
		fmt.Println("  ./filteringData import-s3-fb <gameIds> [level] [env] # 从S3导入购买夺宝模式文件")
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
	case "generate":
		runGenerateMode()
	case "generate2":
		runGenerateMode2()
	case "generate3":
		runGenerateMode3()
	case "multi-game":
		// 支持指定生成模式：./filteringData multi-game generate2
		mode := "generate" // 默认模式
		if len(os.Args) > 2 {
			mode = os.Args[2]
		}
		runMultiGameMode(mode)
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
	default:
		fmt.Printf("未知命令: %s\n", command)
		fmt.Println("支持的命令: generate, generate2, generate3, multi-game, import, importFb, import-s3, import-s3-normal, import-s3-fb")
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

	// 失败统计
	var failedLevels []float64
	var failedTests []string

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
					// 记录失败的档位和测试
					failedLevels = append(failedLevels, rtpNo)
					failedTests = append(failedTests, fmt.Sprintf("RTP%.0f_第%d次", rtpNo, testIndex))
				}

				fmt.Printf("⏱️  [generateFb] RTP等级 %.0f (第%d次生成) 耗时: %v\n", rtpNo, testIndex, time.Since(testStartTime))
			}(rtpNo, rtpVal, testIndex)
		}

		wgLevel.Wait()
		fmt.Printf("⏱️  [generateFb] RTP等级 %.0f 总耗时: %v\n", levelNo, time.Since(levelStart))
	}

	// 输出失败统计
	printFailureSummary("generateFb", config.Game.ID, failedLevels, failedTests)

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

// runGenerateMode3 运行生成模式V3 - 使用RtpLevelsTest配置，10%不中奖+40%不盈利+30%盈利策略
func runGenerateMode3() {
	// 记录程序开始时间
	startTime := time.Now()

	// 初始化随机数种子
	rand.Seed(time.Now().UnixNano())

	// 加载配置文件
	config, err := LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("加载配置文件失败: %v", err)
	}
	fmt.Printf("配置加载成功（V3模式）- 游戏ID: %d, 目标数据量: %d\n", config.Game.ID, config.Tables.DataNum)
	fmt.Printf("🔧 V3策略：10%%不中奖 + 40%%不盈利 + 30%%盈利数据\n")

	// 连接数据库
	db, err := NewDatabase(config, "")
	if err != nil {
		log.Fatalf("数据库连接失败: %v", err)
	}
	defer db.Close()

	// 计算总投注
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

	// 使用RtpLevelsTest配置
	for rtpNum := 0; rtpNum < len(RtpLevelsTest); rtpNum++ {
		// 并发度：CPU 核数
		worker := runtime.NumCPU()
		sem := make(chan struct{}, worker)
		var wg sync.WaitGroup

		for t := 0; t < config.Tables.DataTableNum3; t++ {
			sem <- struct{}{}
			wg.Add(1)

			// 捕获当前循环变量
			rtpNo := RtpLevelsTest[rtpNum].RtpNo
			rtpVal := RtpLevelsTest[rtpNum].Rtp
			testIndex := t + 1

			go func(rtpNo float64, rtpVal float64, testIndex int) {
				defer func() { <-sem; wg.Done() }()

				// 记录单次测试开始时间
				testStartTime := time.Now()
				// 即时输出单次任务开始，便于观察进度
				fmt.Printf("▶️ 开始生成（V3模式）| RTP等级 %.0f | 第%d次 | %s\n", rtpNo, testIndex, testStartTime.Format(time.RFC3339))

				if err := runRtpTestV3(db, config, rtpNo, rtpVal, testIndex, totalBet, winDataAll, noWinDataAll); err != nil {
					log.Printf("RTP测试V3失败: %v", err)
				}

				// 计算并输出单次测试耗时
				testDuration := time.Since(testStartTime)
				fmt.Printf("⏱️  RTP等级 %.0f (第%d次生成-V3模式) 耗时: %v\n", rtpNo, testIndex, testDuration)
			}(rtpNo, rtpVal, testIndex)
		}

		wg.Wait()
	}

	// 计算并输出整个程序的总耗时
	totalDuration := time.Since(startTime)
	fmt.Printf("\n🎉 RTP数据筛选和保存完成（V3模式）！\n")
	fmt.Printf("⏱️  整个程序总耗时: %v\n", totalDuration)
}

// runRtpTestV3 执行单次RTP测试V3 - 优化版本：动态比例调整+RTP下限保证+数量精确控制
func runRtpTestV3(db *Database, config *Config, rtpLevel float64, rtp float64, testNumber int, totalBet float64, winDataAll []GameResultData, noWinDataAll []GameResultData) error {
	var logBuf bytes.Buffer
	printf := func(format string, a ...interface{}) {
		fmt.Fprintf(&logBuf, format, a...)
	}
	testStartTime := time.Now()

	// 任务头分隔线
	printf("\n========== [TASK BEGIN - V3 OPTIMIZED] RtpNo: %.0f | Test: %d | %s =========\n", rtpLevel, testNumber, time.Now().Format(time.RFC3339))

	// 计算允许中的金额
	allowWin := totalBet * rtp
	rtpUpperLimit := rtp + 0.5
	maxAllowWin := totalBet * rtpUpperLimit
	rtpLowerLimit := rtp - 0.1 // RTP下限：目标值-0.1
	minAllowWin := totalBet * rtpLowerLimit

	// 数据统计
	printf("\n数据源统计:\n")
	printf("  - 中奖数据(包含不盈利): %d条\n", len(winDataAll))
	printf("  - 不中奖数据(aw=0): %d条\n", len(noWinDataAll))
	printf("档位: %.0f, 目标RTP: %.4f, 允许中奖金额: %.2f\n", rtpLevel, rtp, allowWin)
	printf("🔧 V3优化策略：动态比例调整 + RTP下限保证 + 数量精确控制\n")
	printf("RTP控制范围: [%.2f, %.2f]，中奖金额范围: [%.2f, %.2f]\n", rtpLowerLimit, rtpUpperLimit, minAllowWin, maxAllowWin)

	// 每任务独立随机源
	seed := time.Now().UnixNano() ^ int64(config.Game.ID)*1_000_003 ^ int64(testNumber)*1_000_033 ^ int64(rtpLevel)*1_000_037
	rng := rand.New(rand.NewSource(seed))

	// 动态计算各阶段的数量目标（根据RTP目标调整）
	totalCount := config.Tables.DataNum
	var noWinCount, notProfitCount, profitCount, remainingCount int

	// 根据RTP目标动态调整比例
	if rtp >= 3.0 {
		// 超高RTP：需要更多盈利数据
		noWinCount = int(float64(totalCount) * 0.05)     // 5%不中奖
		notProfitCount = int(float64(totalCount) * 0.25) // 25%不盈利
		profitCount = int(float64(totalCount) * 0.50)    // 50%盈利
		remainingCount = int(float64(totalCount) * 0.20) // 20%调整
	} else if rtp >= 2.0 {
		// 高RTP：增加盈利数据比例
		noWinCount = int(float64(totalCount) * 0.08)     // 8%不中奖
		notProfitCount = int(float64(totalCount) * 0.32) // 32%不盈利
		profitCount = int(float64(totalCount) * 0.40)    // 40%盈利
		remainingCount = int(float64(totalCount) * 0.20) // 20%调整
	} else {
		// 中低RTP：保持原比例
		noWinCount = int(float64(totalCount) * 0.10)     // 10%不中奖
		notProfitCount = int(float64(totalCount) * 0.40) // 40%不盈利
		profitCount = int(float64(totalCount) * 0.30)    // 30%盈利
		remainingCount = int(float64(totalCount) * 0.20) // 20%调整
	}

	printf("🎯 动态数据分配计划 (RTP=%.2f):\n", rtp)
	printf("  - 不中奖数据: %d 条 (%.1f%%)\n", noWinCount, float64(noWinCount)/float64(totalCount)*100)
	printf("  - 不盈利数据: %d 条 (%.1f%%)\n", notProfitCount, float64(notProfitCount)/float64(totalCount)*100)
	printf("  - 盈利数据: %d 条 (%.1f%%)\n", profitCount, float64(profitCount)/float64(totalCount)*100)
	printf("  - 剩余调整: %d 条 (%.1f%%)\n", remainingCount, float64(remainingCount)/float64(totalCount)*100)

	var data []GameResultData
	var totalWin float64 = 0
	perSpinBet := float64(config.Bet.CS * config.Bet.ML * config.Bet.BL)

	// 第一步：添加不中奖数据 (10%)
	printf("\n📊 第一步：添加不中奖数据\n")
	if len(noWinDataAll) > 0 {
		permNo := rng.Perm(len(noWinDataAll))
		for i := 0; i < noWinCount && i < len(permNo); i++ {
			idx := permNo[i]
			data = append(data, noWinDataAll[idx])
		}
		printf("✅ 添加不中奖数据: %d 条\n", noWinCount)
	}

	// 第二步：添加不盈利数据 (40%)
	printf("\n📊 第二步：添加不盈利数据\n")
	// 从winDataAll中筛选出不盈利数据 (aw > 0 且 aw <= tb)
	var notProfitData []GameResultData
	for _, item := range winDataAll {
		if item.AW > 0 && float64(item.AW) <= float64(item.TB) {
			notProfitData = append(notProfitData, item)
		}
	}
	printf("可用不盈利数据: %d 条\n", len(notProfitData))

	if len(notProfitData) > 0 {
		permNotProfit := rng.Perm(len(notProfitData))
		addedCount := 0
		for i := 0; i < len(permNotProfit) && addedCount < notProfitCount; i++ {
			idx := permNotProfit[i]
			item := notProfitData[idx]
			data = append(data, item)
			totalWin += item.AW
			addedCount++
		}
		printf("✅ 添加不盈利数据: %d 条，累计中奖金额: %.2f\n", addedCount, totalWin)
	}

	// 第三步：添加盈利数据，动态调整筛选条件
	printf("\n📊 第三步：添加盈利数据\n")

	// 根据RTP目标动态调整盈利数据筛选条件
	var profitMinRatio, profitMaxMultiplier float64
	if rtp >= 3.0 {
		profitMinRatio = 1.2     // 超高RTP：放宽下限
		profitMaxMultiplier = 15 // 提高上限
	} else if rtp >= 2.0 {
		profitMinRatio = 1.3     // 高RTP：适度放宽
		profitMaxMultiplier = 10 // 适度提高上限
	} else {
		profitMinRatio = 1.5    // 中低RTP：保持原条件
		profitMaxMultiplier = 6 // 保持原上限
	}

	profitUpperLimit := perSpinBet * rtp * profitMaxMultiplier
	printf("盈利数据筛选条件: aw > %.1f*tb 且 aw <= %.1f*tb (上限: %.2f)\n",
		profitMinRatio, rtp*profitMaxMultiplier, profitUpperLimit)

	// 筛选盈利数据：动态条件
	var suitableProfitData []GameResultData
	for _, item := range winDataAll {
		if item.AW > float64(item.TB)*profitMinRatio && item.AW <= profitUpperLimit {
			suitableProfitData = append(suitableProfitData, item)
		}
	}
	printf("可用盈利数据: %d 条\n", len(suitableProfitData))

	// 按AW降序排序，优先选择大额盈利数据
	sort.Slice(suitableProfitData, func(i, j int) bool {
		return suitableProfitData[i].AW > suitableProfitData[j].AW
	})

	addedProfitCount := 0
	currentProfitWin := 0.0
	for _, item := range suitableProfitData {
		if addedProfitCount >= profitCount {
			break
		}

		// 检查加入这条数据后是否超过RTP上限
		newTotalWin := totalWin + item.AW
		if newTotalWin > maxAllowWin {
			continue
		}

		data = append(data, item)
		totalWin += item.AW
		currentProfitWin += item.AW
		addedProfitCount++
	}
	printf("✅ 添加盈利数据: %d 条，盈利金额: %.2f，累计中奖金额: %.2f\n", addedProfitCount, currentProfitWin, totalWin)

	// 第四步：智能调整剩余数据 - 优化版本
	printf("\n📊 第四步：智能调整剩余数据\n")
	currentCount := len(data)
	needMore := totalCount - currentCount
	printf("当前数据量: %d，目标: %d，还需要: %d\n", currentCount, totalCount, needMore)

	// 计算当前RTP与目标的差距
	currentRTP := totalWin / totalBet
	rtpGap := currentRTP - rtp
	printf("当前RTP: %.6f，目标RTP: %.6f，差距: %.6f\n", currentRTP, rtp, rtpGap)

	// 确保数量达标
	if needMore > 0 {
		printf("🎯 需要补充 %d 条数据以达到目标数量\n", needMore)

		// 计算还需要多少中奖金额才能达到RTP下限
		remainingWinNeeded := minAllowWin - totalWin
		printf("还需要中奖金额: %.2f 才能达到RTP下限 (%.2f)\n", remainingWinNeeded, rtpLowerLimit)

		// 收集所有可用数据（去重）
		usedIds := make(map[int]bool)
		for _, item := range data {
			usedIds[item.ID] = true
		}

		var allAvailableData []GameResultData
		// 优先使用盈利数据
		for _, item := range suitableProfitData {
			if !usedIds[item.ID] {
				allAvailableData = append(allAvailableData, item)
			}
		}
		// 其次使用不盈利数据
		for _, item := range notProfitData {
			if !usedIds[item.ID] {
				allAvailableData = append(allAvailableData, item)
			}
		}
		// 最后使用不中奖数据
		for _, item := range noWinDataAll {
			if !usedIds[item.ID] {
				allAvailableData = append(allAvailableData, item)
			}
		}

		printf("可用补充数据: %d 条\n", len(allAvailableData))

		if remainingWinNeeded > 0 {
			// RTP不足，优先选择大金额数据
			printf("🎯 RTP不足，优先选择大金额数据提升RTP\n")
			sort.Slice(allAvailableData, func(i, j int) bool {
				return allAvailableData[i].AW > allAvailableData[j].AW
			})
		} else {
			// RTP已达标，优先选择中小金额数据保持平衡
			printf("🎯 RTP已达标，优先选择中小金额数据保持平衡\n")
			sort.Slice(allAvailableData, func(i, j int) bool {
				return allAvailableData[i].AW < allAvailableData[j].AW
			})
		}

		// 添加数据直到数量达标
		added := 0
		for _, item := range allAvailableData {
			if added >= needMore {
				break
			}

			// 检查添加后是否超过RTP上限
			newTotalWin := totalWin + item.AW
			if newTotalWin > maxAllowWin {
				continue
			}

			data = append(data, item)
			totalWin += item.AW
			added++
		}
		printf("✅ 补充数据: %d 条，累计中奖金额: %.2f\n", added, totalWin)

		// 如果还是不够，用不中奖数据填充（确保数量达标）
		if len(data) < totalCount {
			remaining := totalCount - len(data)
			printf("🎯 还需要 %d 条数据，用不中奖数据填充确保数量达标\n", remaining)

			permNo := rng.Perm(len(noWinDataAll))
			for i := 0; i < remaining && i < len(permNo); i++ {
				idx := permNo[i]
				data = append(data, noWinDataAll[idx])
			}
			printf("✅ 不中奖数据填充: %d 条\n", remaining)
		}
	}

	// 第五步：精确RTP调整和下限保证
	printf("\n📊 第五步：精确RTP调整和下限保证\n")
	finalRTP := totalWin / totalBet
	rtpDeviation := math.Abs(finalRTP - rtp)
	printf("调整前RTP: %.6f，目标RTP: %.6f，偏差: %.6f\n", finalRTP, rtp, rtpDeviation)

	// 检查RTP下限
	if finalRTP < rtpLowerLimit {
		printf("⚠️ RTP低于下限 (%.2f < %.2f)，尝试提升RTP\n", finalRTP, rtpLowerLimit)

		// 收集所有未使用的中奖数据
		usedIds := make(map[int]bool)
		for _, item := range data {
			usedIds[item.ID] = true
		}

		var allUnusedWinData []GameResultData
		for _, item := range winDataAll {
			if !usedIds[item.ID] && item.AW > 0 {
				allUnusedWinData = append(allUnusedWinData, item)
			}
		}

		if len(allUnusedWinData) > 0 {
			// 按AW降序排序，优先选择大额数据
			sort.Slice(allUnusedWinData, func(i, j int) bool {
				return allUnusedWinData[i].AW > allUnusedWinData[j].AW
			})

			// 尝试替换不中奖数据来提升RTP
			adjustmentCount := 0
			maxAdjustments := 200 // 增加调整次数

			for _, newItem := range allUnusedWinData {
				if adjustmentCount >= maxAdjustments {
					break
				}

				// 找到一条不中奖数据进行替换
				for i, oldItem := range data {
					if oldItem.AW == 0 { // 只替换不中奖数据
						// 计算替换后的RTP
						replaceTotalWin := totalWin - oldItem.AW + newItem.AW
						replaceRTP := replaceTotalWin / totalBet

						// 如果替换后RTP更接近目标且不超过上限
						if replaceRTP >= rtpLowerLimit && replaceTotalWin <= maxAllowWin {
							data[i] = newItem
							totalWin = replaceTotalWin
							finalRTP = replaceRTP
							adjustmentCount++
							printf("🔄 替换不中奖数据: 旧AW=%.2f -> 新AW=%.2f, 新RTP=%.6f\n",
								oldItem.AW, newItem.AW, replaceRTP)
							break
						}
					}
				}
			}

			printf("✅ RTP下限调整完成，调整了 %d 条数据，最终RTP: %.6f\n", adjustmentCount, finalRTP)
		}
	}

	// 如果RTP偏差仍然较大，尝试微调
	if math.Abs(finalRTP-rtp) > 0.05 {
		printf("🎯 RTP偏差较大 (%.6f)，尝试微调\n", math.Abs(finalRTP-rtp))

		// 收集所有未使用的数据
		usedIds := make(map[int]bool)
		for _, item := range data {
			usedIds[item.ID] = true
		}

		var allUnusedData []GameResultData
		for _, item := range winDataAll {
			if !usedIds[item.ID] && item.AW > 0 {
				allUnusedData = append(allUnusedData, item)
			}
		}

		if len(allUnusedData) > 0 {
			// 根据RTP偏差方向选择调整策略
			if finalRTP < rtp {
				// RTP偏低，优先选择大额数据
				sort.Slice(allUnusedData, func(i, j int) bool {
					return allUnusedData[i].AW > allUnusedData[j].AW
				})
				printf("🎯 RTP偏低，尝试添加大额数据提升RTP\n")
			} else {
				// RTP偏高，优先选择小额数据
				sort.Slice(allUnusedData, func(i, j int) bool {
					return allUnusedData[i].AW < allUnusedData[j].AW
				})
				printf("🎯 RTP偏高，尝试添加小额数据降低RTP\n")
			}

			// 尝试替换一些数据来调整RTP
			adjustmentCount := 0
			maxAdjustments := 100

			for _, newItem := range allUnusedData {
				if adjustmentCount >= maxAdjustments {
					break
				}

				// 随机选择一条现有数据进行替换
				if len(data) > 0 {
					replaceIndex := rng.Intn(len(data))
					oldItem := data[replaceIndex]

					// 计算替换后的RTP
					replaceTotalWin := totalWin - oldItem.AW + newItem.AW
					replaceRTP := replaceTotalWin / totalBet
					replaceDeviation := math.Abs(replaceRTP - rtp)

					// 如果替换后RTP更接近目标且不超过上限
					if replaceDeviation < rtpDeviation && replaceTotalWin <= maxAllowWin && replaceRTP >= rtpLowerLimit {
						data[replaceIndex] = newItem
						totalWin = replaceTotalWin
						rtpDeviation = replaceDeviation
						adjustmentCount++
					}
				}
			}

			printf("✅ 精确调整完成，调整了 %d 条数据\n", adjustmentCount)
		}
	}

	// 最终统计和验证
	printf("\n📊 最终统计和验证\n")
	finalRTP = totalWin / totalBet
	rtpDeviation = math.Abs(finalRTP - rtp)

	// 统计各类数据的数量和占比
	var finalNoWinCount, finalNotProfitCount, finalProfitCount int
	for _, item := range data {
		if item.AW == 0 {
			finalNoWinCount++
		} else if item.AW <= float64(item.TB) {
			finalNotProfitCount++
		} else {
			finalProfitCount++
		}
	}

	printf("✅ V3优化策略结果:\n")
	printf("  - 总数据量: %d 条\n", len(data))
	printf("  - 总投注: %.2f\n", totalBet)
	printf("  - 总中奖: %.2f\n", totalWin)
	printf("  - 实际RTP: %.6f\n", finalRTP)
	printf("  - 目标RTP: %.6f\n", rtp)
	printf("  - RTP偏差: %.6f\n", rtpDeviation)
	printf("  - RTP下限: %.6f (%.1f%%)\n", rtpLowerLimit, rtpLowerLimit*100)
	printf("  - 不中奖数据: %d 条 (%.1f%%)\n", finalNoWinCount, float64(finalNoWinCount)/float64(len(data))*100)
	printf("  - 不盈利数据: %d 条 (%.1f%%)\n", finalNotProfitCount, float64(finalNotProfitCount)/float64(len(data))*100)
	printf("  - 盈利数据: %d 条 (%.1f%%)\n", finalProfitCount, float64(finalProfitCount)/float64(len(data))*100)

	// 验证数据量
	if len(data) != config.Tables.DataNum {
		return fmt.Errorf("❌ 数据量不匹配：期望 %d 条, 实际 %d 条", config.Tables.DataNum, len(data))
	}

	// 验证RTP下限
	if finalRTP < rtpLowerLimit {
		return fmt.Errorf("❌ RTP低于下限：实际 %.6f < 下限 %.6f", finalRTP, rtpLowerLimit)
	}

	// 验证RTP上限
	if finalRTP > rtpUpperLimit {
		return fmt.Errorf("❌ RTP超过上限：实际 %.6f > 上限 %.6f", finalRTP, rtpUpperLimit)
	}

	printf("✅ 所有验证通过：数据量正确，RTP在允许范围内 [%.2f, %.2f]\n", rtpLowerLimit, rtpUpperLimit)

	// 打乱输出顺序
	rand.Shuffle(len(data), func(i, j int) {
		data[i], data[j] = data[j], data[i]
	})

	// 保存到JSON文件
	var outputDir string = filepath.Join("output", fmt.Sprintf("%d", config.Game.ID))
	if err := saveToJSON(data, config, rtpLevel, testNumber, outputDir); err != nil {
		return fmt.Errorf("保存JSON文件失败: %v", err)
	}

	// 任务尾分隔线
	printf("========== [TASK END - V3 STRATEGY]   RtpNo: %.0f | Test: %d =========\n\n", rtpLevel, testNumber)
	printf("⏱️  RTP等级 %.0f (第%d次生成-V3策略) 耗时: %v\n", rtpLevel, testNumber, time.Since(testStartTime))

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
