package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/yaml.v3"
)

type GameResult struct {
	RtpLevel float32
	SrNumber int
	SrId     int
	Bet      float64
	Win      float64
	Detail   json.RawMessage
}

const (
	batchSize = 500 // 每 N 条写入一次
)

const (
	targetRTP       = 0.97
	rtpTolerance    = 0.01
	baseTargetCount = 10000
)

type Config struct {
	DSN     string `yaml:"dsn"`
	SrCount int    `yaml:"srCount"`
	// GameId        int          `yaml:"gameId"`
	// CsvFileName   string       `yaml:"csvName"`
	// CsvFbFileName string       `yaml:"csvFbName"`
	// SpPercent     float64      `yaml:"spPercent"`
	BatchConfig []InsertGame `yaml:"batchInsert"`
	RtpLevels   []RtpLevel   `yaml:"rtpLevels"`
}

type InsertGame struct {
	GameId        int     `yaml:"gameId"`
	CsvFileName   string  `yaml:"csvName"`
	CsvFbFileName string  `yaml:"csvFbName"`
	SpPercent     float64 `yaml:"spPercent"`
}

type RtpLevel struct {
	RtpNo float32 `yaml:"rtpNo"`
	Rtp   float32 `yaml:"rtp"`
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
	{RtpNo: 16, Rtp: 3},
	{RtpNo: 17, Rtp: 5},
	{RtpNo: 20, Rtp: 1.2},
}

func genTable(ctx context.Context, pool *pgxpool.Pool, tableName string) error {
	sql := fmt.Sprintf(`
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
		CREATE INDEX IF NOT EXISTS "%s_rtpLevel_idx" ON "%s" ("rtpLevel");
		CREATE INDEX IF NOT EXISTS "%s_srNumber_idx" ON "%s" ("srNumber");
		CREATE INDEX IF NOT EXISTS "%s_srId_idx" ON "%s" ("srId");
		CREATE INDEX IF NOT EXISTS "%s_rtpLevel_srNumber_srId_idx" ON "%s" ("rtpLevel", "srNumber", "srId");
	`, tableName, tableName,
		tableName, tableName,
		tableName, tableName,
		tableName, tableName,
		tableName, tableName)

	_, err := pool.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("创建表失败: %w", err)
	}
	return nil
}

var config Config

func LoadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("无法打开配置文件: %w", err)
	}
	defer file.Close()

	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("解析配置失败: %w", err)
	}

	return &config, nil
}

func main() {
	config, err := LoadConfig("config.yaml")
	if err != nil {
		panic(err)
	}

	if len(config.RtpLevels) > 0 {
		RtpLevels = config.RtpLevels
		fmt.Printf("rtpLevel: %+v \n", RtpLevels)
	}

	for _, game := range config.BatchConfig {
		// csvFile, err := os.Open(game.CsvFileName)
		// if err != nil {
		// 	panic(err)
		// }
		// defer csvFile.Close()

		// reader := csv.NewReader(csvFile)
		// reader.ReuseRecord = false

		// 跳过标题行
		// _, err = reader.Read()
		// if err != nil {
		// panic(fmt.Errorf("读取表头失败: %v", err))
		// }

		ctx := context.Background()
		pool, err := pgxpool.New(ctx, config.DSN)
		if err != nil {
			panic(err)
		}
		defer pool.Close()

		if err := genTable(ctx, pool, fmt.Sprintf("GameResults_%d", game.GameId)); err != nil {
			log.Fatalf("确保表失败: %v", err)
		}

		fmt.Printf("开始处理游戏ID: %d, CSV 文件: %s\n", game.GameId, game.CsvFileName)

		for _, level := range RtpLevels {
			// if level.RtpNo > 1 {
			// 	break
			// }

			fmt.Printf("开始处理游戏ID: %d, RTP 等级: %f, 目标 RTP: %.2f\n", game.GameId, level.RtpNo, level.Rtp)
			for i := 0; i < config.SrCount; i++ {
				if game.CsvFileName != "" {
					setRtpLevel(ctx, pool, game.GameId, game.CsvFileName, level, i+1, game.SpPercent, false)
				}

				if game.CsvFbFileName != "" {
					setFbRtpLevel(ctx, pool, game.GameId, game.CsvFbFileName, level, i+1, game.SpPercent, true)
				}
			}

			fmt.Printf("RTP 等级: %f, RTP 值: %.2f\n", level.RtpNo, level.Rtp)
		}
	}

	fmt.Printf("✅ 所有数据导入完成\n")
}

func setRtpLevel(ctx context.Context, pool *pgxpool.Pool, gameId int, filename string, rtpLevel RtpLevel, srNumber int, spPercent float64, fb bool) {
	fmt.Printf("✅ 任务rtpNo %.2f, rtp: %f fb: %t fileName: %s\n", rtpLevel.RtpNo, rtpLevel.Rtp, fb, filename)
	csvFile, err := os.Open(filename)
	if err != nil {
		// panic(err)
		return
	}
	defer csvFile.Close()

	reader := csv.NewReader(csvFile)
	reader.ReuseRecord = false

	rowCount := 0
	totalCount := 0
	inputCount := 0
	targetCount := baseTargetCount
	if fb {
		targetCount -= 7000
	}

	var winRecords [][]string
	var fillRecords [][]string
	var fillWin float64
	var notWinRecords [][]string
	var selectedRecords [][]string
	var totalWin float64
	reader.Read()
	row, err := reader.Read()

	if err != nil {
		fmt.Println(err.Error())
	}

	bet, err := strconv.ParseFloat(row[0], 64)
	if err != nil {
		bet = 1
		// 处理错误
	}
	minWin := bet * float64(targetCount) * (float64(rtpLevel.Rtp) - rtpTolerance)
	// maxWin := 1 * targetCount * (float64(rtpLevel.Rtp) + rtpTolerance)
	limitWin := bet * float64(targetCount) * float64(rtpLevel.Rtp)
	spLen := 0
	spLimit := int(math.Ceil(spPercent * float64(targetCount)))
	// fmt.Printf("limitWin: %f bet: %f   %s\n", limitWin, bet, row[0])

	for {

		row, err := reader.Read()

		if err != nil {
			fmt.Println(err.Error())
			break // EOF
		}

		if row[0] == "bet" {
			continue
		}
		totalCount++

		winVal, err := strconv.ParseFloat(row[1], 64)

		if err != nil {
			continue // 忽略无法解析的
		}

		if row[2] == "1" {
			spLen++
		}
		if fb && row[2] != "1" {
			continue
		}

		bet, err = strconv.ParseFloat(row[0], 64)

		// 若总数达到目标，检查 RTP
		if totalWin+winVal > limitWin && len(fillRecords) >= targetCount/2 {
			value, err := strconv.ParseFloat(row[0], 64)
			if err != nil {
				fmt.Println("转换错误:", err)
				value = 1
			}
			if len(winRecords)+len(notWinRecords) >= targetCount+100 {
				fmt.Printf("找到符合要求的数据rtp:%f, win: %d, notWin: %d totalCount: %d totalWin: %f spLen: %d spLimit: %d \n", float64(totalWin)/float64(value), len(winRecords), len(notWinRecords), totalCount, totalWin, spLen, spLimit)
				break
			}
		}

		can := true
		if !fb {
			can = rand.Float64() < 0.8
		}

		if !fb {
			if winVal > 0 && (totalWin+winVal < limitWin || totalWin < minWin) && can && len(winRecords) < targetCount {
				// fmt.Printf("total : %f", totalWin)
				winRecords = append(winRecords, row)
				totalWin += winVal
			} else if winVal == 0 {
				notWinRecords = append(notWinRecords, row)
			} else if winVal/bet >= float64(rtpLevel.Rtp) {
				fillRecords = append(fillRecords, row)
				fillWin += winVal
			}
		} else {
			if winVal > bet*0.1 && (totalWin+winVal+fillWin < limitWin) && can {
				// fmt.Printf("total : %f", totalWin)
				winRecords = append(winRecords, row)
				totalWin += winVal
			} else if winVal < bet*0.1 {
				fillWin += winVal
				fillRecords = append(fillRecords, row)
			}
		}
	}

	// fmt.Printf("win : %d\n", len(winRecords))

	selectedRecords = append(selectedRecords, winRecords...)
	if (len(selectedRecords) < targetCount) && (len(notWinRecords) > 0) {
		need := targetCount - len(selectedRecords)
		if need > len(notWinRecords) {
			need = len(notWinRecords)
		}
		selectedRecords = append(selectedRecords, notWinRecords[:need]...)
	}

	cursor := 0
	for totalWin < limitWin {
		add := fillRecords[cursor]
		addWin, _ := strconv.ParseFloat(add[1], 64)

		delIdx := -1
		for idx, row := range selectedRecords {
			win, _ := strconv.ParseFloat(row[1], 64)
			if addWin > win {
				delIdx = idx
				break
			}
		}

		cursor++
		if cursor >= len(fillRecords) {
			cursor = 0
		}

		if delIdx == -1 {
			continue
		}

		addVal, _ := strconv.ParseFloat(add[1], 64)
		delVal, _ := strconv.ParseFloat(selectedRecords[delIdx][1], 64)
		if totalWin+addVal-delVal > limitWin {
			break
		}
		selectedRecords = append(selectedRecords[0:delIdx], selectedRecords[delIdx+1:]...)

		selectedRecords = append(selectedRecords, add)

		totalWin = totalWin + addVal - delVal

	}

	// 打乱数据
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(selectedRecords), func(i, j int) {
		selectedRecords[i], selectedRecords[j] = selectedRecords[j], selectedRecords[i]
	})

	if fb {
		for i := 0; len(selectedRecords) < targetCount && i < len(fillRecords); i++ {
			selectedRecords = append(selectedRecords, fillRecords[i])
		}
	}

	totalBet := bet * float64(len(selectedRecords))
	fmt.Printf("totalCount : %d ,limitWin: %f , fillWin: %f, totalWin: %f, totalBet: %f, rtp: %f\n", totalCount, limitWin, fillWin, totalWin, totalBet, float64(totalWin)/float64(totalBet))
	fmt.Printf("fillRecords: %d selectedRecords: %d \n", len(fillRecords), len(selectedRecords))

	var buffer []GameResult
	for idx, record := range selectedRecords {
		rowCount++

		// fmt.Printf("已导入 %d 条\n", totalCount)

		detail := json.RawMessage(record[3])
		bet, _ := strconv.ParseFloat(record[0], 64)
		win, _ := strconv.ParseFloat(record[1], 64)
		rtpNo := float32(rtpLevel.RtpNo)
		if fb {
			rtpNo = rtpNo + 0.1
		}
		buffer = append(buffer, GameResult{
			RtpLevel: rtpNo,
			SrNumber: srNumber,
			SrId:     idx + 1,
			Bet:      bet,
			Win:      win,
			Detail:   detail,
		})

		if rowCount >= batchSize {
			err := insertBatch(ctx, pool, gameId, buffer)

			if err != nil {
				panic(err)
			}
			inputCount += rowCount
			fmt.Printf("已导入 %d 条\n", inputCount)
			buffer = buffer[:0]
			rowCount = 0
		}
		// }

	}

	// 处理剩余未满一批的记录
	if len(buffer) > 0 {
		// consolog.log("合并了")
		if err := insertBatch(ctx, pool, gameId, buffer); err != nil {
			panic(err)
		}
		fmt.Printf("已导入 %d 条\n", inputCount+len(buffer))
		totalCount += len(buffer)
	}

}

func setFbRtpLevel(ctx context.Context, pool *pgxpool.Pool, gameId int, filename string, rtpLevel RtpLevel, srNumber int, spPercent float64, fb bool) {
	fmt.Printf("✅ 任务rtpNo %f, rtp: %f fb: %t fileName: %s\n", rtpLevel.RtpNo, rtpLevel.Rtp, fb, filename)
	csvFile, err := os.Open(filename)
	if err != nil {
		// panic(err)
		return
	}
	defer csvFile.Close()

	reader := csv.NewReader(csvFile)
	reader.ReuseRecord = false

	rowCount := 0
	totalCount := 0
	inputCount := 0
	targetCount := baseTargetCount
	if fb {
		targetCount -= 7000
	}

	var winRecords [][]string
	var fillRecords [][]string
	var fillWin float64
	var notWinRecords [][]string
	var selectedRecords [][]string
	var totalWin float64
	reader.Read()
	row, err := reader.Read()

	if err != nil {
		fmt.Println(err.Error())
	}

	bet, err := strconv.ParseFloat(row[0], 64)
	if err != nil {
		bet = 1.0
		// 处理错误
	}
	minWin := bet * float64(targetCount) * (float64(rtpLevel.Rtp) - rtpTolerance)
	// maxWin := 1 * targetCount * (float64(rtpLevel.Rtp) + rtpTolerance)
	limitWin := bet * float64(targetCount) * float64(rtpLevel.Rtp)
	spLen := 0
	spLimit := int(math.Ceil(spPercent * float64(targetCount)))
	// fmt.Printf("limitWin: %f bet: %f   %s\n", limitWin, bet, row[0])

	max := 0.0
	for {

		row, err := reader.Read()

		if err != nil {
			fmt.Println(err.Error())
			break // EOF
		}

		if row[0] == "bet" {
			continue
		}
		totalCount++

		winVal, err := strconv.ParseFloat(row[1], 64)

		if err != nil {
			continue // 忽略无法解析的
		}

		if row[2] == "1" {
			spLen++
		}
		if fb && row[2] != "1" {
			continue
		}

		bet, err = strconv.ParseFloat(row[0], 64)

		// 若总数达到目标，检查 RTP
		if len(winRecords) > targetCount && len(fillRecords) > targetCount {
			value, err := strconv.ParseFloat(row[0], 64)
			if err != nil {
				fmt.Println("转换错误:", err)
				value = 1
			}
			// if len(winRecords)+len(notWinRecords) >= targetCount+100 {
			fmt.Printf("找到符合要求的数据rtp:%f, win: %d, notWin: %d totalCount: %d totalWin: %f spLen: %d spLimit: %d \n", float64(totalWin)/float64(value), len(winRecords), len(notWinRecords), totalCount, totalWin, spLen, spLimit)
			break
			// }
		}

		can := true
		if !fb {
			can = rand.Float64() < 0.8
		}
		if !can {
			continue
		}

		if !fb {
			if winVal > 0 && (totalWin+winVal < limitWin || totalWin < minWin) && can {
				// fmt.Printf("total : %f", totalWin)
				winRecords = append(winRecords, row)
				totalWin += winVal
			} else if winVal == 0 {
				notWinRecords = append(notWinRecords, row)
			}
		} else {
			fillVal := bet * 0.7 * float64(rtpLevel.Rtp)
			if 0.7*float64(rtpLevel.Rtp) < 1 {
				fillVal = bet
			}
			if 0.7*float64(rtpLevel.Rtp) > 3 {
				fillVal = bet * 3
			}

			if winVal <= bet*0.7 && len(winRecords) < targetCount {
				// fmt.Printf("total : %f \n", totalWin)
				winRecords = append(winRecords, row)
				totalWin += winVal
			} else if winVal > fillVal && len(fillRecords) < targetCount {
				if winVal > max {
					max = winVal
				}
				fillWin += winVal
				fillRecords = append(fillRecords, row)
			}
		}
	}

	fmt.Printf("fb win : %d totalWin:  %.2f max: %.2f bet: %.2f\n", len(fillRecords), totalWin, max, bet)

	selectedRecords = append(selectedRecords, winRecords...)
	if (len(selectedRecords) < targetCount) && (len(notWinRecords) > 0) {
		need := targetCount - len(selectedRecords)
		if need > len(notWinRecords) {
			need = len(notWinRecords)
		}
		selectedRecords = append(selectedRecords, notWinRecords[:need]...)
	}

	cursor := 0
	for totalWin < limitWin {
		add := fillRecords[cursor]
		addWin, _ := strconv.ParseFloat(add[1], 64)

		delIdx := -1
		for idx, row := range selectedRecords {
			win, _ := strconv.ParseFloat(row[1], 64)
			if addWin > win {
				delIdx = idx
				break
			}
		}

		cursor++
		if cursor >= len(fillRecords) {
			cursor = 0
		}

		if delIdx == -1 {
			continue
		}

		addVal, _ := strconv.ParseFloat(add[1], 64)
		delVal, _ := strconv.ParseFloat(selectedRecords[delIdx][1], 64)

		if totalWin+addVal-delVal > limitWin {
			break
		}

		selectedRecords = append(selectedRecords[0:delIdx], selectedRecords[delIdx+1:]...)

		selectedRecords = append(selectedRecords, add)

		totalWin = totalWin + addVal - delVal

		// fmt.Printf("totalWin: %.2f limitWin: %.2f \n", totalWin, limitWin)

	}

	// 打乱数据
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(selectedRecords), func(i, j int) {
		selectedRecords[i], selectedRecords[j] = selectedRecords[j], selectedRecords[i]
	})

	if fb {
		for i := 0; len(selectedRecords) < targetCount && i < len(fillRecords); i++ {
			selectedRecords = append(selectedRecords, fillRecords[i])
		}
	}

	totalBet := bet * float64(len(selectedRecords))
	fmt.Printf("totalCount : %d ,limitWin: %f , fillWin: %f, totalWin: %f, totalBet: %f, rtp: %f\n", totalCount, limitWin, fillWin, totalWin, totalBet, float64(totalWin)/float64(totalBet))
	fmt.Printf("fillRecords: %d selectedRecords: %d \n", len(fillRecords), len(selectedRecords))

	var buffer []GameResult
	for idx, record := range selectedRecords {
		rowCount++

		// fmt.Printf("已导入 %d 条\n", totalCount)

		detail := json.RawMessage(record[3])
		bet, _ := strconv.ParseFloat(record[0], 64)
		win, _ := strconv.ParseFloat(record[1], 64)
		rtpNo := float32(rtpLevel.RtpNo)
		if fb {
			rtpNo = rtpNo + 0.1
		}
		buffer = append(buffer, GameResult{
			RtpLevel: rtpNo,
			SrNumber: srNumber,
			SrId:     idx + 1,
			Bet:      bet,
			Win:      win,
			Detail:   detail,
		})

		if rowCount >= batchSize {
			err := insertBatch(ctx, pool, gameId, buffer)

			if err != nil {
				panic(err)
			}
			inputCount += rowCount
			fmt.Printf("已导入 %d 条\n", inputCount)
			buffer = buffer[:0]
			rowCount = 0
		}
		// }

	}

	// 处理剩余未满一批的记录
	if len(buffer) > 0 {
		// consolog.log("合并了")
		if err := insertBatch(ctx, pool, gameId, buffer); err != nil {
			panic(err)
		}
		fmt.Printf("已导入 %d 条\n", inputCount+len(buffer))
		totalCount += len(buffer)
	}

}

func MergeAndShuffle(winBuffer, notWinBuffer []GameResult) []GameResult {
	// 1. 合并
	merged := append(winBuffer, notWinBuffer...)

	// 2. 打乱（Fisher–Yates 洗牌）
	rand.Seed(time.Now().UnixNano())
	for i := len(merged) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		merged[i], merged[j] = merged[j], merged[i]
	}

	return merged
}

func insertBatch(ctx context.Context, pool *pgxpool.Pool, gameId int, records []GameResult) error {
	rows := make([][]interface{}, 0, len(records))
	for _, r := range records {
		rows = append(rows, []interface{}{
			r.RtpLevel,
			r.SrNumber,
			r.SrId,
			r.Bet,
			r.Win,
			r.Detail,
		})
	}

	_, err := pool.CopyFrom(ctx,
		pgx.Identifier{fmt.Sprintf("GameResults_%d", gameId)},
		[]string{"rtpLevel", "srNumber", "srId", "bet", "win", "detail"},
		pgx.CopyFromRows(rows),
	)
	return err
}
