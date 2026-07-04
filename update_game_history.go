package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"
)

type historyOrder struct {
	HistoryID int64
	BetTime   time.Time
	Table     string
}

func parseHistoryOrders(xlsxPath string) ([]historyOrder, error) {
	f, err := excelize.OpenFile(xlsxPath)
	if err != nil {
		return nil, fmt.Errorf("打开 Excel 失败: %w", err)
	}
	defer f.Close()

	sheetName := f.GetSheetName(0)
	if sheetName == "" {
		return nil, fmt.Errorf("Excel 中没有工作表")
	}

	rows, err := f.GetRows(sheetName)
	if err != nil {
		return nil, fmt.Errorf("读取工作表失败: %w", err)
	}
	if len(rows) < 2 {
		return nil, fmt.Errorf("Excel 没有数据行")
	}

	header := rows[0]
	colHistoryID := findColumnIndex(header, "注单id")
	colBetTime := findColumnIndex(header, "注单时间")
	if colHistoryID < 0 || colBetTime < 0 {
		return nil, fmt.Errorf("Excel 缺少必要列: 注单id, 注单时间")
	}

	var orders []historyOrder
	for i := 1; i < len(rows); i++ {
		row := rows[i]
		if colHistoryID >= len(row) || colBetTime >= len(row) {
			continue
		}

		historyIDStr := strings.TrimSpace(row[colHistoryID])
		betTimeStr := strings.TrimSpace(row[colBetTime])
		if historyIDStr == "" || betTimeStr == "" {
			continue
		}

		historyID, err := strconv.ParseInt(historyIDStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("第 %d 行 historyId 无效: %s", i+1, historyIDStr)
		}

		betTime, err := parseBetTime(f, sheetName, i+1, colBetTime, betTimeStr)
		if err != nil {
			return nil, fmt.Errorf("第 %d 行注单时间无效: %w", i+1, err)
		}

		tableName := fmt.Sprintf("GameHistory_%s", betTime.Format("20060102"))
		orders = append(orders, historyOrder{
			HistoryID: historyID,
			BetTime:   betTime,
			Table:     tableName,
		})
	}

	if len(orders) == 0 {
		return nil, fmt.Errorf("未解析到有效注单")
	}
	return orders, nil
}

func findColumnIndex(header []string, name string) int {
	for i, col := range header {
		if strings.TrimSpace(col) == name {
			return i
		}
	}
	return -1
}

func parseBetTime(f *excelize.File, sheet string, rowIdx, colIdx int, raw string) (time.Time, error) {
	cellName, err := excelize.CoordinatesToCellName(colIdx+1, rowIdx)
	if err == nil {
		if cellRaw, err := f.GetCellValue(sheet, cellName, excelize.Options{RawCellValue: true}); err == nil && cellRaw != "" {
			if serial, err := strconv.ParseFloat(cellRaw, 64); err == nil {
				return excelize.ExcelDateToTime(serial, false)
			}
		}
	}

	if serial, err := strconv.ParseFloat(raw, 64); err == nil {
		return excelize.ExcelDateToTime(serial, false)
	}

	layouts := []string{
		"2006-01-02 15:04:05",
		"2006/1/2 15:04:05",
		"2006/01/02 15:04:05",
		time.RFC3339,
	}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, raw, time.Local); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("无法解析时间: %s", raw)
}

func groupHistoryOrders(orders []historyOrder) map[string][]int64 {
	grouped := make(map[string][]int64)
	for _, o := range orders {
		grouped[o.Table] = append(grouped[o.Table], o.HistoryID)
	}
	return grouped
}

func buildHistoryUpdateSQL(table string, historyID int64) string {
	return fmt.Sprintf(
		"UPDATE public.\"%s\"\nSET \"updatedAt\" = \"createdAt\"\nWHERE \"historyId\" = %d;",
		table, historyID,
	)
}

func writeHistoryUpdateSQL(outputPath string, orders []historyOrder) error {
	grouped := groupHistoryOrders(orders)
	tables := make([]string, 0, len(grouped))
	for table := range grouped {
		tables = append(tables, table)
	}
	sort.Strings(tables)

	var b strings.Builder
	fmt.Fprintf(&b, "-- 自动生成: 将 GameHistory 分表中指定注单的 updatedAt 同步为 createdAt\n")
	fmt.Fprintf(&b, "-- 生成时间: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Fprintf(&b, "-- 共 %d 条\n\n", len(orders))

	for _, table := range tables {
		ids := grouped[table]
		fmt.Fprintf(&b, "-- ===== %s (%d 条) =====\n", table, len(ids))
		for _, id := range ids {
			b.WriteString(buildHistoryUpdateSQL(table, id))
			b.WriteString("\n\n")
		}
	}

	return os.WriteFile(outputPath, []byte(b.String()), 0644)
}

func executeHistoryUpdates(db *Database, orders []historyOrder, dryRun bool) error {
	grouped := groupHistoryOrders(orders)
	tables := make([]string, 0, len(grouped))
	for table := range grouped {
		tables = append(tables, table)
	}
	sort.Strings(tables)

	ok, missing, failed := 0, 0, 0

	for _, table := range tables {
		fmt.Printf("\n>>> 处理表 %s (%d 条)\n", table, len(grouped[table]))
		for _, historyID := range grouped[table] {
			var createdAt, updatedAt time.Time
			query := fmt.Sprintf(
				`SELECT "createdAt", "updatedAt" FROM public."%s" WHERE "historyId" = $1`,
				table,
			)
			err := db.DB.QueryRow(query, historyID).Scan(&createdAt, &updatedAt)
			if err == sql.ErrNoRows {
				fmt.Printf("  [未找到] historyId=%d\n", historyID)
				missing++
				continue
			}
			if err != nil {
				return fmt.Errorf("查询 %s historyId=%d 失败: %w", table, historyID, err)
			}

			if dryRun {
				fmt.Printf("  [预览] historyId=%d createdAt=%s updatedAt=%s\n",
					historyID, createdAt.Format(time.RFC3339), updatedAt.Format(time.RFC3339))
				ok++
				continue
			}

			updateSQL := fmt.Sprintf(
				`UPDATE public."%s" SET "updatedAt" = "createdAt" WHERE "historyId" = $1`,
				table,
			)
			result, err := db.DB.Exec(updateSQL, historyID)
			if err != nil {
				return fmt.Errorf("更新 %s historyId=%d 失败: %w", table, historyID, err)
			}
			rows, _ := result.RowsAffected()
			if rows == 1 {
				fmt.Printf("  [已更新] historyId=%d\n", historyID)
				ok++
			} else {
				fmt.Printf("  [失败] historyId=%d rowcount=%d\n", historyID, rows)
				failed++
			}
		}
	}

	fmt.Printf("\n完成: 成功/预览 %d 条, 未找到 %d 条, 失败 %d 条\n", ok, missing, failed)
	return nil
}

func runUpdateGameHistoryCommand() {
	xlsxPath := "data-1738001.xlsx"
	outputPath := "update_game_history.sql"
	env := ""
	execute := false
	dryRun := false

	args := os.Args[2:]
	for len(args) > 0 && strings.HasPrefix(args[0], "--") {
		switch args[0] {
		case "--execute":
			execute = true
		case "--dry-run":
			dryRun = true
		case "--env":
			if len(args) < 2 {
				fmt.Println("❌ --env 需要指定环境参数")
				printUpdateGameHistoryUsage()
				os.Exit(1)
			}
			if !IsEnv(args[1]) {
				fmt.Printf("❌ 无效的环境参数: %s\n", args[1])
				printEnvHelp()
				os.Exit(1)
			}
			env = ResolveEnv(args[1])
			args = args[2:]
			continue
		default:
			fmt.Printf("❌ 未知参数: %s\n", args[0])
			printUpdateGameHistoryUsage()
			os.Exit(1)
		}
		args = args[1:]
	}

	needDB := execute || dryRun

	switch len(args) {
	case 0:
	case 1:
		if IsEnv(args[0]) {
			env = ResolveEnv(args[0])
		} else if needDB {
			xlsxPath = args[0]
		} else {
			xlsxPath = args[0]
		}
	case 2:
		if needDB {
			xlsxPath = args[0]
			if !IsEnv(args[1]) {
				fmt.Printf("❌ 无效的环境参数: %s\n", args[1])
				printEnvHelp()
				os.Exit(1)
			}
			env = ResolveEnv(args[1])
		} else {
			xlsxPath = args[0]
			outputPath = args[1]
		}
	case 3:
		if needDB {
			fmt.Println("❌ --execute/--dry-run 模式下参数过多")
			printUpdateGameHistoryUsage()
			os.Exit(1)
		}
		xlsxPath = args[0]
		outputPath = args[1]
		if !IsEnv(args[2]) {
			fmt.Printf("❌ 无效的环境参数: %s\n", args[2])
			printEnvHelp()
			os.Exit(1)
		}
		env = ResolveEnv(args[2])
	default:
		printUpdateGameHistoryUsage()
		os.Exit(1)
	}

	if needDB && env == "" {
		fmt.Println("❌ --execute / --dry-run 必须指定数据库环境")
		printEnvHelp()
		fmt.Println("")
		printUpdateGameHistoryUsage()
		os.Exit(1)
	}

	if !needDB && env != "" {
		fmt.Println("ℹ️  仅生成 SQL 时不需要环境参数，已忽略")
		env = ""
	}

	if !filepath.IsAbs(xlsxPath) {
		if _, err := os.Stat(xlsxPath); os.IsNotExist(err) {
			log.Fatalf("❌ Excel 文件不存在: %s", xlsxPath)
		}
	}

	orders, err := parseHistoryOrders(xlsxPath)
	if err != nil {
		log.Fatalf("❌ 解析 Excel 失败: %v", err)
	}
	fmt.Printf("解析完成: %d 条注单, 涉及 %d 张分表\n", len(orders), len(groupHistoryOrders(orders)))

	if err := writeHistoryUpdateSQL(outputPath, orders); err != nil {
		log.Fatalf("❌ 写入 SQL 失败: %v", err)
	}
	fmt.Printf("SQL 已写入: %s\n", outputPath)

	if !needDB {
		return
	}

	config, err := LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("❌ 加载配置失败: %v", err)
	}

	dbConfig, err := config.GetDatabaseConfig(env)
	if err != nil {
		log.Fatalf("❌ 获取数据库配置失败: %v", err)
	}
	fmt.Printf("连接数据库 [环境: %s, 主机: %s:%d, 库: %s]\n",
		env, dbConfig.Host, dbConfig.Port, dbConfig.Dbname)

	db, err := NewDatabase(config, env)
	if err != nil {
		log.Fatalf("❌ 连接数据库失败: %v", err)
	}
	defer db.Close()

	if dryRun && !execute {
		fmt.Println("预览模式（不执行 UPDATE）")
	}
	if err := executeHistoryUpdates(db, orders, dryRun && !execute); err != nil {
		log.Fatalf("❌ 执行失败: %v", err)
	}
}

func printEnvHelp() {
	fmt.Println("支持的环境: local/l, hk-test/ht, br-test/bt, br-prod/bp, us-prod/up, hk-prod/hp")
}

func printUpdateGameHistoryUsage() {
	fmt.Println("用法:")
	fmt.Println("  ./filteringData fix-history [xlsx] [output.sql]              # 只生成 SQL，不连库")
	fmt.Println("  ./filteringData fix-history --dry-run <env> [xlsx]           # 指定环境预览")
	fmt.Println("  ./filteringData fix-history --execute <env> [xlsx]           # 指定环境执行")
	fmt.Println("  ./filteringData fix-history --env <env> --execute [xlsx]     # 用 --env 指定环境")
	fmt.Println("")
	fmt.Println("说明:")
	fmt.Println("  解析 Excel 注单，按注单时间定位 GameHistory_YYYYMMDD 分表")
	fmt.Println("  将对应 historyId 记录的 updatedAt 更新为 createdAt")
	fmt.Println("  非 local 环境从环境变量读取数据库配置，如 HT_DB_HOST、UP_DB_HOST 等")
	fmt.Println("")
	fmt.Println("示例:")
	fmt.Println("  ./filteringData fix-history")
	fmt.Println("  ./filteringData fix-history data-1738001.xlsx")
	fmt.Println("  ./filteringData fix-history --dry-run ht")
	fmt.Println("  ./filteringData fix-history --execute up")
	fmt.Println("  ./filteringData fix-history --execute data-1738001.xlsx hp")
	fmt.Println("  ./filteringData fix-history --env ht --execute")
}
