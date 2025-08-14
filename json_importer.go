package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// JSONImporter JSON文件导入器
type JSONImporter struct {
	db     *Database
	config *Config
}

// NewJSONImporter 创建新的JSON导入器
func NewJSONImporter(db *Database, config *Config) *JSONImporter {
	return &JSONImporter{
		db:     db,
		config: config,
	}
}

// FileInfo 文件信息结构
type FileInfo struct {
	Path     string
	Name     string
	RtpLevel int
	TestNum  int
	SortKey  string // 用于排序的键
}

// ImportAllFiles 导入所有JSON文件
func (ji *JSONImporter) ImportAllFiles(fileLevelId string) error {
	// 读取目录：按游戏ID分目录，例如 output/93
	outputDir := filepath.Join("output", fmt.Sprintf("%d", ji.config.Game.ID))
	fmt.Printf("📂 导入目录: %s\n", outputDir)

	// 获取所有JSON文件
	files, err := ji.getJSONFiles(outputDir)
	if err != nil {
		return fmt.Errorf("获取JSON文件失败: %v", err)
	}

	if len(files) == 0 {
		return fmt.Errorf("在 %s 目录下没有找到JSON文件", outputDir)
	}

	// 如果指定了fileLevelId，则过滤文件
	if fileLevelId != "" {
		filteredFiles := ji.filterFilesByFileLevelId(files, fileLevelId)
		if len(filteredFiles) == 0 {
			fmt.Printf("❌ 未找到fileLevelId为 %s 的JSON文件\n", fileLevelId)
			fmt.Printf("💡 当前目录包含以下文件:\n")
			for _, file := range files {
				fmt.Printf("   - %s\n", file.Name)
			}
			return fmt.Errorf("未找到匹配的文件")
		}
		files = filteredFiles
		fmt.Printf("✅ 过滤后找到 %d 个匹配的文件\n", len(filteredFiles))
	}

	// 按文件名排序
	sort.Slice(files, func(i, j int) bool {
		return files[i].SortKey < files[j].SortKey
	})

	fmt.Printf("📁 找到 %d 个JSON文件，按顺序处理:\n", len(files))
	for _, file := range files {
		fmt.Printf("  - %s (排序键: %s)\n", file.Name, file.SortKey)
	}

	// 创建目标表
	tableName := fmt.Sprintf("%s%d", ji.config.Tables.OutputTablePrefix, ji.config.Game.ID)
	if err := ji.createTargetTable(tableName); err != nil {
		return fmt.Errorf("创建目标表失败: %v", err)
	}

	// 逐个导入文件
	for _, file := range files {
		fmt.Printf("\n🔄 正在导入文件: %s\n", file.Name)
		if err := ji.importFile(file, tableName); err != nil {
			return fmt.Errorf("导入文件 %s 失败: %v", file.Name, err)
		}
		fmt.Printf("✅ 文件 %s 导入完成\n", file.Name)
	}

	fmt.Printf("\n🎉 所有文件导入完成！\n")
	return nil
}

// ImportAllFilesWithGameId 支持指定 gameId 与 level 过滤
func (ji *JSONImporter) ImportAllFilesWithGameId(gameId int, levelFilter string) error {
	outputDir := filepath.Join("output", fmt.Sprintf("%d", gameId))
	fmt.Printf("📂 导入目录: %s\n", outputDir)

	files, err := ji.getJSONFiles(outputDir)
	if err != nil {
		return fmt.Errorf("获取JSON文件失败: %v", err)
	}
	if len(files) == 0 {
		return fmt.Errorf("在 %s 目录下没有找到JSON文件", outputDir)
	}

	if levelFilter != "" {
		var filtered []FileInfo
		prefix := fmt.Sprintf("GameResults_%s_", levelFilter)
		for _, f := range files {
			if strings.HasPrefix(f.Name, prefix) {
				filtered = append(filtered, f)
			}
		}
		if len(filtered) == 0 {
			fmt.Printf("❌ 未找到fileLevelId为 %s 的JSON文件\n", levelFilter)
			for _, f := range files {
				fmt.Printf("   - %s\n", f.Name)
			}
			return fmt.Errorf("未找到匹配的文件")
		}
		files = filtered
		fmt.Printf("✅ 过滤后找到 %d 个匹配的文件\n", len(filtered))
	}

	sort.Slice(files, func(i, j int) bool { return files[i].SortKey < files[j].SortKey })
	fmt.Printf("📁 找到 %d 个JSON文件，按顺序处理:\n", len(files))
	for _, f := range files {
		fmt.Printf("  - %s (排序键: %s)\n", f.Name, f.SortKey)
	}

	tableName := fmt.Sprintf("%s%d", ji.config.Tables.OutputTablePrefix, gameId)
	if err := ji.createTargetTable(tableName); err != nil {
		return fmt.Errorf("创建目标表失败: %v", err)
	}
	for _, f := range files {
		fmt.Printf("\n🔄 正在导入文件: %s\n", f.Name)
		if err := ji.importFile(f, tableName); err != nil {
			return fmt.Errorf("导入文件 %s 失败: %v", f.Name, err)
		}
		fmt.Printf("✅ 文件 %s 导入完成\n", f.Name)
	}
	fmt.Printf("\n🎉 所有文件导入完成！\n")
	return nil
}

// filterFilesByFileLevelId 根据fileLevelId过滤文件
func (ji *JSONImporter) filterFilesByFileLevelId(files []FileInfo, fileLevelId string) []FileInfo {
	var filteredFiles []FileInfo
	prefix := fmt.Sprintf("GameResults_%s_", fileLevelId)

	for _, file := range files {
		if strings.HasPrefix(file.Name, prefix) {
			filteredFiles = append(filteredFiles, file)
		}
	}

	return filteredFiles
}

// getJSONFiles 获取指定目录下的所有JSON文件
func (ji *JSONImporter) getJSONFiles(dir string) ([]FileInfo, error) {
	var files []FileInfo

	// 遍历目录
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// 只处理文件，不处理目录
		if d.IsDir() {
			return nil
		}

		// 检查是否是JSON文件
		if !strings.HasSuffix(strings.ToLower(d.Name()), ".json") {
			return nil
		}

		// 解析文件名：GameResults_15_1.json -> RtpLevel=15, TestNum=1
		re := regexp.MustCompile(`GameResults_(\d+)_(\d+)\.json`)
		matches := re.FindStringSubmatch(d.Name())
		if len(matches) != 3 {
			log.Printf("⚠️ 跳过不符合命名规则的文件: %s", d.Name())
			return nil
		}

		rtpLevel, _ := strconv.Atoi(matches[1])
		testNum, _ := strconv.Atoi(matches[2])

		// 创建排序键，确保正确的处理顺序
		sortKey := fmt.Sprintf("%02d_%02d", rtpLevel, testNum)

		fileInfo := FileInfo{
			Path:     path,
			Name:     d.Name(),
			RtpLevel: rtpLevel,
			TestNum:  testNum,
			SortKey:  sortKey,
		}

		files = append(files, fileInfo)
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("遍历目录失败: %v", err)
	}

	return files, nil
}

// createTargetTable 创建目标数据表
func (ji *JSONImporter) createTargetTable(tableName string) error {
	query := fmt.Sprintf(`
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

	// 执行创建表语句
	_, err := ji.db.DB.Exec(query)
	if err != nil {
		return fmt.Errorf("创建表失败: %v", err)
	}

	// 创建索引
	indexQueries := []string{
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s_rtpLevel_idx" ON "%s" ("rtpLevel")`, tableName, tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s_srNumber_idx" ON "%s" ("srNumber")`, tableName, tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s_srId_idx" ON "%s" ("srId")`, tableName, tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s_rtpLevel_srNumber_idx" ON "%s" ("rtpLevel", "srNumber")`, tableName, tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s_rtpLevel_srNumber_srId_idx" ON "%s" ("rtpLevel", "srNumber", "srId")`, tableName, tableName),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s_detail_gin_idx" ON "%s" USING GIN ("detail")`, tableName, tableName),
	}

	for _, indexSQL := range indexQueries {
		_, err := ji.db.DB.Exec(indexSQL)
		if err != nil {
			return fmt.Errorf("创建索引失败: %v", err)
		}
	}

	log.Printf("✅ 成功创建目标表: %s", tableName)
	return nil
}

// importFile 流式导入JSON文件
func (ji *JSONImporter) importFile(file FileInfo, tableName string) error {
	// 打开文件
	fileHandle, err := os.Open(file.Path)
	if err != nil {
		return fmt.Errorf("打开JSON文件失败: %v", err)
	}
	defer fileHandle.Close()

	// 读取文件头部信息（rtpLevel, srNumber, metadata等）
	header, err := ji.readFileHeader(file.Path)
	if err != nil {
		return fmt.Errorf("读取文件头部失败: %v", err)
	}

	fmt.Printf("  📊 文件包含 %d 条记录\n", header.totalRecords)

	// 流式处理数据
	batchSize := ji.config.Settings.BatchSize
	var batch []map[string]interface{}
	batchCount := 0
	totalProcessed := 0

	// 跳过到数据数组的开始位置，并返回新的读取器
	newReader, err := ji.skipToDataArray(fileHandle)
	if err != nil {
		return fmt.Errorf("定位数据数组失败: %v", err)
	}

	// 开始流式解析
	decoder := json.NewDecoder(newReader)

	// 读取数组开始标记 '['
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("读取数组开始标记失败: %v", err)
	}
	if token != json.Delim('[') {
		return fmt.Errorf("期望数组开始标记 '['，但得到 %v", token)
	}

	// 逐条读取数据
	for decoder.More() {
		var item map[string]interface{}
		if err := decoder.Decode(&item); err != nil {
			return fmt.Errorf("解析数据项失败: %v", err)
		}

		batch = append(batch, item)
		totalProcessed++

		// 当批次满了或者到达文件末尾时，插入数据库
		if len(batch) >= batchSize {
			batchCount++
			fmt.Printf("  🔄 处理批次 %d (记录 %d-%d)\n", batchCount, totalProcessed-len(batch)+1, totalProcessed)
			fmt.Print("导入档位: ", file.RtpLevel)
			if err := ji.insertBatch(batch, tableName, file.RtpLevel, file.TestNum, batchCount); err != nil {
				return fmt.Errorf("插入批次 %d 失败: %v", batchCount, err)
			}

			// 清空批次
			batch = batch[:0]
		}
	}

	// 处理最后一批（可能不足batchSize）
	if len(batch) > 0 {
		batchCount++
		fmt.Printf("  🔄 处理最后批次 %d (记录 %d-%d)\n", batchCount, totalProcessed-len(batch)+1, totalProcessed)

		if err := ji.insertBatch(batch, tableName, file.RtpLevel, file.TestNum, batchCount); err != nil {
			return fmt.Errorf("插入最后批次失败: %v", err)
		}
	}

	// 读取数组结束标记 ']'
	token, err = decoder.Token()
	if err != nil {
		return fmt.Errorf("读取数组结束标记失败: %v", err)
	}
	if token != json.Delim(']') {
		return fmt.Errorf("期望数组结束标记 ']'，但得到 %v", token)
	}

	fmt.Printf("  ✅ 总共处理 %d 条记录，分 %d 批次\n", totalProcessed, batchCount)
	return nil
}

// FileHeader 文件头部信息
type FileHeader struct {
	rtpLevel     int
	srNumber     int
	totalRecords int
}

// readFileHeader 读取文件头部信息
func (ji *JSONImporter) readFileHeader(filePath string) (*FileHeader, error) {
	// 打开文件来读取头部
	fileHandle, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer fileHandle.Close()

	// 创建新的读取器
	headerReader := bufio.NewReader(fileHandle)

	// 逐行读取，找到rtpLevel, srNumber等信息
	var header FileHeader
	var line string

	for {
		line, err = headerReader.ReadString('\n')
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)

		// 查找rtpLevel
		if strings.Contains(line, `"rtpLevel"`) {
			if idx := strings.Index(line, ":"); idx != -1 {
				valueStr := strings.TrimSpace(line[idx+1:])
				valueStr = strings.TrimRight(valueStr, ",")
				if value, err := strconv.Atoi(valueStr); err == nil {
					header.rtpLevel = value
				}
			}
		}

		// 查找srNumber
		if strings.Contains(line, `"srNumber"`) {
			if idx := strings.Index(line, ":"); idx != -1 {
				valueStr := strings.TrimSpace(line[idx+1:])
				valueStr = strings.TrimRight(valueStr, ",")
				if value, err := strconv.Atoi(valueStr); err == nil {
					header.srNumber = value
				}
			}
		}

		// 如果找到了数据数组的开始，停止读取头部
		if strings.Contains(line, `"data"`) && strings.Contains(line, "[") {
			break
		}
	}

	// 估算总记录数（通过计算文件大小和平均行长度）
	fileInfo, err := fileHandle.Stat()
	if err == nil {
		// 简单估算：假设每条记录平均200字节
		header.totalRecords = int(fileInfo.Size() / 200)
	}

	return &header, nil
}

// skipToDataArray 跳过到数据数组的开始位置
func (ji *JSONImporter) skipToDataArray(file *os.File) (*bufio.Reader, error) {
	// 从文件头开始扫描，定位到 data 数组的 '[' 字符处
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	r := bufio.NewReader(file)
	var offset int64 = 0
	for {
		line, err := r.ReadString('\n')
		if len(line) > 0 {
			if strings.Contains(line, `"data"`) && strings.Contains(line, "[") {
				idx := strings.Index(line, "[")
				if idx >= 0 {
					// 将文件指针定位到 '[' 处
					if _, err := file.Seek(offset+int64(idx), io.SeekStart); err != nil {
						return nil, err
					}
					return bufio.NewReader(file), nil
				}
			}
			offset += int64(len(line))
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}
	return nil, fmt.Errorf("未找到 data 数组的起始位置")
}

// insertBatch 批量插入数据
func (ji *JSONImporter) insertBatch(data []map[string]interface{}, tableName string, rtpLevel, testNum int, batchNum int) error {
	if len(data) == 0 {
		return nil
	}

	// 显示当前批次进度
	fmt.Printf("    🔄 正在处理第 %d 批数据 (%d 条记录)...\n", batchNum, len(data))

	// 开始事务
	tx, err := ji.db.DB.Begin()
	if err != nil {
		return fmt.Errorf("开始事务失败: %v", err)
	}
	defer tx.Rollback()

	// 准备插入语句
	query := fmt.Sprintf(`
		INSERT INTO "%s" ("rtpLevel", "srNumber", "srId", "bet", "win", "detail")
		VALUES ($1, $2, $3, $4, $5, $6)
	`, tableName)

	stmt, err := tx.Prepare(query)
	if err != nil {
		return fmt.Errorf("准备语句失败: %v", err)
	}
	defer stmt.Close()

	// 计算投注金额
	bet := ji.config.Bet.CS * ji.config.Bet.ML * ji.config.Bet.BL

	// 批量插入数据
	for i, item := range data {
		// 将gd字段转换为JSON字符串以适配JSONB类型
		var detailVal interface{}
		if item["gd"] != nil {
			// 将gd字段转换为JSON字符串
			gdJSON, err := json.Marshal(item["gd"])
			if err != nil {
				return fmt.Errorf("序列化gd字段失败: %v", err)
			}
			detailVal = string(gdJSON)
		}

		// 精度修正：将win字段四舍五入到2位小数
		var winValue float64
		if aw, ok := item["aw"].(float64); ok {
			// 四舍五入到2位小数，避免浮点数精度问题
			winValue = math.Round(aw*100) / 100
		} else {
			winValue = 0.0
		}
		rtpLevelVal := float64(rtpLevel)
		_, err := stmt.Exec(
			rtpLevelVal, // rtpLevel
			testNum,     // srNumber
			i+1,         // srId (从1开始)
			bet,         // bet
			winValue,    // win (精度修正后)
			detailVal,   // detail (JSONB)
		)
		if err != nil {
			return fmt.Errorf("插入记录 %d 失败: %v", i+1, err)
		}
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交事务失败: %v", err)
	}

	fmt.Printf("    ✅ 第 %d 批数据处理完成\n", batchNum)
	return nil
}
