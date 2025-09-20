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
	"sync"
	"time"
)

// JSONImporter JSON文件导入器
type JSONImporter struct {
	db     *Database
	config *Config
}

// S3Importer S3文件导入器
type S3Importer struct {
	db       *Database
	config   *Config
	s3Client *S3Client
}

// NewJSONImporter 创建新的JSON导入器
func NewJSONImporter(db *Database, config *Config) *JSONImporter {
	return &JSONImporter{
		db:     db,
		config: config,
	}
}

// NewS3Importer 创建新的S3导入器
func NewS3Importer(db *Database, config *Config) (*S3Importer, error) {
	s3Client, err := NewS3Client(config)
	if err != nil {
		return nil, err
	}

	return &S3Importer{
		db:       db,
		config:   config,
		s3Client: s3Client,
	}, nil
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
	// bet := ji.config.Bet.CS * ji.config.Bet.ML * ji.config.Bet.BL

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

		var totalBet float64
		if aw, ok := item["tb"].(float64); ok {
			// 四舍五入到2位小数，避免浮点数精度问题
			totalBet = math.Round(aw*100) / 100
		} else {
			totalBet = 0.0
		}
		rtpLevelVal := float64(rtpLevel)
		_, err := stmt.Exec(
			rtpLevelVal, // rtpLevel
			testNum,     // srNumber
			i+1,         // srId (从1开始)
			totalBet,    // bet
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

// ImportS3Files 从S3导入多个游戏的文件
func (si *S3Importer) ImportS3Files(gameIDs []int, mode string, levelFilter string) error {
	// 记录总开始时间
	totalStartTime := time.Now()
	fmt.Printf("🔄 启动S3导入模式 (游戏IDs: %v, 模式: %s)\n", gameIDs, mode)

	var allFiles []S3FileInfo
	var err error

	if mode == "auto" {
		// 智能模式：自动检测每个游戏的模式
		allFiles, err = si.importS3FilesAutoMode(gameIDs, levelFilter)
		if err != nil {
			return err
		}
	} else {
		// 传统模式：指定模式
		allFiles, err = si.s3Client.ListS3Files(gameIDs, mode)
		if err != nil {
			return fmt.Errorf("列出S3文件失败: %v", err)
		}
	}

	if len(allFiles) == 0 {
		return fmt.Errorf("在S3中未找到匹配的文件")
	}

	// 如果指定了levelFilter，则过滤文件
	if levelFilter != "" {
		filteredFiles := si.filterS3FilesByLevel(allFiles, levelFilter)
		if len(filteredFiles) == 0 {
			fmt.Printf("❌ 未找到level为 %s 的S3文件\n", levelFilter)
			fmt.Printf("💡 当前S3包含以下文件:\n")
			for _, file := range allFiles {
				fmt.Printf("   - %s (RTP等级: %d)\n", file.Key, file.RtpLevel)
			}
			return fmt.Errorf("未找到匹配的文件")
		}
		allFiles = filteredFiles
		fmt.Printf("✅ 过滤后找到 %d 个匹配的S3文件\n", len(filteredFiles))
	}

	// 按游戏ID和RTP等级排序
	sort.Slice(allFiles, func(i, j int) bool {
		if allFiles[i].GameID != allFiles[j].GameID {
			return allFiles[i].GameID < allFiles[j].GameID
		}
		if allFiles[i].RtpLevel != allFiles[j].RtpLevel {
			return allFiles[i].RtpLevel < allFiles[j].RtpLevel
		}
		return allFiles[i].TestNum < allFiles[j].TestNum
	})

	fmt.Printf("📁 找到 %d 个S3文件，按顺序处理:\n", len(allFiles))
	for _, file := range allFiles {
		fmt.Printf("  - 游戏%d | %s | RTP等级: %d | 测试: %d\n",
			file.GameID, file.Key, file.RtpLevel, file.TestNum)
	}

	// 按游戏ID分组处理
	gameGroups := make(map[int][]S3FileInfo)
	for _, file := range allFiles {
		gameGroups[file.GameID] = append(gameGroups[file.GameID], file)
	}

	// 并行处理不同游戏，但同一游戏内部串行
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errors []error
	gameCount := len(gameGroups)

	fmt.Printf("🚀 开始并行处理 %d 个游戏\n", gameCount)

	for gameID, gameFiles := range gameGroups {
		wg.Add(1)
		go func(gid int, files []S3FileInfo) {
			defer wg.Done()

			gameStartTime := time.Now()
			fmt.Printf("\n🎯 [游戏%d] 开始处理，共 %d 个文件\n", gid, len(files))

			// 创建目标表
			tableName := fmt.Sprintf("%s%d", si.config.Tables.OutputTablePrefix, gid)
			if err := si.createS3TargetTable(tableName); err != nil {
				mu.Lock()
				errors = append(errors, fmt.Errorf("游戏 %d 创建目标表失败: %v", gid, err))
				mu.Unlock()
				fmt.Printf("❌ [游戏%d] 创建目标表失败: %v\n", gid, err)
				return
			}

			// 使用串行流式处理导入文件（避免同一游戏文件的数据库锁冲突）
			if err := si.importS3FilesSequentialStream(files, tableName); err != nil {
				mu.Lock()
				errors = append(errors, fmt.Errorf("游戏 %d 文件导入失败: %v", gid, err))
				mu.Unlock()
				fmt.Printf("❌ [游戏%d] 文件导入失败: %v\n", gid, err)
				return
			}

			gameDuration := time.Since(gameStartTime)
			fmt.Printf("✅ [游戏%d] 所有文件导入完成！(耗时: %v)\n", gid, gameDuration)
		}(gameID, gameFiles)
	}

	// 等待所有游戏处理完成
	wg.Wait()

	// 检查是否有错误
	if len(errors) > 0 {
		return fmt.Errorf("部分游戏导入失败: %v", errors)
	}

	// 计算并显示总耗时
	totalDuration := time.Since(totalStartTime)
	fmt.Printf("\n🎉 所有S3文件导入完成！\n")
	fmt.Printf("⏱️  S3导入总耗时: %v\n", totalDuration)
	return nil
}

// importS3FilesAutoMode 智能模式：自动检测每个游戏的模式并导入
func (si *S3Importer) importS3FilesAutoMode(gameIDs []int, levelFilter string) ([]S3FileInfo, error) {
	var allFiles []S3FileInfo

	for _, gameID := range gameIDs {
		fmt.Printf("🔍 检查游戏 %d 的模式...\n", gameID)

		// 检查游戏有哪些模式
		hasNormal, hasFb, err := si.s3Client.CheckGameModes(gameID)
		if err != nil {
			return nil, fmt.Errorf("检查游戏 %d 模式失败: %v", gameID, err)
		}

		if !hasNormal && !hasFb {
			fmt.Printf("⚠️  游戏 %d 没有找到任何模式的文件\n", gameID)
			continue
		}

		// 先导入normal模式（如果存在）
		if hasNormal {
			fmt.Printf("📁 游戏 %d 发现 normal 模式文件，开始导入...\n", gameID)
			normalFiles, err := si.s3Client.ListS3Files([]int{gameID}, "normal")
			if err != nil {
				return nil, fmt.Errorf("列出游戏 %d normal模式文件失败: %v", gameID, err)
			}
			allFiles = append(allFiles, normalFiles...)
		}

		// 再导入fb模式（如果存在）
		if hasFb {
			fmt.Printf("📁 游戏 %d 发现 fb 模式文件，开始导入...\n", gameID)
			fbFiles, err := si.s3Client.ListS3Files([]int{gameID}, "fb")
			if err != nil {
				return nil, fmt.Errorf("列出游戏 %d fb模式文件失败: %v", gameID, err)
			}
			allFiles = append(allFiles, fbFiles...)
		}

		// 显示游戏模式总结
		if hasNormal && hasFb {
			fmt.Printf("✅ 游戏 %d 完成：normal + fb 模式\n", gameID)
		} else if hasNormal {
			fmt.Printf("✅ 游戏 %d 完成：normal 模式\n", gameID)
		} else {
			fmt.Printf("✅ 游戏 %d 完成：fb 模式\n", gameID)
		}
	}

	return allFiles, nil
}

// filterS3FilesByLevel 根据level过滤S3文件
func (si *S3Importer) filterS3FilesByLevel(files []S3FileInfo, levelFilter string) []S3FileInfo {
	var filteredFiles []S3FileInfo
	level, err := strconv.Atoi(levelFilter)
	if err != nil {
		return filteredFiles
	}

	for _, file := range files {
		if file.RtpLevel == level {
			filteredFiles = append(filteredFiles, file)
		}
	}

	return filteredFiles
}

// importS3File 导入单个S3文件
func (si *S3Importer) importS3File(file S3FileInfo, tableName string) error {
	// 下载S3文件内容
	content, err := si.s3Client.DownloadS3File(file.Key)
	if err != nil {
		return fmt.Errorf("下载S3文件失败: %v", err)
	}

	// 解析JSON数据
	var gameData []GameResultData
	if err := json.Unmarshal(content, &gameData); err != nil {
		return fmt.Errorf("解析JSON数据失败: %v", err)
	}

	// 批量插入数据库
	return si.insertBatch(gameData, tableName, file.RtpLevel, file.TestNum)
}

// importS3FileStream 流式导入单个S3文件
func (si *S3Importer) importS3FileStream(file S3FileInfo, tableName string) error {
	// 获取S3对象流
	result, err := si.s3Client.GetObjectStream(file.Key)
	if err != nil {
		return fmt.Errorf("获取S3对象流失败: %v", err)
	}
	defer result.Body.Close()

	// 流式JSON解析
	decoder := json.NewDecoder(result.Body)

	// 优化批处理大小 - 根据文件大小动态调整
	batchSize := si.calculateOptimalBatchSize(file.Size)
	batch := make([]map[string]interface{}, 0, batchSize)
	batchCount := 0
	totalRecords := 0
	globalSrId := 0 // 全局srId计数器，确保整个文件内连续

	fmt.Printf("📊 文件 %s: 大小=%.2fMB, 批次大小=%d\n",
		file.Key, float64(file.Size)/(1024*1024), batchSize)

	// 流式解析JSON文件结构：{"rtpLevel": 200, "srNumber": 1, "data": [...]}
	var rtpLevel int
	var srNumber int

	// 解析文件头部信息
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("解析JSON token失败: %v", err)
		}

		if key, ok := token.(string); ok {
			switch key {
			case "rtpLevel":
				if err := decoder.Decode(&rtpLevel); err != nil {
					return fmt.Errorf("解析rtpLevel失败: %v", err)
				}
			case "srNumber":
				if err := decoder.Decode(&srNumber); err != nil {
					return fmt.Errorf("解析srNumber失败: %v", err)
				}
			case "data":
				// 进入数据数组
				token, err := decoder.Token()
				if err != nil {
					return fmt.Errorf("读取data数组开始标记失败: %v", err)
				}
				if delim, ok := token.(json.Delim); !ok || delim != '[' {
					return fmt.Errorf("期望数组开始标记 '['，但得到 %v", token)
				}

				fmt.Printf("📊 S3文件信息: RTP等级=%d, 测试编号=%d, 开始流式处理数据\n",
					rtpLevel, srNumber)

				// 流式处理数据数组
				for decoder.More() {
					var item map[string]interface{}
					if err := decoder.Decode(&item); err != nil {
						return fmt.Errorf("解析数据项失败: %v", err)
					}

					batch = append(batch, item)
					totalRecords++

					// 达到批次大小时插入数据库
					if len(batch) >= batchSize {
						batchCount++
						fmt.Printf("  🔄 处理批次 %d (记录 %d-%d)\n", batchCount, totalRecords-len(batch)+1, totalRecords)
						if err := si.insertS3Batch(batch, tableName, rtpLevel, srNumber, batchCount, file.Mode, &globalSrId); err != nil {
							return fmt.Errorf("批量插入失败: %v", err)
						}
						batch = batch[:0] // 清空批次
					}
				}

				// 读取数组结束标记
				token, err = decoder.Token()
				if err != nil {
					return fmt.Errorf("读取数组结束标记失败: %v", err)
				}
				if delim, ok := token.(json.Delim); !ok || delim != ']' {
					return fmt.Errorf("期望数组结束标记 ']'，但得到 %v", token)
				}
				break
			}
		}
	}

	// 插入剩余数据
	if len(batch) > 0 {
		batchCount++
		fmt.Printf("  🔄 处理最后批次 %d (记录 %d-%d)\n", batchCount, totalRecords-len(batch)+1, totalRecords)
		if err := si.insertS3Batch(batch, tableName, rtpLevel, srNumber, batchCount, file.Mode, &globalSrId); err != nil {
			return fmt.Errorf("批量插入剩余数据失败: %v", err)
		}
	}

	fmt.Printf("  ✅ 总共处理 %d 条记录，分 %d 批次\n", totalRecords, batchCount)
	return nil
}

// insertBatch 批量插入数据到数据库 - 优化版本
func (si *S3Importer) insertBatch(data []GameResultData, tableName string, rtpLevel int, testNum int) error {
	if len(data) == 0 {
		return nil
	}

	// 使用批量插入SQL - 优化版本
	query := fmt.Sprintf(`
		INSERT INTO "%s" (rtpLevel, srNumber, srId, bet, win, detail) 
		VALUES %s
	`, tableName, si.generatePlaceholders(len(data)))

	// 准备参数
	args := make([]interface{}, 0, len(data)*6)
	for i, item := range data {
		args = append(args, rtpLevel, testNum, i+1, item.TB, item.AW, item.GD)
	}

	// 开始事务 - 使用带重试机制的事务开始
	tx, err := si.db.BeginWithRetry()
	if err != nil {
		return fmt.Errorf("开始事务失败: %v", err)
	}
	defer tx.Rollback()

	// 执行批量插入
	_, err = tx.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("批量插入失败: %v", err)
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交事务失败: %v", err)
	}

	return nil
}

// insertS3Batch 批量插入S3数据到数据库
func (si *S3Importer) insertS3Batch(data []map[string]interface{}, tableName string, rtpLevel int, testNum int, batchNum int, mode string, globalSrId *int) error {
	if len(data) == 0 {
		return nil
	}

	// 显示当前批次进度
	fmt.Printf("    🔄 正在处理第 %d 批数据 (%d 条记录)...\n", batchNum, len(data))

	// 开始事务
	tx, err := si.db.BeginWithRetry()
	if err != nil {
		return fmt.Errorf("开始事务失败: %v", err)
	}
	defer tx.Rollback()

	// 准备批量插入数据
	values := make([]string, 0, len(data))
	args := make([]interface{}, 0, len(data)*6)
	argIndex := 1

	for _, item := range data {
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

		var totalBet float64
		if tb, ok := item["tb"].(float64); ok {
			// 四舍五入到2位小数，避免浮点数精度问题
			totalBet = math.Round(tb*100) / 100
		} else {
			totalBet = 0.0
		}

		// 根据模式处理rtpLevel：fb模式需要+0.1
		rtpLevelVal := float64(rtpLevel)
		if mode == "fb" {
			rtpLevelVal = float64(rtpLevel) + 0.1
		}

		*globalSrId++ // 递增全局srId

		// 构建VALUES子句
		values = append(values, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)",
			argIndex, argIndex+1, argIndex+2, argIndex+3, argIndex+4, argIndex+5))

		// 添加参数
		args = append(args, rtpLevelVal, testNum, *globalSrId, totalBet, winValue, detailVal)
		argIndex += 6
	}

	// 构建批量插入SQL
	query := fmt.Sprintf(`
		INSERT INTO "%s" ("rtpLevel", "srNumber", "srId", "bet", "win", "detail")
		VALUES %s
	`, tableName, strings.Join(values, ", "))

	// 执行批量插入
	_, err = tx.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("批量插入失败: %v", err)
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交事务失败: %v", err)
	}

	fmt.Printf("    ✅ 第 %d 批数据处理完成\n", batchNum)
	return nil
}

// generatePlaceholders 生成占位符字符串
func (si *S3Importer) generatePlaceholders(count int) string {
	if count <= 0 {
		return ""
	}

	// 生成 (?, ?, ?, ?, ?, ?) 格式的占位符
	placeholder := "($1, $2, $3, $4, $5, $6)"
	result := placeholder

	for i := 1; i < count; i++ {
		offset := i * 6
		result += fmt.Sprintf(", ($%d, $%d, $%d, $%d, $%d, $%d)",
			offset+1, offset+2, offset+3, offset+4, offset+5, offset+6)
	}

	return result
}

// importS3FilesConcurrentStream 并发流式导入S3文件 - 优化版本
func (si *S3Importer) importS3FilesConcurrentStream(files []S3FileInfo, tableName string) error {
	// 动态调整并发数量 - 优化策略
	maxConcurrency := si.calculateOptimalConcurrency(len(files))

	// 创建信号量控制并发
	semaphore := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errors []error
	var successCount int
	var totalProcessed int64
	var totalBytes int64
	startTime := time.Now()

	fmt.Printf("🚀 开始并发流式处理 %d 个文件，最大并发数: %d\n", len(files), maxConcurrency)

	for i, file := range files {
		wg.Add(1)
		go func(index int, f S3FileInfo) {
			defer wg.Done()

			// 获取信号量
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			fmt.Printf("🔄 [%d/%d] 开始处理文件: %s (大小: %.2fMB)\n",
				index+1, len(files), f.Key, float64(f.Size)/(1024*1024))
			startTime := time.Now()

			// 流式处理单个文件
			if err := si.importS3FileStream(f, tableName); err != nil {
				mu.Lock()
				errors = append(errors, fmt.Errorf("文件 %s 处理失败: %v", f.Key, err))
				mu.Unlock()
				fmt.Printf("❌ [%d/%d] 文件处理失败: %s - %v\n", index+1, len(files), f.Key, err)
			} else {
				mu.Lock()
				successCount++
				totalProcessed++
				totalBytes += f.Size
				mu.Unlock()
				duration := time.Since(startTime)
				rate := float64(f.Size) / (1024 * 1024) / duration.Seconds()
				fmt.Printf("✅ [%d/%d] 文件处理完成: %s (耗时: %v, 速度: %.2fMB/s)\n",
					index+1, len(files), f.Key, duration, rate)

				// 定期检查连接健康状态和续期
				if successCount%10 == 0 {
					if err := si.db.CheckConnectionHealth(); err != nil {
						fmt.Printf("⚠️ 连接健康检查失败: %v\n", err)
					}
				}

				// 每处理50个文件延长一次连接生存时间
				if successCount%50 == 0 {
					if err := si.db.ExtendConnection(); err != nil {
						fmt.Printf("⚠️ 连接续期失败: %v\n", err)
					}
				}
			}
		}(i, file)
	}

	wg.Wait()

	// 输出处理结果
	totalDuration := time.Since(startTime)
	avgSpeed := float64(totalBytes) / (1024 * 1024) / totalDuration.Seconds()

	fmt.Printf("\n📊 处理结果统计:\n")
	fmt.Printf("  - 总文件数: %d\n", len(files))
	fmt.Printf("  - 成功处理: %d\n", successCount)
	fmt.Printf("  - 处理失败: %d\n", len(errors))
	fmt.Printf("  - 总耗时: %v\n", totalDuration)
	fmt.Printf("  - 总数据量: %.2fMB\n", float64(totalBytes)/(1024*1024))
	fmt.Printf("  - 平均速度: %.2fMB/s\n", avgSpeed)
	fmt.Printf("  - 平均每文件: %v\n", totalDuration/time.Duration(len(files)))

	// 长时间导入警告
	if totalDuration > 30*time.Minute {
		fmt.Printf("⚠️ 导入时间超过30分钟，建议检查连接配置\n")
	}
	if totalDuration > 60*time.Minute {
		fmt.Printf("⚠️ 导入时间超过1小时，建议优化导入策略\n")
	}

	if len(errors) > 0 {
		fmt.Printf("\n❌ 错误详情:\n")
		for i, err := range errors {
			fmt.Printf("  %d. %v\n", i+1, err)
		}
		return fmt.Errorf("处理过程中出现 %d 个错误", len(errors))
	}

	fmt.Printf("🎉 所有文件处理完成！\n")
	return nil
}

// calculateOptimalConcurrency 计算最优并发数
func (si *S3Importer) calculateOptimalConcurrency(fileCount int) int {
	// 基础并发数
	baseConcurrency := 5

	// 根据文件数量动态调整
	if fileCount <= 10 {
		return min(baseConcurrency, fileCount)
	} else if fileCount <= 50 {
		return min(8, fileCount)
	} else if fileCount <= 100 {
		return min(12, fileCount)
	} else {
		return min(16, fileCount) // 最多16个并发
	}
}

// min 返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// max 返回两个整数中的较大值
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// calculateOptimalBatchSize 计算最优批处理大小
func (si *S3Importer) calculateOptimalBatchSize(fileSize int64) int {
	// 根据文件大小动态调整，平衡内存使用和性能
	if fileSize < 10*1024*1024 { // < 10MB
		return 5000
	} else if fileSize < 30*1024*1024 { // < 30MB
		return 2000
	} else if fileSize < 50*1024*1024 { // < 50MB
		return 1000
	} else if fileSize < 100*1024*1024 { // < 100MB
		return 500
	} else { // >= 100MB
		return 200 // 超大文件使用更小批次
	}
}

// createS3TargetTable 创建S3导入的目标数据表
func (si *S3Importer) createS3TargetTable(tableName string) error {
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
	_, err := si.db.DB.Exec(query)
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
		_, err := si.db.DB.Exec(indexSQL)
		if err != nil {
			return fmt.Errorf("创建索引失败: %v", err)
		}
	}

	fmt.Printf("✅ 成功创建S3目标表: %s", tableName)
	return nil
}

// importS3FilesSequentialStream 串行流式导入S3文件 - 避免同一游戏文件的数据库锁冲突
func (si *S3Importer) importS3FilesSequentialStream(files []S3FileInfo, tableName string) error {
	var errors []error
	var successCount int
	var totalProcessed int64
	var totalBytes int64
	startTime := time.Now()

	fmt.Printf("🚀 开始串行流式处理 %d 个文件（避免数据库锁冲突）\n", len(files))

	for i, file := range files {
		fmt.Printf("🔄 [游戏%d-%s: %d/%d] 开始处理文件: %s (大小: %.2fMB)\n",
			file.GameID, file.Mode, i+1, len(files), file.Key, float64(file.Size)/(1024*1024))
		fileStartTime := time.Now()

		// 流式处理单个文件
		if err := si.importS3FileStream(file, tableName); err != nil {
			errors = append(errors, fmt.Errorf("文件 %s 处理失败: %v", file.Key, err))
			fmt.Printf("❌ [游戏%d-%s: %d/%d] 文件处理失败: %s - %v\n", file.GameID, file.Mode, i+1, len(files), file.Key, err)
		} else {
			successCount++
			totalProcessed++
			totalBytes += file.Size
			fileDuration := time.Since(fileStartTime)
			fmt.Printf("✅ [游戏%d-%s: %d/%d] 文件处理完成: %s (耗时: %v)\n",
				file.GameID, file.Mode, i+1, len(files), file.Key, fileDuration)
		}
	}

	// 输出最终统计
	totalDuration := time.Since(startTime)
	fmt.Printf("\n📊 串行处理完成统计:\n")
	fmt.Printf("  - 总文件数: %d\n", len(files))
	fmt.Printf("  - 成功处理: %d\n", successCount)
	fmt.Printf("  - 失败文件: %d\n", len(errors))
	fmt.Printf("  - 总数据量: %.2f MB\n", float64(totalBytes)/(1024*1024))
	fmt.Printf("  - 总耗时: %v\n", totalDuration)
	if len(files) > 0 {
		fmt.Printf("  - 平均速度: %.2f MB/s\n", float64(totalBytes)/(1024*1024)/totalDuration.Seconds())
	}

	// 如果有错误，返回汇总错误信息
	if len(errors) > 0 {
		fmt.Printf("⚠️  部分文件处理失败:\n")
		for i, err := range errors {
			fmt.Printf("   %d. %v\n", i+1, err)
		}
		return fmt.Errorf("处理过程中出现 %d 个错误，详细信息见上方输出", len(errors))
	}

	return nil
}

// ==================== Fb2模式独立实现 ====================

// ImportS3FilesFb2 从S3导入Fb2模式文件（完全独立的实现）
func (si *S3Importer) ImportS3FilesFb2(gameIDs []int, levelFilter string) error {
	// 记录总开始时间
	totalStartTime := time.Now()
	fmt.Printf("🔄 启动S3 Fb2导入模式 (游戏IDs: %v)\n", gameIDs)

	// 获取所有Fb2文件
	allFiles, err := si.getS3Fb2Files(gameIDs, levelFilter)
	if err != nil {
		return err
	}

	if len(allFiles) == 0 {
		return fmt.Errorf("在S3中未找到匹配的Fb2文件")
	}

	// 按游戏ID分组处理
	gameFiles := make(map[int][]S3FileInfoFb2)
	for _, file := range allFiles {
		gameFiles[file.GameID] = append(gameFiles[file.GameID], file)
	}

	// 处理每个游戏
	for gameID, files := range gameFiles {
		fmt.Printf("\n🎯 开始处理游戏 %d 的 %d 个Fb2文件...\n", gameID, len(files))

		// 创建目标表
		tableName := fmt.Sprintf("%s%d", si.config.Tables.OutputTablePrefix, gameID)
		if err := si.createS3TargetTable(tableName); err != nil {
			return fmt.Errorf("创建表 %s 失败: %v", tableName, err)
		}

		// 导入文件
		if err := si.importS3Fb2FilesSequential(files, tableName); err != nil {
			return fmt.Errorf("导入游戏 %d 的Fb2文件失败: %v", gameID, err)
		}

		fmt.Printf("✅ 游戏 %d Fb2文件导入完成！\n", gameID)
	}

	totalDuration := time.Since(totalStartTime)
	fmt.Printf("\n🎉 S3 Fb2导入完成！总耗时: %v\n", totalDuration)
	return nil
}

// S3FileInfoFb2 Fb2模式S3文件信息结构
type S3FileInfoFb2 struct {
	Key          string // S3对象键
	Size         int64  // 文件大小
	LastModified string // 最后修改时间
	GameID       int    // 游戏ID
	FbType       string // Fb类型：fb1, fb2, fb3
	RtpLevel     int    // RTP等级
	TestNum      int    // 测试编号
}

// getS3Fb2Files 获取S3中的Fb2文件
func (si *S3Importer) getS3Fb2Files(gameIDs []int, levelFilter string) ([]S3FileInfoFb2, error) {
	var allFiles []S3FileInfoFb2

	// 遍历每个游戏ID
	for _, gameID := range gameIDs {
		fmt.Printf("🔍 搜索游戏 %d 的Fb2文件...\n", gameID)

		// 构建S3路径：mpg-slot-data/gameID/fb/
		s3Prefix := fmt.Sprintf("mpg-slot-data/%d/fb/", gameID)

		// 列出该路径下的所有文件
		files, err := si.s3Client.ListS3FilesByPrefix(s3Prefix)
		if err != nil {
			fmt.Printf("⚠️ 搜索 %s 失败: %v\n", s3Prefix, err)
			continue
		}

		// 转换为Fb2格式，根据文件名解析fbType
		for _, file := range files {
			// 从文件名中解析fbType：GameResultData_fb1_1_1.json -> fb1
			fbType := si.extractFbTypeFromFileName(file.Key)

			fb2File := S3FileInfoFb2{
				Key:          file.Key,
				Size:         file.Size,
				LastModified: file.LastModified,
				GameID:       gameID,
				FbType:       fbType,
				RtpLevel:     file.RtpLevel,
				TestNum:      file.TestNum,
			}
			allFiles = append(allFiles, fb2File)
		}

		fmt.Printf("  ✅ 找到 %d 个文件\n", len(files))
	}

	// 如果指定了levelFilter，则过滤文件
	if levelFilter != "" {
		allFiles = si.filterS3Fb2FilesByLevel(allFiles, levelFilter)
	}

	fmt.Printf("📊 总共找到 %d 个Fb2文件\n", len(allFiles))
	return allFiles, nil
}

// extractFbTypeFromFileName 从文件名中提取fbType
func (si *S3Importer) extractFbTypeFromFileName(key string) string {
	// 文件名格式：GameResultData_fb1_1_1.json
	fileName := filepath.Base(key)
	parts := strings.Split(fileName, "_")
	if len(parts) >= 2 {
		// 提取fbType：GameResultData_fb1_1_1.json -> fb1
		return parts[1]
	}
	return "fb1" // 默认返回fb1
}

// filterS3Fb2FilesByLevel 根据RTP等级过滤Fb2文件
func (si *S3Importer) filterS3Fb2FilesByLevel(files []S3FileInfoFb2, levelFilter string) []S3FileInfoFb2 {
	level, err := strconv.Atoi(levelFilter)
	if err != nil {
		fmt.Printf("⚠️ 无效的等级过滤条件: %s\n", levelFilter)
		return files
	}

	var filteredFiles []S3FileInfoFb2
	for _, file := range files {
		if file.RtpLevel == level {
			filteredFiles = append(filteredFiles, file)
		}
	}

	fmt.Printf("🔍 等级过滤: 从 %d 个文件筛选出 %d 个文件 (RTP等级: %d)\n",
		len(files), len(filteredFiles), level)
	return filteredFiles
}

// importS3Fb2FilesSequential 串行导入Fb2文件
func (si *S3Importer) importS3Fb2FilesSequential(files []S3FileInfoFb2, tableName string) error {
	globalSrId := 0

	for i, file := range files {
		fmt.Printf("  📁 处理文件 %d/%d: %s\n", i+1, len(files), file.Key)

		if err := si.importS3Fb2File(file, tableName, &globalSrId); err != nil {
			return fmt.Errorf("导入文件 %s 失败: %v", file.Key, err)
		}
	}

	return nil
}

// importS3Fb2File 导入单个Fb2文件
func (si *S3Importer) importS3Fb2File(file S3FileInfoFb2, tableName string, globalSrId *int) error {
	// 从S3下载文件内容
	content, err := si.s3Client.DownloadS3File(file.Key)
	if err != nil {
		return fmt.Errorf("下载S3文件失败: %v", err)
	}

	// 解析JSON文件
	var jsonData struct {
		RtpLevel int                      `json:"rtpLevel"`
		SrNumber int                      `json:"srNumber"`
		Data     []map[string]interface{} `json:"data"`
	}

	if err := json.Unmarshal(content, &jsonData); err != nil {
		return fmt.Errorf("解析JSON失败: %v", err)
	}

	// 批量插入数据
	return si.insertS3Fb2Batch(jsonData.Data, tableName, file.RtpLevel, file.TestNum, file.FbType, globalSrId)
}

// insertS3Fb2Batch 批量插入Fb2数据到数据库
func (si *S3Importer) insertS3Fb2Batch(data []map[string]interface{}, tableName string, rtpLevel int, testNum int, fbType string, globalSrId *int) error {
	if len(data) == 0 {
		return nil
	}

	// 根据fbType计算rtpLevel偏移
	var rtpOffset float64
	switch fbType {
	case "fb1":
		rtpOffset = 0.1
	case "fb2":
		rtpOffset = 0.2
	case "fb3":
		rtpOffset = 0.3
	default:
		rtpOffset = 0.1 // 默认值
	}

	// 开始事务
	tx, err := si.db.BeginWithRetry()
	if err != nil {
		return fmt.Errorf("开始事务失败: %v", err)
	}
	defer tx.Rollback()

	// 准备批量插入
	values := make([]string, 0, len(data))
	args := make([]interface{}, 0, len(data)*6)

	for _, item := range data {
		var totalBet float64
		if tb, ok := item["tb"].(float64); ok {
			totalBet = math.Round(tb*100) / 100
		}

		var totalWin float64
		if aw, ok := item["aw"].(float64); ok {
			totalWin = math.Round(aw*100) / 100
		}

		// 计算调整后的rtpLevel
		rtpLevelVal := float64(rtpLevel) + rtpOffset

		*globalSrId++

		// 构建VALUES子句
		argIndex := len(args) + 1
		values = append(values, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)",
			argIndex, argIndex+1, argIndex+2, argIndex+3, argIndex+4, argIndex+5))

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

		// 准备参数
		args = append(args, rtpLevelVal, testNum, *globalSrId, totalBet, totalWin, detailVal)
	}

	// 执行批量插入
	query := fmt.Sprintf(`
		INSERT INTO "%s" ("rtpLevel", "srNumber", "srId", "bet", "win", "detail")
		VALUES %s
	`, tableName, strings.Join(values, ","))

	_, err = tx.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("批量插入失败: %v", err)
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交事务失败: %v", err)
	}

	rtpLevelVal := float64(rtpLevel) + rtpOffset
	fmt.Printf("    ✅ 成功插入 %d 条记录 (RTP等级: %.1f, Fb类型: %s)\n", len(data), rtpLevelVal, fbType)
	return nil
}

// Fb2FileInfo Fb2文件信息结构
type Fb2FileInfo struct {
	Path     string
	Name     string
	GameID   int
	FbType   string // fb1, fb2, fb3
	RtpLevel int
	TestNum  int
	SortKey  string // 用于排序的键
}

// ImportFb2Files 导入本地Fb2模式文件
func (ji *JSONImporter) ImportFb2Files(gameID int, levelFilter string) error {
	// 记录总开始时间
	totalStartTime := time.Now()
	fmt.Printf("🔄 启动本地Fb2导入模式 (游戏ID: %d)\n", gameID)

	// 获取所有Fb2文件
	allFiles, err := ji.getFb2Files(gameID, levelFilter)
	if err != nil {
		return err
	}

	if len(allFiles) == 0 {
		return fmt.Errorf("在本地目录中未找到匹配的Fb2文件")
	}

	fmt.Printf("📂 找到 %d 个Fb2文件\n", len(allFiles))

	// 创建目标表
	tableName := fmt.Sprintf("%s%d", ji.config.Tables.OutputTablePrefix, gameID)

	// 先删除现有表（如果存在）
	dropQuery := fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, tableName)
	_, err = ji.db.DB.Exec(dropQuery)
	if err != nil {
		fmt.Printf("⚠️ 删除现有表失败: %v\n", err)
	}

	if err := ji.createTargetTable(tableName); err != nil {
		return fmt.Errorf("创建目标表失败: %v", err)
	}

	// 按fbType分组处理
	fbTypeFiles := make(map[string][]Fb2FileInfo)
	for _, file := range allFiles {
		fbTypeFiles[file.FbType] = append(fbTypeFiles[file.FbType], file)
	}

	// 处理每种fb类型
	for fbType, files := range fbTypeFiles {
		fmt.Printf("\n🎯 开始处理 %s 类型的 %d 个文件...\n", fbType, len(files))

		// 按rtpLevel和testNum排序
		sort.Slice(files, func(i, j int) bool {
			if files[i].RtpLevel != files[j].RtpLevel {
				return files[i].RtpLevel < files[j].RtpLevel
			}
			return files[i].TestNum < files[j].TestNum
		})

		// 导入每个文件
		globalSrId := 1
		for _, file := range files {
			fmt.Printf("  📄 正在导入: %s\n", file.Name)

			if err := ji.importFb2File(file, tableName, &globalSrId); err != nil {
				return fmt.Errorf("导入文件 %s 失败: %v", file.Name, err)
			}

			fmt.Printf("  ✅ 导入完成: %s\n", file.Name)
		}
	}

	totalDuration := time.Since(totalStartTime)
	fmt.Printf("\n🎉 Fb2导入完成！总耗时: %v\n", totalDuration)
	return nil
}

// getFb2Files 获取指定游戏ID的所有Fb2文件
func (ji *JSONImporter) getFb2Files(gameID int, levelFilter string) ([]Fb2FileInfo, error) {
	var allFiles []Fb2FileInfo

	// 新的目录结构：output/24_fb/ (所有fb类型文件都在这个目录下)
	dirPath := filepath.Join("output", fmt.Sprintf("%d_fb", gameID))

	// 检查目录是否存在
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("目录不存在: %s", dirPath)
	}

	// 获取该目录下的所有Fb2文件
	files, err := ji.getFb2FilesFromDir(dirPath, gameID, levelFilter)
	if err != nil {
		return nil, fmt.Errorf("获取目录 %s 的文件失败: %v", dirPath, err)
	}

	allFiles = append(allFiles, files...)

	return allFiles, nil
}

// getFb2FilesFromDir 从指定目录获取Fb2文件
func (ji *JSONImporter) getFb2FilesFromDir(dirPath string, gameID int, levelFilter string) ([]Fb2FileInfo, error) {
	var files []Fb2FileInfo

	err := filepath.WalkDir(dirPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// 只处理JSON文件
		if !strings.HasSuffix(strings.ToLower(d.Name()), ".json") {
			return nil
		}

		// 解析文件名：GameResultData_fb{1|2|3}_{rtpLevel}_{testNum}.json
		re := regexp.MustCompile(`GameResultData_(fb[123])_(\d+)_(\d+)\.json`)
		matches := re.FindStringSubmatch(d.Name())
		if len(matches) != 4 {
			log.Printf("⚠️ 跳过不符合命名规则的文件: %s", d.Name())
			return nil
		}

		fbType := matches[1]        // fb1, fb2, fb3
		rtpLevel, _ := strconv.Atoi(matches[2])
		testNum, _ := strconv.Atoi(matches[3])

		// 如果指定了levelFilter，进行过滤
		if levelFilter != "" {
			levelStr := strconv.Itoa(rtpLevel)
			if levelStr != levelFilter {
				return nil
			}
		}

		// 创建排序键
		sortKey := fmt.Sprintf("%s_%02d_%02d", fbType, rtpLevel, testNum)

		fileInfo := Fb2FileInfo{
			Path:     path,
			Name:     d.Name(),
			GameID:   gameID,
			FbType:   fbType,
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

// importFb2File 导入单个Fb2文件
func (ji *JSONImporter) importFb2File(file Fb2FileInfo, tableName string, globalSrId *int) error {
	// 读取文件内容
	data, err := ji.readFb2JSONFile(file.Path)
	if err != nil {
		return fmt.Errorf("读取文件失败: %v", err)
	}

	if len(data) == 0 {
		fmt.Printf("    ⚠️ 文件为空，跳过: %s\n", file.Name)
		return nil
	}

	// 计算RTP等级偏移（根据fbType）
	var rtpOffset float64
	switch file.FbType {
	case "fb1":
		rtpOffset = 0.1
	case "fb2":
		rtpOffset = 0.2
	case "fb3":
		rtpOffset = 0.3
	default:
		rtpOffset = 0.0
	}

	// 批量插入数据
	if err := ji.insertFb2Batch(data, tableName, file.RtpLevel, file.TestNum, file.FbType, globalSrId, rtpOffset); err != nil {
		return fmt.Errorf("插入数据失败: %v", err)
	}

	return nil
}

// readFb2JSONFile 读取Fb2 JSON文件
func (ji *JSONImporter) readFb2JSONFile(filePath string) ([]map[string]interface{}, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var result struct {
		RtpLevel int                      `json:"rtpLevel"`
		SrNumber int                      `json:"srNumber"`
		Data     []map[string]interface{} `json:"data"`
	}

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}

	return result.Data, nil
}

// insertFb2Batch 批量插入Fb2数据
func (ji *JSONImporter) insertFb2Batch(data []map[string]interface{}, tableName string, rtpLevel int, testNum int, fbType string, globalSrId *int, rtpOffset float64) error {
	if len(data) == 0 {
		return nil
	}

	// 准备批量插入语句
	query := fmt.Sprintf(`
		INSERT INTO "%s" ("rtpLevel", "srNumber", "srId", "bet", "win", "detail")
		VALUES ($1, $2, $3, $4, $5, $6)
	`, tableName)

	// 计算实际RTP等级
	actualRtpLevel := float64(rtpLevel) + rtpOffset

	// 批量插入
	for _, item := range data {
		// 提取数据字段
		bet, _ := item["tb"].(float64)
		win, _ := item["aw"].(float64)

		// 将整个item作为detail存储
		detailJSON, err := json.Marshal(item)
		if err != nil {
			return fmt.Errorf("序列化detail失败: %v", err)
		}

		// 执行插入
		_, err = ji.db.DB.Exec(query, actualRtpLevel, testNum, *globalSrId, bet, win, string(detailJSON))
		if err != nil {
			return fmt.Errorf("插入记录失败: %v (rtpLevel: %.1f, testNum: %d, srId: %d, bet: %.2f, win: %.2f)", err, actualRtpLevel, testNum, *globalSrId, bet, win)
		}

		(*globalSrId)++
	}

	rtpLevelVal := float64(rtpLevel) + rtpOffset
	fmt.Printf("    ✅ 成功插入 %d 条记录 (RTP等级: %.1f, Fb类型: %s)\n", len(data), rtpLevelVal, fbType)
	return nil
}
