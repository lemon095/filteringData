package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

// Database 数据库连接结构体
type Database struct {
	DB     *sql.DB
	Config *Config
	// 连接管理
	lastPingTime time.Time
	pingInterval time.Duration
}

// NewDatabase 创建数据库连接
func NewDatabase(config *Config, env string) (*Database, error) {
	dbConfig, err := config.GetDatabaseConfig(env)
	if err != nil {
		return nil, err
	}

	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s TimeZone=%s",
		dbConfig.Host,
		dbConfig.Port,
		dbConfig.User,
		dbConfig.Password,
		dbConfig.Dbname,
		dbConfig.SSLMode,
		dbConfig.Timezone,
	)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %v", err)
	}

	// 优化连接池配置 - 使用配置文件
	maxOpenConns := 25
	maxIdleConns := 10
	connMaxLifetime := 30 * time.Minute
	connMaxIdleTime := 5 * time.Minute
	pingInterval := 2 * time.Minute

	// 如果配置文件中有设置，使用配置文件的值
	if config.Settings.Database.MaxOpenConns > 0 {
		maxOpenConns = config.Settings.Database.MaxOpenConns
	}
	if config.Settings.Database.MaxIdleConns > 0 {
		maxIdleConns = config.Settings.Database.MaxIdleConns
	}
	if config.Settings.Database.ConnMaxLifetime > 0 {
		connMaxLifetime = time.Duration(config.Settings.Database.ConnMaxLifetime) * time.Minute
	} else if config.Settings.Database.ConnMaxLifetime == 0 {
		connMaxLifetime = 0 // 0表示无限制
	}
	if config.Settings.Database.ConnMaxIdleTime > 0 {
		connMaxIdleTime = time.Duration(config.Settings.Database.ConnMaxIdleTime) * time.Minute
	}
	if config.Settings.Database.PingInterval > 0 {
		pingInterval = time.Duration(config.Settings.Database.PingInterval) * time.Minute
	}

	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)
	if connMaxLifetime > 0 {
		db.SetConnMaxLifetime(connMaxLifetime)
	}
	db.SetConnMaxIdleTime(connMaxIdleTime)

	// 测试连接
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("数据库连接测试失败: %v", err)
	}

	envDisplay := env
	if env == "" {
		envDisplay = config.DefaultEnv
	}
	log.Printf("数据库连接成功 [环境: %s, 主机: %s, 连接池: 最大%d/空闲%d, 生存时间: %v, 空闲时间: %v]",
		envDisplay, dbConfig.Host, maxOpenConns, maxIdleConns, connMaxLifetime, connMaxIdleTime)
	return &Database{
		DB:           db,
		Config:       config,
		lastPingTime: time.Now(),
		pingInterval: pingInterval, // 使用配置的ping间隔
	}, nil
}

// Close 关闭数据库连接
func (d *Database) Close() error {
	if d.DB != nil {
		return d.DB.Close()
	}
	return nil
}

// EnsureConnection 确保数据库连接健康
func (d *Database) EnsureConnection() error {
	// 检查是否需要ping
	if time.Since(d.lastPingTime) < d.pingInterval {
		return nil
	}

	// 执行ping检查
	if err := d.DB.Ping(); err != nil {
		log.Printf("⚠️ 数据库连接检查失败，尝试重连: %v", err)
		// 这里可以添加重连逻辑
		return fmt.Errorf("数据库连接不健康: %v", err)
	}

	// 更新最后ping时间
	d.lastPingTime = time.Now()
	return nil
}

// ExtendConnection 延长连接生存时间
func (d *Database) ExtendConnection() error {
	// 通过执行一个简单查询来"刷新"连接
	_, err := d.DB.Exec("SELECT 1")
	if err != nil {
		log.Printf("⚠️ 连接续期失败: %v", err)
		return err
	}

	// 更新最后ping时间
	d.lastPingTime = time.Now()
	log.Printf("✅ 连接生存时间已延长")
	return nil
}

// CheckConnectionHealth 检查连接健康状态并处理超时
func (d *Database) CheckConnectionHealth() error {
	// 检查连接是否超时
	if time.Since(d.lastPingTime) > 10*time.Minute {
		log.Printf("⚠️ 连接可能已超时，尝试续期...")
		return d.ExtendConnection()
	}

	// 正常健康检查
	return d.EnsureConnection()
}

// BeginWithRetry 带重试机制的事务开始
func (d *Database) BeginWithRetry() (*sql.Tx, error) {
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		// 确保连接健康
		if err := d.EnsureConnection(); err != nil {
			if i < maxRetries-1 {
				log.Printf("⚠️ 连接检查失败，重试中... (重试 %d/%d): %v", i+1, maxRetries, err)
				time.Sleep(time.Duration(i+1) * time.Second)
				continue
			}
			return nil, err
		}

		// 开始事务
		tx, err := d.DB.Begin()
		if err != nil {
			if i < maxRetries-1 {
				log.Printf("⚠️ 开始事务失败，重试中... (重试 %d/%d): %v", i+1, maxRetries, err)
				time.Sleep(time.Duration(i+1) * time.Second)
				continue
			}
			return nil, err
		}
		return tx, nil
	}
	return nil, fmt.Errorf("经过 %d 次重试后仍无法开始事务", maxRetries)
}

// GetTableName 获取源表名（用于读取数据）
func (d *Database) GetTableName() string {
	return fmt.Sprintf("\"%s%d\"", d.Config.Tables.SourceTablePrefix, d.Config.Game.ID)
}

// GetWinData 获取所有中奖数据 (aw > 0 且 aw/tb < 100)
func (d *Database) GetWinData() ([]GameResultData, error) {
	tableName := d.GetTableName()
	query := fmt.Sprintf(`
		SELECT id, tb, aw, gwt, sp, fb, gd, "createdAt", "updatedAt"
		FROM %s 
		WHERE aw > 0 AND aw < tb * 100
		AND fb = %d
		ORDER BY id
	`, tableName, d.Config.Game.Mode)

	rows, err := d.DB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var data []GameResultData
	for rows.Next() {
		var item GameResultData
		err := rows.Scan(
			&item.ID, &item.TB, &item.AW, &item.GWT,
			&item.SP, &item.FB, &item.GD,
			&item.CreatedAt, &item.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, item)
	}

	return data, nil
}

// GetProfitData 获取普通模式的中奖且盈利的数据 (aw > tb, fb = mode)
func (d *Database) GetProfitData() ([]GameResultData, error) {
	tableName := d.GetTableName()
	query := fmt.Sprintf(`
		SELECT id, tb, aw, gwt, sp, fb, gd, "createdAt", "updatedAt"
		FROM %s 
		WHERE aw > 0 AND aw > tb AND fb = %d
		ORDER BY id
	`, tableName, d.Config.Game.Mode)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(d.Config.Settings.Timeout)*time.Second)
	defer cancel()
	rows, err := d.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var data []GameResultData
	for rows.Next() {
		var item GameResultData
		err := rows.Scan(
			&item.ID, &item.TB, &item.AW, &item.GWT,
			&item.SP, &item.FB, &item.GD,
			&item.CreatedAt, &item.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, item)
	}

	return data, nil
}

// GetWinDataFb 获取购买模式的中奖但是亏损的数据 (aw > 0&aw<tb, fb = mode, sp = true, aw < tb*100)
func (d *Database) GetWinDataFb() ([]GameResultData, error) {
	tableName := d.GetTableName()
	query := fmt.Sprintf(`
        SELECT id, tb, aw, gwt, sp, fb, gd, "createdAt", "updatedAt"
        FROM %s 
        WHERE aw > 0 AND aw <= tb AND fb = %d AND sp = true
        ORDER BY id
    `, tableName, d.Config.Game.Mode)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(d.Config.Settings.Timeout)*time.Second)
	defer cancel()
	rows, err := d.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var data []GameResultData
	for rows.Next() {
		var item GameResultData
		err := rows.Scan(
			&item.ID, &item.TB, &item.AW, &item.GWT,
			&item.SP, &item.FB, &item.GD,
			&item.CreatedAt, &item.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, item)
	}

	return data, nil
}

// 购买模式 盈利的中奖数据
func (d *Database) GetProfitDataFb() ([]GameResultData, error) {
	tableName := d.GetTableName()
	query := fmt.Sprintf(`
        SELECT id, tb, aw, gwt, sp, fb, gd, "createdAt", "updatedAt"
        FROM %s 
        WHERE aw > 0 AND aw > tb AND fb = %d AND sp = true
        ORDER BY id
    `, tableName, d.Config.Game.Mode)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(d.Config.Settings.Timeout)*time.Second)
	defer cancel()
	rows, err := d.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var data []GameResultData
	for rows.Next() {
		var item GameResultData
		err := rows.Scan(
			&item.ID, &item.TB, &item.AW, &item.GWT,
			&item.SP, &item.FB, &item.GD,
			&item.CreatedAt, &item.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, item)
	}

	return data, nil
}

// GetNoWinData 获取所有不中奖数据 (aw = 0)
func (d *Database) GetNoWinData() ([]GameResultData, error) {
	tableName := d.GetTableName()
	query := fmt.Sprintf(`
		SELECT id, tb, aw, gwt, sp, fb, gd, "createdAt", "updatedAt"
		FROM %s 
		WHERE aw = 0 And sp != true
		AND fb = %d
		ORDER BY id
	`, tableName, d.Config.Game.Mode)

	rows, err := d.DB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var data []GameResultData
	for rows.Next() {
		var item GameResultData
		err := rows.Scan(
			&item.ID, &item.TB, &item.AW, &item.GWT,
			&item.SP, &item.FB, &item.GD,
			&item.CreatedAt, &item.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, item)
	}

	return data, nil
}

// GetNoWinDataFb 获取购买模式的不中奖数据 (aw = 0, fb = mode, sp = true)
func (d *Database) GetNoWinDataFb() ([]GameResultData, error) {
	tableName := d.GetTableName()
	query := fmt.Sprintf(`
        SELECT id, tb, aw, gwt, sp, fb, gd, "createdAt", "updatedAt"
        FROM %s 
        WHERE aw = 0 AND sp = true AND fb = %d
        ORDER BY id
    `, tableName, d.Config.Game.Mode)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(d.Config.Settings.Timeout)*time.Second)
	defer cancel()
	rows, err := d.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var data []GameResultData
	for rows.Next() {
		var item GameResultData
		err := rows.Scan(
			&item.ID, &item.TB, &item.AW, &item.GWT,
			&item.SP, &item.FB, &item.GD,
			&item.CreatedAt, &item.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, item)
	}

	return data, nil
}

// GetWinDataForFilling 获取用于填充的中奖数据，按金额排序并限制数量
// remainingWin: 需要填充的金额
// excludeIds: 已经使用的数据ID列表
// limit: 限制返回的数据条数
func (d *Database) GetWinDataForFilling(remainingWin float64, excludeIds []int, limit int) ([]GameResultData, error) {
	tableName := d.GetTableName()

	// 构建排除ID的SQL条件
	var excludeCondition string
	var args []interface{}
	argIndex := 1

	if len(excludeIds) > 0 {
		placeholders := make([]string, len(excludeIds))
		for i := range excludeIds {
			placeholders[i] = fmt.Sprintf("$%d", argIndex)
			args = append(args, excludeIds[i])
			argIndex++
		}
		excludeCondition = fmt.Sprintf("AND id NOT IN (%s)", strings.Join(placeholders, ","))
	}

	// 查询条件：
	// 1. 中奖金额 > 0 且 < tb * 100
	// 2. 中奖金额 <= remainingWin（比需要填充金额低的）
	// 3. 排除已使用的ID
	// 4. 按中奖金额降序排列，优先选择金额大的
	// 5. 限制返回条数
	query := fmt.Sprintf(`
        SELECT id, tb, aw, gwt, sp, fb, gd, "createdAt", "updatedAt"
        FROM %s 
        WHERE aw > 0 
        AND fb = %d
        AND aw < tb * 100
        AND aw <= $%d
        %s
        ORDER BY aw DESC
        LIMIT $%d
    `, tableName, d.Config.Game.Mode, argIndex, excludeCondition, argIndex+1)

	args = append(args, remainingWin, limit)

	rows, err := d.DB.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("查询填充数据失败: %v", err)
	}
	defer rows.Close()

	var data []GameResultData
	for rows.Next() {
		var item GameResultData
		err := rows.Scan(
			&item.ID, &item.TB, &item.AW, &item.GWT,
			&item.SP, &item.FB, &item.GD,
			&item.CreatedAt, &item.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, item)
	}

	return data, nil
}

// GetWinDataForFillingFb 获取用于填充的购买模式中奖数据
// 条件：aw > 0 且 aw < tb*100 且 aw <= remainingWin，fb = mode，sp = true
// 排除 excludeIds，按金额从大到小排序，限制返回条数
func (d *Database) GetWinDataForFillingFb(remainingWin float64, excludeIds []int, limit int) ([]GameResultData, error) {
	tableName := d.GetTableName()

	var excludeCondition string
	var args []interface{}
	argIndex := 1

	if len(excludeIds) > 0 {
		placeholders := make([]string, len(excludeIds))
		for i := range excludeIds {
			placeholders[i] = fmt.Sprintf("$%d", argIndex)
			args = append(args, excludeIds[i])
			argIndex++
		}
		excludeCondition = fmt.Sprintf("AND id NOT IN (%s)", strings.Join(placeholders, ","))
	}

	query := fmt.Sprintf(`
        SELECT id, tb, aw, gwt, sp, fb, gd, "createdAt", "updatedAt"
        FROM %s 
        WHERE aw > 0 
        AND aw < tb * 100
        AND aw <= $%d
        AND fb = %d
        AND sp = true
        %s
        ORDER BY aw DESC
        LIMIT $%d
    `, tableName, argIndex, d.Config.Game.Mode, excludeCondition, argIndex+1)

	args = append(args, remainingWin, limit)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(d.Config.Settings.Timeout)*time.Second)
	defer cancel()
	rows, err := d.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("查询购买模式填充数据失败: %v", err)
	}
	defer rows.Close()

	var data []GameResultData
	for rows.Next() {
		var item GameResultData
		err := rows.Scan(
			&item.ID, &item.TB, &item.AW, &item.GWT,
			&item.SP, &item.FB, &item.GD,
			&item.CreatedAt, &item.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, item)
	}

	return data, nil
}

// GetBestSingleMatch 获取最接近目标金额的单条中奖数据
// targetWin: 目标中奖金额
// excludeIds: 已经使用的数据ID列表
// maxDeviation: 最大允许偏差（如0.005表示允许5%的偏差）
func (d *Database) GetBestSingleMatch(targetWin float64, excludeIds []int, maxDeviation float64) (*GameResultData, error) {
	tableName := d.GetTableName()

	// 构建排除ID的SQL条件
	var excludeCondition string
	var args []interface{}
	argIndex := 1

	if len(excludeIds) > 0 {
		placeholders := make([]string, len(excludeIds))
		for i := range excludeIds {
			placeholders[i] = fmt.Sprintf("$%d", argIndex)
			args = append(args, excludeIds[i])
			argIndex++
		}
		excludeCondition = fmt.Sprintf("AND id NOT IN (%s)", strings.Join(placeholders, ","))
	}

	// 查询条件：
	// 1. 中奖金额 > 0 且 < tb * 100
	// 2. 中奖金额在允许偏差范围内
	// 3. 排除已使用的ID
	// 4. 按与目标金额的差值排序，选择最接近的
	query := fmt.Sprintf(`
		SELECT id, tb, aw, gwt, sp, fb, gd, "createdAt", "updatedAt"
		FROM %s 
		WHERE aw > 0
		AND fb = %d
		AND aw < tb * 100
		AND aw >= $%d * (1 - $%d)
		AND aw <= $%d * (1 + $%d)
		%s
		ORDER BY ABS(aw - $%d)
		LIMIT 1
	`, tableName, d.Config.Game.Mode, argIndex, argIndex+1, argIndex+2, argIndex+3, excludeCondition, argIndex+4)

	args = append(args, targetWin, maxDeviation, targetWin, maxDeviation, targetWin)

	var item GameResultData
	err := d.DB.QueryRow(query, args...).Scan(
		&item.ID, &item.TB, &item.AW, &item.GWT,
		&item.SP, &item.FB, &item.GD,
		&item.CreatedAt, &item.UpdatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // 没有找到匹配的数据
		}
		return nil, fmt.Errorf("查询最佳匹配数据失败: %v", err)
	}

	return &item, nil
}

// CleanSpZeroAwData 清理表中 sp=true 且 aw=0 的数据
func (d *Database) CleanSpZeroAwData() error {
	tableName := d.GetTableName()
	query := fmt.Sprintf(`DELETE FROM %s WHERE "sp" = true AND "aw" = 0`, tableName)

	log.Printf("🧹 开始清理表 %s 中 sp=true 且 aw=0 的数据...", tableName)

	result, err := d.DB.Exec(query)
	if err != nil {
		return fmt.Errorf("清理数据失败: %v", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("获取影响行数失败: %v", err)
	}

	log.Printf("✅ 清理完成，删除了 %d 条记录", rowsAffected)
	return nil
}

// ExportTableToSQL 导出表数据到SQL文件（支持自动分割为多个文件，每个文件不超过120MB）
func (d *Database) ExportTableToSQL(outputFile string) error {
	tableName := d.GetTableName()
	log.Printf("🔄 开始导出表 %s 的数据到 %s...", tableName, outputFile)

	// 查询所有数据
	query := fmt.Sprintf(`
		SELECT id, tb, aw, gwt, sp, fb, gd, "createdAt", "updatedAt"
		FROM %s 
		ORDER BY id
	`, tableName)

	rows, err := d.DB.Query(query)
	if err != nil {
		return fmt.Errorf("查询数据失败: %v", err)
	}
	defer rows.Close()

	// 文件大小限制：120MB
	maxFileSize := int64(120 * 1024 * 1024) // 120MB in bytes

	// 获取基础文件名（不含扩展名和路径）
	ext := filepath.Ext(outputFile)
	baseName := strings.TrimSuffix(filepath.Base(outputFile), ext)
	baseDir := filepath.Dir(outputFile)

	var currentFile *os.File
	var currentWriter *bufio.Writer
	var currentFileSize int64
	var fileIndex int
	var totalCount int
	var batchCount int
	var batchValues []string
	batchSize := 1000

	// 创建新文件的函数
	createNewFile := func() error {
		// 关闭旧文件
		if currentWriter != nil {
			if err := currentWriter.Flush(); err != nil {
				return fmt.Errorf("刷新文件失败: %v", err)
			}
		}
		if currentFile != nil {
			if err := currentFile.Close(); err != nil {
				return fmt.Errorf("关闭文件失败: %v", err)
			}
		}

		// 创建新文件名
		fileIndex++
		var newFileName string
		if fileIndex == 1 {
			newFileName = outputFile
		} else {
			newFileName = filepath.Join(baseDir, fmt.Sprintf("%s_part%03d%s", baseName, fileIndex, ext))
		}

		// 创建新文件
		var err error
		currentFile, err = os.Create(newFileName)
		if err != nil {
			return fmt.Errorf("创建文件失败: %v", err)
		}
		currentWriter = bufio.NewWriter(currentFile)
		currentFileSize = 0

		// 写入SQL文件头
		tableNameWithoutQuotes := fmt.Sprintf("%s%d", d.Config.Tables.SourceTablePrefix, d.Config.Game.ID)
		header := fmt.Sprintf("-- SQL导出文件 (第 %d 部分)\n-- 表名: %s\n-- 导出时间: %s\n\n", fileIndex, tableNameWithoutQuotes, time.Now().Format("2006-01-02 15:04:05"))
		if _, err := currentWriter.WriteString(header); err != nil {
			return fmt.Errorf("写入文件头失败: %v", err)
		}
		currentFileSize += int64(len(header))

		log.Printf("  📄 创建文件: %s", newFileName)
		return nil
	}

	// 创建第一个文件
	if err := createNewFile(); err != nil {
		return err
	}
	defer func() {
		if currentWriter != nil {
			currentWriter.Flush()
		}
		if currentFile != nil {
			currentFile.Close()
		}
	}()

	for rows.Next() {
		var item GameResultData
		err := rows.Scan(
			&item.ID, &item.TB, &item.AW, &item.GWT,
			&item.SP, &item.FB, &item.GD,
			&item.CreatedAt, &item.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("扫描数据失败: %v", err)
		}

		// 转义JSONB字段
		gdStr := "NULL"
		if item.GD.Data != nil {
			gdJSON, err := json.Marshal(item.GD.Data)
			if err != nil {
				return fmt.Errorf("序列化JSON字段失败: %v", err)
			}
			// 转义单引号
			gdStr = strings.ReplaceAll(string(gdJSON), "'", "''")
			gdStr = "'" + gdStr + "'"
		}

		// 格式化时间
		createdAtStr := item.CreatedAt.Format("2006-01-02 15:04:05")
		updatedAtStr := item.UpdatedAt.Format("2006-01-02 15:04:05")

		// 构建VALUES子句
		valueStr := fmt.Sprintf("(%d, %.2f, %.2f, %d, %v, %d, %s::jsonb, '%s', '%s')",
			item.ID, item.TB, item.AW, item.GWT, item.SP, item.FB, gdStr, createdAtStr, updatedAtStr)
		batchValues = append(batchValues, valueStr)
		totalCount++

		// 达到批次大小时写入
		if len(batchValues) >= batchSize {
			batchCount++
			insertSQL := fmt.Sprintf("INSERT INTO %s (id, tb, aw, gwt, sp, fb, gd, \"createdAt\", \"updatedAt\") VALUES\n%s;\n\n",
				tableName, strings.Join(batchValues, ",\n"))

			// 检查文件大小，如果写入后会超过限制，先创建新文件
			estimatedSize := int64(len(insertSQL))
			if currentFileSize+estimatedSize > maxFileSize && len(batchValues) > 0 {
				// 先写入当前批次到新文件
				if err := createNewFile(); err != nil {
					return err
				}
			}

			if _, err := currentWriter.WriteString(insertSQL); err != nil {
				return fmt.Errorf("写入SQL失败: %v", err)
			}
			currentFileSize += estimatedSize
			batchValues = batchValues[:0]

			if totalCount%10000 == 0 {
				log.Printf("  📊 已导出 %d 条记录，当前文件大小: %.2f MB...", totalCount, float64(currentFileSize)/(1024*1024))
			}
		}
	}

	// 写入剩余的记录
	if len(batchValues) > 0 {
		batchCount++
		insertSQL := fmt.Sprintf("INSERT INTO %s (id, tb, aw, gwt, sp, fb, gd, \"createdAt\", \"updatedAt\") VALUES\n%s;\n\n",
			tableName, strings.Join(batchValues, ",\n"))

		// 检查是否需要创建新文件
		estimatedSize := int64(len(insertSQL))
		if currentFileSize+estimatedSize > maxFileSize {
			if err := createNewFile(); err != nil {
				return err
			}
		}

		if _, err := currentWriter.WriteString(insertSQL); err != nil {
			return fmt.Errorf("写入SQL失败: %v", err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("遍历数据失败: %v", err)
	}

	// 刷新最后一个文件
	if err := currentWriter.Flush(); err != nil {
		return fmt.Errorf("刷新文件失败: %v", err)
	}

	if fileIndex > 1 {
		log.Printf("✅ 导出完成！共导出 %d 条记录，分 %d 批次写入，生成 %d 个文件", totalCount, batchCount, fileIndex)
		log.Printf("📁 文件列表:")
		for i := 1; i <= fileIndex; i++ {
			var fileName string
			if i == 1 {
				fileName = outputFile
			} else {
				fileName = filepath.Join(baseDir, fmt.Sprintf("%s_part%03d%s", baseName, i, ext))
			}
			// 获取文件大小
			if info, err := os.Stat(fileName); err == nil {
				sizeMB := float64(info.Size()) / (1024 * 1024)
				log.Printf("   - %s (%.2f MB)", fileName, sizeMB)
			} else {
				log.Printf("   - %s", fileName)
			}
		}
	} else {
		log.Printf("✅ 导出完成！共导出 %d 条记录，分 %d 批次写入", totalCount, batchCount)
	}

	return nil
}

// ImportSQLFile 从SQL文件导入数据到表
func (d *Database) ImportSQLFile(sqlFile string) error {
	tableName := d.GetTableName()
	log.Printf("🔄 开始从 %s 导入数据到表 %s...", sqlFile, tableName)

	// 查询当前表中的最大ID（用于显示信息）
	var maxID sql.NullInt64
	err := d.DB.QueryRow(fmt.Sprintf("SELECT COALESCE(MAX(id), 0) FROM %s", tableName)).Scan(&maxID)
	if err != nil {
		log.Printf("⚠️ 查询当前最大ID失败: %v", err)
	} else {
		log.Printf("📊 当前表中最大ID: %d（导入后将使用文件中的ID值）", maxID.Int64)
	}

	// 打开SQL文件
	file, err := os.Open(sqlFile)
	if err != nil {
		return fmt.Errorf("打开SQL文件失败: %v", err)
	}
	defer file.Close()

	// 使用scanner逐行读取
	scanner := bufio.NewScanner(file)
	// 增大缓冲区以处理长行
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 10*1024*1024) // 10MB缓冲区

	var currentSQL strings.Builder
	count := 0
	inStatement := false
	var maxImportedID int64 = 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// 跳过注释行和空行
		if strings.HasPrefix(line, "--") || line == "" {
			continue
		}

		// 检查是否是INSERT语句开始
		if strings.HasPrefix(strings.ToUpper(line), "INSERT") {
			inStatement = true
			currentSQL.Reset()
			currentSQL.WriteString(line)
		} else if inStatement {
			// 继续构建SQL语句
			currentSQL.WriteString(" ")
			currentSQL.WriteString(line)
		}

		// 检查是否是语句结束（以分号结尾）
		if inStatement && strings.HasSuffix(line, ";") {
			sql := currentSQL.String()
			// 移除末尾的分号
			sql = strings.TrimSuffix(sql, ";")
			sql = strings.TrimSpace(sql)

			// 执行SQL语句（使用文件中的ID值）
			// 注意：INSERT语句明确指定了ID字段，PostgreSQL会使用文件中的ID值，而不是序列值
			result, err := d.DB.Exec(sql)
			if err != nil {
				// 检查是否是主键冲突错误
				if strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "violates unique constraint") {
					log.Printf("⚠️ 主键冲突（ID已存在）: %v", err)
					log.Printf("   💡 提示：如需重新导入，请先清空表或删除已存在的记录")
				} else {
					log.Printf("⚠️ 执行SQL语句失败: %v", err)
				}
				// 显示SQL语句的前100个字符用于调试
				sqlPreview := sql
				if len(sql) > 100 {
					sqlPreview = sql[:100]
				}
				log.Printf("   语句内容: %s", sqlPreview)
				// 继续执行其他语句，不中断
				currentSQL.Reset()
				inStatement = false
				continue
			}

			// 尝试从INSERT语句中提取ID值（用于跟踪最大ID）
			rowsAffected, _ := result.RowsAffected()
			if rowsAffected > 0 {
				// 从VALUES子句中提取ID值范围（简化版）
				if idMatch := extractFirstIDFromInsert(sql); idMatch > 0 {
					if idMatch > maxImportedID {
						maxImportedID = idMatch
					}
				}
			}

			count++
			if count%100 == 0 {
				log.Printf("  📊 已执行 %d 条INSERT语句，当前最大ID: %d...", count, maxImportedID)
			}

			currentSQL.Reset()
			inStatement = false
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("读取SQL文件失败: %v", err)
	}

	// 注意：序列更新将在所有文件导入完成后统一进行（由调用方处理）
	// 这样可以确保分割文件导入时序列的一致性

	log.Printf("✅ 文件导入完成！共执行 %d 条INSERT语句", count)
	return nil
}

// extractFirstIDFromInsert 从INSERT语句中提取第一个ID值（简化版）
func extractFirstIDFromInsert(insertSQL string) int64 {
	// 查找 VALUES 后面的第一个数字
	valuesIdx := strings.Index(strings.ToUpper(insertSQL), "VALUES")
	if valuesIdx == -1 {
		return 0
	}

	// 从VALUES后查找第一个括号
	valuesPart := insertSQL[valuesIdx:]
	openBracket := strings.Index(valuesPart, "(")
	if openBracket == -1 {
		return 0
	}

	// 提取括号内的第一个值（ID）
	closeBracket := strings.Index(valuesPart[openBracket:], ",")
	if closeBracket == -1 {
		closeBracket = strings.Index(valuesPart[openBracket:], ")")
	}
	if closeBracket == -1 {
		return 0
	}

	idStr := strings.TrimSpace(valuesPart[openBracket+1 : openBracket+closeBracket])
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return 0
	}

	return id
}

// ImportSQLFileFromS3 从S3导入SQL文件
func (d *Database) ImportSQLFileFromS3(s3Client *S3Client, s3Key string) error {
	tableName := d.GetTableName()
	log.Printf("🔄 开始从S3导入SQL文件 %s 到表 %s...", s3Key, tableName)

	// 从S3下载文件内容
	content, err := s3Client.DownloadS3File(s3Key)
	if err != nil {
		return fmt.Errorf("从S3下载文件失败: %v", err)
	}

	// 将内容写入临时文件
	tmpFile, err := os.CreateTemp("", "sql_import_*.sql")
	if err != nil {
		return fmt.Errorf("创建临时文件失败: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.Write(content); err != nil {
		return fmt.Errorf("写入临时文件失败: %v", err)
	}
	tmpFile.Close()

	// 使用现有的ImportSQLFile方法导入
	return d.ImportSQLFile(tmpFile.Name())
}

// SyncSequenceWithMaxID 将自增序列同步到当前表中的最大ID，确保后续插入的ID连续且不冲突
func (d *Database) SyncSequenceWithMaxID() error {
	tableName := d.GetTableName()
	tableNameWithoutQuotes := strings.Trim(tableName, "\"")
	sequenceName := fmt.Sprintf("\"%s_id_seq\"", tableNameWithoutQuotes)

	var maxID sql.NullInt64
	if err := d.DB.QueryRow(fmt.Sprintf("SELECT COALESCE(MAX(id), 0) FROM %s", tableName)).Scan(&maxID); err != nil {
		return fmt.Errorf("查询最大ID失败: %v", err)
	}

	if maxID.Int64 > 0 {
		updateSeqSQL := fmt.Sprintf("SELECT setval('%s', %d, true)", sequenceName, maxID.Int64)
		if _, err := d.DB.Exec(updateSeqSQL); err != nil {
			return fmt.Errorf("更新序列失败: %v", err)
		}
		log.Printf("✅ 已更新序列 %s 到 %d（确保ID顺序正确）", sequenceName, maxID.Int64)
	} else {
		resetSQL := fmt.Sprintf("SELECT setval('%s', 1, false)", sequenceName)
		if _, err := d.DB.Exec(resetSQL); err != nil {
			return fmt.Errorf("重置序列失败: %v", err)
		}
		log.Printf("ℹ️ 表为空，已将序列 %s 重置为 1", sequenceName)
	}

	return nil
}
