package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
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
	db.SetConnMaxLifetime(connMaxLifetime)
	db.SetConnMaxIdleTime(connMaxIdleTime)

	database := &Database{
		DB:           db,
		Config:       config,
		lastPingTime: time.Now(),
		pingInterval: pingInterval,
	}

	// 测试连接
	if err := database.EnsureConnection(); err != nil {
		return nil, fmt.Errorf("测试数据库连接失败: %v", err)
	}

	return database, nil
}

func (d *Database) Close() error {
	if d.DB != nil {
		return d.DB.Close()
	}
	return nil
}

func (d *Database) EnsureConnection() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return d.DB.PingContext(ctx)
}

func (d *Database) ExtendConnection() error {
	now := time.Now()
	if now.Sub(d.lastPingTime) >= d.pingInterval {
		if err := d.EnsureConnection(); err != nil {
			return err
		}
		d.lastPingTime = now
	}
	return nil
}

func (d *Database) CheckConnectionHealth() error {
	if err := d.ExtendConnection(); err != nil {
		return fmt.Errorf("数据库连接健康检查失败: %v", err)
	}
	return nil
}

func (d *Database) BeginWithRetry() (*sql.Tx, error) {
	var tx *sql.Tx
	var err error
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		tx, err = d.DB.Begin()
		if err == nil {
			return tx, nil
		}
		if i < maxRetries-1 {
			time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
		}
	}
	return nil, fmt.Errorf("开始事务失败（重试%d次）: %v", maxRetries, err)
}

func (d *Database) GetTableName() string {
	return fmt.Sprintf("%s%d", d.Config.Tables.SourceTablePrefix, d.Config.Game.ID)
}

func (d *Database) GetWinData() ([]GameResultData, error) {
	tableName := d.GetTableName()
	query := fmt.Sprintf(`
		SELECT id, tb, aw, gwt, sp, fb, gd
		FROM "%s"
		WHERE aw > 0 AND aw <= tb AND gwt <= 3 AND sp = true
		ORDER BY id
	`, tableName)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(d.Config.Settings.Timeout)*time.Second)
	defer cancel()

	rows, err := d.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("查询中奖数据失败: %v", err)
	}
	defer rows.Close()

	var results []GameResultData
	for rows.Next() {
		var item GameResultData
		if err := rows.Scan(&item.ID, &item.TB, &item.AW, &item.GWT, &item.SP, &item.FB, &item.GD); err != nil {
			return nil, fmt.Errorf("扫描数据失败: %v", err)
		}
		results = append(results, item)
	}

	return results, rows.Err()
}

func (d *Database) GetProfitData() ([]GameResultData, error) {
	tableName := d.GetTableName()
	query := fmt.Sprintf(`
		SELECT id, tb, aw, gwt, sp, fb, gd
		FROM "%s"
		WHERE aw > tb AND gwt <= 3 AND sp = true
		ORDER BY id
	`, tableName)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(d.Config.Settings.Timeout)*time.Second)
	defer cancel()

	rows, err := d.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("查询盈利数据失败: %v", err)
	}
	defer rows.Close()

	var results []GameResultData
	for rows.Next() {
		var item GameResultData
		if err := rows.Scan(&item.ID, &item.TB, &item.AW, &item.GWT, &item.SP, &item.FB, &item.GD); err != nil {
			return nil, fmt.Errorf("扫描数据失败: %v", err)
		}
		results = append(results, item)
	}

	return results, rows.Err()
}

func (d *Database) GetWinDataFb() ([]GameResultData, error) {
	tableName := d.GetTableName()
	query := fmt.Sprintf(`
		SELECT id, tb, aw, gwt, sp, fb, gd
		FROM "%s"
		WHERE aw > 0 AND aw <= tb AND gwt <= 3 AND fb = $1 AND sp = true
		ORDER BY id
	`, tableName)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(d.Config.Settings.Timeout)*time.Second)
	defer cancel()

	rows, err := d.DB.QueryContext(ctx, query, d.Config.Game.Mode)
	if err != nil {
		return nil, fmt.Errorf("查询购买模式中奖数据失败: %v", err)
	}
	defer rows.Close()

	var results []GameResultData
	for rows.Next() {
		var item GameResultData
		if err := rows.Scan(&item.ID, &item.TB, &item.AW, &item.GWT, &item.SP, &item.FB, &item.GD); err != nil {
			return nil, fmt.Errorf("扫描数据失败: %v", err)
		}
		results = append(results, item)
	}

	return results, rows.Err()
}

func (d *Database) GetProfitDataFb() ([]GameResultData, error) {
	tableName := d.GetTableName()
	query := fmt.Sprintf(`
		SELECT id, tb, aw, gwt, sp, fb, gd
		FROM "%s"
		WHERE aw > tb AND gwt <= 3 AND fb = $1 AND sp = true
		ORDER BY id
	`, tableName)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(d.Config.Settings.Timeout)*time.Second)
	defer cancel()

	rows, err := d.DB.QueryContext(ctx, query, d.Config.Game.Mode)
	if err != nil {
		return nil, fmt.Errorf("查询购买模式盈利数据失败: %v", err)
	}
	defer rows.Close()

	var results []GameResultData
	for rows.Next() {
		var item GameResultData
		if err := rows.Scan(&item.ID, &item.TB, &item.AW, &item.GWT, &item.SP, &item.FB, &item.GD); err != nil {
			return nil, fmt.Errorf("扫描数据失败: %v", err)
		}
		results = append(results, item)
	}

	return results, rows.Err()
}

func (d *Database) GetNoWinData() ([]GameResultData, error) {
	tableName := d.GetTableName()
	excludeSp := d.Config.Game.ExcludeSpInNoWin
	query := fmt.Sprintf(`
		SELECT id, tb, aw, gwt, sp, fb, gd
		FROM "%s"
		WHERE aw = 0 %s
		ORDER BY id
	`, tableName, func() string {
		if excludeSp {
			return "AND sp = false"
		}
		return ""
	}())

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(d.Config.Settings.Timeout)*time.Second)
	defer cancel()

	rows, err := d.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("查询不中奖数据失败: %v", err)
	}
	defer rows.Close()

	var results []GameResultData
	for rows.Next() {
		var item GameResultData
		if err := rows.Scan(&item.ID, &item.TB, &item.AW, &item.GWT, &item.SP, &item.FB, &item.GD); err != nil {
			return nil, fmt.Errorf("扫描数据失败: %v", err)
		}
		results = append(results, item)
	}

	return results, rows.Err()
}

func (d *Database) GetNoWinDataFb() ([]GameResultData, error) {
	tableName := d.GetTableName()
	excludeSp := d.Config.Game.ExcludeSpInNoWin
	query := fmt.Sprintf(`
		SELECT id, tb, aw, gwt, sp, fb, gd
		FROM "%s"
		WHERE aw = 0 AND fb = $1 %s
		ORDER BY id
	`, tableName, func() string {
		if excludeSp {
			return "AND sp = false"
		}
		return ""
	}())

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(d.Config.Settings.Timeout)*time.Second)
	defer cancel()

	rows, err := d.DB.QueryContext(ctx, query, d.Config.Game.Mode)
	if err != nil {
		return nil, fmt.Errorf("查询购买模式不中奖数据失败: %v", err)
	}
	defer rows.Close()

	var results []GameResultData
	for rows.Next() {
		var item GameResultData
		if err := rows.Scan(&item.ID, &item.TB, &item.AW, &item.GWT, &item.SP, &item.FB, &item.GD); err != nil {
			return nil, fmt.Errorf("扫描数据失败: %v", err)
		}
		results = append(results, item)
	}

	return results, rows.Err()
}

func (d *Database) GetWinDataForFilling(remainingWin float64, excludeIds []int, limit int) ([]GameResultData, error) {
	tableName := d.GetTableName()
	var excludeClause string
	if len(excludeIds) > 0 {
		excludeClause = fmt.Sprintf("AND id NOT IN (%s)", strings.Trim(strings.Replace(fmt.Sprint(excludeIds), " ", ",", -1), "[]"))
	}

	query := fmt.Sprintf(`
		SELECT id, tb, aw, gwt, sp, fb, gd
		FROM "%s"
		WHERE aw > 0 AND aw <= $1 AND gwt <= 3 AND sp = true %s
		ORDER BY aw DESC
		LIMIT $2
	`, tableName, excludeClause)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(d.Config.Settings.Timeout)*time.Second)
	defer cancel()

	rows, err := d.DB.QueryContext(ctx, query, remainingWin, limit)
	if err != nil {
		return nil, fmt.Errorf("查询填充数据失败: %v", err)
	}
	defer rows.Close()

	var results []GameResultData
	for rows.Next() {
		var item GameResultData
		if err := rows.Scan(&item.ID, &item.TB, &item.AW, &item.GWT, &item.SP, &item.FB, &item.GD); err != nil {
			return nil, fmt.Errorf("扫描数据失败: %v", err)
		}
		results = append(results, item)
	}

	return results, rows.Err()
}

func (d *Database) GetWinDataForFillingFb(remainingWin float64, excludeIds []int, limit int) ([]GameResultData, error) {
	tableName := d.GetTableName()
	var excludeClause string
	if len(excludeIds) > 0 {
		excludeClause = fmt.Sprintf("AND id NOT IN (%s)", strings.Trim(strings.Replace(fmt.Sprint(excludeIds), " ", ",", -1), "[]"))
	}

	query := fmt.Sprintf(`
		SELECT id, tb, aw, gwt, sp, fb, gd
		FROM "%s"
		WHERE aw > 0 AND aw <= $1 AND gwt <= 3 AND fb = $2 AND sp = true %s
		ORDER BY aw DESC
		LIMIT $3
	`, tableName, excludeClause)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(d.Config.Settings.Timeout)*time.Second)
	defer cancel()

	rows, err := d.DB.QueryContext(ctx, query, remainingWin, d.Config.Game.Mode, limit)
	if err != nil {
		return nil, fmt.Errorf("查询购买模式填充数据失败: %v", err)
	}
	defer rows.Close()

	var results []GameResultData
	for rows.Next() {
		var item GameResultData
		if err := rows.Scan(&item.ID, &item.TB, &item.AW, &item.GWT, &item.SP, &item.FB, &item.GD); err != nil {
			return nil, fmt.Errorf("扫描数据失败: %v", err)
		}
		results = append(results, item)
	}

	return results, rows.Err()
}

func (d *Database) GetBestSingleMatch(targetWin float64, excludeIds []int, maxDeviation float64) (*GameResultData, error) {
	tableName := d.GetTableName()
	var excludeClause string
	if len(excludeIds) > 0 {
		excludeClause = fmt.Sprintf("AND id NOT IN (%s)", strings.Trim(strings.Replace(fmt.Sprint(excludeIds), " ", ",", -1), "[]"))
	}

	query := fmt.Sprintf(`
		SELECT id, tb, aw, gwt, sp, fb, gd
		FROM "%s"
		WHERE aw > 0 AND gwt <= 3 AND sp = true %s
		ORDER BY ABS(aw - $1)
		LIMIT 1
	`, tableName, excludeClause)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(d.Config.Settings.Timeout)*time.Second)
	defer cancel()

	var item GameResultData
	err := d.DB.QueryRowContext(ctx, query, targetWin).Scan(&item.ID, &item.TB, &item.AW, &item.GWT, &item.SP, &item.FB, &item.GD)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("查询最佳匹配数据失败: %v", err)
	}

	deviation := item.AW - targetWin
	if deviation < 0 {
		deviation = -deviation
	}
	if deviation > maxDeviation {
		return nil, nil
	}

	return &item, nil
}

func (d *Database) CleanSpZeroAwData() error {
	tableName := d.GetTableName()
	query := fmt.Sprintf(`
		DELETE FROM "%s"
		WHERE sp = true AND aw = 0
	`, tableName)

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

// ExportTableToSQL 导出表数据到SQL文件（占位实现）
func (d *Database) ExportTableToSQL(outputFile string) error {
	return fmt.Errorf("此功能在当前分支中未实现")
}

// ExportTableSchema 导出表结构到SQL文件（占位实现）
func (d *Database) ExportTableSchema(schemaFile string) error {
	return fmt.Errorf("此功能在当前分支中未实现")
}

// ImportSQLFile 从SQL文件导入数据（占位实现）
func (d *Database) ImportSQLFile(sqlFile string) error {
	return fmt.Errorf("此功能在当前分支中未实现")
}

// SyncSequenceWithMaxID 同步序列与最大ID（占位实现）
func (d *Database) SyncSequenceWithMaxID() error {
	return fmt.Errorf("此功能在当前分支中未实现")
}

// ImportSQLFileFromS3 从S3导入SQL文件（占位实现）
func (d *Database) ImportSQLFileFromS3(s3Client interface{}, key string) error {
	return fmt.Errorf("此功能在当前分支中未实现")
}
