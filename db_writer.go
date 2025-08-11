package main

import (
	"fmt"
	"log"
	"strings"
)

// DBWriter 数据库写入器
type DBWriter struct {
	DB     *Database
	Config *Config
}

// NewDBWriter 创建数据库写入器
func NewDBWriter(db *Database, config *Config) *DBWriter {
	return &DBWriter{
		DB:     db,
		Config: config,
	}
}

// GetOutputTableName 获取输出表名
func (w *DBWriter) GetOutputTableName() string {
	return fmt.Sprintf("%s%d", w.Config.Tables.OutputTablePrefix, w.Config.Game.ID)
}

// GetQuotedOutputTableName 获取带引号的输出表名（用于SQL查询）
func (w *DBWriter) GetQuotedOutputTableName() string {
	return fmt.Sprintf("\"%s\"", w.GetOutputTableName())
}

// CheckOutputTableExists 检查输出表是否存在
func (w *DBWriter) CheckOutputTableExists() (bool, error) {
	tableName := w.GetOutputTableName()
	query := `
		SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name = $1
		)
	`

	var exists bool
	err := w.DB.DB.QueryRow(query, tableName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("检查表是否存在失败: %v", err)
	}

	return exists, nil
}

// CreateOutputTable 创建输出表
func (w *DBWriter) CreateOutputTable() error {
	tableName := w.GetQuotedOutputTableName()

	createTableSQL := fmt.Sprintf(`
		CREATE TABLE %s (
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
		)
	`, tableName, w.GetOutputTableName())

	_, err := w.DB.DB.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("创建表失败: %v", err)
	}

	log.Printf("✅ 成功创建表: %s", w.GetOutputTableName())
	return nil
}

// ClearOutputTable 清空输出表
func (w *DBWriter) ClearOutputTable() error {
	tableName := w.GetQuotedOutputTableName()

	deleteSQL := fmt.Sprintf("DELETE FROM %s", tableName)
	result, err := w.DB.DB.Exec(deleteSQL)
	if err != nil {
		return fmt.Errorf("清空表失败: %v", err)
	}

	rowsAffected, _ := result.RowsAffected()
	log.Printf("🗑️  清空表 %s，删除了 %d 条记录", w.GetOutputTableName(), rowsAffected)
	return nil
}

// InsertData 批量插入数据
func (w *DBWriter) InsertData(data []GameResult) error {
	if len(data) == 0 {
		return nil
	}

	tableName := w.GetQuotedOutputTableName()

	// 构建批量插入SQL
	valueStrings := make([]string, 0, len(data))
	valueArgs := make([]interface{}, 0, len(data)*6) // 6个字段（不包括id, createdAt, updatedAt）

	for i, item := range data {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)",
			i*6+1, i*6+2, i*6+3, i*6+4, i*6+5, i*6+6))

		valueArgs = append(valueArgs,
			item.RtpLevel, // 等级
			item.SrNumber, // 第几次
			item.SrId,     // 顺序id
			item.Bet,      // 投注
			item.Win,      // 盈利
			item.Detail,   // 数据
		)
	}

	insertSQL := fmt.Sprintf(`
		INSERT INTO %s ("rtpLevel", "srNumber", "srId", "bet", "win", "detail")
		VALUES %s
	`, tableName, strings.Join(valueStrings, ","))

	_, err := w.DB.DB.Exec(insertSQL, valueArgs...)
	if err != nil {
		return fmt.Errorf("批量插入数据失败: %v", err)
	}

	log.Printf("✅ 成功插入 %d 条数据到表: %s", len(data), w.GetOutputTableName())
	return nil
}

// SaveFilteredData 保存筛选后的数据到数据库
func (w *DBWriter) SaveFilteredData(data []GameResult) error {
	log.Printf("📊 开始保存筛选数据到数据库...")

	// 检查表是否存在
	exists, err := w.CheckOutputTableExists()
	if err != nil {
		return fmt.Errorf("检查输出表失败: %v", err)
	}

	if !exists {
		// 创建表
		log.Printf("📝 表 %s 不存在，正在创建...", w.GetOutputTableName())
		if err := w.CreateOutputTable(); err != nil {
			return err
		}
	} else {
		// 表存在，清空旧数据
		log.Printf("📝 表 %s 存在，继续插入...", w.GetOutputTableName())
		// log.Printf("🔄 表 %s 已存在，清空旧数据...", w.GetOutputTableName())
		// if err := w.ClearOutputTable(); err != nil {
		// 	return err
		// }
	}

	// 批量插入数据
	if err := w.InsertData(data); err != nil {
		return err
	}

	// 验证插入结果
	count, err := w.GetRecordCount()
	if err != nil {
		log.Printf("⚠️  无法验证插入结果: %v", err)
	} else {
		log.Printf("✅ 数据库保存完成！表 %s 共有 %d 条记录", w.GetOutputTableName(), count)
	}

	return nil
}

// GetRecordCount 获取输出表的记录数
func (w *DBWriter) GetRecordCount() (int, error) {
	tableName := w.GetQuotedOutputTableName()

	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	err := w.DB.DB.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("获取记录数失败: %v", err)
	}

	return count, nil
}

// GetDataSample 获取数据样本（前几条记录）
func (w *DBWriter) GetDataSample(limit int) ([]GameResultData, error) {
	tableName := w.GetQuotedOutputTableName()

	query := fmt.Sprintf(`
		SELECT id, tb, aw, gwt, sp, fb, gd, "createdAt", "updatedAt"
		FROM %s 
		ORDER BY id
		LIMIT $1
	`, tableName)

	rows, err := w.DB.DB.Query(query, limit)
	if err != nil {
		return nil, fmt.Errorf("查询数据样本失败: %v", err)
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
			return nil, fmt.Errorf("扫描数据失败: %v", err)
		}
		data = append(data, item)
	}

	return data, nil
}
