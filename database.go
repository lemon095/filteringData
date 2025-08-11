package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/lib/pq"
)

// Database 数据库连接结构体
type Database struct {
	DB     *sql.DB
	Config *Config
}

// NewDatabase 创建数据库连接
func NewDatabase(config *Config) (*Database, error) {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s TimeZone=%s",
		config.Database.Postgres.Host,
		config.Database.Postgres.Port,
		config.Database.Postgres.Username,
		config.Database.Postgres.Password,
		config.Database.Postgres.Database,
		config.Database.Postgres.SSLMode,
		config.Database.Postgres.Timezone,
	)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %v", err)
	}

	// 测试连接
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("数据库连接测试失败: %v", err)
	}

	log.Println("数据库连接成功")
	return &Database{DB: db, Config: config}, nil
}

// Close 关闭数据库连接
func (d *Database) Close() error {
	if d.DB != nil {
		return d.DB.Close()
	}
	return nil
}

// GetTableName 获取源表名（用于读取数据）
func (d *Database) GetTableName() string {
	return fmt.Sprintf("\"%s%d\"", d.Config.Tables.SourceTablePrefix, d.Config.Game.ID)
}

// CheckTableExists 检查源表是否存在
func (d *Database) CheckTableExists() (bool, error) {
	// 获取不带引号的表名用于信息模式查询
	plainTableName := fmt.Sprintf("%s%d", d.Config.Tables.SourceTablePrefix, d.Config.Game.ID)
	query := `
		SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name = $1
		);`

	var exists bool
	err := d.DB.QueryRow(query, plainTableName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("检查表存在性失败: %v", err)
	}

	return exists, nil
}

// GetTotalCount 获取符合基础条件的数据总数
func (d *Database) GetTotalCount() (int, error) {
	tableName := d.GetTableName()
	query := fmt.Sprintf(`
		SELECT COUNT(*) 
		FROM %s 
		WHERE aw < tb * 100
	`, tableName)

	var count int
	err := d.DB.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("获取数据总数失败: %v", err)
	}

	return count, nil
}

// GetWinData 获取所有中奖数据 (aw > 0 且 aw/tb < 100)
func (d *Database) GetWinData() ([]GameResultData, error) {
	tableName := d.GetTableName()
	query := fmt.Sprintf(`
		SELECT id, tb, aw, gwt, sp, fb, gd, "createdAt", "updatedAt"
		FROM %s 
		WHERE aw > 0 AND aw < tb * 100
		ORDER BY id
	`, tableName)

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

// GetNoWinData 获取所有不中奖数据 (aw = 0)
func (d *Database) GetNoWinData() ([]GameResultData, error) {
	tableName := d.GetTableName()
	query := fmt.Sprintf(`
		SELECT id, tb, aw, gwt, sp, fb, gd, "createdAt", "updatedAt"
		FROM %s 
		WHERE aw = 0 And sp != true
		ORDER BY id
	`, tableName)

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
        AND aw < tb * 100
        AND aw <= $%d
        %s
        ORDER BY (CASE WHEN gwt IN (2,3,4) THEN 1 ELSE 0 END), aw DESC
        LIMIT $%d
    `, tableName, argIndex, excludeCondition, argIndex+1)

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
		AND aw < tb * 100
		AND aw >= $%d * (1 - $%d)
		AND aw <= $%d * (1 + $%d)
		%s
		ORDER BY ABS(aw - $%d)
		LIMIT 1
	`, tableName, argIndex, argIndex+1, argIndex+2, argIndex+3, excludeCondition, argIndex+4)

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
