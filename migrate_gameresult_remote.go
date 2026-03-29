package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

// migrateGameIDs 源库中要迁移的游戏 ID 列表
var migrateGameIDs = []int{
	58, 33, 24, 59, 62, 64, 70, 82, 86, 107, 93, 130,
	1338274, 1418544, 1432733, 1489936, 1601012, 1615454, 1702123, 1804577,
}

// migrateFBValues 迁移时按 fb 分桶拉取与上限控制
var migrateFBValues = []int{0, 1, 2, 3}

const migrateBatchSize = 500

// 每个 fb 最多迁移条数（按 id 升序；源不足则只迁现有条数）
const (
	migrateMaxRowsFB0 = 2000
	migrateMaxRowsFB1 = 500
	migrateMaxRowsFB2 = 500
	migrateMaxRowsFB3 = 500
)

func migrateMaxRowsForFB(fb int) int {
	switch fb {
	case 0:
		return migrateMaxRowsFB0
	case 1:
		return migrateMaxRowsFB1
	case 2:
		return migrateMaxRowsFB2
	case 3:
		return migrateMaxRowsFB3
	default:
		return 0
	}
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// 目标库连接从环境变量读取（勿把密码写进代码或提交仓库）：
//   MIGRATE_TARGET_HOST, MIGRATE_TARGET_PORT, MIGRATE_TARGET_USER, MIGRATE_TARGET_PASSWORD
// 可选：MIGRATE_TARGET_DBNAME（未设置时默认数据库名 mpg）、MIGRATE_TARGET_SSLMODE（默认 disable）

func targetDBConfigFromEnv() (DatabaseConfig, error) {
	host := os.Getenv("MIGRATE_TARGET_HOST")
	portStr := os.Getenv("MIGRATE_TARGET_PORT")
	user := os.Getenv("MIGRATE_TARGET_USER")
	pass := os.Getenv("MIGRATE_TARGET_PASSWORD")
	dbname := os.Getenv("MIGRATE_TARGET_DBNAME")
	if dbname == "" {
		dbname = "mpg"
	}
	ssl := os.Getenv("MIGRATE_TARGET_SSLMODE")
	if ssl == "" {
		ssl = "disable"
	}
	tz := os.Getenv("MIGRATE_TARGET_TIMEZONE")
	if tz == "" {
		tz = "UTC"
	}
	if host == "" || user == "" || pass == "" {
		return DatabaseConfig{}, fmt.Errorf("请设置环境变量 MIGRATE_TARGET_HOST, MIGRATE_TARGET_USER, MIGRATE_TARGET_PASSWORD（及可选 PORT/DBNAME/SSLMODE）")
	}
	port := 5432
	if portStr != "" {
		p, err := strconv.Atoi(portStr)
		if err != nil {
			return DatabaseConfig{}, fmt.Errorf("MIGRATE_TARGET_PORT 无效: %w", err)
		}
		port = p
	}
	return DatabaseConfig{
		Host:     host,
		Port:     port,
		User:     user,
		Password: pass,
		Dbname:   dbname,
		SSLMode:  ssl,
		Timezone: tz,
	}, nil
}

func openSQLDB(cfg DatabaseConfig) (*sql.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s TimeZone=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Dbname, cfg.SSLMode, cfg.Timezone)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// openTargetDatabaseForJSONImport 使用 MIGRATE_TARGET_* 连接远程库并包装为 *Database。
// 供 import-remote 写入 GameResults_*，以及 generate4-remote 只读拉取 GameResultData_* 源表。
func openTargetDatabaseForJSONImport(cfg *Config) (*Database, error) {
	targetCfg, err := targetDBConfigFromEnv()
	if err != nil {
		return nil, err
	}
	sqlDB, err := openSQLDB(targetCfg)
	if err != nil {
		return nil, fmt.Errorf("连接目标库: %w", err)
	}
	maxOpen, maxIdle, connLife, connIdle, pingInterval := configureSQLDBPool(sqlDB, cfg)
	log.Printf("import-remote 目标库已连接 [%s:%d db=%s 连接池:最大%d/空闲%d 生存:%v 空闲:%v]",
		targetCfg.Host, targetCfg.Port, targetCfg.Dbname, maxOpen, maxIdle, connLife, connIdle)
	return &Database{
		DB:           sqlDB,
		Config:       cfg,
		lastPingTime: time.Now(),
		pingInterval: pingInterval,
	}, nil
}

func regclassExists(db *sql.DB, fqName string) (bool, error) {
	var reg sql.NullString
	err := db.QueryRow(`SELECT to_regclass($1)::text`, fqName).Scan(&reg)
	if err != nil {
		return false, err
	}
	return reg.Valid && reg.String != "", nil
}

func gameResultDataTableName(prefix string, gameID int) string {
	return fmt.Sprintf("%s%d", prefix, gameID)
}

func quotedTableIdent(prefix string, gameID int) string {
	return fmt.Sprintf(`"%s"`, gameResultDataTableName(prefix, gameID))
}

func createGameResultDataTableOnRemote(db *sql.DB, prefix string, gameID int) error {
	tableName := gameResultDataTableName(prefix, gameID)
	seqName := fmt.Sprintf("%s%d_id_seq", prefix, gameID)
	q := fmt.Sprintf(`
CREATE SEQUENCE IF NOT EXISTS "%s";

CREATE TABLE IF NOT EXISTS "%s" (
	id INTEGER PRIMARY KEY DEFAULT nextval('"%s"'),
	tb DOUBLE PRECISION NOT NULL,
	aw DOUBLE PRECISION NOT NULL,
	gwt INTEGER NOT NULL,
	sp BOOLEAN NOT NULL DEFAULT false,
	fb INTEGER NOT NULL DEFAULT 0,
	gd JSONB,
	"createdAt" TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
	"updatedAt" TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

ALTER TABLE "%s"
	ALTER COLUMN id SET DEFAULT nextval('"%s"');
`, seqName, tableName, seqName, tableName, seqName)
	_, err := db.Exec(q)
	return err
}

func syncSequence(db *sql.DB, prefix string, gameID int) error {
	tableName := gameResultDataTableName(prefix, gameID)
	seqName := fmt.Sprintf("%s%d_id_seq", prefix, gameID)
	q := fmt.Sprintf(
		`SELECT setval('"%s"', COALESCE((SELECT MAX(id) FROM "%s"), 1))`,
		seqName, tableName,
	)
	_, err := db.Exec(q)
	return err
}

func runMigrateGameResultToRemote(sourceEnv string) error {
	cfg, err := LoadConfig("config.yaml")
	if err != nil {
		return fmt.Errorf("加载 config.yaml: %w", err)
	}
	if sourceEnv == "" {
		sourceEnv = cfg.DefaultEnv
	}

	targetCfg, err := targetDBConfigFromEnv()
	if err != nil {
		return err
	}

	srcDB, err := NewDatabase(cfg, sourceEnv)
	if err != nil {
		return fmt.Errorf("连接源库: %w", err)
	}
	defer srcDB.Close()

	dstDB, err := openSQLDB(targetCfg)
	if err != nil {
		return fmt.Errorf("连接目标库: %w", err)
	}
	defer dstDB.Close()

	prefix := cfg.Tables.SourceTablePrefix
	if prefix == "" {
		prefix = "GameResultData_"
	}

	log.Printf("📡 源库: config.yaml 的 environments[%s] | 目标库: %s:%d db=%s",
		sourceEnv, targetCfg.Host, targetCfg.Port, targetCfg.Dbname)
	log.Printf("ℹ️  数据路径: 源库 SELECT → 本进程 → 目标库 INSERT；跨网络时下行(读源)+上行(写目标)都会走流量，总量约等于所选行（含 gd JSON）的字节体积。")

	var noSourceTableGameIDs []int
	var noDataAnyFBGameIDs []int

	totalGames := len(migrateGameIDs)
	for gi, gameID := range migrateGameIDs {
		srcTable := quotedTableIdent(prefix, gameID)
		existsSrc, err := regclassExists(srcDB.DB, fmt.Sprintf("public.%s", srcTable))
		if err != nil {
			return fmt.Errorf("检查源表 %s: %w", srcTable, err)
		}
		if !existsSrc {
			log.Printf("⏭️  [%d/%d] 跳过 gameId=%d：源库不存在表 %s", gi+1, totalGames, gameID, srcTable)
			noSourceTableGameIDs = append(noSourceTableGameIDs, gameID)
			continue
		}

		dstFQ := fmt.Sprintf(`public.%s`, srcTable)
		existsDst, err := regclassExists(dstDB, dstFQ)
		if err != nil {
			return fmt.Errorf("检查目标表 gameId=%d: %w", gameID, err)
		}
		if existsDst {
			log.Printf("⏭️  [%d/%d] 跳过 gameId=%d：目标库已存在表 %s，不导入", gi+1, totalGames, gameID, srcTable)
			continue
		}

		var totalToCopy int64
		for _, fb := range migrateFBValues {
			var c int64
			q := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE "fb" = $1`, srcTable)
			if err := srcDB.DB.QueryRow(q, fb).Scan(&c); err != nil {
				return fmt.Errorf("统计 fb=%d gameId=%d: %w", fb, gameID, err)
			}
			if c == 0 {
				continue
			}
			capN := int64(migrateMaxRowsForFB(fb))
			totalToCopy += min64(c, capN)
		}
		if totalToCopy == 0 {
			log.Printf("⏭️  [%d/%d] 跳过 gameId=%d：fb=0/1/2/3 均无数据", gi+1, totalGames, gameID)
			noDataAnyFBGameIDs = append(noDataAnyFBGameIDs, gameID)
			continue
		}

		gameStart := time.Now()
		log.Printf("▶️  [%d/%d] gameId=%d 开始迁移，预计约 %d 行（fb=0≤%d，fb=1/2/3≤%d/%d/%d，按 id 升序）",
			gi+1, totalGames, gameID, totalToCopy, migrateMaxRowsFB0, migrateMaxRowsFB1, migrateMaxRowsFB2, migrateMaxRowsFB3)

		if err := createGameResultDataTableOnRemote(dstDB, prefix, gameID); err != nil {
			return fmt.Errorf("目标库建表 gameId=%d: %w", gameID, err)
		}
		log.Printf("✅ gameId=%d 已在目标库创建表 %s", gameID, srcTable)

		insertSQL := fmt.Sprintf(
			`INSERT INTO %s (id, tb, aw, gwt, sp, fb, gd, "createdAt", "updatedAt") VALUES `,
			srcTable,
		)

		for _, fb := range migrateFBValues {
			var sourceCnt int64
			countQ := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE "fb" = $1`, srcTable)
			if err := srcDB.DB.QueryRow(countQ, fb).Scan(&sourceCnt); err != nil {
				return err
			}
			if sourceCnt == 0 {
				log.Printf("   [%d/%d gameId=%d] fb=%d：无数据，跳过", gi+1, totalGames, gameID, fb)
				continue
			}
			limitCap := migrateMaxRowsForFB(fb)
			cnt := min64(sourceCnt, int64(limitCap))

			fbStart := time.Now()
			log.Printf("   [%d/%d gameId=%d] fb=%d：源 %d 行，本次复制 %d 行（上限 %d）…",
				gi+1, totalGames, gameID, fb, sourceCnt, cnt, limitCap)

			ctx := context.Background()
			rows, err := srcDB.DB.QueryContext(ctx,
				fmt.Sprintf(`SELECT id, tb, aw, gwt, sp, fb, gd, "createdAt", "updatedAt" FROM %s WHERE "fb" = $1 ORDER BY id LIMIT $2`, srcTable),
				fb, limitCap,
			)
			if err != nil {
				return fmt.Errorf("读取 gameId=%d fb=%d: %w", gameID, fb, err)
			}

			batch := make([]GameResultData, 0, migrateBatchSize)
			inserted := int64(0)
			logProgress := func(justFlushed int) {
				if cnt <= 0 {
					return
				}
				pct := float64(inserted) / float64(cnt) * 100
				elapsed := time.Since(fbStart).Seconds()
				var rps float64
				if elapsed > 0.01 {
					rps = float64(inserted) / elapsed
				}
				log.Printf("      fb=%d 进度 %d/%d (%.1f%%) 最近一批 %d 行 ~%.0f 行/秒",
					fb, inserted, cnt, pct, justFlushed, rps)
			}
			flush := func() error {
				if len(batch) == 0 {
					return nil
				}
				batchN := len(batch)
				var sb strings.Builder
				sb.WriteString(insertSQL)
				args := make([]interface{}, 0, len(batch)*9)
				for i, it := range batch {
					if i > 0 {
						sb.WriteString(",")
					}
					na := len(args)
					sb.WriteString(fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
						na+1, na+2, na+3, na+4, na+5, na+6, na+7, na+8, na+9))
					var gd interface{}
					if it.GD.Data != nil {
						b, err := it.GD.Value()
						if err != nil {
							return err
						}
						gd = b
					} else {
						gd = nil
					}
					args = append(args, it.ID, it.TB, it.AW, it.GWT, it.SP, it.FB, gd, it.CreatedAt, it.UpdatedAt)
				}
				_, err := dstDB.Exec(sb.String(), args...)
				if err != nil {
					return err
				}
				inserted += int64(batchN)
				logProgress(batchN)
				return nil
			}

			for rows.Next() {
				var it GameResultData
				var gdBytes []byte
				if err := rows.Scan(&it.ID, &it.TB, &it.AW, &it.GWT, &it.SP, &it.FB, &gdBytes, &it.CreatedAt, &it.UpdatedAt); err != nil {
					rows.Close()
					return err
				}
				if len(gdBytes) > 0 {
					_ = it.GD.Scan(gdBytes)
				}
				batch = append(batch, it)
				if len(batch) >= migrateBatchSize {
					if err := flush(); err != nil {
						rows.Close()
						return fmt.Errorf("写入 gameId=%d fb=%d: %w", gameID, fb, err)
					}
					batch = batch[:0]
				}
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return err
			}
			if err := flush(); err != nil {
				return fmt.Errorf("写入 gameId=%d fb=%d: %w", gameID, fb, err)
			}
			if inserted != cnt {
				log.Printf("   ⚠️ gameId=%d fb=%d：计数核对 inserted=%d count=%d", gameID, fb, inserted, cnt)
			}
			log.Printf("   ✅ gameId=%d fb=%d：完成 %d 行，耗时 %v", gameID, fb, inserted, time.Since(fbStart).Round(time.Millisecond))
		}

		if err := syncSequence(dstDB, prefix, gameID); err != nil {
			return fmt.Errorf("同步序列 gameId=%d: %w", gameID, err)
		}
		log.Printf("🎉 [%d/%d] gameId=%d 迁移完成，共 %d 行，本游戏耗时 %v", gi+1, totalGames, gameID, totalToCopy, time.Since(gameStart).Round(time.Millisecond))
	}

	log.Println("======== 未导出汇总（仅：源表不存在，或源表存在但 fb=0/1/2/3 均无任何行）========")
	if len(noSourceTableGameIDs) == 0 && len(noDataAnyFBGameIDs) == 0 {
		log.Println("无：上述两类均未出现。")
	} else {
		if len(noSourceTableGameIDs) > 0 {
			log.Printf("源表不存在（未迁移任何数据）gameId 共 %d 个: %v", len(noSourceTableGameIDs), noSourceTableGameIDs)
		}
		if len(noDataAnyFBGameIDs) > 0 {
			log.Printf("源表存在但 fb=0/1/2/3 均无数据 gameId 共 %d 个: %v", len(noDataAnyFBGameIDs), noDataAnyFBGameIDs)
		}
	}

	return nil
}
