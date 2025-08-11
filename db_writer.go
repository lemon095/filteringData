package main

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
