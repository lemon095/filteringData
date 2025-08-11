# 数据导入工具使用说明

## 概述

这个工具支持两种运行模式：

1. **生成模式 (generate)**：生成 RTP 测试数据并保存为 JSON 文件
2. **导入模式 (import)**：将 JSON 文件导入到 PostgreSQL 数据库

## 使用方法

### 1. 生成模式

```bash
./filteringData generate
```

- 从数据库读取原始数据
- 生成 RTP 测试数据
- 保存为 JSON 文件到 `output/` 目录
- 文件命名格式：`GameResults_{RtpLevel}_{TestNumber}.json`

### 2. 导入模式

```bash
./filteringData import
```

- 自动扫描 `output/` 目录下的所有 JSON 文件
- 按文件名严格排序（RtpLevel_TestNumber）
- 批量导入到数据库表 `GameResults_{gameId}`

## 文件处理顺序

工具会严格按照以下顺序处理文件：

- 按 RtpLevel 升序排列
- 同一 RtpLevel 内按 TestNumber 升序排列
- 例如：`GameResults_15_1.json` → `GameResults_15_2.json` → `GameResults_16_1.json`

## 数据库表结构

导入时会自动创建目标表（如果不存在）：

```sql
CREATE TABLE "GameResults_93" (
    "id" SERIAL PRIMARY KEY,
    "rtpLevel" NUMERIC NOT NULL,
    "srNumber" INTEGER NOT NULL,
    "srId" INTEGER NOT NULL,
    "bet" NUMERIC NOT NULL,
    "win" NUMERIC NOT NULL,
    "detail" JSONB,
    "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 索引

自动创建以下索引以提高查询性能：

- `rtpLevel_idx`：RTP 等级索引
- `srNumber_idx`：测试次数索引
- `srId_idx`：序列 ID 索引
- `rtpLevel_srNumber_idx`：RTP 等级+测试次数复合索引
- `rtpLevel_srNumber_srId_idx`：三字段复合索引
- `detail_gin_idx`：JSONB 字段 GIN 索引

## 批量处理

- 默认批次大小：1000 条记录
- 可在 `config.yaml` 中配置：`settings.batch_size`
- 使用数据库事务确保数据一致性

## JSON 文件格式

生成的 JSON 文件包含以下结构：

```json
{
  "rtpLevel": 15.0,
  "srNumber": 1,
  "data": [
    {
      "ID": 1,
      "TB": 100,
      "AW": 150.5,
      "GD": {
        "Data": {...}
      }
    }
  ],
  "metadata": {
    "bet": 100.0,
    "totalRecords": 1000,
    "generatedAt": "2024-01-01T12:00:00Z",
    "gameId": 93
  }
}
```

## 配置说明

确保 `config.yaml` 包含以下配置：

```yaml
game:
  id: 93

tables:
  output_table_prefix: "GameResults_"

settings:
  batch_size: 1000
```

## 注意事项

1. **文件顺序**：确保 JSON 文件按正确顺序命名
2. **数据库连接**：确保 PostgreSQL 服务运行且配置正确
3. **权限**：确保有创建表和插入数据的权限
4. **数据完整性**：JSON 格式保持原始数据的完整性和类型
