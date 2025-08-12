# FilteringData

一个用 Go 语言开发的数据过滤项目。

## 项目结构

```
filteringData/
├── main.go                 # 主程序入口
├── config.go               # 配置文件处理
├── models.go               # 数据结构定义
├── database.go             # 数据库连接和操作
├── simple_rtp_filter.go    # 简化RTP控制筛选器
├── db_writer.go            # 数据库写入器
├── progress_monitor.go     # 进度监控工具
├── go.mod                  # Go模块文件
├── go.sum                  # Go模块依赖锁定文件
├── config.yaml             # 项目配置文件（不提交到版本控制）
├── config.example.yaml     # 配置文件示例
├── .gitignore              # Git忽略文件
└── README.md               # 项目说明文档
```

## 配置文件

### 创建配置文件

1. 复制配置文件示例：

```bash
cp config.example.yaml config.yaml
```

2. 编辑 `config.yaml` 文件，修改以下配置：
   - **数据库配置**: PostgreSQL 连接信息
   - **游戏 ID**: 您的游戏标识符
   - **CSV 表前缀**: 数据表前缀
   - **奖项比例**: 大奖、巨奖、超巨奖、夺宝比例
   - **玩法比例**: 普通玩法和特殊玩法比例
   - **RTP 配置**: 各玩法的返奖率

## 快速开始

### 运行项目

```bash
go run main.go
```

### 构建项目

```bash
go build -o bin/filteringData main.go
```

### 运行构建的二进制文件

```bash
./bin/filteringData
```

### 查看磁盘空间（df -h）

```bash
df -h
```

输出字段含义（macOS 常见格式）：

- **Filesystem**: 文件系统设备或卷名（例如 `/dev/disk3s1`）
- **Size**: 总容量（人类可读单位，如 Gi）
- **Used**: 已使用容量
- **Avail**: 可用容量
- **Capacity/Use%**: 使用率百分比（已用/总量）
- **iused/ifree/%iused**: 已用/可用 inode 以及 inode 使用率（inode 数量表示可创建的文件/目录个数）
- **Mounted on**: 挂载点（该卷挂载到的目录）

说明：

- Linux 发行版通常显示 `Use%` 而非 `Capacity`，并且可能不显示 inode 列；核心含义相同。
- 当 `Use%` 或 `Capacity` 接近 100% 时，写入/构建可能失败；若 `%iused` 100%，表示 inode 用尽，即使容量未满也无法新建文件。

## 运行方式（main.go）

当前可执行支持以下子命令：

```bash
# 1) 生成普通流程 JSON（输出到 output/<gameId>）
./filteringData generate

# 2) 从 output/<gameId> 导入到数据库（全量）
./filteringData import

# 3) 仅导入指定 rtpLevel 的文件（例如 93）
./filteringData import 93

# 4) 生成“购买夺宝”模式 JSON（输出到 output/<gameId>_fb）
./filteringData generateFb

# 5) 从 output/<gameId>_fb 导入到数据库
./filteringData importFb

# 6) 仅导入指定 rtpLevel 的“购买夺宝”文件
./filteringData importFb 93
```

补充说明：

- 运行前需配置 `config.yaml`，其中 `game.id` 用于确定读写子目录；`game.isFb=true` 时可使用购买模式。
- 普通导入与购买导入默认写入同一张目标表：`"<output_table_prefix><gameId>"`（例如 `"GameResults_93"`）。购买导入会将 `rtpLevel` 写成数值型（如 `13.1`）。
- 生成的 JSON 文件命名形如：`GameResults_<rtpLevel>_<srNumber>.json`。

## 开发环境

- Go 1.21+

## 功能特性

- [x] PostgreSQL 数据库连接
- [x] YAML 配置文件支持
- [x] 数据过滤功能（异常数据过滤：aw < tb \* 100）
- [x] 分层数据筛选（按奖励类型和玩法类型）
- [x] 比例控制筛选
- [x] 随机抽样算法
- [x] 统计信息计算
- [x] JSON 结果输出
- [ ] 日志记录优化
- [ ] 单元测试

## 数据筛选逻辑

### 核心特性

- **RTP 约束筛选**: 支持普通玩法和特殊玩法的 RTP 目标控制
- **动态数量调整**: 大奖、巨奖、超巨奖数量可根据数据可用性动态调整
- **智能算法**: 使用贪心算法和多次尝试来优化 RTP 达标率
- **容差控制**: 允许 ±0.005 的 RTP 偏差

### 筛选条件

1. **异常数据过滤**: `aw < tb * 100` (盈利不能超过投注的 100 倍)
2. **RTP 约束**:
   - 普通玩法 RTP: 目标值 ± 0.005
   - 特殊玩法 RTP: 目标值 ± 0.005
3. **奖励类型筛选** (数量可动态调整):
   - 大奖 (gwt=2): 按配置比例筛选，数据不足时减少
   - 巨奖 (gwt=3): 按配置比例筛选，数据不足时减少
   - 超巨奖 (gwt=4): 按配置比例筛选，数据不足时减少
4. **玩法类型筛选**:
   - 普通玩法 (sp=false): 按配置比例筛选，优化 RTP
   - 特殊玩法 (sp=true): 按配置比例筛选，优化 RTP

### 筛选流程

1. 检查数据表是否存在
2. 获取所有符合基础条件的数据
3. 按奖励类型和玩法类型分类数据
4. 使用智能算法进行多次尝试筛选:
   - 选择奖励数据（动态调整数量）
   - 使用贪心算法优化玩法数据的 RTP
   - 计算 RTP 偏差得分
   - 选择最优解
5. 验证 RTP 达标情况
6. 输出详细统计信息和 JSON 结果文件

### 算法优势

- **自适应**: 根据实际数据可用性调整筛选策略
- **精确控制**: 通过多次尝试确保 RTP 尽可能接近目标
- **灵活性**: 支持数量不足时的优雅降级
- **透明度**: 提供详细的 RTP 分析和达标情况报告

## 数据库表结构

程序需要以下字段的 PostgreSQL 表：

- `id`: 主键
- `tb`: 投注额 (integer)
- `aw`: 盈利额 (double precision)
- `gwt`: 奖励类型 (integer, 2=大奖, 3=巨奖, 4=超巨奖)
- `sp`: 是否特殊玩法 (boolean, true=特殊, false=普通)
- `fb`: 是否为购买 (integer)
- `gd`: 原数据 (jsonb)
- `createdAt`: 创建时间 (timestamp)
- `updatedAt`: 更新时间 (timestamp)

## 贡献

欢迎提交 Issue 和 Pull Request！

## 许可证

MIT License
