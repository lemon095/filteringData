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

#### 多环境数据库配置

项目支持多环境数据库配置，可以轻松切换不同的数据库环境：

```yaml
# 默认环境（不指定环境时使用）
default_env: "local"

# 多环境数据库配置
environments:
  # 本地环境（默认）
  local:
    host: "127.0.0.1"
    port: 5432
    username: "postgres"
    password: "123666"
    database: "postgres"
    sslmode: "disable"
    timezone: "Asia/Shanghai"

  # 香港测试环境
  hk-test:
    host: "mpg-rds-aurora-ae.cluster-c34408aa43zx.ap-east-1.rds.amazonaws.com"
    port: 5432
    username: "devadmin"
    password: "*ztnEY8n12"
    database: "mpgdb"
    sslmode: "require"
    timezone: "Asia/Shanghai"

  # 巴西测试环境
  br-test:
    host: "mpg-db-test.cnwcy0eo4x3k.sa-east-1.rds.amazonaws.com"
    port: 5432
    username: "devadmin"
    password: "*ztnEY8n12"
    database: "mpgdb"
    sslmode: "require"
    timezone: "Asia/Shanghai"

  # 更多环境...
```

#### 其他配置项

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

### 基础命令

```bash
# 1) 生成普通流程 JSON（输出到 output/<gameId>）
./filteringData generate

# 2) 生成"购买夺宝"模式 JSON（输出到 output/<gameId>_fb）
./filteringData generateFb
```

### 多环境导入命令

支持多环境数据库连接，可以将数据导入到不同的数据库环境中。

#### 普通模式导入 (import)

```bash
# 导入所有文件（使用默认环境：本地）
./filteringData import

# 按档位导入（在 output/<config.Game.ID>/ 目录下查找指定档位文件）
./filteringData import 103                    # 导入档位103，使用默认环境
./filteringData import 103 bt                 # 导入档位103，使用巴西测试环境

# 按游戏ID导入（导入整个 output/<gameId>/ 目录）
./filteringData import 93                     # 如果output/93/目录存在，导入所有文件
./filteringData import 93 bt                  # 导入output/93/，使用巴西测试环境

# 完整参数导入
./filteringData import 93 level1 bt           # 导入output/93/中level1档位，使用巴西测试环境
```

#### 购买夺宝模式导入 (importFb)

```bash
# 导入所有FB文件（使用默认环境）
./filteringData importFb

# 按档位导入FB文件
./filteringData importFb 103                  # 导入档位103，使用默认环境
./filteringData importFb 103 bt               # 导入档位103，使用巴西测试环境

# 按游戏ID导入FB文件（导入整个 output/<gameId>_fb/ 目录）
./filteringData importFb 93                   # 如果output/93_fb/目录存在，导入所有文件
./filteringData importFb 93 bt                # 导入output/93_fb/，使用巴西测试环境

# 完整参数导入
./filteringData importFb 93 level1 bt         # 导入output/93_fb/中level1档位，使用巴西测试环境
```

### 环境代码说明

支持以下环境代码（支持完整名称和简短别名）：

| 环境名称  | 简短代码 | 说明             | 数据库主机                                                         |
| --------- | -------- | ---------------- | ------------------------------------------------------------------ |
| `local`   | `l`      | 本地环境（默认） | 127.0.0.1                                                          |
| `hk-test` | `ht`     | 香港测试环境     | mpg-rds-aurora-ae.cluster-c34408aa43zx.ap-east-1.rds.amazonaws.com |
| `br-test` | `bt`     | 巴西测试环境     | mpg-db-test.cnwcy0eo4x3k.sa-east-1.rds.amazonaws.com               |
| `br-prod` | `bp`     | 巴西正式环境     | mpg-db-cluster.cluster-cnwcy0eo4x3k.sa-east-1.rds.amazonaws.com    |
| `us-prod` | `up`     | 美国正式环境     | mpg-db-cluster.cluster-cvwq2uy263dd.us-east-1.rds.amazonaws.com    |
| `hk-prod` | `hp`     | 香港正式环境     | mpgdb.cluster-c34408aa43zx.ap-east-1.rds.amazonaws.com             |

### 使用示例

```bash
# 常用命令示例
./filteringData importFb 103 bt               # 导入103_fb到巴西测试环境
./filteringData import 105 bp                 # 导入档位105到巴西正式环境
./filteringData importFb 93 us-prod           # 导入93_fb到美国正式环境（完整环境名）
./filteringData import local                  # 导入所有文件到本地环境

# 错误处理示例
./filteringData import 103                    # 如果output/103不存在但output/103_fb存在
                                             # 会提示使用: ./filteringData importFb 103
```

### 命令逻辑说明

1. **目录检测逻辑**：
   - `import <number>`：先检查 `output/<number>/` 是否存在
     - 存在：当作 gameId，导入整个目录
     - 不存在：当作档位 ID，在 `output/<config.Game.ID>/` 下查找对应文件
2. **文件过滤规则**：

   - 档位过滤：查找 `GameResults_<档位>_*.json` 格式的文件
   - 例如：档位 103 会匹配 `GameResults_103_1.json`, `GameResults_103_2.json` 等

3. **环境配置**：
   - 不指定环境时使用默认环境（local）
   - 环境配置在 `config.yaml` 的 `environments` 部分
   - 每个环境有独立的数据库连接配置

补充说明：

- 运行前需配置 `config.yaml`，其中 `game.id` 用于确定读写子目录；`game.isFb=true` 时可使用购买模式。
- 普通导入与购买导入默认写入同一张目标表：`"<output_table_prefix><gameId>"`（例如 `"GameResults_93"`）。购买导入会将 `rtpLevel` 写成数值型（如 `13.1`）。
- 生成的 JSON 文件命名形如：`GameResults_<rtpLevel>_<srNumber>.json`。

### 文件同步到远端（rsync）

如下命令用于仅同步本地输出目录中名称匹配 `GameResults_15*` 的文件到远端服务器指定目录：

```bash
rsync -avz \
  -e "ssh -i /Users/wangfukang/Desktop/mpgKey/ec2-server-ape.pem -o StrictHostKeyChecking=no" \
  --include 'GameResults_15*' --exclude '*' \
  /Users/wangfukang/Desktop/project-go/filteringData/output/93/ \
  ec2-user@43.198.187.137:/home/ec2-user/filteringData/output/93/
```

```bash
rsync -avz \
  -e "ssh -i /Users/shihao/Desktop/shihao/hk.pem -o StrictHostKeyChecking=no" \
  --include 'GameResults_1*' --exclude '*' \
  /Users/shihao/Desktop/lemon/filteringData/output/93/ \
  ec2-user@43.198.187.137:/home/ec2-user/filteringData/output/93/

  #压缩
  rsync -avzz \
  -e "ssh -i /Users/wangfukang/Desktop/mpgKey/ec2-server-ape.pem -o StrictHostKeyChecking=no" \
  --include 'GameResults_1*' --exclude '*' \
  /Users/wangfukang/Desktop/project-go/filteringData/output/112/ \
  ec2-user@18.162.45.129:/home/ec2-user/filteringData/output/112/


  #压缩
  rsync -avzz \
  -e "ssh -i /Users/wangfukang/Desktop/mpgKey/巴西.pem -o StrictHostKeyChecking=no" \
  --include 'GameResults_1*' --exclude '*' \
  /Users/wangfukang/Desktop/project-go/filteringData/output/112/ \
  ec2-user@18.229.148.69:/home/ec2-user/filteringData/output/112/


  rsync -azvh --progress \
  -e "ssh -i /Users/wangfukang/Desktop/mpgKey/ec2-server-ape.pem -o StrictHostKeyChecking=no" \
  /Users/wangfukang/Desktop/project-go/filteringData/output/105/ \
  ec2-user@18.162.45.129:/home/ec2-user/filteringData/output/105/
```

参数说明：

- **-a (archive)**: 归档模式，递归复制，并尽量保留权限、时间戳、符号链接等元数据。
- **-v (verbose)**: 输出详细过程，便于观察同步进度与匹配结果。
- **-z (compress)**: 传输时启用压缩，降低网络带宽占用（CPU 与网络做权衡）。
- **-e "ssh ..."**: 指定远程 shell 为 `ssh` 并附带选项。
  - **-i /path/to/key.pem**: 指定 SSH 私钥文件用于免密认证。
  - **-o StrictHostKeyChecking=no**: 首次连接自动接受主机指纹（降低交互，存在一定安全风险）。
- **--include 'GameResults_15\*'**: 仅包含匹配该模式的文件。
- **--exclude '\*'**: 排除其他一切未被包含规则匹配到的文件。
- **源路径 `/.../output/93/`（带斜杠）**: 表示同步该目录“内容”。若不带末尾斜杠则会在目标下创建一层 `93` 目录。
- **目标路径 `user@host:/path/.../93/`**: 目标机上的接收目录，需确保用户拥有写入权限。

更稳妥的写法（确保能遍历子目录，同时只同步 15 档位文件）：

```bash
rsync -avz \
  -e "ssh -i /Users/wangfukang/Desktop/mpgKey/ec2-server-ape.pem -o StrictHostKeyChecking=no" \
  --include '*/' --include 'GameResults_15*' --exclude '*' \
  /Users/wangfukang/Desktop/project-go/filteringData/output/93/ \
  ec2-user@43.198.187.137:/home/ec2-user/filteringData/output/93/
```

补充建议：

- **首次执行**: 若目标目录不存在，可先在远端创建：`ssh -i <key> ec2-user@<ip> 'mkdir -p /home/ec2-user/filteringData/output/93'`。
- **显示进度**: 追加 `--progress` 可查看每个文件的实时进度。
- **安全性**: 生产环境建议移除 `-o StrictHostKeyChecking=no`，并在已知主机中预先加入主机指纹。

### 全量同步整个项目目录到远端

将本地整个项目目录内容同步到远端目录（不做 include/exclude 过滤）：

```bash
rsync -av \
  -e "ssh -i /Users/wangfukang/Desktop/mpgKey/ec2-server-ape.pem -o StrictHostKeyChecking=no" \
  /Users/wangfukang/Desktop/project-go/filteringData/ \
  ec2-user@43.198.187.137:/home/ec2-user/filteringData/
```

参数与路径语义说明：

- **-a (archive)**: 归档模式，包含递归、保留时间戳权限等（等价于 `-rlptgoD`）。
- **-v (verbose)**: 详细输出，便于排错与确认同步范围。
- **-e "ssh ..."**: 指定传输通道为 SSH 并附带选项。
  - **-i /path/to/key.pem**: 使用指定私钥免密登录远端。
  - **-o StrictHostKeyChecking=no**: 自动接受主机指纹（便捷但降低安全性，生产建议去掉）。
- **源路径以斜杠结尾 `/.../filteringData/`**: 表示“同步该目录的内容”。
  - 若去掉尾部斜杠（`/.../filteringData`），则会在目标目录下创建一层 `filteringData` 子目录。
- **目标路径 `/home/ec2-user/filteringData/`**: 表示把内容放入该目录内；建议先确保该目录存在（如：`ssh -i <key> ec2-user@<ip> 'mkdir -p /home/ec2-user/filteringData'`）。

重要提示：

- 该命令会把“整个项目目录”同步过去。rsync 默认不会参考 `.gitignore`，因此 `.git/`、`output/`、本地构建产物等也会被传输，除非显式排除。
- 若你不希望传输输出大文件或版本目录，建议添加排除规则：

```bash
rsync -av \
  -e "ssh -i /Users/wangfukang/Desktop/mpgKey/ec2-server-ape.pem -o StrictHostKeyChecking=no" \
  --exclude '.git/' --exclude 'output/' --exclude 'filteringData' \
  /Users/wangfukang/Desktop/project-go/filteringData/ \
  ec2-user@43.198.187.137:/home/ec2-user/filteringData/
```

```bash
rsync -av \
  -e "ssh -i /Users/wangfukang/Desktop/mpgKey/ec2-server-ape.pem -o StrictHostKeyChecking=no" \
  --exclude '.git/' --exclude 'output/' --exclude 'filteringData' \
  /Users/wangfukang/Desktop/project-go/filteringData/ \
  ec2-user@43.198.187.137:/home/ec2-user/filteringData/
```

常用可选项：

- **--progress**: 显示每个文件的实时进度。
- **--dry-run**: 试运行（不实际传输），用于先检查会同步/删除哪些文件。
- **--delete**: 让目标与源完全镜像（删除目标端“源中不存在”的文件）。谨慎使用，建议先配合 `--dry-run`。
- **--chmod**: 统一设置权限（如 `--chmod=Du=rwx,Dgo=rx,Fu=rw,Fgo=r`）。
- **--chown**: 设置目标端所有者（如 `--chown=ec2-user:ec2-user`，需要目标端权限支持）。

小贴士：

- 路径中若包含空格，请使用引号包裹。
- `-a` 会尝试保留所有者/用户组；非 root 账号可能无法在目标端保留 owner/group（出现提示属正常，不影响文件内容）。

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

## 多环境功能详解

### 功能特性

- **多环境支持**: 支持本地、测试、正式等多个数据库环境
- **环境隔离**: 每个环境使用独立的数据库连接配置
- **简化命令**: 支持环境代码简写，提高操作效率
- **智能检测**: 自动检测目录结构，提供友好的错误提示
- **向后兼容**: 保持与旧版本命令的兼容性

### 环境配置管理

#### 添加新环境

在 `config.yaml` 的 `environments` 部分添加新环境：

```yaml
environments:
  # 添加新的测试环境
  new-test:
    host: "your-new-test-host.com"
    port: 5432
    username: "testuser"
    password: "testpass"
    database: "testdb"
    sslmode: "require"
    timezone: "Asia/Shanghai"
```

同时在 `config.go` 的环境映射表中添加对应的简短代码：

```go
var envMapping = map[string]string{
    // 现有环境...
    "new-test": "new-test",
    "nt":       "new-test",  // 简短代码
}
```

#### 环境连接验证

程序启动时会显示连接的环境信息：

```
数据库连接成功 [环境: br-test, 主机: mpg-db-test.cnwcy0eo4x3k.sa-east-1.rds.amazonaws.com]
```

### 最佳实践

1. **开发环境**: 使用 `local` 环境进行本地开发和测试
2. **测试环境**: 使用 `*-test` 环境进行功能验证
3. **生产环境**: 使用 `*-prod` 环境进行正式部署
4. **环境隔离**: 不同环境的数据完全隔离，避免误操作
5. **权限管理**: 生产环境建议使用只读用户进行数据查询

### 故障排除

#### 常见问题

1. **连接超时**:

   ```
   数据库连接测试失败: context deadline exceeded
   ```

   - 检查网络连接和防火墙设置
   - 确认数据库服务器地址和端口正确

2. **认证失败**:

   ```
   数据库连接测试失败: password authentication failed
   ```

   - 检查用户名和密码是否正确
   - 确认用户是否有访问权限

3. **SSL 连接问题**:

   ```
   数据库连接测试失败: SSL connection error
   ```

   - 检查 `sslmode` 配置是否正确
   - 本地环境通常使用 `disable`，云环境使用 `require`

4. **环境不存在**:
   ```
   环境 'unknown-env' 不存在
   ```
   - 检查环境代码是否正确
   - 查看支持的环境列表：`./filteringData import --help`

### 安全注意事项

1. **配置文件安全**:

   - `config.yaml` 包含敏感信息，不要提交到版本控制
   - 使用 `.gitignore` 排除配置文件

2. **密码管理**:

   - 定期更换数据库密码
   - 使用强密码策略

3. **网络安全**:

   - 生产环境建议使用 VPN 或专网连接
   - 限制数据库访问 IP 白名单

4. **权限控制**:
   - 不同环境使用不同的数据库用户
   - 最小权限原则，只授予必要的权限

## 贡献

欢迎提交 Issue 和 Pull Request！

## 许可证

MIT License
