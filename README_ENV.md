# 环境变量配置说明

## 概述

现在系统支持通过环境变量来配置不同环境的数据库连接信息。这样可以避免在配置文件中硬编码敏感信息，提高安全性。

## 环境变量设置

### 环境变量命名规则

每个环境都有对应的环境变量前缀：
- `HT_` - 香港测试环境 (hk-test)
- `BT_` - 巴西测试环境 (br-test)  
- `BP_` - 巴西生产环境 (br-prod)
- `UP_` - 美国生产环境 (us-prod)
- `HP_` - 香港生产环境 (hk-prod)

### 必需的环境变量

每个环境都需要设置以下环境变量：

```bash
# 数据库主机地址
{ENV_PREFIX}_DB_HOST=数据库主机地址

# 数据库端口
{ENV_PREFIX}_DB_PORT=5432

# 数据库用户名
{ENV_PREFIX}_DB_USER=用户名

# 数据库密码
{ENV_PREFIX}_DB_PASSWORD=密码

# 数据库名称
{ENV_PREFIX}_DB_NAME=数据库名
```

### 示例配置

```bash
# 香港测试环境 (ht)
export HT_DB_HOST=mpg-rds-aurora-ae.cluster-c34408aa43zx.ap-east-1.rds.amazonaws.com
export HT_DB_PORT=5432
export HT_DB_USER=devadmin
export HT_DB_PASSWORD=*ztnEY8n12
export HT_DB_NAME=filtering_data

# 巴西测试环境 (bt)
export BT_DB_HOST=mpg-rds-aurora-ae.cluster-c34408aa43zx.ap-east-1.rds.amazonaws.com
export BT_DB_PORT=5432
export BT_DB_USER=devadmin
export BT_DB_PASSWORD=*ztnEY8n12
export BT_DB_NAME=filtering_data

# 巴西生产环境 (bp)
export BP_DB_HOST=mpg-rds-aurora-ae.cluster-c34408aa43zx.ap-east-1.rds.amazonaws.com
export BP_DB_PORT=5432
export BP_DB_USER=devadmin
export BP_DB_PASSWORD=*ztnEY8n12
export BP_DB_NAME=filtering_data

# 美国生产环境 (up)
export UP_DB_HOST=mpg-rds-aurora-ae.cluster-c34408aa43zx.ap-east-1.rds.amazonaws.com
export UP_DB_PORT=5432
export UP_DB_USER=devadmin
export UP_DB_PASSWORD=*ztnEY8n12
export UP_DB_NAME=filtering_data

# 香港生产环境 (hp)
export HP_DB_HOST=mpg-rds-aurora-ae.cluster-c34408aa43zx.ap-east-1.rds.amazonaws.com
export HP_DB_PORT=5432
export HP_DB_USER=devadmin
export HP_DB_PASSWORD=*ztnEY8n12
export HP_DB_NAME=filtering_data
```

## 使用方法

### 1. 设置环境变量

在运行程序之前，确保设置了相应的环境变量：

```bash
# 方法1: 直接在命令行设置
export HT_DB_HOST=your-host
export HT_DB_PORT=5432
# ... 其他变量

# 方法2: 使用 .env 文件 (需要额外工具支持)
# 创建 .env 文件并设置变量

# 方法3: 在脚本中设置
#!/bin/bash
export HT_DB_HOST=your-host
export HT_DB_PORT=5432
# ... 其他变量
./filteringData import 112 ht
```

### 2. 运行命令

设置环境变量后，可以使用相应的环境代码运行命令：

```bash
# 香港测试环境
./filteringData import 112 ht

# 巴西测试环境  
./filteringData import 112 bt

# 巴西生产环境
./filteringData import 112 bp

# 美国生产环境
./filteringData import 112 up

# 香港生产环境
./filteringData import 112 hp
```

## 配置文件说明

`config.yaml` 文件中的数据库配置现在使用环境变量占位符：

```yaml
environments:
  hk-test:
    host: "${HT_DB_HOST}"
    port: "${HT_DB_PORT}"
    user: "${HT_DB_USER}"
    password: "${HT_DB_PASSWORD}"
    dbname: "${HT_DB_NAME}"
    sslmode: "require"
    timezone: "UTC"
  # ... 其他环境
```

## 注意事项

1. **安全性**: 不要在代码仓库中提交包含真实密码的配置文件
2. **环境变量**: 确保在运行程序前设置了所有必需的环境变量
3. **默认值**: local 环境仍然使用硬编码配置，不需要环境变量
4. **错误处理**: 如果环境变量未设置，程序会显示相应的错误信息

## 故障排除

如果遇到数据库连接问题：

1. 检查环境变量是否正确设置：`echo $HT_DB_HOST`
2. 检查环境变量值是否正确
3. 检查网络连接和数据库服务状态
4. 查看程序输出的错误信息
