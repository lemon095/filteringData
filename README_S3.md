# S3 配置说明

## 环境变量配置

为了避免在代码中暴露AWS凭证，建议使用环境变量：

### 1. 设置环境变量

```bash
# 设置AWS访问密钥
export AWS_ACCESS_KEY_ID="your_aws_access_key_id_here"
export AWS_SECRET_ACCESS_KEY="your_aws_secret_access_key_here"

# 可选：设置AWS区域
export AWS_DEFAULT_REGION="ap-east-1"
```

### 2. 或者在运行时设置

```bash
# 运行S3导入命令时设置环境变量
AWS_ACCESS_KEY_ID="your_key" AWS_SECRET_ACCESS_KEY="your_secret" ./filteringData import-s3 112,103,105
```

### 3. 配置文件优先级

系统会按以下优先级使用AWS凭证：
1. 环境变量 `AWS_ACCESS_KEY_ID` 和 `AWS_SECRET_ACCESS_KEY`
2. 配置文件 `config.yaml` 中的 `access_key_id` 和 `secret_access_key`

## 安全建议

- 永远不要将真实的AWS凭证提交到Git仓库
- 使用环境变量或AWS IAM角色
- 定期轮换访问密钥
- 使用最小权限原则
