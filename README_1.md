### 单档位上传

```bash
rsync -avz \
  -e "ssh -i /Users/shihao/Desktop/shihao/hk.pem -o StrictHostKeyChecking=no" \
  --include 'GameResults_1*' --exclude '*' \
  /Users/shihao/Desktop/lemon/filteringData/output/93/ \
  ec2-user@43.198.187.137:/home/ec2-user/filteringData/output/93/

  rsync -avz \
  -e "ssh -i /Users/shihao/Desktop/shihao/hk.pem -o StrictHostKeyChecking=no" \
  --include 'GameResults_9*' --exclude '*' \
  /Users/shihao/Desktop/lemon/filteringData/output/93/ \
  ec2-user@43.198.187.137:/home/ec2-user/filteringData/output/93/
```

### 目录上传

```bash
rsync -av \
  -e "ssh -i /Users/shihao/Desktop/shihao/hk.pem -o StrictHostKeyChecking=no" \
  /Users/shihao/Desktop/lemon/filteringData/output/93_fb/ \
  ec2-user@43.198.187.137:/home/ec2-user/filteringData/output/93_fb/
```

```
ssh -i /Users/shihao/Desktop/shihao/hk.pem ec2-user@43.198.187.137
```

### 查看磁盘空间

```
df -h
```

### 导表执行

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
