### 单档位上传

```bash
rsync -avz \
  -e "ssh -i /Users/shihao/Desktop/shihao/hk.pem -o StrictHostKeyChecking=no" \
  --include 'GameResults_1*' --exclude '*' \
  /Users/shihao/Desktop/lemon/filteringData/output/93/ \
  ec2-user@18.162.45.129:/home/ec2-user/filteringData/output/93/

  rsync -avz \
  -e "ssh -i /Users/shihao/Desktop/shihao/hk.pem -o StrictHostKeyChecking=no" \
  --include 'GameResults_9*' --exclude '*' \
  /Users/shihao/Desktop/lemon/filteringData/output/93/ \
  ec2-user@18.162.45.129:/home/ec2-user/filteringData/output/93/
```

###使用压缩上传

```bash
rsync -azvh --progress \
  -e "ssh -i /Users/shihao/Desktop/shihao/hk.pem -o StrictHostKeyChecking=no" \
  /Users/shihao/Desktop/lemon/filteringData/output/1881268/ \
  ec2-user@18.162.45.129:/home/ec2-user/filteringData/output/1881268/
```

###夺宝购买数据验证

```bash
SELECT count(1) FROM public."GameResults_93"
WHERE "rtpLevel" in ('1.1','2.1','3.1','4.1','5.1','6.1','7.1','8.1','9.1','10.1','11.1','12.1','13.1','14.1','15.1');


DELETE FROM public."GameResults_93"
WHERE "rtpLevel" in ('1.1','2.1','3.1','4.1','5.1','6.1','7.1','8.1','9.1','10.1','11.1','12.1','13.1','14.1','15.1');

SELECT sum(win)/sum(bet) as "rtp", count(1), "rtpLevel" FROM public."GameResults_93"  group by "rtpLevel"

TRUNCATE TABLE "GameResults_92" ;


```

### 表自增 id 从 1 开始

```bash
# 先删
DELETE FROM public."GameResults_108"

# 修改索引
ALTER SEQUENCE "GameResults_108_id_seq" RESTART WITH 1;
```

### 目录上传

```bash
rsync -av \
  -e "ssh -i /Users/shihao/Desktop/shihao/hk.pem -o StrictHostKeyChecking=no" \
  /Users/shihao/Desktop/lemon/filteringData/output/93_fb/ \
  ec2-user@18.162.45.129:/home/ec2-user/filteringData/output/93_fb/
```

```
ssh -i /Users/shihao/Desktop/shihao/hk.pem ec2-user@18.162.45.129
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

### 根据 psid 查记录

```base
SELECT DISTINCT t.*
FROM "GameResults_108" t,
     jsonb_array_elements(t.detail) AS elem  -- 展开detail数组为单行元素
WHERE elem->>'psid' = '215656635046687212';  -- 提取psid并比较
```
