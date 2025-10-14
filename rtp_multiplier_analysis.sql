SELECT 
    "rtpLevel" as "RTP档位",
    COUNT(1) as "总数量",
    ROUND(SUM(win)/SUM(bet), 6) as "RTP",
    
    -- 不中奖占比（小数形式）
    ROUND(COUNT(CASE WHEN win = 0 THEN 1 END) * 1.0 / COUNT(1), 4) as "noWin",
    
    -- 中奖占比（小数形式）
    ROUND(COUNT(CASE WHEN win > 0 THEN 1 END) * 1.0 / COUNT(1), 4) as "win",
    
    -- 0-1倍占比（小数形式）
    ROUND(COUNT(CASE WHEN win > 0 AND win <= bet THEN 1 END) * 1.0 / COUNT(1), 4) as "0-1",

	 -- 1-2倍占比（小数形式）
    ROUND(COUNT(CASE WHEN win > bet AND win <= bet * 2 THEN 1 END) * 1.0 / COUNT(1), 4) as "1-2",
    
    -- 2-5倍占比（小数形式）
    ROUND(COUNT(CASE WHEN win > bet * 2 AND win <= bet * 5 THEN 1 END) * 1.0 / COUNT(1), 4) as "2-5",
    
    -- 5-10倍占比（小数形式）
    ROUND(COUNT(CASE WHEN win > bet * 5 AND win <= bet * 10 THEN 1 END) * 1.0 / COUNT(1), 4) as "5-10",
    
    -- 10-20倍占比（小数形式）
    ROUND(COUNT(CASE WHEN win > bet * 10 AND win <= bet * 20 THEN 1 END) * 1.0 / COUNT(1), 4) as "10-20",
    
    -- 20-50倍占比（小数形式）
    ROUND(COUNT(CASE WHEN win > bet * 20 AND win <= bet * 50 THEN 1 END) * 1.0 / COUNT(1), 4) as "20-50",
    
    -- 50-100倍占比（小数形式）
    ROUND(COUNT(CASE WHEN win > bet * 50 AND win <= bet * 100 THEN 1 END) * 1.0 / COUNT(1), 4) as "50-100",
    
    -- 100-500倍占比（小数形式）
    ROUND(COUNT(CASE WHEN win > bet * 100 AND win <= bet * 500 THEN 1 END) * 1.0 / COUNT(1), 4) as "100-500",
    
    -- 500倍以上占比（小数形式）
    ROUND(COUNT(CASE WHEN win > bet * 500 THEN 1 END) * 1.0 / COUNT(1), 4) as "500倍以上"
FROM public."GameResults_60"
GROUP BY "rtpLevel"
ORDER BY "rtpLevel";


WITH base_stats AS (
    -- 先计算基础统计量（各区间计数、总记录数等）
    SELECT 
        count(1) FILTER (WHERE "aw"/"tb" > 0 AND "aw"/"tb" <= 1) AS "0-1",
        count(1) FILTER (WHERE "aw"/"tb" > 1 AND "aw"/"tb" <= 2) AS "1-2",
        count(1) FILTER (WHERE "aw"/"tb" > 2 AND "aw"/"tb" <= 3) AS "2-3",
        count(1) FILTER (WHERE "aw"/"tb" > 3 AND "aw"/"tb" <= 5) AS "3-5",
        count(1) FILTER (WHERE "aw"/"tb" > 5 AND "aw"/"tb" <= 10) AS "5-10",
        count(1) FILTER (WHERE "aw"/"tb" > 10 AND "aw"/"tb" <= 15) AS "10-15",
        count(1) FILTER (WHERE "aw"/"tb" > 15 AND "aw"/"tb" <= 20) AS "15-20",
        count(1) FILTER (WHERE "aw"/"tb" > 20 AND "aw"/"tb" <= 50) AS "20-50",
        count(1) FILTER (WHERE "aw"/"tb" > 50) AS ">50",
        count(1) FILTER (WHERE "aw" > 0) AS "win",
        count(1) FILTER (WHERE "aw" = 0) AS "noWin",
        sum("aw")::numeric / sum("tb") AS "rtp",  -- 转换为numeric避免精度丢失
        count(1) AS total  -- 总记录数
    FROM "GameResultData_20063"
)
-- 计算各区间的概率（占比 = 区间计数 / 总记录数）
SELECT 
    round("0-1"::numeric / total, 4) AS "0-1概率",
    round("1-2"::numeric / total, 4) AS "1-2概率",
    round("2-3"::numeric / total, 4) AS "2-3概率",
    round("3-5"::numeric / total, 4) AS "3-5概率",
    round("5-10"::numeric / total, 4) AS "5-10概率",
    round("10-15"::numeric / total, 4) AS "10-15概率",
    round("15-20"::numeric / total, 4) AS "15-20概率",
    round("20-50"::numeric / total, 4) AS "20-50概率",
    round(">50"::numeric / total, 4) AS ">50概率",
    round("win"::numeric / total, 4) AS "赢的概率",
    round("noWin"::numeric / total, 4) AS "未赢的概率",
    round("rtp", 4) AS "rtp",  -- 保留4位小数
    total AS "总记录数"
FROM base_stats;