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