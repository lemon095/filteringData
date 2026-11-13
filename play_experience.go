package main

import (
	"math"
	"math/rand"
)

// 体验优化作用档位：rtp_levels.go 中 1–13、20/30/40/50
func shouldOptimizePlayExperience(rtpLevel float64) bool {
	n := int(rtpLevel)
	if float64(n) != rtpLevel {
		return false
	}
	if n >= 1 && n <= 13 {
		return true
	}
	switch n {
	case 20, 30, 40, 50:
		return true
	}
	return false
}

type playPrizeKind int

const (
	prizeDry playPrizeKind = iota
	prizeFeedback          // (0, 2]
	prizeMid               // (2, 5]
	prizeBig               // (5, 20]
	prizeMega              // >= 20
)

func itemMultiplier(item GameResultData) float64 {
	if item.TB <= 0 {
		if item.AW > 0 {
			return math.Inf(1)
		}
		return 0
	}
	return item.AW / item.TB
}

func classifyPlayPrize(item GameResultData) playPrizeKind {
	if item.AW <= 0 {
		return prizeDry
	}
	m := itemMultiplier(item)
	if m <= 0 || math.IsInf(m, 0) {
		// TB 异常但有 AW：按金额粗分，避免落入 dry 影响连黑统计
		if item.AW > 0 {
			return prizeFeedback
		}
		return prizeDry
	}
	if m <= 2 {
		return prizeFeedback
	}
	if m <= 5 {
		return prizeMid
	}
	if m <= 20 {
		return prizeBig
	}
	return prizeMega
}

func jitterGap(rng *rand.Rand, base, jitter int) int {
	if jitter <= 0 {
		return base
	}
	g := base + rng.Intn(2*jitter+1) - jitter
	if g < 1 {
		return 1
	}
	return g
}

// OptimizePlayExperience 体验序列重排：只改顺序，不改条数/金额。
// - 大奖(5-20x)/特大奖(>=20x) 不能相邻、不能太近、间距带随机抖动
// - 连续 dry 达 4 后，随后 3 局内至少 1 次任意奖励；缓冲内优先用 ≤10x（实际只用 feedback/mid）
// - 前 10% 不出现特大奖(>=20x)
func OptimizePlayExperience(data []GameResultData, rtpLevel float64, rng *rand.Rand, printf func(string, ...interface{})) {
	if len(data) == 0 || rng == nil || !shouldOptimizePlayExperience(rtpLevel) {
		return
	}
	if printf == nil {
		printf = func(string, ...interface{}) {}
	}

	n := len(data)
	var dry, feedback, mid, big, mega []GameResultData
	var kept []GameResultData
	skippedOver100 := 0
	for _, item := range data {
		m := itemMultiplier(item)
		if item.AW > 0 && !math.IsInf(m, 0) && m > 99 {
			skippedOver100++
			continue // 超过99倍不参与（入口选数已排除）
		}
		kept = append(kept, item)
		switch classifyPlayPrize(item) {
		case prizeDry:
			dry = append(dry, item)
		case prizeFeedback:
			feedback = append(feedback, item)
		case prizeMid:
			mid = append(mid, item)
		case prizeBig:
			big = append(big, item)
		case prizeMega:
			mega = append(mega, item)
		}
	}
	if skippedOver100 > 0 {
		printf("⚠️ [体验优化] 丢弃 %d 条 >99x 数据（不允许选中）\n", skippedOver100)
		if len(kept) != n {
			printf("⚠️ [体验优化] 存在 >99x，条数将变化 %d→%d，跳过体验重排以保持条数\n", n, len(kept))
			return
		}
	}

	printf("🎮 [体验优化] 档位=%.0f 分类: dry=%d feedback=%d mid=%d big=%d mega=%d\n",
		rtpLevel, len(dry), len(feedback), len(mid), len(big), len(mega))

	shuffleSlice := func(s []GameResultData) {
		rng.Shuffle(len(s), func(i, j int) { s[i], s[j] = s[j], s[i] })
	}
	shuffleSlice(dry)
	shuffleSlice(feedback)
	shuffleSlice(mid)
	shuffleSlice(big)
	shuffleSlice(mega)

	// 底材：dry + feedback + mid（缓冲只从 feedback/mid 取，保证 ≤5x ≤10x）
	base := make([]GameResultData, 0, len(dry)+len(feedback)+len(mid))
	base = append(base, dry...)
	base = append(base, feedback...)
	base = append(base, mid...)
	shuffleSlice(base)

	fixDryBuffer(base, rng)

	first10 := int(math.Ceil(float64(n) * 0.1))
	if first10 < 0 {
		first10 = 0
	}

	// 先按当前底材长度估算插入位，再把 big/mega 插入完整序列
	result := insertSpacedPrizes(base, big, mega, first10, rng, printf)

	if len(result) != n {
		printf("⚠️ [体验优化] 重排后条数异常: 期望 %d 实际 %d，回退原顺序\n", n, len(result))
		return
	}

	// 再扫一遍连黑缓冲（插入 big/mega 后可能拉长 dry 间隔，也可能在缓冲窗外插入）
	fixDryBuffer(result, rng)
	// 插入后若 mega 落进前10%，再挪出去
	enforceNoMegaInFirst10(result, first10, rng)

	copy(data, result)
	logPlayExperienceStats(data, printf)
}

// fixDryBuffer：连续 dry≥4 后，随后 3 局内至少 1 次 ≤10x 奖励（用 feedback/mid 交换）
func fixDryBuffer(seq []GameResultData, rng *rand.Rand) {
	if len(seq) == 0 {
		return
	}

	isDry := func(item GameResultData) bool {
		return classifyPlayPrize(item) == prizeDry
	}
	isSoftWin := func(item GameResultData) bool {
		k := classifyPlayPrize(item)
		return k == prizeFeedback || k == prizeMid
	}

	for i := 0; i < len(seq); {
		if !isDry(seq[i]) {
			i++
			continue
		}
		// 统计从 i 开始的 dry streak
		j := i
		for j < len(seq) && isDry(seq[j]) {
			j++
		}
		streak := j - i
		if streak < 4 {
			i = j
			continue
		}

		// 缓冲窗口：streak 结束后的最多 3 局
		bufStart := j
		bufEnd := j + 3
		if bufEnd > len(seq) {
			bufEnd = len(seq)
		}

		// 缓冲内优先保证有 ≤10x 奖励（feedback/mid）；若是 >10x 则尽量换成 soft
		hasSoft := false
		highInBuf := -1
		for k := bufStart; k < bufEnd; k++ {
			if isSoftWin(seq[k]) {
				hasSoft = true
				break
			}
			if !isDry(seq[k]) {
				m := itemMultiplier(seq[k])
				if m > 0 && m <= 10 {
					hasSoft = true
					break
				}
				if m > 10 && highInBuf < 0 {
					highInBuf = k
				}
			}
		}
		if hasSoft {
			i = j
			continue
		}

		findSoftAfter := func(from int) int {
			for k := from; k < len(seq); k++ {
				if isSoftWin(seq[k]) {
					return k
				}
			}
			return -1
		}

		if highInBuf >= 0 {
			if softIdx := findSoftAfter(bufEnd); softIdx >= 0 {
				seq[highInBuf], seq[softIdx] = seq[softIdx], seq[highInBuf]
				i = j
				continue
			}
			// 换不到 soft 时，已有任意奖励也算过关
			i = j
			continue
		}

		// 窗口内全是 dry：用 soft 换入
		swapIdx := -1
		for k := bufStart; k < bufEnd; k++ {
			if isDry(seq[k]) {
				swapIdx = k
				break
			}
		}
		if swapIdx < 0 {
			i = j
			continue
		}
		if softIdx := findSoftAfter(bufEnd); softIdx >= 0 {
			seq[swapIdx], seq[softIdx] = seq[softIdx], seq[swapIdx]
		}
		i = j
	}

	// 硬约束：连续 dry 不得超过 6 —— 若仍超限，强制与后方 soft 交换
	streak := 0
	for i := 0; i < len(seq); i++ {
		if isDry(seq[i]) {
			streak++
			if streak > 6 {
				softIdx := -1
				for k := i + 1; k < len(seq); k++ {
					if isSoftWin(seq[k]) {
						softIdx = k
						break
					}
				}
				if softIdx < 0 {
					for k := 0; k < i; k++ {
						if isSoftWin(seq[k]) {
							softIdx = k
							break
						}
					}
				}
				if softIdx >= 0 {
					seq[i], seq[softIdx] = seq[softIdx], seq[i]
					streak = 0
				}
			}
		} else {
			streak = 0
		}
	}
	_ = rng
}

func isHighPrize(item GameResultData) bool {
	k := classifyPlayPrize(item)
	return k == prizeBig || k == prizeMega
}

func insertSpacedPrizes(base, big, mega []GameResultData, first10 int, rng *rand.Rand, printf func(string, ...interface{})) []GameResultData {
	// 用占位标记：先把 big/mega 按约束插入到「可插入槽」列表
	type slotMark struct {
		item GameResultData
		kind playPrizeKind
	}

	seq := make([]GameResultData, len(base))
	copy(seq, base)

	// 先插更稀缺的 mega，再插 big（同类内部已打乱）
	highItems := make([]slotMark, 0, len(big)+len(mega))
	for _, it := range mega {
		highItems = append(highItems, slotMark{item: it, kind: prizeMega})
	}
	for _, it := range big {
		highItems = append(highItems, slotMark{item: it, kind: prizeBig})
	}

	placedPos := make([]int, 0, len(highItems))

	minGapFor := func(kind playPrizeKind) int {
		if kind == prizeMega {
			return jitterGap(rng, 25, 6) // 19~31
		}
		return jitterGap(rng, 12, 4) // 8~16
	}

	for _, hi := range highItems {
		need := minGapFor(hi.kind)
		candidates := make([]int, 0, len(seq)+1)

		// 可插入位置：0..len(seq)（插到 index 处，原元素后移）
		lo := 0
		if hi.kind == prizeMega {
			lo = first10 // 前10% 禁 mega
		}
		for pos := lo; pos <= len(seq); pos++ {
			ok := true
			for _, p := range placedPos {
				d := pos - p
				if d < 0 {
					d = -d
				}
				// 相邻禁止：距离至少 2（中间隔至少 1 个非高奖）
				// 同时满足 need：与已放置高奖的间距
				if d < need {
					ok = false
					break
				}
				// 插入后与左右元素也不能是高奖相邻：检查 pos-1 与 pos（插入后原 pos 变成右边）
			}
			if !ok {
				continue
			}
			if pos > 0 && isHighPrize(seq[pos-1]) {
				continue
			}
			if pos < len(seq) && isHighPrize(seq[pos]) {
				continue
			}
			candidates = append(candidates, pos)
		}

		// 放宽：若无候选，降低 need 再试
		if len(candidates) == 0 {
			for relax := 1; relax <= 3 && len(candidates) == 0; relax++ {
				relaxedNeed := need - relax*3
				if relaxedNeed < 2 {
					relaxedNeed = 2
				}
				for pos := lo; pos <= len(seq); pos++ {
					ok := true
					for _, p := range placedPos {
						d := pos - p
						if d < 0 {
							d = -d
						}
						if d < relaxedNeed {
							ok = false
							break
						}
					}
					if !ok {
						continue
					}
					if pos > 0 && isHighPrize(seq[pos-1]) {
						continue
					}
					if pos < len(seq) && isHighPrize(seq[pos]) {
						continue
					}
					candidates = append(candidates, pos)
				}
			}
		}

		insertAt := len(seq)
		if len(candidates) > 0 {
			insertAt = candidates[rng.Intn(len(candidates))]
		} else {
			// 保底：找任意不相邻位置
			found := false
			for pos := lo; pos <= len(seq); pos++ {
				if pos > 0 && isHighPrize(seq[pos-1]) {
					continue
				}
				if pos < len(seq) && isHighPrize(seq[pos]) {
					continue
				}
				insertAt = pos
				found = true
				break
			}
			if !found {
				printf("⚠️ [体验优化] 无法为 kind=%d 找到合法插入位，追加到末尾\n", hi.kind)
				insertAt = len(seq)
			}
		}

		// 插入
		seq = append(seq, GameResultData{})
		copy(seq[insertAt+1:], seq[insertAt:])
		seq[insertAt] = hi.item

		// 更新已放置位置（insertAt 之后的 +1）
		for i := range placedPos {
			if placedPos[i] >= insertAt {
				placedPos[i]++
			}
		}
		placedPos = append(placedPos, insertAt)
	}

	return seq
}

func enforceNoMegaInFirst10(seq []GameResultData, first10 int, rng *rand.Rand) {
	if first10 <= 0 || first10 >= len(seq) {
		return
	}
	for i := 0; i < first10; i++ {
		if classifyPlayPrize(seq[i]) != prizeMega {
			continue
		}
		// 与后段非高奖（优先 soft）交换
		swapIdx := -1
		for k := first10; k < len(seq); k++ {
			if !isHighPrize(seq[k]) {
				swapIdx = k
				break
			}
		}
		if swapIdx < 0 {
			for k := first10; k < len(seq); k++ {
				if classifyPlayPrize(seq[k]) == prizeBig {
					swapIdx = k
					break
				}
			}
		}
		if swapIdx >= 0 {
			seq[i], seq[swapIdx] = seq[swapIdx], seq[i]
		}
	}
	_ = rng
}

func logPlayExperienceStats(data []GameResultData, printf func(string, ...interface{})) {
	maxDry := 0
	dry := 0
	var highPos []int

	for i, item := range data {
		k := classifyPlayPrize(item)
		if k == prizeDry {
			dry++
			if dry > maxDry {
				maxDry = dry
			}
		} else {
			dry = 0
		}
		if k == prizeBig || k == prizeMega {
			highPos = append(highPos, i)
		}
	}
	if dry > maxDry {
		maxDry = dry
	}

	// 复查：连续 dry≥4 后窗口
	violations := 0
	for i := 0; i < len(data); {
		if classifyPlayPrize(data[i]) != prizeDry {
			i++
			continue
		}
		j := i
		for j < len(data) && classifyPlayPrize(data[j]) == prizeDry {
			j++
		}
		if j-i >= 4 {
			bufEnd := j + 3
			if bufEnd > len(data) {
				bufEnd = len(data)
			}
			ok := false
			for k := j; k < bufEnd; k++ {
				if classifyPlayPrize(data[k]) != prizeDry {
					ok = true
					break
				}
			}
			if !ok && j < len(data) {
				// 若已到表尾且窗口不足，不记违规
				if bufEnd-j >= 1 {
					violations++
				}
			} else if !ok && j >= len(data) {
				violations++
			}
		}
		i = j
	}

	minHighGap := -1
	adjacentHigh := 0
	for i := 1; i < len(highPos); i++ {
		gap := highPos[i] - highPos[i-1]
		if gap == 1 {
			adjacentHigh++
		}
		if minHighGap < 0 || gap < minHighGap {
			minHighGap = gap
		}
	}

	printf("📊 [体验优化] 校验: maxDry=%d dryBufferViolations=%d highCount=%d minHighGap=%d adjacentHigh=%d\n",
		maxDry, violations, len(highPos), minHighGap, adjacentHigh)
}
