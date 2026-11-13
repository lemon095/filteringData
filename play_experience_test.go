package main

import (
	"math/rand"
	"testing"
)

func makeItem(id int, tb, aw float64) GameResultData {
	return GameResultData{ID: id, TB: tb, AW: aw}
}

func TestOptimizePlayExperience_DryBufferAndSpacing(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	// 构造：大量 dry、若干 soft、若干 big/mega
	var data []GameResultData
	id := 1
	for i := 0; i < 60; i++ {
		data = append(data, makeItem(id, 1, 0))
		id++
	}
	for i := 0; i < 25; i++ {
		data = append(data, makeItem(id, 1, 1.5)) // feedback
		id++
	}
	for i := 0; i < 15; i++ {
		data = append(data, makeItem(id, 1, 3)) // mid
		id++
	}
	for i := 0; i < 8; i++ {
		data = append(data, makeItem(id, 1, 10)) // big 5-20
		id++
	}
	for i := 0; i < 4; i++ {
		data = append(data, makeItem(id, 1, 30)) // mega
		id++
	}

	beforeSum := 0.0
	for _, it := range data {
		beforeSum += it.AW
	}

	OptimizePlayExperience(data, 9, rng, func(string, ...interface{}) {})

	if len(data) != 112 {
		t.Fatalf("len changed: %d", len(data))
	}
	afterSum := 0.0
	for _, it := range data {
		afterSum += it.AW
	}
	if afterSum != beforeSum {
		t.Fatalf("sum aw changed: %v -> %v", beforeSum, afterSum)
	}

	// 最大连黑 <= 6
	maxDry := 0
	dry := 0
	for _, it := range data {
		if classifyPlayPrize(it) == prizeDry {
			dry++
			if dry > maxDry {
				maxDry = dry
			}
		} else {
			dry = 0
		}
	}
	if maxDry > 6 {
		t.Fatalf("maxDry=%d > 6", maxDry)
	}

	// dry>=4 后 3 局内有奖励
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
					// 缓冲内最好 ≤10x
					m := itemMultiplier(data[k])
					if m > 10 {
						t.Fatalf("buffer reward >10x at %d: mult=%v", k, m)
					}
					ok = true
					break
				}
			}
			if !ok && j < len(data) {
				t.Fatalf("dry streak %d at %d without buffer reward", j-i, i)
			}
		}
		i = j
	}

	// 高奖不相邻
	lastHigh := -999
	first10 := int(float64(len(data))*0.1 + 0.999999) // ceil-ish
	if first10 < 1 {
		first10 = 1
	}
	for i, it := range data {
		k := classifyPlayPrize(it)
		if k == prizeMega && i < len(data)/10 {
			t.Fatalf("mega in first 10%% at %d", i)
		}
		if k == prizeBig || k == prizeMega {
			if i-lastHigh == 1 {
				t.Fatalf("adjacent high prizes at %d", i)
			}
			lastHigh = i
		}
	}
}

func TestShouldOptimizePlayExperience(t *testing.T) {
	if !shouldOptimizePlayExperience(9) || !shouldOptimizePlayExperience(50) {
		t.Fatal("expected 9/50 enabled")
	}
	if shouldOptimizePlayExperience(14) || shouldOptimizePlayExperience(200) {
		t.Fatal("expected 14/200 disabled")
	}
}
