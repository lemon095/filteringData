package main

// RtpLevel RTP等级结构体
type RtpLevel struct {
	RtpNo float64 `json:"rtpNo"`
	Rtp   float64 `json:"rtp"`
}

// RtpLevels 标准RTP等级配置
var RtpLevels = []RtpLevel{
	{RtpNo: 1, Rtp: 0.6},
	{RtpNo: 2, Rtp: 0.7},
	{RtpNo: 3, Rtp: 0.75},
	{RtpNo: 4, Rtp: 0.8},
	{RtpNo: 5, Rtp: 0.85},
	{RtpNo: 6, Rtp: 0.9},
	{RtpNo: 7, Rtp: 0.91},
	{RtpNo: 8, Rtp: 0.92},
	{RtpNo: 9, Rtp: 0.93},
	{RtpNo: 10, Rtp: 0.94},
	{RtpNo: 11, Rtp: 0.95},
	{RtpNo: 12, Rtp: 0.96},
	{RtpNo: 13, Rtp: 0.97},
	{RtpNo: 20, Rtp: 0.2},
	{RtpNo: 30, Rtp: 0.3},
	{RtpNo: 40, Rtp: 0.4},
	{RtpNo: 50, Rtp: 0.5},
	{RtpNo: 120, Rtp: 1.2},
	{RtpNo: 150, Rtp: 1.5},
	{RtpNo: 14, Rtp: 1.5},
	{RtpNo: 15, Rtp: 2},
	{RtpNo: 200, Rtp: 2.0},
	{RtpNo: 300, Rtp: 3.0},
	{RtpNo: 500, Rtp: 5.0},
}

// RtpLevelsTest 测试RTP等级配置
var RtpLevelsTest = []RtpLevel{
	{RtpNo: 120, Rtp: 1.2},
	{RtpNo: 150, Rtp: 1.5},
	{RtpNo: 14, Rtp: 1.5},
	{RtpNo: 15, Rtp: 2},
	{RtpNo: 200, Rtp: 2.0},
	{RtpNo: 300, Rtp: 3.0},
	{RtpNo: 500, Rtp: 5.0},
}

// FbRtpLevels 购买夺宝RTP等级配置
var FbRtpLevels = []RtpLevel{
	{RtpNo: 1, Rtp: 0.6},
	{RtpNo: 2, Rtp: 0.7},
	{RtpNo: 3, Rtp: 0.75},
	{RtpNo: 4, Rtp: 0.8},
	{RtpNo: 5, Rtp: 0.8},
	{RtpNo: 6, Rtp: 0.8},
	{RtpNo: 7, Rtp: 0.8},
	{RtpNo: 8, Rtp: 0.8},
	{RtpNo: 9, Rtp: 0.8},
	{RtpNo: 10, Rtp: 0.8},
	{RtpNo: 11, Rtp: 0.8},
	{RtpNo: 12, Rtp: 0.8},
	{RtpNo: 13, Rtp: 0.8},
	{RtpNo: 14, Rtp: 0.8},
	{RtpNo: 15, Rtp: 0.8},
	{RtpNo: 20, Rtp: 0.2},
	{RtpNo: 30, Rtp: 0.3},
	{RtpNo: 40, Rtp: 0.4},
	{RtpNo: 50, Rtp: 0.5},
	{RtpNo: 120, Rtp: 1.2},
	{RtpNo: 150, Rtp: 1.5},
	{RtpNo: 200, Rtp: 2.0},
	{RtpNo: 300, Rtp: 3.0},
	{RtpNo: 500, Rtp: 5.0},
}
