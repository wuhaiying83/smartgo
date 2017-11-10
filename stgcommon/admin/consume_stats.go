package admin

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
)

// OffsetWrapper 消费者统计
// Author rongzhihong
// Since 2017/9/19
type ConsumeStats struct {
	ConsumeTps  int64                                    `json:"consumeTps"`
	OffsetTable map[*message.MessageQueue]*OffsetWrapper `json:"offsetTable"`
}

// NewConsumeStats 初始化
// Author rongzhihong
// Since 2017/9/19
func NewConsumeStats() *ConsumeStats {
	consumeStats := new(ConsumeStats)
	consumeStats.OffsetTable = make(map[*message.MessageQueue]*OffsetWrapper)
	return consumeStats
}

// ComputeTotalDiff 偏移量差值
// Author rongzhihong
// Since 2017/9/19
func (stats *ConsumeStats) ComputeTotalDiff() int64 {
	diffTotal := int64(0)
	for _, wrapper := range stats.OffsetTable {
		diff := wrapper.BrokerOffset - wrapper.ConsumerOffset
		diffTotal += diff
	}
	return diffTotal
}

// OffsetWrapper 消费者统计
// Author rongzhihong
// Since 2017/9/19
type ConsumeStatsPlus struct {
	ConsumeTps  int64                     `json:"consumeTps"`
	OffsetTable map[string]*OffsetWrapper `json:"offsetTable"` // key: Topic@BrokerName@QueueId
}

// NewConsumeStats 初始化
// Author rongzhihong
// Since 2017/9/19
func NewConsumeStatsPlus() *ConsumeStatsPlus {
	consumeStats := new(ConsumeStatsPlus)
	consumeStats.OffsetTable = make(map[string]*OffsetWrapper)
	return consumeStats
}
