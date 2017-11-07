package models

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
)

// ConnectionOnline 在线进程列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type ConnectionOnline struct {
	ClusterName     string `json:"clusterName"`     // 集群名称
	Topic           string `json:"topic"`           // 集群名称
	ProduceNums     int    `json:"produceNums"`     // 生产进程总数
	ConsumerGroupId string `json:"consumerGroupId"` // 消费组ID
	ConsumeNums     int    `json:"consumeNums"`     // 消费进程总数
}

// ConnectionDetail 在线进程详情
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type ConnectionDetail struct {
	ConsumerOnLine *ConsumerOnLine `json:"consumer"`
	ProducerOnLine *ProducerOnLine `json:"producer"`
}

// ProducerOnLine 在线生产进程
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type ProducerOnLine struct {
	ClusterName     string             `json:"clusterName"`     // 集群名称
	Topic           string             `json:"topic"`           // topic名称
	ProducerGroupId string             `json:"producerGroupId"` // 生产组者组ID
	Describe        string             `json:"describe"`        // 查询结果的描述
	Connection      []*body.Connection `json:"groups"`          // 在线生产进程
}

// ConsumerOnLine 在线消费进程
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type ConsumerOnLine struct {
	ClusterName string                  `json:"clusterName"` // 集群名称
	Topic       string                  `json:"topic"`       // topic名称
	Describe    string                  `json:"describe"`    // 查询结果的描述
	Connection  []*ConsumerConnectionVo `json:"groups"`      // 在线消费进程
}

// ConsumerConnection 消费者进程
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/7/14
type ConsumerConnectionVo struct {
	ConsumerGroupId     string                 `json:"consumerGroupId"`     // 消费者组ID
	ClientId            string                 `json:"clientId"`            // 消费者客户端实例
	ClientAddr          string                 `json:"clientAddr"`          // 消费者客户端地址
	Language            string                 `json:"language"`            // 客户端语言
	Version             int                    `json:"version"`             // mq版本号
	ConsumeTps          int64                  `json:"consumeTps"`          // 实时消费Tps
	ConsumeFromWhere    string                 `json:"consumeFromWhere"`    // 从哪里开始消费
	ConsumeType         string                 `json:"consumeType"`         // 消费类型(主动、被动)
	DiffTotal           int64                  `json:"diffTotal"`           // 消息堆积总数
	MessageModel        string                 `json:"messageModel"`        // 消息模式(集群、广播)
	SubscribeTopicTable []*SubscribeTopicTable `json:"subscribeTopicTable"` // 消费者订阅Topic列表
}

type SubscribeTopicTable struct {
	Topic           string   `json:"topic"`
	SubString       string   `json:"subString"`
	ClassFilterMode bool     `json:"classFilterMode"`
	TagsSet         []string `json:"tags"`
	CodeSet         []int    `json:"codeSet"`
	SubVersion      int64    `json:"subVersion"`
}

func ToSubscribeTopicTable(subscribeData *heartbeat.SubscriptionData) *SubscribeTopicTable {
	subscribeTopicTable := &SubscribeTopicTable{
		Topic:     subscribeData.Topic,
		SubString: subscribeData.SubString,
	}
	return subscribeTopicTable
}
