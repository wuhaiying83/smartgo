package client

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker/client/rebalance"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	set "github.com/deckarep/golang-set"
	"net"
)

// ConsumerManager 消费者管理
// Author gaoyanlei
// Since 2017/8/9
type ConsumerManager struct {
	consumerTable             *sync.Map
	ConsumerIdsChangeListener rebalance.ConsumerIdsChangeListener
	ChannelExpiredTimeout     int64
}

// NewConsumerOffsetManager 初始化ConsumerOffsetManager
// Author gaoyanlei
// Since 2017/8/9
func NewConsumerManager(consumerIdsChangeListener rebalance.ConsumerIdsChangeListener) *ConsumerManager {
	var consumerManager = new(ConsumerManager)
	consumerManager.consumerTable = sync.NewMap()
	consumerManager.ConsumerIdsChangeListener = consumerIdsChangeListener
	consumerManager.ChannelExpiredTimeout = 1000 * 120
	return consumerManager
}

func (cm *ConsumerManager) GetConsumerGroupInfo(group string) *ConsumerGroupInfo {
	value, err := cm.consumerTable.Get(group)
	if err != nil {
		return nil
	}

	if consumerGroupInfo, ok := value.(*ConsumerGroupInfo); ok {
		return consumerGroupInfo
	}

	return nil
}

func (cm *ConsumerManager) FindSubscriptionData(group, topic string) *heartbeat.SubscriptionData {
	consumerGroupInfo := cm.GetConsumerGroupInfo(group)
	if consumerGroupInfo != nil {
		return consumerGroupInfo.FindSubscriptionData(topic)
	}
	return nil
}

// registerConsumer 注册Consumer
// Author gaoyanlei
// Since 2017/8/24
func (cm *ConsumerManager) RegisterConsumer(group string, conn net.Conn, consumeType heartbeat.ConsumeType,
	messageModel heartbeat.MessageModel, consumeFromWhere heartbeat.ConsumeFromWhere, subList set.Set) bool {
	consumerGroupInfo := cm.GetConsumerGroupInfo(group)
	if nil == consumerGroupInfo {
		tmp := NewConsumerGroupInfo(group, consumeType, messageModel, consumeFromWhere)
		prev, err := cm.consumerTable.Put(group, tmp)
		if err != nil || prev == nil {
			consumerGroupInfo = tmp
		} else {
			if consumerGroupInfo, ok := prev.(*ConsumerGroupInfo); ok {
				consumerGroupInfo = consumerGroupInfo
			}
		}
	}
	r1 := consumerGroupInfo.UpdateChannel(conn, consumeType, messageModel, consumeFromWhere)
	// TODO
	return r1
}

func (cm *ConsumerManager) UnregisterConsumer(group string, channelInfo *ChannelInfo) {

}

// ScanNotActiveChannel 扫描不活跃的通道
// Author rongzhihong
// Since 2017/9/11
func (cm *ConsumerManager) ScanNotActiveChannel() {
	iterator := cm.consumerTable.Iterator()
	for iterator.HasNext() {
		key, value, _ := iterator.Next()
		group, ok := key.(string)
		if !ok {
			continue
		}
		consumerGroupInfo, ok := value.(*ConsumerGroupInfo)
		if !ok {
			continue
		}
		channelInfoTable := consumerGroupInfo.ConnTable
		chanIterator := channelInfoTable.Iterator()
		for chanIterator.HasNext() {
			_, clientValue, _ := chanIterator.Next()
			channelInfo, ok := clientValue.(*ChannelInfo)
			if !ok {
				continue
			}
			diff := timeutil.CurrentTimeMillis() - channelInfo.LastUpdateTimestamp
			if diff > cm.ChannelExpiredTimeout {
				logger.Warn("SCAN: remove expired channel from ConsumerManager consumerTable. channel=%s, consumerGroup=%s",
					channelInfo.Addr, group)
				channelInfo.Conn.Close()
				chanIterator.Remove()
			}
		}

		if channelInfoTable.IsEmpty() {
			logger.Warn("SCAN: remove expired channel from ConsumerManager consumerTable, all clear, consumerGroup=%s", group)
			iterator.Remove()
		}
	}
}

// ScanNotActiveChannel 扫描不活跃的通道
// Author rongzhihong
// Since 2017/9/11
func (cm *ConsumerManager) DoChannelCloseEvent(remoteAddr string, conn net.Conn) {
	iterator := cm.consumerTable.Iterator()
	for iterator.HasNext() {
		key, value, _ := iterator.Next()
		group, ok := key.(string)
		if !ok {
			continue
		}
		consumerGroupInfo, ok := value.(*ConsumerGroupInfo)
		if !ok {
			continue
		}
		isRemoved := consumerGroupInfo.doChannelCloseEvent(remoteAddr, conn)
		if isRemoved {
			if consumerGroupInfo.ConnTable.IsEmpty() {
				remove, err := cm.consumerTable.Remove(group)
				if err != nil {
					logger.Error(err)
					continue
				}
				if remove != nil {
					logger.Info("ungister consumer ok, no any connection, and remove consumer group, %s", group)
				}
			}
			cm.ConsumerIdsChangeListener.ConsumerIdsChanged(group, consumerGroupInfo.getAllChannel())
		}
	}
}
