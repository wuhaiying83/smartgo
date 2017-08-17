package process

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"strings"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
)
// MQAdminImpl: 运维方法
// Author: yintongqiang
// Since:  2017/8/13

type MQAdminImpl struct {
	mQClientFactory *MQClientInstance
}

func NewMQAdminImpl(mQClientFactory *MQClientInstance) *MQAdminImpl {
	return &MQAdminImpl{
		mQClientFactory:mQClientFactory    }
}

func (adminImpl *MQAdminImpl)MaxOffset(mq message.MessageQueue) int64 {
	brokerAddr := adminImpl.mQClientFactory.FindBrokerAddressInPublish(mq.BrokerName)
	if strings.EqualFold(brokerAddr, "") {
		adminImpl.mQClientFactory.UpdateTopicRouteInfoFromNameServerByTopic(mq.Topic)
		brokerAddr = adminImpl.mQClientFactory.FindBrokerAddressInPublish(mq.BrokerName)
	}
	if !strings.EqualFold(brokerAddr, "") {
		return adminImpl.mQClientFactory.MQClientAPIImpl.GetMaxOffset(brokerAddr, mq.Topic, mq.QueueId, 1000 * 3)
	} else {
		panic("The broker[" + mq.BrokerName + "] not exist")
	}
	return -1
}

func (adminImpl *MQAdminImpl)CreateTopic(key, newTopic string, queueNum, topicSysFlag int) {
	topicRouteData := adminImpl.mQClientFactory.MQClientAPIImpl.GetTopicRouteInfoFromNameServer(key, 1000 * 3)
	brokerDataList := topicRouteData.BrokerDatas
	if brokerDataList != nil && len(brokerDataList) > 0 {
		for _, brokerData := range brokerDataList {
			addr := brokerData.BrokerAddrs[stgcommon.MASTER_ID]
			if !strings.EqualFold(addr, "") {
				topicConfig := stgcommon.TopicConfig{TopicName:newTopic, ReadQueueNums:queueNum, WriteQueueNums:queueNum, TopicSysFlag:topicSysFlag}
				adminImpl.mQClientFactory.MQClientAPIImpl.CreateTopic(addr, key, topicConfig, 1000 * 3)
			}
		}
	}
}