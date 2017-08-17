package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker/mqtrace"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer/listener"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	commonprotocol "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sysflag"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgstorelog"
	"fmt"
)

type SendMessageProcessor struct {
	*AbstractSendMessageProcessor
	BrokerController *BrokerController
}

func (self *SendMessageProcessor) ProcessRequest(request protocol.RemotingCommand, // TODO ChannelHandlerContext ctx
) *protocol.RemotingCommand {

	if request.Code == commonprotocol.CONSUMER_SEND_MSG_BACK {
		return self.consumerSendMsgBack(request)
	}

	requestHeader := self.parseRequestHeader(request)
	if requestHeader == nil {
		return nil
	}
	mqtraceContext := self.buildMsgContext(requestHeader)
	// TODO  this.executeSendMessageHookBefore(ctx, request, mqtraceContext)
	response := self.sendMessage(request, mqtraceContext, requestHeader)
	// TODO this.executeSendMessageHookAfter(response, mqtraceContext);
	return response
}

// consumerSendMsgBack 客户端返回未消费消息
// Author gaoyanlei
// Since 2017/8/17
func (self *SendMessageProcessor) consumerSendMsgBack( // TODO ChannelHandlerContext ctx
	request protocol.RemotingCommand) (remotingCommand *protocol.RemotingCommand) {
	response := protocol.CreateResponseCommand()
	requestHeader := header.NewConsumerSendMsgBackRequestHeader()

	// 消息轨迹：记录消费失败的消息
	if len(requestHeader.OriginMsgId) > 0 {
		context := new(mqtrace.ConsumeMessageContext)
		context.ConsumerGroup = requestHeader.Group
		context.Topic = requestHeader.OriginTopic
		// TODO context.ClientHost=RemotingHelper.parseChannelRemoteAddr(ctx.channel()
		context.Success = false
		context.Status = string(listener.RECONSUME_LATER)
		messageIds := make(map[string]int64)
		messageIds[requestHeader.OriginMsgId] = requestHeader.Offset
		context.MessageIds = messageIds
		// TODO this.executeConsumeMessageHookAfter(context);
	}

	// 确保订阅组存在
	subscriptionGroupConfig := self.BrokerController.SubscriptionGroupManager.findSubscriptionGroupConfig(requestHeader.Group)
	if subscriptionGroupConfig == nil {
		response.Code = commonprotocol.SUBSCRIPTION_GROUP_NOT_EXIST
		response.Remark = "subscription group not exist"
	}

	// 检查Broker权限
	if constant.IsWriteable(self.BrokerController.BrokerConfig.BrokerPermission) {
		response.Code = commonprotocol.NO_PERMISSION
		response.Remark = "the broker[" + self.BrokerController.BrokerConfig.BrokerIP1 + "] sending message is forbidden"
		return response
	}

	// 如果重试队列数目为0，则直接丢弃消息
	if subscriptionGroupConfig.RetryQueueNums <= 0 {
		response.Code = commonprotocol.SUCCESS
		response.Remark = nil
		return response
	}

	newTopic := stgcommon.GetRetryTopic(requestHeader.Group)
	queueIdInt := 0
	if queueIdInt < 0 {
		num := (self.Rand.Int() % 99999999) % subscriptionGroupConfig.RetryQueueNums
		if num > 0 {
			queueIdInt = num
		} else {
			queueIdInt = -num
		}
	}

	// 如果是单元化模式，则对 topic 进行设置
	topicSysFlag := 0
	if requestHeader.UnitMode {
		topicSysFlag = sysflag.TopicBuildSysFlag(false, true)
	}

	// 检查topic是否存在
	topicConfig, err := self.BrokerController.TopicConfigManager.createTopicInSendMessageBackMethod(newTopic, subscriptionGroupConfig.RetryQueueNums,
		constant.PERM_WRITE|constant.PERM_READ, topicSysFlag)
	if topicConfig == nil || err != nil {
		response.Code = commonprotocol.SYSTEM_ERROR
		response.Remark = "topic[" + newTopic + "] not exist"
		return response
	}

	// 检查topic权限
	if !constant.IsWriteable(topicConfig.Perm) {
		response.Code = commonprotocol.NO_PERMISSION
		response.Remark = "the topic[" + newTopic + "] sending message is forbidden"
		return response
	}

	// 查询消息，这里如果堆积消息过多，会访问磁盘
	// 另外如果频繁调用，是否会引起gc问题，需要关注
	// TODO  msgExt :=self.BrokerController.getMessageStore().lookMessageByOffset(requestHeader.getOffset());
	msgExt := new(message.MessageExt)
	if nil == msgExt {
		response.Code = commonprotocol.SYSTEM_ERROR
		response.Remark = "look message by offset failed, " + string(requestHeader.Offset)
		return response
	}

	// 构造消息
	retryTopic := msgExt.GetProperty(message.PROPERTY_RETRY_TOPIC)
	if "" == retryTopic {
		message.PutProperty(&msgExt.Message, message.PROPERTY_RETRY_TOPIC, msgExt.Topic)
	}
	msgExt.SetWaitStoreMsgOK(false)

	// 客户端自动决定定时级别
	delayLevel := requestHeader.DelayLevel

	// 死信消息处理
	if msgExt.ReconsumeTimes >= subscriptionGroupConfig.RetryMaxTimes || delayLevel < 0 {
		newTopic = stgcommon.GetDLQTopic(requestHeader.Group)
		if queueIdInt < 0 {
			num := (self.Rand.Int() % 99999999) % DLQ_NUMS_PER_GROUP
			if num > 0 {
				queueIdInt = num
			} else {
				queueIdInt = -num
			}
		}

		topicConfig, err =
			self.BrokerController.TopicConfigManager.createTopicInSendMessageBackMethod(
				newTopic, DLQ_NUMS_PER_GROUP, constant.PERM_WRITE, 0)
		if nil == topicConfig {
			response.Code = commonprotocol.SYSTEM_ERROR
			response.Remark = "topic[" + newTopic + "] not exist"
			return response
		}
	} else {
		if 0 == delayLevel {
			delayLevel = 3 + msgExt.ReconsumeTimes
		}

		msgExt.SetDelayTimeLevel(delayLevel)
	}

	msgInner := new(stgstorelog.MessageExtBrokerInner)
	msgInner.Topic = newTopic
	msgInner.Body = msgExt.Body
	msgInner.Flag = (msgExt.Flag)
	message.SetPropertiesMap(&msgInner.Message, msgExt.Properties)
	// TODO msgInner.PropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
	// TODO msgInner.TagsCode(MessageExtBrokerInner.tagsString2tagsCode(null, msgExt.getTags()));

	msgInner.QueueId = queueIdInt
	msgInner.SysFlag = msgExt.SysFlag
	msgInner.BornTimestamp = msgExt.BornTimestamp
	msgInner.BornHost = msgExt.BornHost
	msgInner.StoreHost = self.StoreHost
	msgInner.ReconsumeTimes = msgExt.ReconsumeTimes + 1

	// 保存源生消息的 msgId
	originMsgId := message.GetOriginMessageId(msgExt.Message)
	if originMsgId == "" || len(originMsgId) <= 0 {
		originMsgId = msgExt.MsgId

	}
	message.SetOriginMessageId(&msgInner.Message, originMsgId)

	// TODO this.brokerController.getMessageStore().putMessage(msgInner)
	putMessageResult := new(stgstorelog.PutMessageResult)

	if putMessageResult != nil {
		switch putMessageResult.PutMessageStatus {
		case stgstorelog.PUTMESSAGE_PUT_OK:
			backTopic := msgExt.Topic
			correctTopic := msgExt.GetProperty(message.PROPERTY_RETRY_TOPIC)
			if correctTopic == "" || len(correctTopic) <= 0 {
				backTopic = correctTopic
			}
			fmt.Println(backTopic)
			// TODO self.BrokerController.getBrokerStatsManager().incSendBackNums(requestHeader.getGroup(), backTopic);

			response.Code = commonprotocol.SUCCESS
			response.Remark = nil

			return response
		default:
			break
		}
		response.Code = commonprotocol.SYSTEM_ERROR
		response.Remark = putMessageResult.PutMessageStatus.PutMessageString()
		return response
	}
	response.Code = commonprotocol.SYSTEM_ERROR
	response.Remark = "putMessageResult is null"
	return response
}

// sendMessage 正常消息
// Author gaoyanlei
// Since 2017/8/17
func (self *SendMessageProcessor) sendMessage( // TODO final ChannelHandlerContext ctx,
	request protocol.RemotingCommand, mqtraceContext mqtrace.SendMessageContext, requestHeader *header.SendMessageRequestHeader) *protocol.RemotingCommand {
	response := protocol.CreateResponseCommand()
	responseHeader := new(header.SendMessageResponseHeader)
	response.Opaque = request.Opaque
	response.Code = -1
	self.msgCheck(requestHeader, response)
	if response.Code != -1 {
		return response
	}

	body := request.Body

	queueIdInt := requestHeader.QueueId

	topicConfig := self.BrokerController.TopicConfigManager.selectTopicConfig(requestHeader.Topic)

	if queueIdInt < 0 {
		num := (self.Rand.Int() % 99999999) % topicConfig.WriteQueueNums
		if num > 0 {
			queueIdInt = num
		} else {
			queueIdInt = -num
		}

	}

	sysFlag := requestHeader.SysFlag
	if stgcommon.MULTI_TAG == topicConfig.TopicFilterType {
		sysFlag |= sysflag.MultiTagsFlag
	}
	msgInner := new(stgstorelog.MessageExtBrokerInner)
	msgInner.Topic = requestHeader.Topic
	msgInner.Body = body
	message.SetPropertiesMap(&msgInner.Message, message.String2messageProperties(requestHeader.Properties))
	msgInner.PropertiesString = requestHeader.Properties
	msgInner.TagsCode = stgstorelog.TagsString2tagsCode(topicConfig.TopicFilterType, msgInner.GetTags())
	msgInner.QueueId = queueIdInt
	msgInner.SysFlag = sysFlag
	msgInner.BornTimestamp = requestHeader.BornTimestamp
	// TODO 	msgInner.BornHost =requestHeader.BornTimestamp
	msgInner.StoreHost = self.StoreHost
	if requestHeader.ReconsumeTimes == 0 {
		msgInner.ReconsumeTimes = 0
	} else {
		msgInner.ReconsumeTimes = requestHeader.ReconsumeTimes
	}

	if self.BrokerController.BrokerConfig.RejectTransactionMessage {
		traFlag := msgInner.GetProperty(message.PROPERTY_TRANSACTION_PREPARED)
		if len(traFlag) > 0 {
			response.Code = commonprotocol.NO_PERMISSION
			response.Remark = "the broker[" + self.BrokerController.BrokerConfig.BrokerIP1 + "] sending transaction message is forbidden"
			return response
		}
	}

	// TODO this.brokerController.getMessageStore().putMessage(msgInner)
	putMessageResult := new(stgstorelog.PutMessageResult)

	if putMessageResult != nil {
		sendOK := false
		switch putMessageResult.PutMessageStatus {
		case stgstorelog.PUTMESSAGE_PUT_OK:
			sendOK = true
			response.Code = commonprotocol.SUCCESS
		case stgstorelog.FLUSH_DISK_TIMEOUT:
			response.Code = commonprotocol.FLUSH_DISK_TIMEOUT
			sendOK = true
			break
		case stgstorelog.FLUSH_SLAVE_TIMEOUT:
			response.Code = commonprotocol.FLUSH_SLAVE_TIMEOUT
			sendOK = true
		case stgstorelog.SLAVE_NOT_AVAILABLE:
			response.Code = commonprotocol.SLAVE_NOT_AVAILABLE
			sendOK = true

		case stgstorelog.CREATE_MAPEDFILE_FAILED:
			response.Code = commonprotocol.SYSTEM_ERROR
			response.Remark = "create maped file failed, please make sure OS and JDK both 64bit."
		case stgstorelog.MESSAGE_ILLEGAL:
			response.Code = commonprotocol.MESSAGE_ILLEGAL
			response.Remark = "the message is illegal, maybe length not matched."
			break
		case stgstorelog.SERVICE_NOT_AVAILABLE:
			response.Code = commonprotocol.SERVICE_NOT_AVAILABLE
			response.Remark = "service not available now, maybe disk full, " + self.diskUtil() + ", maybe your broker machine memory too small."
		case stgstorelog.PUTMESSAGE_UNKNOWN_ERROR:
			response.Code = commonprotocol.SYSTEM_ERROR
			response.Remark = "UNKNOWN_ERROR"
		default:
			response.Code = commonprotocol.SYSTEM_ERROR
			response.Remark = "UNKNOWN_ERROR DEFAULT"
		}

		if sendOK {
			//TODO   this.brokerController.getBrokerStatsManager().incTopicPutNums(msgInner.getTopic());
			//TODO this.brokerController.getBrokerStatsManager().incTopicPutSize(msgInner.getTopic(),
			//TODO 	putMessageResult.getAppendMessageResult().getWroteBytes());
			//TODO this.brokerController.getBrokerStatsManager().incBrokerPutNums();
			response.Remark = nil
			responseHeader.MsgId = putMessageResult.AppendMessageResult.MsgId
			responseHeader.QueueId = queueIdInt
			responseHeader.QueueOffset = putMessageResult.AppendMessageResult.LogicsOffset

			DoResponse( // TODO  ctx
				request, response)
			if self.BrokerController.BrokerConfig.LongPollingEnable {
				// TODO 	  this.brokerController.getPullRequestHoldService().notifyMessageArriving(
				// TODO requestHeader.getTopic(), queueIdInt,
				// TODO 	putMessageResult.getAppendMessageResult().getLogicsOffset() + 1);
			}

		}

	} else {
		response.Code = commonprotocol.SYSTEM_ERROR
		response.Remark = "store putMessage return null"
	}

	return response
}

func (self *SendMessageProcessor) diskUtil() string {
	// TODO
	return ""
}