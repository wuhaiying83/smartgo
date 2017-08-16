package message
// MessageExt: 消息扩展
// Author: yintongqiang
// Since:  2017/8/10

type MessageExt struct {
	//// 消息主题
	//Topic                     string
	//// 消息标志，系统不做干预，完全由应用决定如何使用
	//Flag                      int
	//// 消息属性，都是系统属性，禁止应用设置
	//Properties                map[string]string
	//// 消息体
	//Body                      []byte
	Message
	// 队列ID <PUT>
	QueueId                   int
	// 存储记录大小
	StoreSize                 int
	// 队列偏移量
	QueueOffset               int
	// 消息标志位 <PUT>
	SysFlag                   int
	// 消息在客户端创建时间戳 <PUT>
	BornTimestamp             int64
	// 消息来自哪里 <PUT>
	BornHost                  string
	// 消息在服务器存储时间戳
	StoreTimestamp            int64
	// 消息存储在哪个服务器 <PUT>
	StoreHost                 string
	// 消息ID
	MsgId                     string
	// 消息对应的Commit Log Offset
	CommitLogOffset           int64
	// 消息体CRC
	BodyCRC                   int
	// 当前消息被某个订阅组重新消费了几次（订阅组之间独立计数）
	ReconsumeTimes            int
	PreparedTransactionOffset int64
}
