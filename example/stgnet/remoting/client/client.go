package main

import (
	"fmt"
	"time"

	cmprotocol "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
)

var (
	remotingClient remoting.RemotingClient
)

func main() {
	initClient()
	remotingClient.Start()
	fmt.Println("remoting client start success")

	var (
		addr     = "10.122.1.200:11000"
		request  *protocol.RemotingCommand
		response *protocol.RemotingCommand
		err      error
	)

	topicStatsInfoRequestHeader := &namesrv.GetTopicStatsInfoRequestHeader{}
	topicStatsInfoRequestHeader.Topic = "testTopic"

	// 同步消息
	request = protocol.CreateRequestCommand(cmprotocol.GET_TOPIC_STATS_INFO, topicStatsInfoRequestHeader)
	response, err = remotingClient.InvokeSync(addr, request, 3000)
	if err != nil {
		fmt.Printf("Send Mssage[Sync] failed: %s\n", err)
	} else {
		if response.Code == cmprotocol.SUCCESS {
			fmt.Printf("Send Mssage[Sync] success. response: body[%s]\n", string(response.Body))
		} else {
			fmt.Printf("Send Mssage[Sync] failed: code[%d] err[%s]\n", response.Code, response.Remark)
		}
	}

	// 异步消息
	err = remotingClient.InvokeAsync(addr, request, 3000, func(responseFuture *remoting.ResponseFuture) {
		response := responseFuture.GetRemotingCommand()
		if response == nil {
			if responseFuture.IsSendRequestOK() {
				fmt.Printf("Send Mssage[Async] failed: send unreachable\n")
				return
			}

			if responseFuture.IsTimeout() {
				fmt.Printf("Send Mssage[Async] failed: send timeout\n")
				return
			}

			fmt.Printf("Send Mssage[Async] failed: unknow reseaon\n")
			return
		}

		if response.Code == cmprotocol.SUCCESS {
			fmt.Printf("Send Mssage[Async] success. response: body[%s]\n", string(response.Body))
		} else {
			fmt.Printf("Send Mssage[Async] failed: code[%d] err[%s]\n", response.Code, response.Remark)
		}
	})

	go func() {
		var i int
		timer := time.NewTimer(3 * time.Second)
		for {
			<-timer.C
			sendHearBeat(addr)
			i++
			timer.Reset(2 * time.Second)
			if i == 10 {
				break
			}
		}

	}()

	select {}
}

func sendHearBeat(addr string) {
	request := protocol.CreateRequestCommand(cmprotocol.HEART_BEAT, nil)
	response, err := remotingClient.InvokeSync(addr, request, 3000)
	if err != nil {
		fmt.Printf("Send HeartBeat[Sync] failed: %s\n", err)
	} else {
		if response.Code == cmprotocol.SUCCESS {
			fmt.Printf("Send HeartBeat[Sync] success. response: body[%s]\n", string(response.Body))
		} else {
			fmt.Printf("Send HeartBeat[Sync] failed: code[%d] err[%s]\n", response.Code, response.Remark)
		}
	}
}

func initClient() {
	remotingClient = remoting.NewDefalutRemotingClient()
	remotingClient.UpdateNameServerAddressList([]string{"10.122.1.100:10000"})
}