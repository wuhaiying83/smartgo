package main

import (
	"flag"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
)

func main() {
	host := flag.String("h", "10.112.68.189:9876", "host")
	topic := flag.String("t", "cloudzone1", "topic")
	tag := flag.String("tag", "tagA", "tag")
	producerGroupId := flag.String("pg", "producerGroupId-200", "producerGroupId")

	goThreadNum := flag.Int("n", 1000, "thread num")
	everyThreadNum := flag.Int("c", 5000, "thread/per send count")
	//sendsize := flag.Int("s", 100, "send data size")
	//async := flag.Bool("a", false, "sync & async")
	flag.Parse()

	var wg sync.WaitGroup
	defaultMQProducer := process.NewDefaultMQProducer(*producerGroupId)
	defaultMQProducer.SetNamesrvAddr(*host)
	defaultMQProducer.Start()

	var success int64
	var failed int64
	total := (*goThreadNum) * (*everyThreadNum)
	start := time.Now()
	for i := 0; i < *goThreadNum; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < *everyThreadNum; j++ {
				sendResult, err := defaultMQProducer.Send(message.NewMessage(*topic, *tag, []byte("I'm so diao!呵呵"+strconv.Itoa(n)+"-"+strconv.Itoa(j))))
				if err != nil {
					atomic.AddInt64(&failed, 1)
					fmt.Println("send msg err: ----> ", err.Error())
					continue
				}
				if sendResult != nil {
					atomic.AddInt64(&success, 1)
					//fmt.Println(sendResult.ToString())
				}
			}
		}(i)
	}

	wg.Wait()
	end := time.Now()
	spend := end.Sub(start)
	tps := success * 1000000000 / (end.UnixNano() - start.UnixNano())
	fmt.Printf("Send Mssage. Time: %v, Total: %d, Thread: %d, EveryThreadNum: %d, Success: %d, Failed: %d, Tps: %d\n", spend, total, *goThreadNum, *everyThreadNum, success, failed, tps)
}
