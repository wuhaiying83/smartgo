package stgbroker

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgbroker/filtersrv"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"net"
	"runtime"
	"strings"
	"time"
)

const (
	WINDOWS = "windows" // windows operating system
)

// FilterServerManager FilterServer管理
// Author rongzhihong
// Since 2017/9/8
type FilterServerManager struct {
	ticker                       *timeutil.Ticker
	brokerController             *BrokerController
	FilterServerMaxIdleTimeMills int64
	filterServerTable            *sync.Map
}

// FilterServerInfo FilterServer基本信息
// Author rongzhihong
// Since 2017/9/8
type FilterServerInfo struct {
	filterServerAddr    string
	lastUpdateTimestamp int64
}

// NewFilterServerManager 初始化FilterServerManager
// Author rongzhihong
// Since 2017/9/8
func NewFilterServerManager(bc *BrokerController) *FilterServerManager {
	fsm := new(FilterServerManager)
	fsm.brokerController = bc
	fsm.ticker = timeutil.NewTicker(30, 5)
	fsm.FilterServerMaxIdleTimeMills = 30000
	fsm.filterServerTable = sync.NewMap()
	return fsm
}

// Start 启动;定时检查Filter Server个数，数量不符合，则创建
// Author rongzhihong
// Since 2017/9/8
func (fsm *FilterServerManager) Start() {
	fsm.ticker.Do(func(tm time.Time) {
		fsm.createFilterServer()
	})
}

// Shutdown 停止检查Filter Server的定时任务
// Author rongzhihong
// Since 2017/9/8
func (fsm *FilterServerManager) Shutdown() {
	if fsm.ticker != nil {
		fsm.ticker.Stop()
	}
}

// Shutdown 停止检查Filter Server的定时任务
// Author rongzhihong
// Since 2017/9/8
func (fsm *FilterServerManager) createFilterServer() {
	more := fsm.brokerController.BrokerConfig.FilterServerNums - fsm.filterServerTable.Size()
	cmd := fsm.buildStartCommand()

	var index int32 = 0
	for index = 0; index < more; index++ {
		filtersrv.CallShell(cmd)
	}
}

// buildStartCommand 组装CMD命令
// Author rongzhihong
// Since 2017/9/8
func (fsm *FilterServerManager) buildStartCommand() string {
	var config = ""

	if fsm.brokerController.ConfigFile != nil {
		config = fmt.Sprintf("-c %s", fsm.brokerController.ConfigFile)
	}

	if len(fsm.brokerController.BrokerConfig.NamesrvAddr) > 0 {
		config += fmt.Sprintf(" -n %s", fsm.brokerController.BrokerConfig.NamesrvAddr)
	}

	if isWindowsOS() {
		return fmt.Sprintf("start /b %s\\bin\\mqfiltersrv.exe %s", fsm.brokerController.BrokerConfig.SmartGoHome, config)
	} else {
		return fmt.Sprintf("sh %s/bin/startfsrv.sh %s", fsm.brokerController.BrokerConfig.SmartGoHome, config)
	}
}

// RegisterFilterServer 注册FilterServer
// Author rongzhihong
// Since 2017/9/8
func (fsm *FilterServerManager) RegisterFilterServer(conn net.Conn, filterServerAddr string) {
	bean, err := fsm.filterServerTable.Get(conn)
	if err != nil {
		logger.Error(err)
		return
	}

	if bean == nil {
		filterServerInfo := new(FilterServerInfo)
		filterServerInfo.filterServerAddr = filterServerAddr
		filterServerInfo.lastUpdateTimestamp = timeutil.CurrentTimeMillis()
		fsm.filterServerTable.Put(conn, filterServerInfo)
		logger.Info("Receive a New Filter Server<%v>", filterServerAddr)
		return
	}

	if filterServerInfo, ok := bean.(*FilterServerInfo); ok {
		filterServerInfo.lastUpdateTimestamp = timeutil.CurrentTimeMillis()
		fsm.filterServerTable.Put(conn, filterServerInfo)
	}
}

// ScanNotActiveChannel 10s向Broker注册一次，Broker如果发现30s没有注册，则删除它
// Author rongzhihong
// Since 2017/9/8
func (fsm *FilterServerManager) ScanNotActiveChannel() {
	it := fsm.filterServerTable.Iterator()
	for it.HasNext() {
		key, value, _ := it.Next()
		var timestamp int64 = 0
		if bean, ok := value.(*FilterServerInfo); ok {
			timestamp = bean.lastUpdateTimestamp
		} else {
			continue
		}

		currentTimeMillis := timeutil.CurrentTimeMillis()
		if (currentTimeMillis - timestamp) > fsm.FilterServerMaxIdleTimeMills {
			logger.Info("The Filter Server<%v> expired, remove it", key)
			it.Remove()
			if channel, ok := key.(net.Conn); ok {
				channel.Close()
			}
		}
	}
}

// doChannelCloseEvent 组装CMD命令
// Author rongzhihong
// Since 2017/9/8
func (fsm *FilterServerManager) doChannelCloseEvent(remoteAddr string, conn net.Conn) {
	old, err := fsm.filterServerTable.Remove(conn)
	if err != nil {
		logger.Error(fmt.Sprintf("The Filter Server Remove conn, throw:%s", err.Error()))
	}

	if value, ok := old.(*FilterServerInfo); ok {
		logger.Warn("The Filter Server<%s> connection<%s> closed, remove it", value.filterServerAddr, remoteAddr)
	}
}

// BuildNewFilterServerList FilterServer地址列表
// Author rongzhihong
// Since 2017/9/8
func (fsm *FilterServerManager) BuildNewFilterServerList() (filterServerAdds []string) {
	it := fsm.filterServerTable.Iterator()
	for it.HasNext() {
		_, value, _ := it.Next()
		if bean, ok := value.(*FilterServerInfo); ok {
			filterServerAdds = append(filterServerAdds, bean.filterServerAddr)
		}
	}
	return
}

// isWindowsOS check current os is windows
// if current is windows operating system, return true ; otherwise return false
// Author rongzhihong
// Since 2017/9/8
func isWindowsOS() bool {
	return strings.EqualFold(runtime.GOOS, WINDOWS)
}
