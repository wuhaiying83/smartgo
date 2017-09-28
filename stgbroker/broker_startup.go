package stgbroker

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/parseutil"
	"git.oschina.net/cloudzone/smartgo/stgstorelog"
	"git.oschina.net/cloudzone/smartgo/stgstorelog/config"
	"github.com/toolkits/file"
	"os"
	"strings"
)

// SmartgoBrokerConfig 启动smartgoBroker所必需的配置项
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/26
type SmartgoBrokerConfig struct {
	BrokerClusterName string
	BrokerName        string
	DeleteWhen        int
	FileReservedTime  int
	BrokerRole        string
	FlushDiskType     string
}

// ToString 打印smartgoBroker配置项
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/26
func (self *SmartgoBrokerConfig) ToString() string {
	format := "SmartgoBrokerConfig [BrokerClusterName=%s, BrokerName=%s, DeleteWhen=%d, FileReservedTime=%d, BrokerRole=%s, FlushDiskType=%s]"
	info := fmt.Sprintf(format, self.BrokerClusterName, self.BrokerName, self.DeleteWhen, self.FileReservedTime, self.BrokerRole, self.FlushDiskType)
	return info
}

// IsBlank 判断配置项是否读取成功
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/26
func (self *SmartgoBrokerConfig) IsBlank() bool {
	return self == nil || strings.TrimSpace(self.BrokerClusterName) == "" || strings.TrimSpace(self.BrokerName) == ""
}

// Start 启动BrokerController
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/20
func Start() *BrokerController {
	controller := CreateBrokerController()
	controller.Start()

	formatBroker := "the broker[%s, %s] boot success."
	tips := fmt.Sprintf(formatBroker, controller.BrokerConfig.BrokerName, controller.GetBrokerAddr())

	if "" != controller.BrokerConfig.NamesrvAddr {
		formatNamesrv := "the broker[%s, %s] boot success, and the name server is %s"
		tips = fmt.Sprintf(formatNamesrv, controller.BrokerConfig.BrokerName, controller.GetBrokerAddr(), controller.BrokerConfig.NamesrvAddr)
	}
	fmt.Println(tips)
	return controller
}

// CreateBrokerController 创建BrokerController对象
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/20
func CreateBrokerController() *BrokerController {
	cfgName := "smartgoBroker.toml"
	brokerConfigPath := "../../conf/" + cfgName
	if !file.IsExist(brokerConfigPath) {
		// 通过IDEA编辑器，启动test()用例、启动main()入口，两种方式读取conf得到的相对路径有所区别;  如果在服务器通过cmd命令行编译打包，则可以正常读取
		// TODO:为了兼容能够直接在IDEA上面利用conf/smartgoBroker.toml默认配置文件目录  Add: tianuliang,<tianuliang@gmail.com> Since: 2017/9/27
		brokerConfigPath = stgcommon.GetSmartgoConfigDir() + cfgName
		fmt.Printf("idea special brokerConfigPath = %s \n", brokerConfigPath)
	}

	// 读取并转化*.toml配置项的值
	var cfg SmartgoBrokerConfig
	parseutil.ParseConf(brokerConfigPath, &cfg)
	fmt.Println(cfg.ToString())

	// 初始化brokerConfig，并校验broker启动的所必需的SmartGoHome、Namesrv配置
	brokerConfig := stgcommon.NewBrokerConfig(cfg.BrokerName, cfg.BrokerClusterName)
	if !checkBrokerConfig(brokerConfig) {
		os.Exit(0)
	}

	// 初始化brokerConfig
	messageStoreConfig := stgstorelog.NewMessageStoreConfig()
	if !checkMessageStoreConfig(messageStoreConfig, brokerConfig) {
		os.Exit(0)
	}

	// 构建BrokerController结构体
	controller := NewBrokerController(brokerConfig, messageStoreConfig)
	controller.ConfigFile = brokerConfigPath

	// 初始化controller
	initResult := controller.Initialize()
	if !initResult {
		fmt.Println("the broker initialize failed")
		controller.Shutdown()
		os.Exit(0)
	}

	return controller
}

// checBrokerConfig 校验broker启动的所必需的SmartGoHome、namesrv配置
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/22
func checkBrokerConfig(brokerConfig *stgcommon.BrokerConfig) bool {
	// 如果没有设置home环境变量，则启动失败
	if "" == brokerConfig.SmartGoHome {
		errMsg := fmt.Sprintf("Please set the '%s' variable in your environment to match the location of the Smartgo installation\n", stgcommon.SMARTGO_HOME_ENV)
		fmt.Printf(errMsg)
		return false
	}

	// 检测环境变量NAMESRV_ADDR
	nameSrvAddr := brokerConfig.NamesrvAddr
	if strings.TrimSpace(nameSrvAddr) == "" {
		errMsg := fmt.Sprintf("Please set the '%s' variable in your environment\n", stgcommon.NAMESRV_ADDR_ENV)
		fmt.Printf(errMsg)
		return false
	}

	// 检测NameServer环境变量设置是否正确 IP:PORT
	addrs := strings.Split(strings.TrimSpace(nameSrvAddr), ";")
	if addrs == nil || len(addrs) == 0 {
		errMsg := fmt.Sprintf("the %s=%s environment variable is invalid. \n", stgcommon.NAMESRV_ADDR_ENV, addrs)
		fmt.Printf(errMsg)
		return false
	}
	for _, addr := range addrs {
		if !stgcommon.CheckIpAndPort(addr) {
			format := "The Name Server Address[%s] illegal, please set it as follows, \"127.0.0.1:9876;192.168.0.1:9876\"\n"
			fmt.Printf(format, addr)
			return false
		}
	}

	return true
}

// checkMessageStoreConfig 校验messageStoreConfig配置
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/22
func checkMessageStoreConfig(messageStoreConfig *stgstorelog.MessageStoreConfig, brokerConfig *stgcommon.BrokerConfig) bool {
	// 如果是slave，修改默认值（修改命中消息在内存的最大比例40为30【40-10】）
	if messageStoreConfig.BrokerRole == config.SLAVE {
		ratio := messageStoreConfig.AccessMessageInMemoryMaxRatio - 10
		messageStoreConfig.AccessMessageInMemoryMaxRatio = ratio
	}

	// BrokerId的处理（switch-case语法：只要匹配到一个case，则顺序往下执行，直到遇到break，因此若没有break则不管后续case匹配与否都会执行）
	switch messageStoreConfig.BrokerRole {
	//如果是同步master也会执行下述case中brokerConfig.setBrokerId(MixAll.MASTER_ID);语句，直到遇到break
	case config.ASYNC_MASTER:
	case config.SYNC_MASTER:
		brokerConfig.BrokerId = stgcommon.MASTER_ID
	case config.SLAVE:
		if brokerConfig.BrokerId <= 0 {
			fmt.Printf("Slave's brokerId[%d] must be > 0 \n", brokerConfig.BrokerId)
			return false
		}
	default:

	}
	return true
}
