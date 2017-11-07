package topic

import (
	"git.oschina.net/cloudzone/cloudcommon-go/logger"
	"git.oschina.net/cloudzone/cloudcommon-go/web/resp"
	"git.oschina.net/cloudzone/smartgo/stgweb/models"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules/topicService"
	"github.com/kataras/iris/context"
	"strings"
)

// CreateTopic 创建Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func CreateTopic(ctx context.Context) {
	topicVo := new(models.CreateTopic)
	if err := ctx.ReadJSON(topicVo); err != nil {
		logger.Warn("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, err.Error()))
		return
	}
	if err := topicVo.Validate(); err != nil {
		logger.Warn("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ParamNotValid, err.Error()))
		return
	}

	err := topicService.Default().CreateTopic(strings.TrimSpace(topicVo.Topic), strings.TrimSpace(topicVo.ClusterName))
	if err != nil {
		logger.Warn("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, err.Error()))
		return
	}

	responseBody := &models.ResultVo{Result: true}
	ctx.JSON(resp.NewSuccessResponse(responseBody))
}

// TopicList 查询所有Topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func TopicList(ctx context.Context) {
	params, err := topicService.Default().GetTopicList()
	if err != nil {
		logger.Warn("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, err.Error()))
		return
	}
	ctx.JSON(resp.NewSuccessResponse(params))
}
