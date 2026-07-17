package handlers

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bestruirui/octopus/internal/helper"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/server/middleware"
	"github.com/bestruirui/octopus/internal/server/resp"
	"github.com/bestruirui/octopus/internal/server/router"
	"github.com/bestruirui/octopus/internal/task"
	"github.com/bestruirui/octopus/internal/utils/safe"
	"github.com/gin-gonic/gin"
)

func init() {
	router.NewGroupRouter("/api/v1/channel").
		Use(middleware.Auth()).
		Use(middleware.RequireJSON()).
		AddRoute(
			router.NewRoute("/list", http.MethodGet).
				Handle(listChannel),
		).
		AddRoute(
			router.NewRoute("/create", http.MethodPost).
				Handle(createChannel),
		).
		AddRoute(
			router.NewRoute("/update", http.MethodPost).
				Handle(updateChannel),
		).
		AddRoute(
			router.NewRoute("/enable", http.MethodPost).
				Handle(enableChannel),
		).
		AddRoute(
			router.NewRoute("/delete/:id", http.MethodDelete).
				Handle(deleteChannel),
		).
		AddRoute(
			router.NewRoute("/fetch-model", http.MethodPost).
				Handle(fetchModel),
		)
	router.NewGroupRouter("/api/v1/channel").
		Use(middleware.Auth()).
		AddRoute(
			router.NewRoute("/sync", http.MethodPost).
				Handle(syncChannel),
		).
		AddRoute(
			router.NewRoute("/last-sync-time", http.MethodGet).
				Handle(getLastSyncTime),
		).
		AddRoute(
			router.NewRoute("/reset-circuit", http.MethodPost).
				Handle(resetCircuitBreaker),
		).
		AddRoute(
			router.NewRoute("/probe-all", http.MethodPost).
				Handle(probeAllChannels),
		)
}

func listChannel(c *gin.Context) {
	channels, err := op.ChannelList(c.Request.Context())
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return
	}
	channelIDs := make([]int, 0, len(channels))
	for _, channel := range channels {
		channelIDs = append(channelIDs, channel.ID)
	}
	bindingMap, err := op.SiteChannelBindingMapByChannelIDs(channelIDs, c.Request.Context())
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return
	}
	for i, channel := range channels {
		stats := op.StatsChannelGet(channel.ID)
		channels[i].Stats = &stats
		if binding, ok := bindingMap[channel.ID]; ok {
			channels[i].Managed = true
			channels[i].ManagedSource = &model.ManagedChannelSource{
				SiteID:          binding.SiteID,
				SiteAccountID:   binding.SiteAccountID,
				SiteUserGroupID: binding.SiteUserGroupID,
				GroupKey:        binding.GroupKey,
			}
		}
	}
	resp.Success(c, channels)
}

func createChannel(c *gin.Context) {
	var channel model.Channel
	if err := c.ShouldBindJSON(&channel); err != nil {
		resp.InvalidJSON(c)
		return
	}
	if channel.ProxyMode == "" {
		channel.ProxyMode = model.ProxyUsageModeDirect
	}
	if err := channel.ProxyMode.Validate(false); err != nil {
		resp.Error(c, http.StatusBadRequest, err.Error())
		return
	}
	if channel.ProxyMode == model.ProxyUsageModePool && (channel.ProxyConfigID == nil || *channel.ProxyConfigID <= 0) {
		resp.Error(c, http.StatusBadRequest, "proxy config id is required when proxy mode is pool")
		return
	}
	if channel.ProxyMode == model.ProxyUsageModePool {
		if _, err := op.ProxyURLForConfig(*channel.ProxyConfigID, c.Request.Context()); err != nil {
			resp.Error(c, http.StatusBadRequest, err.Error())
			return
		}
	}
	if channel.ProxyMode != model.ProxyUsageModePool {
		channel.ProxyConfigID = nil
	}
	if err := op.ChannelCreate(&channel, c.Request.Context()); err != nil {
		resp.ErrorWithAppError(c, http.StatusInternalServerError, channelError(codeChannelCreateFailed, "channel create failed", err))
		return
	}
	stats := op.StatsChannelGet(channel.ID)
	channel.Stats = &stats
	createdChannel := channel
	safe.Go("channel-create-postprocess", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		modelStr := createdChannel.Model + "," + createdChannel.CustomModel
		modelArray := strings.Split(modelStr, ",")
		helper.LLMPriceAddToDB(modelArray, ctx)
		helper.ChannelBaseUrlDelayUpdate(&createdChannel, ctx)
		helper.ChannelAutoGroup(&createdChannel, ctx)
	})
	resp.Success(c, channel)
}

func updateChannel(c *gin.Context) {
	var req model.ChannelUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		resp.InvalidJSON(c)
		return
	}
	channel, err := op.ChannelUpdate(&req, c.Request.Context())
	if err != nil {
		resp.ErrorWithAppError(c, http.StatusInternalServerError, channelError(codeChannelUpdateFailed, "channel update failed", err))
		return
	}
	stats := op.StatsChannelGet(channel.ID)
	channel.Stats = &stats
	updatedChannel := *channel
	safe.Go("channel-update-postprocess", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		modelStr := updatedChannel.Model + "," + updatedChannel.CustomModel
		modelArray := strings.Split(modelStr, ",")
		helper.LLMPriceAddToDB(modelArray, ctx)
		helper.ChannelBaseUrlDelayUpdate(&updatedChannel, ctx)
		helper.ChannelAutoGroup(&updatedChannel, ctx)
	})
	resp.Success(c, channel)
}

func enableChannel(c *gin.Context) {
	var request struct {
		ID      int  `json:"id"`
		Enabled bool `json:"enabled"`
	}
	if err := c.ShouldBindJSON(&request); err != nil {
		resp.InvalidJSON(c)
		return
	}
	if err := op.ChannelEnabled(request.ID, request.Enabled, c.Request.Context()); err != nil {
		resp.ErrorWithAppError(c, http.StatusInternalServerError, channelError(codeChannelUpdateFailed, "channel update failed", err))
		return
	}
	resp.Success(c, nil)
}

func deleteChannel(c *gin.Context) {
	id := c.Param("id")
	idNum, err := strconv.Atoi(id)
	if err != nil {
		resp.InvalidParam(c)
		return
	}
	if err := op.ChannelDel(idNum, c.Request.Context()); err != nil {
		resp.ErrorWithAppError(c, http.StatusInternalServerError, channelError(codeChannelDeleteFailed, "channel delete failed", err))
		return
	}
	resp.Success(c, nil)
}
func fetchModel(c *gin.Context) {
	var request model.Channel
	if err := c.ShouldBindJSON(&request); err != nil {
		resp.InvalidJSON(c)
		return
	}
	models, err := helper.FetchModels(c.Request.Context(), request)
	if err != nil {
		resp.ErrorWithAppError(c, http.StatusInternalServerError, channelError(codeChannelFetchModelsFailed, "channel fetch models failed", err))
		return
	}
	resp.Success(c, models)
}

func syncChannel(c *gin.Context) {
	task.SyncModelsTask()
	resp.Success(c, nil)
}

func getLastSyncTime(c *gin.Context) {
	time := task.GetLastSyncModelsTime()
	resp.Success(c, time)
}

func resetCircuitBreaker(c *gin.Context) {
	var req struct {
		ChannelID int `json:"channel_id"`
	}
	// 绑定失败或 channel_id<=0：重置全部（熔断 + 粘性）
	if err := c.ShouldBindJSON(&req); err != nil || req.ChannelID <= 0 {
		circuits, stickies := op.ResetAllBalancerStateWithStats()
		resp.Success(c, gin.H{
			"reset":    "all",
			"circuits": circuits,
			"stickies": stickies,
		})
		return
	}
	op.ResetBalancerStateForChannel(req.ChannelID)
	resp.Success(c, gin.H{"reset": req.ChannelID})
}

// probeAllChannels 对全部渠道实测探测端点格式，结论确定的改正 Type 并锁定。
// 用于功能上线后一次性回填现有渠道；服务端限流（并发 5），避免撞上游限流。
func probeAllChannels(c *gin.Context) {
	channels, err := op.ChannelList(c.Request.Context())
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return
	}

	type probeRow struct {
		ChannelID   int    `json:"channel_id"`
		ChannelName string `json:"channel_name"`
		CurrentType int    `json:"current_type"`
		DetectedType int    `json:"detected_type"`
		Conclusive  bool   `json:"conclusive"`
		Changed     bool   `json:"changed"`
		Locked      bool   `json:"locked"`
		Reason      string `json:"reason"`
	}

	rows := make([]probeRow, len(channels))
	sem := make(chan struct{}, 5)
	var wg sync.WaitGroup
	for i := range channels {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()
			ch := channels[idx]
			outcome := helper.ProbeChannelEndpoint(c.Request.Context(), ch)
			row := probeRow{
				ChannelID: ch.ID, ChannelName: ch.Name,
				CurrentType: int(outcome.CurrentType), DetectedType: int(outcome.DetectedType),
				Conclusive: outcome.Conclusive, Changed: outcome.Changed, Reason: outcome.Reason,
			}
			if outcome.Conclusive {
				locked := true
				updateReq := &model.ChannelUpdateRequest{ID: ch.ID, TypeLocked: &locked, BypassManagedCheck: true}
				if outcome.Changed {
					detected := outcome.DetectedType
					updateReq.Type = &detected
				}
				if _, err := op.ChannelUpdate(updateReq, c.Request.Context()); err != nil {
					row.Reason = "update failed: " + err.Error()
				} else {
					row.Locked = true
				}
			}
			rows[idx] = row
		}(i)
	}
	wg.Wait()

	changed, locked := 0, 0
	for _, r := range rows {
		if r.Changed && r.Locked {
			changed++
		}
		if r.Locked {
			locked++
		}
	}
	resp.Success(c, gin.H{"total": len(rows), "changed": changed, "locked": locked, "results": rows})
}
