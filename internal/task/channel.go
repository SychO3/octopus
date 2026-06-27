package task

import (
	"context"
	"sync"
	"time"

	"github.com/bestruirui/octopus/internal/helper"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/utils/log"
)

func ChannelBaseUrlDelayTask() {
	log.Debugf("channel base url delay task started")
	startTime := time.Now()
	defer func() {
		log.Debugf("channel base url delay task finished, update time: %s", time.Since(startTime))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	channels, err := op.ChannelList(ctx)
	if err != nil {
		log.Errorf("failed to list channels: %v", err)
		return
	}
	for _, channel := range channels {
		helper.ChannelBaseUrlDelayUpdate(&channel, ctx)
	}
}

// ChannelProbeUnlockedTask 周期性对"未锁定且启用"的渠道实测探测端点格式：
// 探准则改正 type 并锁定（TypeLocked），使上游临时挂/限流恢复后自动补锁。
// 已锁定的渠道跳过（不重复探测、不烧 key），探测结论不确定的留待下次。
func ChannelProbeUnlockedTask() {
	log.Debugf("channel probe-unlocked task started")
	startTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	channels, err := op.ChannelList(ctx)
	if err != nil {
		log.Errorf("probe-unlocked: failed to list channels: %v", err)
		cancel()
		return
	}

	var newlyLocked int
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 5)
	for i := range channels {
		ch := channels[i]
		if ch.TypeLocked || !ch.Enabled {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(channel model.Channel) {
			defer wg.Done()
			defer func() { <-sem }()
			outcome := helper.ProbeChannelEndpoint(ctx, channel)
			if !outcome.Conclusive {
				return
			}
			locked := true
			req := &model.ChannelUpdateRequest{ID: channel.ID, TypeLocked: &locked, BypassManagedCheck: true}
			if outcome.Changed {
				detected := outcome.DetectedType
				req.Type = &detected
			}
			if _, err := op.ChannelUpdate(req, ctx); err != nil {
				log.Warnf("probe-unlocked: failed to lock channel %d: %v", channel.ID, err)
				return
			}
			mu.Lock()
			newlyLocked++
			mu.Unlock()
		}(ch)
	}
	wg.Wait()
	log.Infof("channel probe-unlocked task finished in %s, newly locked %d", time.Since(startTime), newlyLocked)
}

