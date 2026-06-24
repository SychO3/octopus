package relay

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/helper"
	"github.com/bestruirui/octopus/internal/impersonate"
	dbmodel "github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/outlierwindow"
	"github.com/bestruirui/octopus/internal/relay/balancer"
	"github.com/bestruirui/octopus/internal/server/resp"
	"github.com/bestruirui/octopus/internal/transformer/inbound"
	"github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	outAnthropic "github.com/bestruirui/octopus/internal/transformer/outbound/anthropic"
	openaiOutbound "github.com/bestruirui/octopus/internal/transformer/outbound/openai"
	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/bestruirui/octopus/internal/utils/safe"
	"github.com/gin-gonic/gin"
	"github.com/samber/lo"
	"github.com/tmaxmax/go-sse"
)

type streamHeartbeatWriter interface {
	Write([]byte) (int, error)
	Flush()
}

func streamHeartbeatInterval() time.Duration {
	interval, err := op.SettingGetInt(dbmodel.SettingKeySSEHeartbeatInterval)
	if err != nil || interval <= 0 {
		return 0
	}
	return time.Duration(interval) * time.Second
}

func newStreamHeartbeatTicker() (*time.Ticker, <-chan time.Time) {
	interval := streamHeartbeatInterval()
	if interval <= 0 {
		return nil, nil
	}
	ticker := time.NewTicker(interval)
	return ticker, ticker.C
}

func writeSSEHeartbeat(writer streamHeartbeatWriter) error {
	if _, err := writer.Write([]byte(":\n\n")); err != nil {
		return err
	}
	writer.Flush()
	return nil
}

func Handler(inboundType inbound.InboundType, c *gin.Context) {
	// 解析请求
	rawBody, internalRequest, inAdapter, err := parseRequest(inboundType, c)
	if err != nil {
		return
	}
	supportedModels := c.GetString("supported_models")
	if supportedModels != "" {
		supportedModelsArray := strings.Split(supportedModels, ",")
		if !slices.Contains(supportedModelsArray, internalRequest.Model) {
			resp.ErrorWithCode(c, http.StatusBadRequest, CodeRelayModelNotSupported, "model not supported")
			return
		}
	}

	requestModel := internalRequest.Model
	apiKeyID := c.GetInt("api_key_id")

	// 获取通道分组
	group, err := op.GroupGetEnabledMap(requestModel, c.Request.Context())
	if err != nil {
		resp.ErrorWithCode(c, http.StatusNotFound, CodeRelayModelNotFound, "model not found")
		return
	}

	// 创建迭代器（策略排序 + 粘性优先）
	iter := balancer.NewIterator(group, apiKeyID, requestModel)
	if iter.Len() == 0 {
		resp.ErrorWithCode(c, http.StatusServiceUnavailable, CodeRelayNoAvailableChannel, "no available channel")
		return
	}

	// === 早期心跳 ===
	// 在所有 forward / 重试 / 退避之前启动早期心跳协程，覆盖前置阶段（连接慢、failover、退避叠加）
	// 期间向客户端发 SSE 注释字节，避免被 Cloudflare 在 120s 零字节阈值上判 524。
	// 仅对流式请求生效；非流式无法发送 SSE 注释（破坏 application/json 协议），
	// 不施加任何本地超时——上游慢响应应让其自然完成或由上游/CF 自身处理。
	isStream := internalRequest.Stream != nil && *internalRequest.Stream
	hb := startEarlyHeartbeat(c, isStream)
	defer hb.Stop()

	// 初始化 Metrics
	metrics := NewRelayMetrics(apiKeyID, requestModel, rawBody, internalRequest)
	responsesPassthroughRequired := internalRequest.HasOpenAIResponsesPassthrough()
	responsesPassthroughCapableFound := false

	// 请求级上下文
	req := &relayRequest{
		c:               c,
		inAdapter:       inAdapter,
		internalRequest: internalRequest,
		metrics:         metrics,
		apiKeyID:        apiKeyID,
		requestModel:    requestModel,
		groupID:         group.ID,
		groupSessionTTL: group.SessionKeepTime,
		iter:            iter,
		rawBody:         rawBody,
		heartbeat:       hb,
	}

	var lastErr error
	var lastResult attemptResult

	// 同通道重试次数：启用时使用配置值，否则 1 次（不重试）
	maxSameChannelRetries := 1
	if group.RetryEnabled {
		maxSameChannelRetries = group.MaxRetries
		if maxSameChannelRetries <= 0 {
			maxSameChannelRetries = 3
		}
	}

	for iter.Next() {
		select {
		case <-c.Request.Context().Done():
			log.Debugf("request context canceled, stopping retry")
			metrics.SaveWithChannelStats(c.Request.Context(), false, context.Canceled, iter.Attempts(), false)
			return
		default:
		}

		item := iter.Item()

		// 获取通道
		channel, err := op.ChannelGet(item.ChannelID, c.Request.Context())
		if err != nil {
			log.Warnf("failed to get channel %d: %v", item.ChannelID, err)
			iter.Skip(item.ChannelID, 0, fmt.Sprintf("channel_%d", item.ChannelID), fmt.Sprintf("channel not found: %v", err))
			lastErr = err
			continue
		}
		if !channel.Enabled {
			iter.Skip(channel.ID, 0, channel.Name, "channel disabled")
			continue
		}
		if responsesPassthroughRequired {
			if channel.Type == outbound.OutboundTypeOpenAIResponse {
				responsesPassthroughCapableFound = true
			} else {
				iter.Skip(channel.ID, 0, channel.Name, "openai responses passthrough required")
				continue
			}
		}

		// 出站适配器
		outAdapter := outbound.Get(channel.Type)
		if outAdapter == nil {
			iter.Skip(channel.ID, 0, channel.Name, fmt.Sprintf("unsupported channel type: %d", channel.Type))
			continue
		}

		// 类型兼容性检查
		if internalRequest.IsEmbeddingRequest() && !outbound.IsEmbeddingChannelType(channel.Type) {
			iter.Skip(channel.ID, 0, channel.Name, "channel type not compatible with embedding request")
			continue
		}
		if internalRequest.IsChatRequest() && !outbound.IsChatChannelType(channel.Type) {
			iter.Skip(channel.ID, 0, channel.Name, "channel type not compatible with chat request")
			continue
		}

		// 设置实际模型
		internalRequest.Model = item.ModelName

		log.Debugf("request model %s, mode: %d, forwarding to channel: %s model: %s (attempt %d/%d, sticky=%t)",
			requestModel, group.Mode, channel.Name, item.ModelName,
			iter.Index()+1, iter.Len(), iter.IsSticky())

		selectOpts := dbmodel.ChannelKeySelectOptions{
			ExcludeKeyIDs:  make(map[int]struct{}),
			PreferredKeyID: iter.StickyKeyID(),
		}
		var usedKey dbmodel.ChannelKey
		for {
			usedKey = channel.GetChannelKey(selectOpts)
			if usedKey.ChannelKey == "" {
				break
			}
			if !iter.SkipCircuitBreak(channel.ID, usedKey.ID, channel.Name) {
				break
			}
			selectOpts.ExcludeKeyIDs[usedKey.ID] = struct{}{}
			usedKey = dbmodel.ChannelKey{}
		}
		if usedKey.ChannelKey == "" {
			if len(selectOpts.ExcludeKeyIDs) == 0 {
				iter.Skip(channel.ID, 0, channel.Name, "no available key")
			}
			continue
		}

		// 同通道重试循环
		var result attemptResult
		for retryNum := 0; retryNum < maxSameChannelRetries; retryNum++ {
			// 重试前等待退避
			if retryNum > 0 {
				delay := computeBackoff(retryNum, result.RetryAfter)
				log.Infof("same-channel retry %d/%d for %s, waiting %v",
					retryNum, maxSameChannelRetries, channel.Name, delay)
				select {
				case <-c.Request.Context().Done():
					log.Debugf("request context canceled during retry backoff")
					metrics.SaveWithChannelStats(c.Request.Context(), false, context.Canceled, iter.Attempts(), false)
					return
				case <-time.After(delay):
				}

				// 重建 outAdapter 以重置流式状态（toolIndex, toolCalls 等）
				outAdapter = outbound.Get(channel.Type)
			}

			// 构造尝试级上下文
			ra := &relayAttempt{
				relayRequest:         req,
				outAdapter:           outAdapter,
				channel:              channel,
				usedKey:              usedKey,
				firstTokenTimeOutSec: group.FirstTokenTimeOut,
			}

			result = ra.attempt()
			if result.Success || result.Written || result.Canceled || result.ResetConversation || result.FirstTokenTimeout || !isRetryableStatus(result.StatusCode) {
				break
			}
		}

		// 同通道重试耗尽后记录熔断器失败
		if !result.Success && !result.Written && !result.Canceled && !result.ResetConversation {
			failureKind := circuitFailureKind(group.RetryEnabled, result.StatusCode)
			balancer.RecordFailure(channel.ID, usedKey.ID, internalRequest.Model, failureKind)
			outlierwindow.Report(channel.ID, false, result.StatusCode, time.Now())
			if failureKind == balancer.FailureHard {
				maybeLearnManagedRoute(c.Request.Context(), channel.ID, internalRequest.Model, inboundType, result.Err)
			}
		}

		if result.Success {
			outlierwindow.Report(channel.ID, true, result.StatusCode, time.Now())
			metrics.SaveWithChannelStats(c.Request.Context(), true, nil, iter.Attempts(), false)
			return
		}
		if result.Canceled {
			metrics.SaveWithChannelStats(c.Request.Context(), false, result.Err, iter.Attempts(), false)
			return
		}
		if result.ResetConversation {
			metrics.SaveWithChannelStats(c.Request.Context(), false, result.Err, iter.Attempts(), false)
			if publicErr, ok := classifyWSPublicError(result.Err, result.StatusCode); ok {
				hb.FlushOrError(c, publicErr.Status, publicErr.Message)
			} else {
				hb.FlushOrError(c, result.StatusCode, result.Err.Error())
			}
			return
		}
		if result.Written {
			metrics.SaveWithChannelStats(c.Request.Context(), false, result.Err, iter.Attempts(), false)
			return
		}
		lastErr = result.Err
		lastResult = result
	}

	// 所有候选通道均失败
	if responsesPassthroughRequired && !responsesPassthroughCapableFound {
		err := fmt.Errorf("openai responses native tools require an openai responses channel")
		metrics.SaveWithChannelStats(c.Request.Context(), false, err, iter.Attempts(), false)
		hb.FlushOrError(c, http.StatusBadRequest, "当前请求包含 OpenAI Responses 原生工具，仅支持 OpenAI Responses 通道直通")
		return
	}
	metrics.SaveWithChannelStats(c.Request.Context(), false, lastErr, iter.Attempts(), false)

	// 透传 429/503 状态码和 Retry-After 头，让客户端 SDK 的重试机制接管
	if isPassthroughStatus(lastResult.StatusCode) {
		if lastResult.RetryAfter > 0 {
			c.Header("Retry-After", fmt.Sprintf("%d", int(lastResult.RetryAfter.Seconds())))
		}
		hb.FlushOrError(c, lastResult.StatusCode, "channel failed")
		return
	}
	if lastResult.StatusCode > 0 {
		hb.FlushOrError(c, lastResult.StatusCode, "channel failed")
		return
	}
	hb.FlushOrError(c, http.StatusBadGateway, "channel failed")
}

func circuitFailureKind(retryEnabled bool, statusCode int) balancer.FailureKind {
	if retryEnabled && isPassthroughStatus(statusCode) {
		return balancer.FailureSoftRateLimit
	}
	return balancer.FailureHard
}

// attempt 统一管理一次通道尝试的完整生命周期
func (ra *relayAttempt) attempt() attemptResult {
	span := ra.iter.StartAttempt(ra.channel.ID, ra.usedKey.ID, ra.channel.Name)

	// 转发请求
	statusCode, fwdErr := ra.forward()

	// 更新 channel key 状态
	ra.usedKey.StatusCode = statusCode
	ra.usedKey.LastUseTimeStamp = time.Now().Unix()

	if fwdErr == nil {
		// ====== 成功 ======
		// Only collect response if NOT using passthrough (passthrough collects at stream end)
		if !ra.shouldPassthroughAnthropic() {
			ra.collectResponse()
		}
		ra.usedKey.TotalCost += ra.metrics.Stats.InputCost + ra.metrics.Stats.OutputCost
		op.ChannelKeyUpdate(ra.usedKey)

		span.End(dbmodel.AttemptSuccess, statusCode, "")

		// Channel 维度统计
		op.StatsChannelUpdate(ra.channel.ID, dbmodel.StatsMetrics{
			WaitTime:       span.Duration().Milliseconds(),
			RequestSuccess: 1,
		})

		// 熔断器：记录成功
		balancer.RecordSuccess(ra.channel.ID, ra.usedKey.ID, ra.internalRequest.Model)
		// 会话保持：更新粘性记录
		balancer.SetSticky(ra.apiKeyID, ra.requestModel, ra.channel.ID, ra.usedKey.ID)

		return attemptResult{Success: true}
	}

	// ====== 失败 ======
	if isClientCancellation(ra.requestContext(), fwdErr) {
		written := ra.streamPayloadWritten.Load()
		if written {
			ra.collectResponse()
		}
		op.ChannelKeyUpdate(ra.usedKey)
		span.End(dbmodel.AttemptFailed, statusCode, fwdErr.Error())
		return attemptResult{
			Success:    false,
			Written:    written,
			Canceled:   true,
			Err:        fwdErr,
			StatusCode: statusCode,
		}
	}

	op.ChannelKeyUpdate(ra.usedKey)
	span.End(dbmodel.AttemptFailed, statusCode, fwdErr.Error())

	// Channel 维度统计
	op.StatsChannelUpdate(ra.channel.ID, dbmodel.StatsMetrics{
		WaitTime:      span.Duration().Milliseconds(),
		RequestFailed: 1,
	})

	// 注意：熔断器记录已移至 Handler() 的同通道重试循环外，
	// 避免重试期间过早触发熔断

	written := ra.streamPayloadWritten.Load()
	if written {
		ra.collectResponse()
	}
	firstTokenTimeout := isFirstTokenTimeout(nil, fwdErr)
	return attemptResult{
		Success:           false,
		Written:           written,
		ResetConversation: statusCode == http.StatusConflict && needsConversationRestart(relayErrorMessage(fwdErr)),
		FirstTokenTimeout: firstTokenTimeout,
		Err:               fmt.Errorf("channel %s failed: %v", ra.channel.Name, fwdErr),
		StatusCode:        statusCode,
		RetryAfter:        ra.retryAfter,
	}
}

// parseRequest 解析并验证入站请求
// 返回值中的 rawBody 为客户端原始请求字节，供同格式直通路径重用。
func parseRequest(inboundType inbound.InboundType, c *gin.Context) ([]byte, *model.InternalLLMRequest, model.Inbound, error) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return nil, nil, nil, err
	}

	inAdapter := inbound.Get(inboundType)
	internalRequest, err := inAdapter.TransformRequest(c.Request.Context(), body)
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return nil, nil, nil, err
	}

	// Pass through the original query parameters
	internalRequest.Query = c.Request.URL.Query()

	if err := internalRequest.Validate(); err != nil {
		resp.Error(c, http.StatusBadRequest, err.Error())
		return nil, nil, nil, err
	}

	return body, internalRequest, inAdapter, nil
}

// forward 转发请求到上游服务
func (ra *relayAttempt) forward() (int, error) {
	ctx := ra.requestContext()

	// 尝试上游 WebSocket（仅 OpenAI Response outbound 类型；必须是客户端 WS 入站且新开关显式启用）
	if ra.channel.Type == outbound.OutboundTypeOpenAIResponse &&
		ra.internalRequest.RawAPIFormat == model.APIFormatOpenAIResponse {

		shouldTryWS := false
		if ra.shouldPassthroughOpenAIResponses() {
			shouldTryWS = false
		} else if ra.internalRequest.IsOpenAIExactReplayRequest() {
			shouldTryWS = false
		} else if ra.c == nil {
			wsMode := effectiveResponsesWSMode(ra.channel)
			shouldTryWS = shouldEnableResponsesWS(ra.channel) && wsMode != responsesWSModeOff
		} else if requiresUpstreamWSContinuation(ra.internalRequest) {
			// Safety: HTTP ingress must not proactively use upstream WS for fresh requests,
			// but an explicit continuation cannot be safely failovered as ordinary HTTP.
			shouldTryWS = true
		}

		if shouldTryWS {
			statusCode, err := ra.forwardViaWS(ctx)
			if statusCode != -1 {
				return statusCode, err
			}
			if requiresUpstreamWSContinuation(ra.internalRequest) {
				balancer.DeleteSticky(ra.apiKeyID, ra.requestModel)
				return http.StatusConflict, fmt.Errorf("upstream continuation transport unavailable; please restart the conversation")
			}
			ra.metrics.SetWSRecovery(dbmodel.RelayLogWSRecoveryDowngrade)
			// statusCode == -1 means WS not available, fall through to HTTP
		}
	}

	return ra.forwardViaHTTP(ctx)
}

// forwardViaWS attempts to forward via upstream WebSocket.
// Returns statusCode=-1 if WS is not available (caller should fall through to HTTP).
func (ra *relayAttempt) forwardViaWS(ctx context.Context) (int, error) {
	if ra.c == nil && effectiveResponsesWSMode(ra.channel) == responsesWSModePassthrough && !ra.internalRequest.IsOpenAIExactReplayRequest() {
		return ra.forwardViaWSPassthrough(ctx)
	}
	continuation := requiresUpstreamWSContinuation(ra.internalRequest)
	preferredConnID := ""
	if continuation {
		preferredConnID, _ = getWSResponseConn(currentPreviousResponseID(ra.internalRequest))
	}
	pc := TryUpstreamWSWithPreference(ctx, ra.channel, ra.channel.GetBaseUrl(), ra.usedKey.ChannelKey, ra.usedKey.ID, ra.clientRequestHeaders(), preferredConnID)
	if pc == nil {
		log.Debugf("upstream WS unavailable for channel %s (key=%d, continuation=%t)", ra.channel.Name, ra.usedKey.ID, continuation)
		return -1, nil // WS not available
	}

	log.Debugf("using upstream WebSocket for channel %s (key=%d)", ra.channel.Name, ra.usedKey.ID)
	log.Debugf("upstream WS selected (channel=%s, key=%d, continuation=%t, previous_response_id=%s)",
		ra.channel.Name, ra.usedKey.ID, continuation, currentPreviousResponseID(ra.internalRequest))

	// Build the Responses API request body
	responsesReq := openaiOutbound.ConvertToResponsesRequest(ra.internalRequest)
	reqBody, err := json.Marshal(responsesReq)
	if err != nil {
		wsUpstreamPool.Put(pc)
		return -1, nil // fall through to HTTP
	}
	ra.metrics.SetTransportRequestPayload(reqBody, ra.internalRequest.Model)

	// Send response.create message
	if err := wsUpstreamPool.SendResponseCreate(ctx, pc, reqBody); err != nil {
		log.Warnf("upstream WS send failed for channel %s: %v", ra.channel.Name, err)
		log.Debugf("upstream WS send failed before stream start (channel=%s, key=%d, continuation=%t, err=%v)",
			ra.channel.Name, ra.usedKey.ID, continuation, err)
		wsUpstreamPool.RemoveConn(pc)
		if isUpstreamWSConnectionBroken(err) {
			log.Debugf("upstream WS send failure eligible for redial (channel=%s, key=%d, continuation=%t)",
				ra.channel.Name, ra.usedKey.ID, continuation)
			statusCode, redialErr, recovered := ra.retryViaFreshUpstreamWS(ctx, reqBody)
			if recovered || redialErr != nil {
				return statusCode, redialErr
			}
			if requiresUpstreamWSContinuation(ra.internalRequest) {
				balancer.DeleteSticky(ra.apiKeyID, ra.requestModel)
				return http.StatusConflict, fmt.Errorf("upstream continuation transport unavailable; please restart the conversation")
			}
		}
		wsUpstreamPool.RecordWSFailure(ra.channel.ID)
		return -1, nil // fall through to HTTP
	}

	// Read events from WS and process through the transform pipeline
	ra.metrics.UsedWS = true
	ra.metrics.SetWSExecMode(dbmodel.RelayLogWSExecModeTransform)
	if ra.metrics.WSMode == nil {
		ra.metrics.SetWSMode(defaultWSModeForRequest(ra.internalRequest))
	}
	reader := newWSUpstreamReader(pc, ra.channel.ID, ra.usedKey.ID)
	err = ra.handleWSStreamResponse(ctx, reader)
	if err != nil {
		reader.CloseWithError()
		log.Debugf("upstream WS stream failed (channel=%s, key=%d, continuation=%t, written=%t, status=%d, err=%v)",
			ra.channel.Name, ra.usedKey.ID, continuation, ra.getStreamWriter().Written(), reader.StatusCode(), err)
		if requiresUpstreamWSContinuation(ra.internalRequest) && !ra.streamPayloadWritten.Load() && shouldReconnectUpstreamWSBeforeReplay(err) {
			log.Debugf("upstream WS stream failure eligible for reconnect before replay (channel=%s, key=%d, previous_response_id=%s)",
				ra.channel.Name, ra.usedKey.ID, currentPreviousResponseID(ra.internalRequest))
			statusCode, redialErr, recovered := ra.retryViaFreshUpstreamWS(ctx, reqBody)
			if recovered || redialErr != nil {
				return statusCode, redialErr
			}
		}
		if requiresUpstreamWSContinuation(ra.internalRequest) && isContinuationTransportFailure(err) {
			balancer.DeleteSticky(ra.apiKeyID, ra.requestModel)
			return http.StatusConflict, fmt.Errorf("upstream continuation transport unavailable; please restart the conversation")
		}
		if ra.requestContext().Err() == nil {
			wsUpstreamPool.RecordWSFailure(ra.channel.ID)
		}
		return reader.StatusCode(), err
	}

	reader.Close()
	wsUpstreamPool.RecordWSSuccess(ra.channel.ID)
	ra.recordSuccessfulWSAffinity(pc)
	return 200, nil
}

func (ra *relayAttempt) retryViaFreshUpstreamWS(ctx context.Context, reqBody []byte) (int, error, bool) {
	log.Debugf("attempting fresh upstream WS redial (channel=%s, key=%d, previous_response_id=%s)",
		ra.channel.Name, ra.usedKey.ID, currentPreviousResponseID(ra.internalRequest))
	redialed := TryUpstreamWS(ctx, ra.channel, ra.channel.GetBaseUrl(), ra.usedKey.ChannelKey, ra.usedKey.ID, ra.clientRequestHeaders(), true)
	if redialed == nil {
		log.Debugf("fresh upstream WS redial unavailable (channel=%s, key=%d)", ra.channel.Name, ra.usedKey.ID)
		return 0, nil, false
	}

	retryErr := wsUpstreamPool.SendResponseCreate(ctx, redialed, reqBody)
	if retryErr != nil {
		log.Warnf("upstream WS redial send failed for channel %s: %v", ra.channel.Name, retryErr)
		log.Debugf("fresh upstream WS redial send failed (channel=%s, key=%d, err=%v)", ra.channel.Name, ra.usedKey.ID, retryErr)
		wsUpstreamPool.RemoveConn(redialed)
		wsUpstreamPool.RecordWSFailure(ra.channel.ID)
		if requiresUpstreamWSContinuation(ra.internalRequest) {
			balancer.DeleteSticky(ra.apiKeyID, ra.requestModel)
			return http.StatusConflict, fmt.Errorf("upstream continuation transport unavailable; please restart the conversation"), true
		}
		return -1, nil, true
	}

	ra.metrics.UsedWS = true
	ra.metrics.SetWSExecMode(dbmodel.RelayLogWSExecModeTransform)
	if ra.metrics.WSMode == nil {
		ra.metrics.SetWSMode(defaultWSModeForRequest(ra.internalRequest))
	}
	ra.metrics.SetWSRecovery(dbmodel.RelayLogWSRecoveryReconnect)
	reader := newWSUpstreamReader(redialed, ra.channel.ID, ra.usedKey.ID)
	streamErr := ra.handleWSStreamResponse(ctx, reader)
	if streamErr != nil {
		reader.CloseWithError()
		log.Debugf("fresh upstream WS redial stream failed (channel=%s, key=%d, status=%d, err=%v)",
			ra.channel.Name, ra.usedKey.ID, reader.StatusCode(), streamErr)
		if requiresUpstreamWSContinuation(ra.internalRequest) && isContinuationTransportFailure(streamErr) {
			balancer.DeleteSticky(ra.apiKeyID, ra.requestModel)
			return http.StatusConflict, fmt.Errorf("upstream continuation transport unavailable; please restart the conversation"), true
		}
		if ra.requestContext().Err() == nil {
			wsUpstreamPool.RecordWSFailure(ra.channel.ID)
		}
		return reader.StatusCode(), streamErr, true
	}
	log.Debugf("fresh upstream WS redial succeeded (channel=%s, key=%d, previous_response_id=%s)",
		ra.channel.Name, ra.usedKey.ID, currentPreviousResponseID(ra.internalRequest))
	reader.Close()
	wsUpstreamPool.RecordWSSuccess(ra.channel.ID)
	ra.recordSuccessfulWSAffinity(redialed)
	return http.StatusOK, nil, true
}

func isContinuationTransportFailure(err error) bool {
	message := relayErrorMessage(err)
	return isUpstreamWSConnectionBroken(err) ||
		needsConversationRestart(message) ||
		strings.Contains(message, "ws stream ended before first event")
}

func (ra *relayAttempt) clientRequestHeaders() http.Header {
	if ra == nil || ra.c == nil || ra.c.Request == nil {
		return nil
	}
	return ra.c.Request.Header
}

// handleWSStreamResponse processes events from an upstream WebSocket reader.
func (ra *relayAttempt) handleWSStreamResponse(ctx context.Context, reader *wsUpstreamReader) error {
	// 交接早期心跳给本函数内层 ticker
	ra.heartbeat.Hand()

	// Determine client writer
	writer := ra.getStreamWriter()

	// Set SSE response headers (for HTTP clients; WS clients handle this differently)
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("Connection", "keep-alive")
	writer.Header().Set("X-Accel-Buffering", "no")

	heartbeatTicker, heartbeatC := newStreamHeartbeatTicker()
	if heartbeatTicker != nil {
		defer heartbeatTicker.Stop()
	}

	firstToken := true
	var firstTokenTimer *time.Timer
	var firstTokenC <-chan time.Time
	if ra.firstTokenTimeOutSec > 0 {
		firstTokenTimer = time.NewTimer(time.Duration(ra.firstTokenTimeOutSec) * time.Second)
		firstTokenC = firstTokenTimer.C
		defer func() {
			if firstTokenTimer != nil {
				firstTokenTimer.Stop()
			}
		}()
	}

	// 异步读取上游 WS 事件，使主循环可以与 heartbeat/ctx/firstToken 并行 select
	type wsReadResult struct {
		data []byte
		err  error
	}
	results := make(chan wsReadResult, 1)
	safe.Go("relay-ws-stream-read", func() {
		defer close(results)
		for {
			eventData, err := reader.ReadEvent(ctx)
			results <- wsReadResult{data: eventData, err: err}
			if err != nil {
				return
			}
		}
	})

	for {
		select {
		case <-ctx.Done():
			if isLocalRelayBudgetExceeded(ctx, contextError(ctx)) {
				return contextError(ctx)
			}
			log.Debugf("client disconnected during ws stream")
			return nil
		case <-firstTokenC:
			log.Warnf("first token timeout (%ds) on ws stream, switching channel", ra.firstTokenTimeOutSec)
			return ra.firstTokenTimeoutError()
		case <-heartbeatC:
			if err := writeSSEHeartbeat(writer); err != nil {
				return err
			}
		case r, ok := <-results:
			if !ok {
				if firstToken {
					return fmt.Errorf("ws stream ended before first event")
				}
				log.Debugf("ws stream end")
				return nil
			}
			if r.err != nil {
				if r.err == io.EOF {
					if firstToken {
						return fmt.Errorf("ws stream ended before first event")
					}
					log.Debugf("ws stream end")
					return nil
				}
				return fmt.Errorf("ws stream read error: %w", r.err)
			}

			// Transform through outbound → internal → inbound pipeline
			data, err := ra.transformStreamData(ctx, string(r.data))
			if err != nil || len(data) == 0 {
				continue
			}

			if firstToken {
				ra.metrics.SetFirstTokenTime(time.Now())
				firstToken = false
				if firstTokenTimer != nil {
					if !firstTokenTimer.Stop() {
						select {
						case <-firstTokenTimer.C:
						default:
						}
					}
					firstTokenTimer = nil
					firstTokenC = nil
				}
			}

			if _, writeErr := writer.Write(data); writeErr != nil {
				return writeErr
			}
			if writer.Written() {
				ra.streamPayloadWritten.Store(true)
			}
			writer.Flush()
		}
	}
}

// forwardViaHTTP forwards the request using traditional HTTP.
func (ra *relayAttempt) forwardViaHTTP(ctx context.Context) (int, error) {
	// Anthropic→Anthropic 同格式直通：绕过 Internal model 往返转换，避免字段丢失、
	// 内容块重排、thinking 签名错位等在长上下文下触发上游 520 的问题。
	if ra.shouldPassthroughAnthropic() {
		return ra.forwardViaHTTPPassthroughAnthropic(ctx)
	}
	if ra.shouldPassthroughOpenAIResponses() {
		return ra.forwardViaHTTPPassthroughOpenAIResponses(ctx)
	}

	// 构建出站请求
	outboundRequest, err := ra.outAdapter.TransformRequest(
		ctx,
		ra.internalRequest,
		ra.channel.GetBaseUrl(),
		ra.usedKey.ChannelKey,
	)
	if err != nil {
		log.Warnf("failed to create request: %v", err)
		return 0, fmt.Errorf("failed to create request: %w", err)
	}
	if err := ra.applyParamOverride(outboundRequest); err != nil {
		return 0, err
	}

	// 剥离下游客户端带的 beta query param，上游不认识
	if q := outboundRequest.URL.Query(); q.Has("beta") {
		q.Del("beta")
		outboundRequest.URL.RawQuery = q.Encode()
	}

	// 复制请求头 + 客户端模拟
	ra.copyHeaders(outboundRequest)
	impersonate.ApplyClientHeaders(outboundRequest, ra.channel, ra.internalRequest.Model)
	// CustomHeader 优先级最高，在模拟之后再覆盖
	if len(ra.channel.CustomHeader) > 0 {
		for _, header := range ra.channel.CustomHeader {
			outboundRequest.Header.Set(header.HeaderKey, header.HeaderValue)
		}
	}
	if ra.channel.Type == outbound.OutboundTypeOpenAIResponse {
		outboundRequest.Header.Set("Content-Type", "application/json")
	}

	// 发送请求
	response, err := ra.sendRequest(outboundRequest)
	if err != nil {
		return 0, fmt.Errorf("failed to send request: %w", err)
	}
	defer response.Body.Close()

	// 检查响应状态
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		ra.retryAfter = parseRetryAfter(response.Header.Get("Retry-After"))
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return response.StatusCode, fmt.Errorf("failed to read response body: %w", err)
		}
		statusCode := normalizeUpstreamStatusCode(response.StatusCode, string(body))
		log.Warnf("upstream error from channel %s: status=%d, body=%s", ra.channel.Name, response.StatusCode, string(body))
		return statusCode, fmt.Errorf("upstream error: %d: %s", response.StatusCode, string(body))
	}

	// 处理响应
	if ra.internalRequest.Stream != nil && *ra.internalRequest.Stream {
		if err := ra.handleStreamResponse(ctx, response); err != nil {
			return 0, err
		}
		return response.StatusCode, nil
	}
	if err := ra.handleResponse(ctx, response); err != nil {
		return 0, err
	}
	return response.StatusCode, nil
}

func defaultWSModeForRequest(req *model.InternalLLMRequest) dbmodel.RelayLogWSMode {
	if requiresUpstreamWSContinuation(req) {
		return dbmodel.RelayLogWSModeContinuation
	}
	return dbmodel.RelayLogWSModeFresh
}

func readOutboundRequestBody(req *http.Request) ([]byte, error) {
	if req == nil || req.Body == nil {
		return nil, nil
	}
	if req.GetBody != nil {
		bodyReader, err := req.GetBody()
		if err != nil {
			return nil, err
		}
		defer bodyReader.Close()
		return io.ReadAll(bodyReader)
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	return body, nil
}

// getStreamWriter returns the appropriate stream writer for the current request.
func (ra *relayAttempt) getStreamWriter() StreamWriter {
	if ra.streamWriter != nil {
		return ra.streamWriter
	}
	return ra.c.Writer
}

// applyParamOverride merges channel-level JSON request overrides and records the final upstream payload.
func (ra *relayAttempt) applyParamOverride(outboundRequest *http.Request) error {
	if err := helper.ApplyParamOverride(outboundRequest, ra.channel.ParamOverride); err != nil {
		return err
	}
	if requestBody, readErr := readOutboundRequestBody(outboundRequest); readErr == nil {
		ra.metrics.SetTransportRequestPayload(requestBody, ra.internalRequest.Model)
	}
	return nil
}

// copyHeaders 复制请求头，过滤 hop-by-hop 头
func (ra *relayAttempt) copyHeaders(outboundRequest *http.Request) {
	if ra.c != nil {
		for key, values := range ra.c.Request.Header {
			lowerKey := strings.ToLower(key)
			if hopByHopHeaders[lowerKey] {
				continue
			}
			// anthropic-beta 需要与出站默认值合并去重，避免覆盖掉
			// 透传路径预置的 prompt-caching / extended-cache-ttl 基线。
			if lowerKey == "anthropic-beta" {
				existing := outboundRequest.Header.Get(key)
				for _, value := range values {
					existing = mergeBetaHeader(existing, value)
				}
				if existing != "" {
					outboundRequest.Header.Set(key, existing)
				}
				continue
			}
			for _, value := range values {
				outboundRequest.Header.Set(key, value)
			}
		}
	}
	if outboundRequest.Header.Get("User-Agent") == "" {
		outboundRequest.Header.Set("User-Agent", "")
	}
}

func (ra *relayAttempt) logAnthropicPassthroughRequestSummary(outboundRequest *http.Request) {
	if ra == nil || outboundRequest == nil {
		return
	}
	body, err := readOutboundRequestBody(outboundRequest)
	if err != nil {
		log.Infof("anthropic_passthrough.request_summary channel=%q body_read_error=%q", ra.channel.Name, err.Error())
		return
	}
	summary := summarizeAnthropicRequestBody(body)
	hash := sha256.Sum256(body)
	log.Infof(
		"anthropic_passthrough.request_summary channel=%q url=%q body_len=%d body_sha256=%x model=%q stream=%t messages=%d tools=%d empty_signature_thinking=%d last_role=%q headers=%s",
		ra.channel.Name,
		outboundRequest.URL.String(),
		len(body),
		hash[:8],
		summary.Model,
		summary.Stream,
		summary.Messages,
		summary.Tools,
		summary.EmptySignatureThinking,
		summary.LastRole,
		summarizeAnthropicForwardHeaders(outboundRequest.Header),
	)
}

type anthropicRequestBodySummary struct {
	Model                  string
	Stream                 bool
	Messages               int
	Tools                  int
	EmptySignatureThinking int
	LastRole               string
}

func summarizeAnthropicRequestBody(body []byte) anthropicRequestBodySummary {
	var payload struct {
		Model    string `json:"model"`
		Stream   bool   `json:"stream"`
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
		Tools []json.RawMessage `json:"tools"`
	}
	var summary anthropicRequestBodySummary
	if err := json.Unmarshal(body, &payload); err != nil {
		return summary
	}
	summary.Model = payload.Model
	summary.Stream = payload.Stream
	summary.Messages = len(payload.Messages)
	summary.Tools = len(payload.Tools)
	if len(payload.Messages) > 0 {
		summary.LastRole = payload.Messages[len(payload.Messages)-1].Role
	}
	for _, msg := range payload.Messages {
		summary.EmptySignatureThinking += countEmptySignatureThinkingBlocks(msg.Content)
	}
	return summary
}

func countEmptySignatureThinkingBlocks(raw json.RawMessage) int {
	if len(raw) == 0 {
		return 0
	}
	var blocks []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return 0
	}
	count := 0
	for _, block := range blocks {
		var typ string
		if err := json.Unmarshal(block["type"], &typ); err != nil || typ != "thinking" {
			continue
		}
		signatureRaw, ok := block["signature"]
		if !ok {
			continue
		}
		var signature string
		if err := json.Unmarshal(signatureRaw, &signature); err == nil && strings.TrimSpace(signature) == "" {
			count++
		}
	}
	return count
}

func summarizeAnthropicForwardHeaders(headers http.Header) string {
	if headers == nil {
		return "{}"
	}
	keys := []string{
		"Accept",
		"Content-Type",
		"Anthropic-Version",
		"Anthropic-Beta",
		"User-Agent",
		"X-App",
		"X-Stainless-Runtime",
		"X-Stainless-Lang",
		"X-Stainless-Package-Version",
		"X-Stainless-Runtime-Version",
		"X-Stainless-Os",
		"X-Stainless-Arch",
		"X-Stainless-Retry-Count",
		"X-Stainless-Timeout",
	}
	values := make(map[string]string, len(keys)+3)
	for _, key := range keys {
		if value := strings.TrimSpace(headers.Get(key)); value != "" {
			values[key] = value
		}
	}
	values["has_x_api_key"] = fmt.Sprintf("%t", strings.TrimSpace(headers.Get("X-API-Key")) != "")
	values["has_authorization"] = fmt.Sprintf("%t", strings.TrimSpace(headers.Get("Authorization")) != "")
	values["has_x_claude_code_session_id"] = fmt.Sprintf("%t", strings.TrimSpace(headers.Get("X-Claude-Code-Session-Id")) != "")
	b, err := json.Marshal(values)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// mergeBetaHeader 合并两个逗号分隔的 anthropic-beta 字段值，去重并保留先后顺序。
func mergeBetaHeader(existing, incoming string) string {
	seen := make(map[string]struct{}, 8)
	merged := make([]string, 0, 8)
	for _, source := range []string{existing, incoming} {
		for _, entry := range strings.Split(source, ",") {
			normalized := strings.TrimSpace(entry)
			if normalized == "" {
				continue
			}
			if _, ok := seen[normalized]; ok {
				continue
			}
			seen[normalized] = struct{}{}
			merged = append(merged, normalized)
		}
	}
	return strings.Join(merged, ",")
}

// sendRequest 发送 HTTP 请求
func (ra *relayAttempt) sendRequest(req *http.Request) (*http.Response, error) {
	httpClient, err := helper.ChannelHTTPClientWithContext(req.Context(), ra.channel)
	if err != nil {
		log.Warnf("failed to get http client: %v", err)
		return nil, err
	}

	req = ra.attachFirstTokenBudget(req)

	response, err := httpClient.Do(req)
	if err != nil {
		if timeoutErr := ra.firstTokenTimeoutIfNeeded(req.Context(), err); timeoutErr != nil {
			ra.closeFirstTokenBudget()
			return nil, timeoutErr
		}
		if isClientCancellation(req.Context(), err) {
			log.Infof("request canceled before upstream response: %v", err)
		} else {
			log.Warnf("failed to send request: %v", err)
		}
		ra.closeFirstTokenBudget()
		return nil, err
	}

	if response != nil && response.Body != nil && ra.firstTokenBudget != nil {
		response.Body = &closeWithFuncReadCloser{
			ReadCloser: response.Body,
			onClose:    ra.closeFirstTokenBudget,
		}
	}

	return response, nil
}

// errEmptyUpstreamStream marks 200 SSE streams that ended without forwarding
// any payload to the client; they must fail the attempt instead of being
// recorded as zero-token successes (issue #65). The message must not contain
// detectRouteMismatchTarget trigger substrings ("text/event-stream",
// "/responses", "/messages", "anthropic-version", "responses api"), which
// would corrupt managed route learning.
var errEmptyUpstreamStream = errors.New("upstream stream ended without forwarding any payload")

// handleStreamResponse 处理流式响应
func (ra *relayAttempt) handleStreamResponse(ctx context.Context, response *http.Response) error {
	defer ra.closeFirstTokenBudget()

	if ct := response.Header.Get("Content-Type"); ct != "" && !strings.Contains(strings.ToLower(ct), "text/event-stream") {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 16*1024))
		return fmt.Errorf("upstream returned non-SSE content-type %q for stream request: %s", ct, string(body))
	}

	// 交接早期心跳给本函数内层 ticker，避免双路 flush 竞争
	ra.heartbeat.Hand()

	writer := ra.getStreamWriter()

	// 设置 SSE 响应头
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("Connection", "keep-alive")
	writer.Header().Set("X-Accel-Buffering", "no")

	heartbeatTicker, heartbeatC := newStreamHeartbeatTicker()
	if heartbeatTicker != nil {
		defer heartbeatTicker.Stop()
	}

	firstToken := true

	disconnect := func() error {
		err := contextError(ctx)
		if timeoutErr := ra.firstTokenTimeoutIfNeeded(ctx, err); timeoutErr != nil {
			return timeoutErr
		}
		if isLocalRelayBudgetExceeded(ctx, err) {
			return err
		}
		log.Debugf("client disconnected, stopping stream: written=%t first_token_seen=%t elapsed=%s", ra.streamPayloadWritten.Load(), !firstToken, time.Since(ra.metrics.StartTime))
		return err
	}

	type sseReadResult struct {
		data string
		err  error
	}
	results := make(chan sseReadResult, 1)
	safe.Go("relay-stream-read", func() {
		defer close(results)
		readCfg := &sse.ReadConfig{MaxEventSize: maxSSEEventSize}
		for ev, err := range sse.Read(response.Body, readCfg) {
			if err != nil {
				results <- sseReadResult{err: err}
				return
			}
			results <- sseReadResult{data: ev.Data}
		}
	})

	var firstTokenTimer *time.Timer
	var firstTokenC <-chan time.Time
	if firstToken && ra.firstTokenTimeOutSec > 0 && ra.firstTokenBudget == nil {
		firstTokenTimer = time.NewTimer(time.Duration(ra.firstTokenTimeOutSec) * time.Second)
		firstTokenC = firstTokenTimer.C
		defer func() {
			if firstTokenTimer != nil {
				firstTokenTimer.Stop()
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			// 上游 EOF 与客户端断连可能同时发生，select 在多就绪 case 中随机选择，
			// 此时优先消费 results 中已就绪的终止信号，避免把正常结束的流误判为断连。
			select {
			case r, ok := <-results:
				if !ok || (r.err != nil && r.err == io.EOF) {
					if !ra.streamPayloadWritten.Load() {
						return errEmptyUpstreamStream
					}
					log.Debugf("stream end")
					return nil
				}
				if r.err != nil {
					if timeoutErr := ra.firstTokenTimeoutIfNeeded(ctx, r.err); timeoutErr != nil {
						return timeoutErr
					}
					// 断连取消沿出站请求传播、被读侧先观察到时，按断连处理而非上游流失败
					if !errors.Is(r.err, context.Canceled) {
						log.Warnf("failed to read event: %v", r.err)
						return fmt.Errorf("failed to read stream event: %w", r.err)
					}
				}
			default:
			}
			return disconnect()
		case <-firstTokenC:
			log.Warnf("first token timeout (%ds), switching channel", ra.firstTokenTimeOutSec)
			_ = response.Body.Close()
			return ra.firstTokenTimeoutError()
		case <-heartbeatC:
			if err := writeSSEHeartbeat(writer); err != nil {
				return err
			}
		case r, ok := <-results:
			if !ok {
				if !ra.streamPayloadWritten.Load() {
					return errEmptyUpstreamStream
				}
				log.Debugf("stream end")
				return nil
			}
			if r.err != nil {
				if timeoutErr := ra.firstTokenTimeoutIfNeeded(ctx, r.err); timeoutErr != nil {
					return timeoutErr
				}
				if errors.Is(r.err, context.Canceled) && contextError(ctx) != nil {
					return disconnect()
				}
				log.Warnf("failed to read event: %v", r.err)
				return fmt.Errorf("failed to read stream event: %w", r.err)
			}

			data, err := ra.transformStreamData(ctx, r.data)
			if err != nil || len(data) == 0 {
				continue
			}
			if firstToken {
				ra.metrics.SetFirstTokenTime(time.Now())
				firstToken = false
				ra.stopFirstTokenTimer()
				if firstTokenTimer != nil {
					if !firstTokenTimer.Stop() {
						select {
						case <-firstTokenTimer.C:
						default:
						}
					}
					firstTokenTimer = nil
					firstTokenC = nil
				}
			}

			ra.streamPayloadWritten.Store(true)
			ra.getStreamWriter().Write(data)
			ra.getStreamWriter().Flush()
		}
	}
}

// transformStreamData 转换流式数据
func (ra *relayAttempt) transformStreamData(ctx context.Context, data string) ([]byte, error) {
	events, ok, err := ra.decodeOutboundStreamEvents(ctx, []byte(data))
	if err != nil {
		log.Warnf("failed to transform stream events: %v", err)
		return nil, err
	}
	if ok {
		return ra.encodeInboundStreamEvents(ctx, events)
	}

	internalStream, err := ra.decodeOutboundStreamResponse(ctx, []byte(data))
	if err != nil {
		log.Warnf("failed to transform stream: %v", err)
		return nil, err
	}
	if internalStream == nil {
		return nil, nil
	}

	return ra.encodeInboundStreamResponse(ctx, internalStream)
}

func (ra *relayAttempt) decodeOutboundStreamEvents(ctx context.Context, data []byte) ([]model.StreamEvent, bool, error) {
	outEventAdapter, ok := ra.outAdapter.(model.OutboundStreamEventTransformer)
	if !ok {
		return nil, false, nil
	}
	if _, ok := ra.inAdapter.(model.InboundStreamEventTransformer); !ok {
		return nil, false, nil
	}
	events, err := outEventAdapter.TransformStreamEvent(ctx, data)
	if err != nil {
		return nil, true, err
	}
	return events, true, nil
}

func (ra *relayAttempt) encodeInboundStreamEvents(ctx context.Context, events []model.StreamEvent) ([]byte, error) {
	if len(events) == 0 {
		return nil, nil
	}
	inEventAdapter, ok := ra.inAdapter.(model.InboundStreamEventTransformer)
	if !ok {
		return nil, nil
	}
	inStream, err := inEventAdapter.TransformStreamEvents(ctx, events)
	if err != nil {
		log.Warnf("failed to transform inbound stream events: %v", err)
		return nil, err
	}
	return inStream, nil
}

func (ra *relayAttempt) decodeOutboundStreamResponse(ctx context.Context, data []byte) (*model.InternalLLMResponse, error) {
	return ra.outAdapter.TransformStream(ctx, data)
}

func (ra *relayAttempt) encodeInboundStreamResponse(ctx context.Context, internalStream *model.InternalLLMResponse) ([]byte, error) {
	inStream, err := ra.inAdapter.TransformStream(ctx, internalStream)
	if err != nil {
		log.Warnf("failed to transform stream: %v", err)
		return nil, err
	}
	return inStream, nil
}

// handleResponse 处理非流式响应
func (ra *relayAttempt) handleResponse(ctx context.Context, response *http.Response) error {
	internalResponse, err := ra.outAdapter.TransformResponse(ctx, response)
	if err != nil {
		log.Warnf("failed to transform response: %v", err)
		return fmt.Errorf("failed to transform outbound response: %w", err)
	}

	inResponse, err := ra.inAdapter.TransformResponse(ctx, internalResponse)
	if err != nil {
		log.Warnf("failed to transform response: %v", err)
		return fmt.Errorf("failed to transform inbound response: %w", err)
	}

	ra.c.Data(http.StatusOK, "application/json", inResponse)
	return nil
}

// collectResponse 收集响应信息
func (ra *relayAttempt) collectResponse() {
	if ra == nil || ra.inAdapter == nil || ra.metrics == nil {
		return
	}
	internalResponse, err := ra.inAdapter.GetInternalResponse(ra.requestContext())
	if err != nil {
		log.Debugf("collectResponse: failed to get internal response: %v", err)
		return
	}
	if internalResponse == nil {
		log.Debugf("collectResponse: internal response is nil (stream may not be complete)")
		return
	}

	actualModel := strings.TrimSpace(internalResponse.Model)
	if actualModel == "" && ra.internalRequest != nil {
		actualModel = strings.TrimSpace(ra.internalRequest.Model)
	}
	ra.metrics.SetInternalResponse(internalResponse, actualModel)
}

// shouldPassthroughAnthropic 判定是否走 Anthropic→Anthropic 原生直通路径。
// 条件：客户端以 Anthropic Messages 格式到达、通道出站类型为 Anthropic、原始 body 已保留。
func (ra *relayAttempt) shouldPassthroughAnthropic() bool {
	if ra == nil || ra.internalRequest == nil || ra.channel == nil {
		return false
	}
	if len(ra.rawBody) == 0 {
		return false
	}
	if ra.internalRequest.RawAPIFormat != model.APIFormatAnthropicMessage {
		return false
	}
	return ra.channel.Type == outbound.OutboundTypeAnthropic
}

// shouldPassthroughOpenAIResponses 判定是否走 OpenAI Responses→OpenAI Responses 原生直通路径。
// 同协议 HTTP/SSE 请求默认直通，避免 Responses 原生事件、未知字段或输出项在内部模型往返时被重组。
func (ra *relayAttempt) shouldPassthroughOpenAIResponses() bool {
	if ra == nil || ra.internalRequest == nil || ra.channel == nil {
		return false
	}
	if ra.c == nil {
		return false
	}
	if len(ra.rawBody) == 0 {
		return false
	}
	if ra.internalRequest.RawAPIFormat != model.APIFormatOpenAIResponse {
		return false
	}
	if ra.internalRequest.IsOpenAIExactReplayRequest() || requiresUpstreamWSContinuation(ra.internalRequest) {
		return false
	}
	return ra.channel.Type == outbound.OutboundTypeOpenAIResponse
}

// forwardViaHTTPPassthroughOpenAIResponses 直通 OpenAI Responses 原始 JSON/SSE。
// 客户端原始 body 只改顶层 model 后发上游，响应原样写回客户端；旁路解析仅用于 metrics。
func (ra *relayAttempt) forwardViaHTTPPassthroughOpenAIResponses(ctx context.Context) (int, error) {
	openaiOut, ok := ra.outAdapter.(*openaiOutbound.ResponseOutbound)
	if !ok {
		return ra.forwardViaHTTPStandard(ctx)
	}

	outboundRequest, err := openaiOut.TransformRequestRaw(
		ctx,
		ra.rawBody,
		ra.internalRequest.Model,
		ra.channel.GetBaseUrl(),
		ra.usedKey.ChannelKey,
		ra.internalRequest.Query,
	)
	if err != nil {
		log.Warnf("failed to create passthrough request: %v", err)
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	if err := ra.applyParamOverride(outboundRequest); err != nil {
		return 0, err
	}
	// 剥离下游客户端带的 beta query param
	if q := outboundRequest.URL.Query(); q.Has("beta") {
		q.Del("beta")
		outboundRequest.URL.RawQuery = q.Encode()
	}
	ra.copyHeaders(outboundRequest)
	impersonate.ApplyClientHeaders(outboundRequest, ra.channel, ra.internalRequest.Model)
	// CustomHeader 优先级最高
	if len(ra.channel.CustomHeader) > 0 {
		for _, header := range ra.channel.CustomHeader {
			outboundRequest.Header.Set(header.HeaderKey, header.HeaderValue)
		}
	}
	outboundRequest.Header.Set("Content-Type", "application/json")

	response, err := ra.sendRequest(outboundRequest)
	if err != nil {
		return 0, fmt.Errorf("failed to send request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		ra.retryAfter = parseRetryAfter(response.Header.Get("Retry-After"))
		body, readErr := io.ReadAll(response.Body)
		if readErr != nil {
			return response.StatusCode, fmt.Errorf("failed to read response body: %w", readErr)
		}
		statusCode := normalizeUpstreamStatusCode(response.StatusCode, string(body))
		log.Warnf("upstream error from channel %s: status=%d, body=%s", ra.channel.Name, response.StatusCode, string(body))
		return statusCode, fmt.Errorf("upstream error: %d: %s", response.StatusCode, string(body))
	}

	if ra.internalRequest.Stream != nil && *ra.internalRequest.Stream {
		if err := ra.handleStreamResponsePassthroughOpenAIResponses(ctx, response); err != nil {
			return 0, err
		}
		return response.StatusCode, nil
	}
	if err := ra.handleResponsePassthroughOpenAIResponses(ctx, response); err != nil {
		return 0, err
	}
	return response.StatusCode, nil
}

func (ra *relayAttempt) handleStreamResponsePassthroughOpenAIResponses(ctx context.Context, response *http.Response) error {
	defer ra.closeFirstTokenBudget()

	if ct := response.Header.Get("Content-Type"); ct != "" && !strings.Contains(strings.ToLower(ct), "text/event-stream") {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 16*1024))
		return fmt.Errorf("upstream returned non-SSE content-type %q for stream request: %s", ct, string(body))
	}

	// 交接早期心跳给本函数内层 ticker
	ra.heartbeat.Hand()

	writer := ra.getStreamWriter()
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("Connection", "keep-alive")
	writer.Header().Set("X-Accel-Buffering", "no")

	heartbeatTicker, heartbeatC := newStreamHeartbeatTicker()
	if heartbeatTicker != nil {
		defer heartbeatTicker.Stop()
	}

	firstToken := true
	type rawReadResult struct {
		chunk []byte
		err   error
	}
	results := make(chan rawReadResult, 1)
	safe.Go("relay-stream-read", func() {
		defer close(results)
		buf := make([]byte, 32*1024)
		for {
			n, err := response.Body.Read(buf)
			if n > 0 {
				chunk := append([]byte(nil), buf[:n]...)
				results <- rawReadResult{chunk: chunk}
			}
			if err != nil {
				results <- rawReadResult{err: err}
				return
			}
		}
	})
	var rawStream bytes.Buffer

	finishStream := func(c context.Context) error {
		if !ra.streamPayloadWritten.Load() {
			return errEmptyUpstreamStream
		}
		ra.collectOpenAIResponsesPassthroughMetrics(c, rawStream.Bytes())
		log.Debugf("stream end")
		return nil
	}

	disconnect := func() error {
		err := contextError(ctx)
		if timeoutErr := ra.firstTokenTimeoutIfNeeded(ctx, err); timeoutErr != nil {
			return timeoutErr
		}
		if isLocalRelayBudgetExceeded(ctx, err) {
			return err
		}
		// 客户端收到终态事件后立即断连是 SDK 标准行为，此时上游 EOF 可能尚未到达；
		// 缓存流已含终态事件说明响应已完整送达，按正常结束处理，避免误记失败且丢失 usage。
		if streamReachedTerminalEvent(rawStream.Bytes(), responsesPassthroughTerminalEvents) {
			return finishStream(context.Background())
		}
		log.Debugf("client disconnected, stopping stream: written=%t raw_bytes=%d first_token_seen=%t elapsed=%s", ra.streamPayloadWritten.Load(), rawStream.Len(), !firstToken, time.Since(ra.metrics.StartTime))
		if rawStream.Len() > 0 {
			ra.collectOpenAIResponsesPassthroughMetrics(context.Background(), rawStream.Bytes())
		}
		return err
	}

	var firstTokenTimer *time.Timer
	var firstTokenC <-chan time.Time
	if firstToken && ra.firstTokenTimeOutSec > 0 && ra.firstTokenBudget == nil {
		firstTokenTimer = time.NewTimer(time.Duration(ra.firstTokenTimeOutSec) * time.Second)
		firstTokenC = firstTokenTimer.C
		defer func() {
			if firstTokenTimer != nil {
				firstTokenTimer.Stop()
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			// 上游 EOF 与客户端断连可能同时发生，select 在多就绪 case 中随机选择，
			// 此时优先消费 results 中已就绪的终止信号，避免把正常结束的流误判为断连。
			select {
			case r, ok := <-results:
				if !ok || (r.err != nil && r.err == io.EOF) {
					return finishStream(context.Background())
				}
				if r.err != nil {
					if timeoutErr := ra.firstTokenTimeoutIfNeeded(ctx, r.err); timeoutErr != nil {
						return timeoutErr
					}
					// 断连取消沿出站请求传播、被读侧先观察到时，按断连处理而非上游流失败
					if !errors.Is(r.err, context.Canceled) {
						log.Warnf("failed to read event: %v", r.err)
						return fmt.Errorf("failed to read stream event: %w", r.err)
					}
				}
			default:
			}
			return disconnect()
		case <-firstTokenC:
			log.Warnf("first token timeout (%ds), switching channel", ra.firstTokenTimeOutSec)
			_ = response.Body.Close()
			return ra.firstTokenTimeoutError()
		case <-heartbeatC:
			if err := writeSSEHeartbeat(writer); err != nil {
				return err
			}
		case r, ok := <-results:
			if !ok {
				return finishStream(ctx)
			}
			if r.err != nil {
				if r.err == io.EOF {
					return finishStream(ctx)
				}
				if timeoutErr := ra.firstTokenTimeoutIfNeeded(ctx, r.err); timeoutErr != nil {
					return timeoutErr
				}
				if errors.Is(r.err, context.Canceled) && contextError(ctx) != nil {
					return disconnect()
				}
				log.Warnf("failed to read event: %v", r.err)
				return fmt.Errorf("failed to read stream event: %w", r.err)
			}
			if len(r.chunk) == 0 {
				continue
			}
			if _, werr := writer.Write(r.chunk); werr != nil {
				return werr
			}
			ra.streamPayloadWritten.Store(true)
			_, _ = rawStream.Write(r.chunk)
			writer.Flush()

			if firstToken {
				ra.metrics.SetFirstTokenTime(time.Now())
				firstToken = false
				ra.stopFirstTokenTimer()
				if firstTokenTimer != nil {
					if !firstTokenTimer.Stop() {
						select {
						case <-firstTokenTimer.C:
						default:
						}
					}
					firstTokenTimer = nil
					firstTokenC = nil
				}
			}
		}
	}
}

func (ra *relayAttempt) collectOpenAIResponsesPassthroughMetrics(ctx context.Context, rawStream []byte) {
	if len(rawStream) == 0 {
		return
	}
	outEventAdapter, outOk := ra.outAdapter.(model.OutboundStreamEventTransformer)
	inEventAdapter, inOk := ra.inAdapter.(model.InboundStreamEventTransformer)
	if outOk && inOk {
		readCfg := &sse.ReadConfig{MaxEventSize: maxSSEEventSize}
		for ev, err := range sse.Read(bytes.NewReader(rawStream), readCfg) {
			if err != nil {
				log.Debugf("openai responses passthrough metrics parse skipped: %v", err)
				return
			}
			if events, terr := outEventAdapter.TransformStreamEvent(ctx, []byte(ev.Data)); terr == nil && len(events) > 0 {
				_, _ = inEventAdapter.TransformStreamEvents(ctx, events)
			}
		}
		return
	}
	readCfg := &sse.ReadConfig{MaxEventSize: maxSSEEventSize}
	for ev, err := range sse.Read(bytes.NewReader(rawStream), readCfg) {
		if err != nil {
			log.Debugf("openai responses passthrough metrics parse skipped: %v", err)
			return
		}
		if internalStream, terr := ra.outAdapter.TransformStream(ctx, []byte(ev.Data)); terr == nil && internalStream != nil {
			_, _ = ra.inAdapter.TransformStream(ctx, internalStream)
		}
	}
}

// responsesPassthroughTerminalEvents / anthropicPassthroughTerminalEvents 定义各协议
// SSE 流的终态事件类型；缓存流中出现终态事件即视为上游响应已完整送达。
var (
	responsesPassthroughTerminalEvents = map[string]struct{}{
		"response.completed":  {},
		"response.failed":     {},
		"response.incomplete": {},
		"error":               {},
	}
	anthropicPassthroughTerminalEvents = map[string]struct{}{
		"message_stop": {},
		"error":        {},
	}
)

// streamReachedTerminalEvent 报告缓存的原始 SSE 流是否已包含协议终态事件。
// 客户端 SDK 收到终态事件后会立即断连而不等上游 EOF，断连取消会沿出站请求
// 传播打断上游读取；此时读取被取消不代表流未完成。
func streamReachedTerminalEvent(rawStream []byte, terminalTypes map[string]struct{}) bool {
	if len(rawStream) == 0 {
		return false
	}
	readCfg := &sse.ReadConfig{MaxEventSize: maxSSEEventSize}
	for ev, err := range sse.Read(bytes.NewReader(rawStream), readCfg) {
		if err != nil {
			break
		}
		typ := strings.TrimSpace(ev.Type)
		if typ == "" {
			var head struct {
				Type string `json:"type"`
			}
			if json.Unmarshal([]byte(ev.Data), &head) == nil {
				typ = head.Type
			}
		}
		if _, ok := terminalTypes[typ]; ok {
			return true
		}
	}
	return false
}

func (ra *relayAttempt) handleResponsePassthroughOpenAIResponses(ctx context.Context, response *http.Response) error {
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	contentType := response.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/json"
	}
	ra.c.Data(response.StatusCode, contentType, body)

	sidecarResp := &http.Response{
		StatusCode: response.StatusCode,
		Header:     response.Header.Clone(),
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
	if internalResponse, terr := ra.outAdapter.TransformResponse(ctx, sidecarResp); terr == nil && internalResponse != nil {
		_, _ = ra.inAdapter.TransformResponse(ctx, internalResponse)
	}
	return nil
}

// forwardViaHTTPPassthroughAnthropic 直通路径：客户端原始 body 原样转发；上游响应原样写回客户端；
// 旁路解析 SSE/JSON 仅用于 metrics（token 统计、计费），不参与写回客户端的字节流。
func (ra *relayAttempt) forwardViaHTTPPassthroughAnthropic(ctx context.Context) (int, error) {
	anthropicOut, ok := ra.outAdapter.(*outAnthropic.MessageOutbound)
	if !ok {
		// 通道注册异常，回退到标准路径
		return ra.forwardViaHTTPStandard(ctx)
	}

	outboundRequest, err := anthropicOut.TransformRequestRaw(
		ctx,
		ra.rawBody,
		ra.internalRequest.Model,
		ra.channel.GetBaseUrl(),
		ra.usedKey.ChannelKey,
		ra.internalRequest.Query,
	)
	if err != nil {
		log.Warnf("failed to create passthrough request: %v", err)
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	// 记录实际上行 payload；直通路径会在这里把顶层 model 改写成命中的上游模型。
	if err := ra.applyParamOverride(outboundRequest); err != nil {
		return 0, err
	}

	// 剥离下游客户端带的 beta query param，上游不认识
	if q := outboundRequest.URL.Query(); q.Has("beta") {
		q.Del("beta")
		outboundRequest.URL.RawQuery = q.Encode()
	}

	// 复制客户端请求头（hop-by-hop 过滤保证 x-api-key/authorization/host/content-length
	// /accept-encoding 不会覆盖出站设置的关键头；anthropic-beta / anthropic-version /
	// user-agent / x-stainless-* 等原样透传）
	ra.copyHeaders(outboundRequest)
	impersonate.ApplyClientHeaders(outboundRequest, ra.channel, ra.internalRequest.Model)
	// CustomHeader 优先级最高，在模拟之后再覆盖
	if len(ra.channel.CustomHeader) > 0 {
		for _, header := range ra.channel.CustomHeader {
			outboundRequest.Header.Set(header.HeaderKey, header.HeaderValue)
		}
	}
	ra.normalizeAnthropicPassthroughStreamHeaders(outboundRequest)
	ra.logAnthropicPassthroughRequestSummary(outboundRequest)

	// 发送请求
	response, err := ra.sendRequest(outboundRequest)
	if err != nil {
		return 0, fmt.Errorf("failed to send request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		ra.retryAfter = parseRetryAfter(response.Header.Get("Retry-After"))
		body, readErr := io.ReadAll(response.Body)
		if readErr != nil {
			return response.StatusCode, fmt.Errorf("failed to read response body: %w", readErr)
		}
		statusCode := normalizeUpstreamStatusCode(response.StatusCode, string(body))
		log.Warnf("upstream error from channel %s: status=%d, body=%s", ra.channel.Name, response.StatusCode, string(body))
		return statusCode, fmt.Errorf("upstream error: %d: %s", response.StatusCode, string(body))
	}

	if ra.isAnthropicPassthroughStreamRequest() {
		if err := ra.handleStreamResponsePassthroughAnthropic(ctx, response); err != nil {
			return 0, err
		}
		return response.StatusCode, nil
	}
	if err := ra.handleResponsePassthroughAnthropic(ctx, response); err != nil {
		return 0, err
	}
	return response.StatusCode, nil
}

func (ra *relayAttempt) isAnthropicPassthroughStreamRequest() bool {
	if ra == nil {
		return false
	}
	if len(ra.rawBody) > 0 && summarizeAnthropicRequestBody(ra.rawBody).Stream {
		return true
	}
	return ra.internalRequest != nil && ra.internalRequest.Stream != nil && *ra.internalRequest.Stream
}

func (ra *relayAttempt) normalizeAnthropicPassthroughStreamHeaders(outboundRequest *http.Request) {
	if outboundRequest == nil {
		return
	}
	if ra.isAnthropicPassthroughStreamRequest() {
		outboundRequest.Header.Set("Accept", "text/event-stream")
	}
}

// forwardViaHTTPStandard 是 forwardViaHTTP 的原路径（直通判定失败时的兜底）。
// 留作显式出口，避免 passthrough 失败时的递归。
func (ra *relayAttempt) forwardViaHTTPStandard(ctx context.Context) (int, error) {
	outboundRequest, err := ra.outAdapter.TransformRequest(
		ctx,
		ra.internalRequest,
		ra.channel.GetBaseUrl(),
		ra.usedKey.ChannelKey,
	)
	if err != nil {
		log.Warnf("failed to create request: %v", err)
		return 0, fmt.Errorf("failed to create request: %w", err)
	}
	if err := ra.applyParamOverride(outboundRequest); err != nil {
		return 0, err
	}
	// 剥离下游客户端带的 beta query param
	if q := outboundRequest.URL.Query(); q.Has("beta") {
		q.Del("beta")
		outboundRequest.URL.RawQuery = q.Encode()
	}
	ra.copyHeaders(outboundRequest)
	impersonate.ApplyClientHeaders(outboundRequest, ra.channel, ra.internalRequest.Model)
	// CustomHeader 优先级最高
	if len(ra.channel.CustomHeader) > 0 {
		for _, header := range ra.channel.CustomHeader {
			outboundRequest.Header.Set(header.HeaderKey, header.HeaderValue)
		}
	}

	response, err := ra.sendRequest(outboundRequest)
	if err != nil {
		return 0, fmt.Errorf("failed to send request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		ra.retryAfter = parseRetryAfter(response.Header.Get("Retry-After"))
		body, readErr := io.ReadAll(response.Body)
		if readErr != nil {
			return response.StatusCode, fmt.Errorf("failed to read response body: %w", readErr)
		}
		statusCode := normalizeUpstreamStatusCode(response.StatusCode, string(body))
		log.Warnf("upstream error from channel %s: status=%d, body=%s", ra.channel.Name, response.StatusCode, string(body))
		return statusCode, fmt.Errorf("upstream error: %d: %s", response.StatusCode, string(body))
	}

	if ra.internalRequest.Stream != nil && *ra.internalRequest.Stream {
		if err := ra.handleStreamResponse(ctx, response); err != nil {
			return 0, err
		}
		return response.StatusCode, nil
	}
	if err := ra.handleResponse(ctx, response); err != nil {
		return 0, err
	}
	return response.StatusCode, nil
}

// handleStreamResponsePassthroughAnthropic 将上游 SSE 事件**原样**转发给客户端（不经过
// outbound→inbound 双向转换），同时用 outbound.TransformStream 旁路解析事件供 metrics 聚合使用。
func (ra *relayAttempt) handleStreamResponsePassthroughAnthropic(ctx context.Context, response *http.Response) error {
	defer ra.closeFirstTokenBudget()

	if ct := response.Header.Get("Content-Type"); ct != "" && !strings.Contains(strings.ToLower(ct), "text/event-stream") {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 16*1024))
		if converted, err := ra.convertOpenAIJSONToAnthropicSSE(ctx, body, response); err != nil {
			return err
		} else if len(converted) > 0 {
			writer := ra.getStreamWriter()
			writer.Header().Set("Content-Type", "text/event-stream")
			writer.Header().Set("Cache-Control", "no-cache")
			writer.Header().Set("Connection", "keep-alive")
			writer.Header().Set("X-Accel-Buffering", "no")
			if _, werr := writer.Write(converted); werr != nil {
				return werr
			}
			ra.streamPayloadWritten.Store(true)
			writer.Flush()
			ra.collectAnthropicPassthroughMetrics(ctx, converted)
			ra.collectResponse()
			return nil
		}
		return fmt.Errorf("upstream returned non-SSE content-type %q for stream request: %s", ct, string(body))
	}

	// 交接早期心跳给本函数内层 ticker
	ra.heartbeat.Hand()

	writer := ra.getStreamWriter()

	// 设置 SSE 响应头
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("Connection", "keep-alive")
	writer.Header().Set("X-Accel-Buffering", "no")

	heartbeatTicker, heartbeatC := newStreamHeartbeatTicker()
	if heartbeatTicker != nil {
		defer heartbeatTicker.Stop()
	}

	firstToken := true
	type rawReadResult struct {
		chunk []byte
		err   error
	}
	results := make(chan rawReadResult, 1)
	safe.Go("relay-stream-read", func() {
		defer close(results)
		buf := make([]byte, 32*1024)
		for {
			n, err := response.Body.Read(buf)
			if n > 0 {
				chunk := append([]byte(nil), buf[:n]...)
				results <- rawReadResult{chunk: chunk}
			}
			if err != nil {
				results <- rawReadResult{err: err}
				return
			}
		}
	})
	var rawStream bytes.Buffer
	var openAIStream model.OutboundStreamEventTransformer
	var openAIProtocol string
	var openAIStreamBuffer []byte
	var openAIChatTerminal openAIChatTerminalState
	var nativeAnthropicPending bytes.Buffer
	inboundStream, _ := ra.inAdapter.(model.InboundStreamEventTransformer)

	flushOpenAIChatTerminal := func() error {
		if openAIProtocol != "openai_chat" || inboundStream == nil {
			return nil
		}
		if openAIChatTerminalIsEmptyStop(&openAIChatTerminal) {
			return errEmptyUpstreamStream
		}
		events := flushOpenAIChatTerminalEvents(&openAIChatTerminal)
		if len(events) == 0 {
			return nil
		}
		converted, err := inboundStream.TransformStreamEvents(ctx, events)
		if err != nil {
			return err
		}
		if len(converted) == 0 {
			return nil
		}
		if rawStream.Len() == 0 && isEmptyAnthropicAssistantStopSSE(converted) {
			return errEmptyUpstreamStream
		}
		_, _ = rawStream.Write(converted)
		if _, werr := writer.Write(converted); werr != nil {
			return werr
		}
		ra.streamPayloadWritten.Store(true)
		writer.Flush()
		return nil
	}

	finishStream := func(c context.Context) error {
		if err := flushOpenAIChatTerminal(); err != nil {
			return err
		}
		if rawStream.Len() == 0 {
			return errEmptyUpstreamStream
		}
		ra.collectAnthropicPassthroughMetrics(c, rawStream.Bytes())
		ra.collectResponse()
		log.Debugf("stream end")
		return nil
	}

	disconnect := func() error {
		err := contextError(ctx)
		if timeoutErr := ra.firstTokenTimeoutIfNeeded(ctx, err); timeoutErr != nil {
			return timeoutErr
		}
		if isLocalRelayBudgetExceeded(ctx, err) {
			return err
		}
		// 客户端收到终态事件后立即断连是 SDK 标准行为，此时上游 EOF 可能尚未到达；
		// 缓存流已含终态事件说明响应已完整送达，按正常结束处理，避免误记失败且丢失 usage。
		if streamReachedTerminalEvent(rawStream.Bytes(), anthropicPassthroughTerminalEvents) {
			return finishStream(context.Background())
		}
		log.Debugf("client disconnected, stopping stream: written=%t raw_bytes=%d first_token_seen=%t elapsed=%s", ra.streamPayloadWritten.Load(), rawStream.Len(), !firstToken, time.Since(ra.metrics.StartTime))
		if rawStream.Len() > 0 {
			ra.collectAnthropicPassthroughMetrics(context.Background(), rawStream.Bytes())
			ra.collectResponse()
		}
		return err
	}

	var firstTokenTimer *time.Timer
	var firstTokenC <-chan time.Time
	if firstToken && ra.firstTokenTimeOutSec > 0 && ra.firstTokenBudget == nil {
		firstTokenTimer = time.NewTimer(time.Duration(ra.firstTokenTimeOutSec) * time.Second)
		firstTokenC = firstTokenTimer.C
		defer func() {
			if firstTokenTimer != nil {
				firstTokenTimer.Stop()
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			// 上游 EOF 与客户端断连可能同时发生，select 在多就绪 case 中随机选择，
			// 此时优先消费 results 中已就绪的终止信号，避免把正常结束的流误判为断连。
			select {
			case r, ok := <-results:
				if !ok || (r.err != nil && r.err == io.EOF) {
					return finishStream(context.Background())
				}
				if r.err != nil {
					if timeoutErr := ra.firstTokenTimeoutIfNeeded(ctx, r.err); timeoutErr != nil {
						return timeoutErr
					}
					// 断连取消沿出站请求传播、被读侧先观察到时，按断连处理而非上游流失败
					if !errors.Is(r.err, context.Canceled) {
						log.Warnf("failed to read event: %v", r.err)
						return fmt.Errorf("failed to read stream event: %w", r.err)
					}
				}
			default:
			}
			return disconnect()
		case <-firstTokenC:
			log.Warnf("first token timeout (%ds), switching channel", ra.firstTokenTimeOutSec)
			_ = response.Body.Close()
			return ra.firstTokenTimeoutError()
		case <-heartbeatC:
			if err := writeSSEHeartbeat(writer); err != nil {
				return err
			}
		case r, ok := <-results:
			if !ok {
				return finishStream(ctx)
			}
			if r.err != nil {
				if r.err == io.EOF {
					return finishStream(ctx)
				}
				if timeoutErr := ra.firstTokenTimeoutIfNeeded(ctx, r.err); timeoutErr != nil {
					return timeoutErr
				}
				if errors.Is(r.err, context.Canceled) && contextError(ctx) != nil {
					return disconnect()
				}
				log.Warnf("failed to read event: %v", r.err)
				return fmt.Errorf("failed to read stream event: %w", r.err)
			}

			if len(r.chunk) == 0 {
				continue
			}
			chunk := r.chunk
			if converted, protocol, err := convertOpenAIStreamChunkToAnthropic(ctx, r.chunk, openAIProtocol, &openAIStream, inboundStream, &openAIStreamBuffer, &openAIChatTerminal); err != nil {
				return err
			} else if protocol != "" {
				openAIProtocol = protocol
				chunk = converted
			}
			if len(chunk) == 0 {
				continue
			}
			if openAIProtocol == "" {
				candidate := append(append([]byte(nil), nativeAnthropicPending.Bytes()...), chunk...)
				hasPayload, isTerminal, hasCompleteFrame, passthrough, err := classifyAnthropicSSEPayload(candidate)
				if err != nil {
					return err
				}
				if !hasCompleteFrame {
					nativeAnthropicPending.Reset()
					_, _ = nativeAnthropicPending.Write(candidate)
					continue
				}
				if !hasPayload && !passthrough {
					nativeAnthropicPending.Reset()
					_, _ = nativeAnthropicPending.Write(candidate)
					if isTerminal {
						return errEmptyUpstreamStream
					}
					continue
				}
				chunk = candidate
				nativeAnthropicPending.Reset()
			}
			_, _ = rawStream.Write(chunk)
			if _, werr := writer.Write(chunk); werr != nil {
				return werr
			}
			ra.streamPayloadWritten.Store(true)
			writer.Flush()

			if firstToken {
				ra.metrics.SetFirstTokenTime(time.Now())
				firstToken = false
				ra.stopFirstTokenTimer()
				if firstTokenTimer != nil {
					if !firstTokenTimer.Stop() {
						select {
						case <-firstTokenTimer.C:
						default:
						}
					}
					firstTokenTimer = nil
					firstTokenC = nil
				}
			}
		}
	}
}

func (ra *relayAttempt) convertOpenAIJSONToAnthropicSSE(ctx context.Context, body []byte, response *http.Response) ([]byte, error) {
	var envelope struct {
		Object  string          `json:"object"`
		Choices json.RawMessage `json:"choices"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, nil
	}
	var outAdapter model.Outbound
	switch {
	case len(envelope.Choices) > 0 || strings.Contains(envelope.Object, "chat.completion"):
		outAdapter = &openaiOutbound.ChatOutbound{}
	case envelope.Object == "response":
		outAdapter = &openaiOutbound.ResponseOutbound{}
	default:
		return nil, nil
	}

	sidecar := &http.Response{
		StatusCode: response.StatusCode,
		Header:     response.Header.Clone(),
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
	internalResponse, err := outAdapter.TransformResponse(ctx, sidecar)
	if err != nil {
		return nil, fmt.Errorf("failed to convert OpenAI JSON stream fallback from Anthropic channel: %w", err)
	}
	rendered, err := inbound.Get(inbound.InboundTypeAnthropic).TransformResponse(ctx, internalResponse)
	if err != nil {
		return nil, fmt.Errorf("failed to render converted Anthropic JSON fallback: %w", err)
	}
	if anthropicMessageJSONIsEmptyStop(rendered) {
		return nil, errEmptyUpstreamStream
	}
	return anthropicMessageJSONToSSE(rendered)
}

func anthropicMessageJSONIsEmptyStop(body []byte) bool {
	var msg struct {
		Type    string            `json:"type"`
		Content []json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(body, &msg); err != nil {
		return false
	}
	if msg.Type != "" && msg.Type != "message" {
		return false
	}
	return len(msg.Content) == 0
}

func anthropicMessageJSONToSSE(body []byte) ([]byte, error) {
	var msg struct {
		ID           string                     `json:"id"`
		Type         string                     `json:"type"`
		Role         string                     `json:"role"`
		Model        string                     `json:"model"`
		Content      []json.RawMessage          `json:"content"`
		StopReason   *string                    `json:"stop_reason"`
		StopSequence *string                    `json:"stop_sequence"`
		Usage        *model.Usage               `json:"usage"`
		Raw          map[string]json.RawMessage `json:"-"`
	}
	if err := json.Unmarshal(body, &msg); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	startMsg := map[string]any{
		"id":      msg.ID,
		"type":    "message",
		"role":    msg.Role,
		"model":   msg.Model,
		"content": []any{},
	}
	if msg.Usage != nil {
		startMsg["usage"] = msg.Usage
	}
	startEvent := map[string]any{"type": "message_start", "message": startMsg}
	if err := writeAnthropicJSONSSE(&out, "message_start", startEvent); err != nil {
		return nil, err
	}
	for idx, rawBlock := range msg.Content {
		var block map[string]any
		if err := json.Unmarshal(rawBlock, &block); err != nil {
			return nil, err
		}
		if err := writeAnthropicContentBlockSSE(&out, idx, block); err != nil {
			return nil, err
		}
	}
	delta := map[string]any{}
	if msg.StopReason != nil {
		delta["stop_reason"] = *msg.StopReason
	}
	if msg.StopSequence != nil {
		delta["stop_sequence"] = *msg.StopSequence
	}
	msgDelta := map[string]any{"type": "message_delta", "delta": delta}
	if msg.Usage != nil {
		msgDelta["usage"] = msg.Usage
	}
	if err := writeAnthropicJSONSSE(&out, "message_delta", msgDelta); err != nil {
		return nil, err
	}
	if err := writeAnthropicJSONSSE(&out, "message_stop", map[string]any{"type": "message_stop"}); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func writeAnthropicContentBlockSSE(out *bytes.Buffer, idx int, block map[string]any) error {
	blockType, _ := block["type"].(string)
	startBlock := block
	var deltas []map[string]any
	switch blockType {
	case "text":
		text, _ := block["text"].(string)
		startBlock = map[string]any{"type": "text", "text": ""}
		if text != "" {
			deltas = append(deltas, map[string]any{"type": "text_delta", "text": text})
		}
	case "thinking":
		thinking, _ := block["thinking"].(string)
		signature, _ := block["signature"].(string)
		startBlock = map[string]any{"type": "thinking", "thinking": ""}
		if thinking != "" {
			deltas = append(deltas, map[string]any{"type": "thinking_delta", "thinking": thinking})
		}
		if signature != "" {
			deltas = append(deltas, map[string]any{"type": "signature_delta", "signature": signature})
		}
	case "tool_use":
		startBlock = cloneJSONMap(block)
		startBlock["input"] = map[string]any{}
		if input, ok := block["input"]; ok && input != nil {
			encoded, err := json.Marshal(input)
			if err != nil {
				return err
			}
			if string(encoded) != "{}" && string(encoded) != "null" {
				deltas = append(deltas, map[string]any{"type": "input_json_delta", "partial_json": string(encoded)})
			}
		}
	}
	if err := writeAnthropicJSONSSE(out, "content_block_start", map[string]any{
		"type":          "content_block_start",
		"index":         idx,
		"content_block": startBlock,
	}); err != nil {
		return err
	}
	for _, delta := range deltas {
		if err := writeAnthropicJSONSSE(out, "content_block_delta", map[string]any{
			"type":  "content_block_delta",
			"index": idx,
			"delta": delta,
		}); err != nil {
			return err
		}
	}
	if err := writeAnthropicJSONSSE(out, "content_block_stop", map[string]any{
		"type":  "content_block_stop",
		"index": idx,
	}); err != nil {
		return err
	}
	return nil
}

func cloneJSONMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func writeAnthropicJSONSSE(out *bytes.Buffer, eventName string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	out.WriteString("event:")
	out.WriteString(eventName)
	out.WriteString("\n")
	out.WriteString("data:")
	out.Write(data)
	out.WriteString("\n\n")
	return nil
}

func (ra *relayAttempt) collectAnthropicPassthroughMetrics(ctx context.Context, rawStream []byte) {
	if len(rawStream) == 0 {
		return
	}
	outEventAdapter, outOk := ra.outAdapter.(model.OutboundStreamEventTransformer)
	inEventAdapter, inOk := ra.inAdapter.(model.InboundStreamEventTransformer)
	if outOk && inOk {
		readCfg := &sse.ReadConfig{MaxEventSize: maxSSEEventSize}
		for ev, err := range sse.Read(bytes.NewReader(rawStream), readCfg) {
			if err != nil {
				log.Debugf("anthropic passthrough metrics parse skipped: %v", err)
				return
			}
			if events, terr := outEventAdapter.TransformStreamEvent(ctx, []byte(ev.Data)); terr == nil && len(events) > 0 {
				_, _ = inEventAdapter.TransformStreamEvents(ctx, events)
			}
		}
		return
	}
	readCfg := &sse.ReadConfig{MaxEventSize: maxSSEEventSize}
	for ev, err := range sse.Read(bytes.NewReader(rawStream), readCfg) {
		if err != nil {
			log.Debugf("anthropic passthrough metrics parse skipped: %v", err)
			return
		}
		if internalStream, terr := ra.outAdapter.TransformStream(ctx, []byte(ev.Data)); terr == nil && internalStream != nil {
			_, _ = ra.inAdapter.TransformStream(ctx, internalStream)
		}
	}
}

// handleResponsePassthroughAnthropic 非流式直通：upstream JSON 原样写回客户端；旁路解析用于 metrics。
func (ra *relayAttempt) handleResponsePassthroughAnthropic(ctx context.Context, response *http.Response) error {
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}
	if converted, ok, err := convertOpenAIResponseToAnthropic(ctx, body, response); err != nil {
		return err
	} else if ok {
		body = converted
	}

	contentType := response.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/json"
	}
	ra.c.Data(http.StatusOK, contentType, body)

	// 旁路解析：复用 outbound.TransformResponse → inbound.TransformResponse 的 storedResponse
	// 写入，以便 collectResponse 收集 usage 与成本。
	sidecarResp := &http.Response{
		StatusCode: response.StatusCode,
		Header:     response.Header.Clone(),
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
	if internalResponse, terr := ra.outAdapter.TransformResponse(ctx, sidecarResp); terr == nil && internalResponse != nil {
		_, _ = ra.inAdapter.TransformResponse(ctx, internalResponse)
		ra.collectResponse()
	}
	return nil
}

func convertOpenAIResponseToAnthropic(ctx context.Context, body []byte, response *http.Response) ([]byte, bool, error) {
	var envelope struct {
		Object  string          `json:"object"`
		Choices json.RawMessage `json:"choices"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, false, nil
	}
	var outAdapter model.Outbound
	switch {
	case len(envelope.Choices) > 0 || strings.Contains(envelope.Object, "chat.completion"):
		outAdapter = &openaiOutbound.ChatOutbound{}
	case envelope.Object == "response":
		outAdapter = &openaiOutbound.ResponseOutbound{}
	default:
		return nil, false, nil
	}
	sidecar := &http.Response{
		StatusCode: response.StatusCode,
		Header:     response.Header.Clone(),
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
	internalResponse, err := outAdapter.TransformResponse(ctx, sidecar)
	if err != nil {
		return nil, false, fmt.Errorf("failed to convert OpenAI response from Anthropic channel: %w", err)
	}
	if _, ok := outAdapter.(*openaiOutbound.ChatOutbound); ok {
		mergeSplitOpenAIChatChoices(internalResponse)
	}
	converted, err := inbound.Get(inbound.InboundTypeAnthropic).TransformResponse(ctx, internalResponse)
	if err != nil {
		return nil, false, fmt.Errorf("failed to render converted Anthropic response: %w", err)
	}
	if anthropicMessageJSONIsEmptyStop(converted) {
		return nil, true, errEmptyUpstreamStream
	}
	return converted, true, nil
}

func mergeSplitOpenAIChatChoices(response *model.InternalLLMResponse) {
	if response == nil || len(response.Choices) <= 1 {
		return
	}

	merged := response.Choices[0]
	useDelta := false
	for _, choice := range response.Choices {
		if choice.Delta != nil {
			useDelta = true
			break
		}
	}
	if useDelta {
		if merged.Delta == nil {
			merged.Delta = &model.Message{Role: "assistant"}
		}
		if merged.Delta.Role == "" {
			merged.Delta.Role = "assistant"
		}
		trimOpenAIChatLeadingWhitespaceContent(merged.Delta)
	} else {
		if merged.Message == nil {
			merged.Message = &model.Message{Role: "assistant"}
		}
		if merged.Message.Role == "" {
			merged.Message.Role = "assistant"
		}
	}

	for idx := 1; idx < len(response.Choices); idx++ {
		choice := response.Choices[idx]
		src := choice.Message
		if src == nil {
			src = choice.Delta
		}
		if useDelta {
			mergeOpenAIChatMessage(merged.Delta, src)
		} else {
			mergeOpenAIChatMessage(merged.Message, src)
		}
		if merged.FinishReason == nil && choice.FinishReason != nil {
			merged.FinishReason = choice.FinishReason
		}
	}

	response.Choices = []model.Choice{merged}
}

func trimOpenAIChatLeadingWhitespaceContent(msg *model.Message) {
	if msg == nil || msg.Content.Content == nil || strings.TrimSpace(*msg.Content.Content) != "" {
		return
	}
	*msg.Content.Content = ""
}

func mergeOpenAIChatMessage(dst, src *model.Message) {
	if dst == nil || src == nil {
		return
	}
	if dst.Role == "" {
		dst.Role = src.Role
	}
	if src.ReasoningContent != nil && *src.ReasoningContent != "" {
		if dst.ReasoningContent == nil {
			dst.ReasoningContent = lo.ToPtr("")
		}
		*dst.ReasoningContent += *src.ReasoningContent
	}
	if src.Reasoning != nil && *src.Reasoning != "" {
		if dst.Reasoning == nil {
			dst.Reasoning = lo.ToPtr("")
		}
		*dst.Reasoning += *src.Reasoning
	}
	if src.ReasoningSignature != nil && *src.ReasoningSignature != "" && dst.ReasoningSignature == nil {
		dst.ReasoningSignature = src.ReasoningSignature
	}
	if len(src.ReasoningBlocks) > 0 {
		dst.ReasoningBlocks = append(dst.ReasoningBlocks, src.ReasoningBlocks...)
	}
	if src.Content.Content != nil && *src.Content.Content != "" {
		if dst.Content.Content == nil {
			dst.Content.Content = lo.ToPtr("")
		}
		*dst.Content.Content += *src.Content.Content
	}
	if len(src.Content.MultipleContent) > 0 {
		dst.Content.MultipleContent = append(dst.Content.MultipleContent, src.Content.MultipleContent...)
	}
	if len(src.ToolCalls) > 0 {
		dst.ToolCalls = append(dst.ToolCalls, src.ToolCalls...)
	}
	if src.Refusal != "" {
		dst.Refusal += src.Refusal
	}
}

func convertOpenAIStreamChunkToAnthropic(
	ctx context.Context,
	chunk []byte,
	currentProtocol string,
	outAdapter *model.OutboundStreamEventTransformer,
	inAdapter model.InboundStreamEventTransformer,
	pending *[]byte,
	chatTerminal *openAIChatTerminalState,
) ([]byte, string, error) {
	if len(chunk) == 0 || inAdapter == nil {
		return nil, currentProtocol, nil
	}
	*pending = append(*pending, chunk...)
	frames := popCompleteSSEFrames(pending)
	if len(frames) == 0 {
		return nil, currentProtocol, nil
	}

	protocol := currentProtocol
	var out bytes.Buffer
	for _, frame := range frames {
		if protocol == "" {
			protocol = detectOpenAIStreamProtocol(frame)
		}
		switch protocol {
		case "openai_chat":
			if *outAdapter == nil {
				*outAdapter = &openaiOutbound.ChatOutbound{}
			}
		case "openai_responses":
			if *outAdapter == nil {
				*outAdapter = &openaiOutbound.ResponseOutbound{}
			}
		default:
			out.Write(frame)
			continue
		}

		events, err := convertOpenAIStreamFrame(ctx, frame, *outAdapter)
		if err != nil {
			return nil, protocol, err
		}
		if protocol == "openai_chat" {
			events = deferOpenAIChatTerminalEvents(events, chatTerminal)
		}
		if len(events) == 0 {
			continue
		}
		converted, err := inAdapter.TransformStreamEvents(ctx, events)
		if err != nil {
			return nil, protocol, err
		}
		out.Write(converted)
	}
	return out.Bytes(), protocol, nil
}

type openAIChatTerminalState struct {
	stop              *model.StreamEvent
	usage             *model.StreamEvent
	done              bool
	leadingWhitespace []model.StreamEvent
	pendingStart      *model.StreamEvent
}

func deferOpenAIChatTerminalEvents(events []model.StreamEvent, terminal *openAIChatTerminalState) []model.StreamEvent {
	if terminal == nil || len(events) == 0 {
		return events
	}
	if openAIChatEventsAreTerminalWithoutPayload(events) {
		for _, event := range events {
			switch event.Kind {
			case model.StreamEventKindMessageStart:
				ev := event
				terminal.pendingStart = &ev
			case model.StreamEventKindMessageStop:
				ev := event
				terminal.stop = &ev
			case model.StreamEventKindUsageDelta:
				ev := event
				terminal.usage = &ev
			case model.StreamEventKindDone:
				terminal.done = true
			}
		}
		return nil
	}
	if openAIChatEventsAreControlWithoutPayload(events) {
		for _, event := range events {
			switch event.Kind {
			case model.StreamEventKindMessageStart:
				ev := event
				terminal.pendingStart = &ev
			case model.StreamEventKindUsageDelta:
				ev := event
				terminal.usage = &ev
			case model.StreamEventKindDone:
				terminal.done = true
			}
		}
		return nil
	}
	if openAIChatEventsAreOnlyMessageStart(events) {
		ev := events[0]
		terminal.pendingStart = &ev
		return nil
	}
	if terminal.pendingStart != nil {
		if openAIChatEventsHaveSubstantivePayload(events) || openAIChatEventsAreWhitespaceTerminal(events) {
			events = append([]model.StreamEvent{*terminal.pendingStart}, events...)
			terminal.pendingStart = nil
		}
	}
	if len(terminal.leadingWhitespace) > 0 {
		if openAIChatEventsHaveSubstantivePayload(events) {
			terminal.leadingWhitespace = nil
		} else {
			held := append([]model.StreamEvent(nil), terminal.leadingWhitespace...)
			terminal.leadingWhitespace = nil
			events = append(held, events...)
		}
	}
	if openAIChatEventsAreWhitespaceTerminal(events) {
		terminal.leadingWhitespace = append([]model.StreamEvent(nil), events...)
		return nil
	}
	filtered := events[:0]
	for _, event := range events {
		switch event.Kind {
		case model.StreamEventKindMessageStop:
			ev := event
			terminal.stop = &ev
		case model.StreamEventKindUsageDelta:
			ev := event
			terminal.usage = &ev
		case model.StreamEventKindDone:
			terminal.done = true
		default:
			filtered = append(filtered, event)
		}
	}
	return filtered
}

func flushOpenAIChatTerminalEvents(terminal *openAIChatTerminalState) []model.StreamEvent {
	if terminal == nil || terminal.stop == nil {
		if terminal != nil && len(terminal.leadingWhitespace) > 0 {
			events := terminal.leadingWhitespace
			terminal.leadingWhitespace = nil
			return events
		}
		return nil
	}
	events := make([]model.StreamEvent, 0, len(terminal.leadingWhitespace)+2)
	if terminal.pendingStart != nil {
		events = append(events, *terminal.pendingStart)
		terminal.pendingStart = nil
	}
	if len(terminal.leadingWhitespace) > 0 {
		events = append(events, terminal.leadingWhitespace...)
		terminal.leadingWhitespace = nil
	}
	events = append(events, *terminal.stop)
	if terminal.usage != nil {
		events = append(events, *terminal.usage)
	} else if terminal.done {
		events = append(events, model.StreamEvent{Kind: model.StreamEventKindDone})
	}
	terminal.stop = nil
	terminal.usage = nil
	terminal.done = false
	return events
}

func openAIChatTerminalIsEmptyStop(terminal *openAIChatTerminalState) bool {
	return terminal != nil && terminal.stop != nil && terminal.pendingStart != nil && len(terminal.leadingWhitespace) == 0
}

func isEmptyAnthropicAssistantStopSSE(data []byte) bool {
	if len(data) == 0 || bytes.Contains(data, []byte(`"type":"content_block_delta"`)) {
		return false
	}
	hasMessageStart := false
	hasMessageStop := false
	readCfg := &sse.ReadConfig{MaxEventSize: maxSSEEventSize}
	for ev, err := range sse.Read(bytes.NewReader(data), readCfg) {
		if err != nil {
			return false
		}
		var event struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal([]byte(ev.Data), &event); err != nil {
			return false
		}
		switch event.Type {
		case "message_start":
			hasMessageStart = true
		case "message_delta":
		case "message_stop":
			hasMessageStop = true
		default:
			return false
		}
	}
	return hasMessageStart && hasMessageStop
}

func classifyAnthropicSSEPayload(data []byte) (hasPayload bool, isTerminal bool, hasCompleteFrame bool, passthrough bool, err error) {
	pending := append([]byte(nil), data...)
	frames := popCompleteSSEFrames(&pending)
	if len(frames) == 0 {
		return false, false, false, false, nil
	}
	for _, frame := range frames {
		readCfg := &sse.ReadConfig{MaxEventSize: maxSSEEventSize}
		readAny := false
		for ev, readErr := range sse.Read(bytes.NewReader(frame), readCfg) {
			if readErr != nil {
				return false, false, true, false, readErr
			}
			readAny = true
			eventType := strings.TrimSpace(ev.Type)
			if eventType == "" {
				eventType = eventTypeFromSSEData(ev.Data)
			}
			switch eventType {
			case "", "ping":
				continue
			case "message_start", "content_block_start", "content_block_stop", "message_delta":
				if eventType == "content_block_start" && anthropicContentBlockStartHasPayload(ev.Data) {
					hasPayload = true
				}
			case "content_block_delta":
				if anthropicContentBlockDeltaHasPayload(ev.Data) {
					hasPayload = true
				}
			case "message_stop":
				isTerminal = true
			case "error":
				hasPayload = true
			default:
				return false, false, true, true, nil
			}
		}
		if !readAny {
			continue
		}
	}
	return hasPayload, isTerminal, true, false, nil
}

func eventTypeFromSSEData(data string) string {
	var event struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(data), &event); err != nil {
		return ""
	}
	return event.Type
}

func anthropicContentBlockStartHasPayload(data string) bool {
	var event struct {
		ContentBlock *struct {
			Type string `json:"type"`
			ID   string `json:"id"`
			Name string `json:"name"`
			Data string `json:"data"`
		} `json:"content_block"`
	}
	if err := json.Unmarshal([]byte(data), &event); err != nil || event.ContentBlock == nil {
		return false
	}
	return event.ContentBlock.Type == "tool_use" ||
		(event.ContentBlock.Type == "redacted_thinking" && event.ContentBlock.Data != "") ||
		event.ContentBlock.ID != "" ||
		event.ContentBlock.Name != ""
}

func anthropicContentBlockDeltaHasPayload(data string) bool {
	var event struct {
		Delta *struct {
			Text        *string `json:"text"`
			Thinking    *string `json:"thinking"`
			Signature   *string `json:"signature"`
			PartialJSON *string `json:"partial_json"`
		} `json:"delta"`
	}
	if err := json.Unmarshal([]byte(data), &event); err != nil || event.Delta == nil {
		return false
	}
	return (event.Delta.Text != nil && *event.Delta.Text != "") ||
		(event.Delta.Thinking != nil && *event.Delta.Thinking != "") ||
		(event.Delta.Signature != nil && *event.Delta.Signature != "") ||
		(event.Delta.PartialJSON != nil && *event.Delta.PartialJSON != "")
}

func openAIChatEventsAreOnlyMessageStart(events []model.StreamEvent) bool {
	if len(events) != 1 {
		return false
	}
	return events[0].Kind == model.StreamEventKindMessageStart
}

func openAIChatEventsAreTerminalWithoutPayload(events []model.StreamEvent) bool {
	if len(events) == 0 || openAIChatEventsHaveSubstantivePayload(events) {
		return false
	}
	hasStop := false
	for _, event := range events {
		switch event.Kind {
		case model.StreamEventKindMessageStart, model.StreamEventKindUsageDelta, model.StreamEventKindDone:
		case model.StreamEventKindMessageStop:
			hasStop = true
		default:
			return false
		}
	}
	return hasStop
}

func openAIChatEventsAreControlWithoutPayload(events []model.StreamEvent) bool {
	if len(events) == 0 || openAIChatEventsHaveSubstantivePayload(events) {
		return false
	}
	hasControl := false
	for _, event := range events {
		switch event.Kind {
		case model.StreamEventKindMessageStart, model.StreamEventKindUsageDelta, model.StreamEventKindDone:
			hasControl = true
		default:
			return false
		}
	}
	return hasControl
}

func openAIChatEventsAreWhitespaceTerminal(events []model.StreamEvent) bool {
	if len(events) == 0 {
		return false
	}
	hasWhitespaceText := false
	hasStop := false
	for _, event := range events {
		switch event.Kind {
		case model.StreamEventKindTextDelta:
			if event.Delta == nil || strings.TrimSpace(event.Delta.Text) != "" || event.Delta.Refusal != "" {
				return false
			}
			hasWhitespaceText = true
		case model.StreamEventKindMessageStart:
		case model.StreamEventKindMessageStop:
			hasStop = true
		case model.StreamEventKindUsageDelta, model.StreamEventKindDone:
		default:
			return false
		}
	}
	return hasWhitespaceText && hasStop
}

func openAIChatEventsHaveSubstantivePayload(events []model.StreamEvent) bool {
	for _, event := range events {
		switch event.Kind {
		case model.StreamEventKindTextDelta:
			if event.Delta != nil && (strings.TrimSpace(event.Delta.Text) != "" || event.Delta.Refusal != "") {
				return true
			}
		case model.StreamEventKindThinkingDelta, model.StreamEventKindSignatureDelta,
			model.StreamEventKindContentBlockStart, model.StreamEventKindToolCallStart, model.StreamEventKindToolCallDelta:
			return true
		}
	}
	return false
}

func popCompleteSSEFrames(pending *[]byte) [][]byte {
	var frames [][]byte
	for {
		data := *pending
		idx, sepLen := completeSSEFrameIndex(data)
		if idx < 0 {
			return frames
		}
		end := idx + sepLen
		frame := append([]byte(nil), data[:end]...)
		frames = append(frames, frame)
		*pending = append([]byte(nil), data[end:]...)
	}
}

func completeSSEFrameIndex(data []byte) (int, int) {
	if idx := bytes.Index(data, []byte("\n\n")); idx >= 0 {
		return idx, 2
	}
	if idx := bytes.Index(data, []byte("\r\n\r\n")); idx >= 0 {
		return idx, 4
	}
	return -1, 0
}

func detectOpenAIStreamProtocol(frame []byte) string {
	data := firstSSEData(frame)
	if len(data) == 0 || bytes.Equal(bytes.TrimSpace(data), []byte("[DONE]")) {
		return ""
	}
	var envelope struct {
		Object  string          `json:"object"`
		Choices json.RawMessage `json:"choices"`
		Type    string          `json:"type"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return ""
	}
	if len(envelope.Choices) > 0 || strings.Contains(envelope.Object, "chat.completion") {
		return "openai_chat"
	}
	if strings.HasPrefix(envelope.Type, "response.") || envelope.Type == "error" {
		return "openai_responses"
	}
	return ""
}

func convertOpenAIStreamFrame(ctx context.Context, frame []byte, outAdapter model.OutboundStreamEventTransformer) ([]model.StreamEvent, error) {
	data := firstSSEData(frame)
	if len(data) == 0 {
		return nil, nil
	}
	if chat, ok := outAdapter.(*openaiOutbound.ChatOutbound); ok {
		stream, err := chat.TransformStream(ctx, data)
		if err != nil {
			return nil, err
		}
		mergeSplitOpenAIChatChoices(stream)
		return model.StreamEventsFromInternalResponse(stream), nil
	}
	return outAdapter.TransformStreamEvent(ctx, data)
}

func firstSSEData(frame []byte) []byte {
	readCfg := &sse.ReadConfig{MaxEventSize: maxSSEEventSize}
	for ev, err := range sse.Read(bytes.NewReader(frame), readCfg) {
		if err != nil {
			return nil
		}
		return []byte(ev.Data)
	}
	return nil
}
