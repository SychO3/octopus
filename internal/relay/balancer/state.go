package balancer

import "github.com/bestruirui/octopus/internal/op"

func init() {
	op.RegisterRelayBalancerStateReset(ResetStateByChannel)
	op.RegisterRelayBalancerStateResetAll(ResetStateAll)
	op.RegisterRelayBalancerStateResetAllWithStats(ResetStateAllWithStats)
}

func ResetStateByChannel(channelID int) {
	resetCircuitBreakerByChannel(channelID)
	resetStickyByChannel(channelID)
}

// ResetStateAll 清空全部熔断条目 + 全部会话粘性。
// 以前只清熔断、不清 sticky，导致「重置全部」后请求仍粘在坏渠道上，看起来像按钮没用。
func ResetStateAll() {
	_, _ = ResetStateAllWithStats()
}

// ResetStateAllWithStats 同 ResetStateAll，并返回清理数量便于 API 反馈。
func ResetStateAllWithStats() (circuits int, stickies int) {
	circuits = resetCircuitBreakerAll()
	stickies = resetStickyAll()
	return circuits, stickies
}
