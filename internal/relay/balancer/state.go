package balancer

import "github.com/bestruirui/octopus/internal/op"

func init() {
	op.RegisterRelayBalancerStateReset(ResetStateByChannel)
	op.RegisterRelayBalancerStateResetAll(ResetStateAll)
}

func ResetStateByChannel(channelID int) {
	resetCircuitBreakerByChannel(channelID)
	resetStickyByChannel(channelID)
}

func ResetStateAll() {
	globalBreaker.Range(func(key, _ any) bool {
		globalBreaker.Delete(key)
		return true
	})
}
