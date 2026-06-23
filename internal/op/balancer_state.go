package op

var resetRelayBalancerStateForChannel func(int)
var resetRelayBalancerStateAll func()

func RegisterRelayBalancerStateReset(fn func(int)) {
	resetRelayBalancerStateForChannel = fn
}

func RegisterRelayBalancerStateResetAll(fn func()) {
	resetRelayBalancerStateAll = fn
}

func resetBalancerStateForChannel(channelID int) {
	if resetRelayBalancerStateForChannel != nil {
		resetRelayBalancerStateForChannel(channelID)
	}
}

// ResetBalancerStateForChannel 导出：手动重置指定 channel 的熔断器
func ResetBalancerStateForChannel(channelID int) {
	resetBalancerStateForChannel(channelID)
}

// ResetAllBalancerState 导出：重置所有 channel 的熔断器
func ResetAllBalancerState() {
	if resetRelayBalancerStateAll != nil {
		resetRelayBalancerStateAll()
	}
}

func resetBalancerStateForChannels(channelIDs ...int) {
	if resetRelayBalancerStateForChannel == nil || len(channelIDs) == 0 {
		return
	}
	seen := make(map[int]struct{}, len(channelIDs))
	for _, channelID := range channelIDs {
		if channelID == 0 {
			continue
		}
		if _, ok := seen[channelID]; ok {
			continue
		}
		seen[channelID] = struct{}{}
		resetRelayBalancerStateForChannel(channelID)
	}
}
