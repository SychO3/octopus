package op

var resetRelayBalancerStateForChannel func(int)
var resetRelayBalancerStateAll func()
var resetRelayBalancerStateAllWithStats func() (circuits int, stickies int)

func RegisterRelayBalancerStateReset(fn func(int)) {
	resetRelayBalancerStateForChannel = fn
}

func RegisterRelayBalancerStateResetAll(fn func()) {
	resetRelayBalancerStateAll = fn
}

func RegisterRelayBalancerStateResetAllWithStats(fn func() (int, int)) {
	resetRelayBalancerStateAllWithStats = fn
}

func resetBalancerStateForChannel(channelID int) {
	if resetRelayBalancerStateForChannel != nil {
		resetRelayBalancerStateForChannel(channelID)
	}
}

// ResetBalancerStateForChannel 导出：手动重置指定 channel 的熔断器与粘性
func ResetBalancerStateForChannel(channelID int) {
	resetBalancerStateForChannel(channelID)
}

// ResetAllBalancerState 导出：重置所有 channel 的熔断器与粘性
func ResetAllBalancerState() {
	if resetRelayBalancerStateAllWithStats != nil {
		resetRelayBalancerStateAllWithStats()
		return
	}
	if resetRelayBalancerStateAll != nil {
		resetRelayBalancerStateAll()
	}
}

// ResetAllBalancerStateWithStats 重置全部熔断 + 粘性，并返回清理数量
func ResetAllBalancerStateWithStats() (circuits int, stickies int) {
	if resetRelayBalancerStateAllWithStats != nil {
		return resetRelayBalancerStateAllWithStats()
	}
	if resetRelayBalancerStateAll != nil {
		resetRelayBalancerStateAll()
	}
	return 0, 0
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
