package sitesync

import (
	"context"
	"net/http"

	"github.com/bestruirui/octopus/internal/model"
)

// populateGroupRatiosFromPricing calls GET /api/pricing on the upstream site
// and fills each group's GroupRatio from the top-level "group_ratio" map.
func populateGroupRatiosFromPricing(ctx context.Context, siteRecord *model.Site, account *model.SiteAccount, accessToken string, userID int, groups []model.SiteUserGroup) {
	if len(groups) == 0 || siteRecord == nil {
		return
	}

	requestURL := buildSiteURL(siteRecord.BaseURL, "/api/pricing")
	payload, _, err := anyRouterRequestJSONWithCookies(ctx, siteRecord, http.MethodGet, requestURL, nil,
		anyRouterAuthHeaders(accessToken, userID), account)
	if err != nil || payload == nil {
		return
	}

	// group_ratio is at top level: {"group_ratio": {"default": 1.0, "vip": 0.8}}
	groupRatioRaw, ok := payload["group_ratio"]
	if !ok {
		return
	}
	ratioMap, ok := groupRatioRaw.(map[string]any)
	if !ok {
		return
	}

	for i := range groups {
		if ratio := jsonFloat(ratioMap[groups[i].GroupKey]); ratio > 0 {
			groups[i].GroupRatio = ratio
		} else if _, exists := ratioMap[groups[i].GroupKey]; !exists {
			// 分组不在上游 group_ratio 中 → 该 key 不可用此分组，标记为 -1 阻止投影
			groups[i].GroupRatio = -1
		}
	}
}
