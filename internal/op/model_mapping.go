package op

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
)

var (
	modelMappingCache        map[string]string
	modelMappingReverseCache map[string]string
	modelMappingCacheLock    sync.RWMutex
)

func ModelMappingResolve(requestModel string) string {
	modelMappingCacheLock.RLock()
	defer modelMappingCacheLock.RUnlock()
	if modelMappingCache == nil {
		return requestModel
	}
	if actual, ok := modelMappingCache[strings.ToLower(requestModel)]; ok {
		return actual
	}
	return requestModel
}

// ModelMappingReverse maps an upstream model name back to the user-facing request name.
func ModelMappingReverse(actualModel string) string {
	modelMappingCacheLock.RLock()
	defer modelMappingCacheLock.RUnlock()
	if modelMappingReverseCache == nil {
		return actualModel
	}
	if request, ok := modelMappingReverseCache[strings.ToLower(actualModel)]; ok {
		return request
	}
	return actualModel
}

func ModelMappingRefreshCache(ctx context.Context) error {
	var mappings []model.ModelMapping
	if err := db.GetDB().WithContext(ctx).Where("enabled = ?", true).Find(&mappings).Error; err != nil {
		return err
	}
	cache := make(map[string]string, len(mappings))
	reverse := make(map[string]string, len(mappings))
	for _, m := range mappings {
		req := strings.TrimSpace(m.RequestName)
		act := strings.TrimSpace(m.ActualName)
		cache[strings.ToLower(req)] = act
		reverse[strings.ToLower(act)] = req
	}
	modelMappingCacheLock.Lock()
	modelMappingCache = cache
	modelMappingReverseCache = reverse
	modelMappingCacheLock.Unlock()
	return nil
}

func ModelMappingList(ctx context.Context) ([]model.ModelMapping, error) {
	var mappings []model.ModelMapping
	if err := db.GetDB().WithContext(ctx).Order("id ASC").Find(&mappings).Error; err != nil {
		return nil, err
	}
	return mappings, nil
}

func ModelMappingCreate(req *model.ModelMappingCreateRequest, ctx context.Context) (*model.ModelMapping, error) {
	mapping := &model.ModelMapping{
		RequestName: strings.TrimSpace(req.RequestName),
		ActualName:  strings.TrimSpace(req.ActualName),
		Enabled:     true,
	}
	if req.Enabled != nil {
		mapping.Enabled = *req.Enabled
	}
	if mapping.RequestName == "" || mapping.ActualName == "" {
		return nil, fmt.Errorf("request_name and actual_name are required")
	}
	if err := db.GetDB().WithContext(ctx).Create(mapping).Error; err != nil {
		return nil, err
	}
	_ = ModelMappingRefreshCache(ctx)
	return mapping, nil
}

func ModelMappingUpdate(req *model.ModelMappingUpdateRequest, ctx context.Context) (*model.ModelMapping, error) {
	mapping := &model.ModelMapping{}
	if err := db.GetDB().WithContext(ctx).First(mapping, req.ID).Error; err != nil {
		return nil, err
	}
	mapping.RequestName = strings.TrimSpace(req.RequestName)
	mapping.ActualName = strings.TrimSpace(req.ActualName)
	if req.Enabled != nil {
		mapping.Enabled = *req.Enabled
	}
	if mapping.RequestName == "" || mapping.ActualName == "" {
		return nil, fmt.Errorf("request_name and actual_name are required")
	}
	if err := db.GetDB().WithContext(ctx).Save(mapping).Error; err != nil {
		return nil, err
	}
	_ = ModelMappingRefreshCache(ctx)
	return mapping, nil
}

func ModelMappingDelete(id int, ctx context.Context) error {
	if err := db.GetDB().WithContext(ctx).Delete(&model.ModelMapping{}, id).Error; err != nil {
		return err
	}
	_ = ModelMappingRefreshCache(ctx)
	return nil
}
