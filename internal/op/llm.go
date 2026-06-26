package op

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/utils/cache"
)

var llmModelCache = cache.New[string, model.LLMPrice](16)

func LLMList(ctx context.Context) ([]model.LLMInfo, error) {
	models := make([]model.LLMInfo, 0, llmModelCache.Len())
	for m, cost := range llmModelCache.GetAll() {
		models = append(models, model.LLMInfo{
			Name:     m,
			LLMPrice: cost,
		})
	}
	return models, nil
}

func LLMUpdate(model model.LLMInfo, ctx context.Context) error {
	_, ok := llmModelCache.Get(model.Name)
	if !ok {
		return fmt.Errorf("model not found")
	}
	if err := db.GetDB().WithContext(ctx).Save(model).Error; err != nil {
		return err
	}
	llmModelCache.Set(model.Name, model.LLMPrice)
	return nil
}

func LLMDelete(modelName string, ctx context.Context) error {
	_, ok := llmModelCache.Get(modelName)
	if !ok {
		return fmt.Errorf("model not found")
	}
	if err := db.GetDB().WithContext(ctx).Delete(&model.LLMInfo{Name: modelName}).Error; err != nil {
		return err
	}
	llmModelCache.Del(modelName)
	return nil
}
func LLMBatchDelete(modelNames []string, ctx context.Context) error {
	if len(modelNames) == 0 {
		return nil
	}
	if err := db.GetDB().WithContext(ctx).Where("name IN ?", modelNames).Delete(&model.LLMInfo{}).Error; err != nil {
		return err
	}
	llmModelCache.Del(modelNames...)
	return nil
}
func LLMCreate(model model.LLMInfo, ctx context.Context) error {
	model.Name = strings.ToLower(model.Name)
	_, ok := llmModelCache.Get(model.Name)
	if ok {
		return fmt.Errorf("model already exists")
	}
	if err := db.GetDB().WithContext(ctx).Create(&model).Error; err != nil {
		return err
	}
	llmModelCache.Set(model.Name, model.LLMPrice)
	return nil
}
func LLMBatchCreate(llmInfos []model.LLMInfo, ctx context.Context) error {
	if len(llmInfos) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(llmInfos))
	newLLMInfos := make([]model.LLMInfo, 0, len(llmInfos))
	for _, llmInfo := range llmInfos {
		llmInfo.Name = strings.ToLower(llmInfo.Name)
		if _, ok := seen[llmInfo.Name]; ok {
			continue
		}
		if _, ok := llmModelCache.Get(llmInfo.Name); ok {
			continue
		}
		seen[llmInfo.Name] = struct{}{}
		newLLMInfos = append(newLLMInfos, llmInfo)
	}
	if len(newLLMInfos) == 0 {
		return nil
	}
	if err := db.GetDB().WithContext(ctx).Create(&newLLMInfos).Error; err != nil {
		return err
	}
	for _, llmInfo := range newLLMInfos {
		llmModelCache.Set(llmInfo.Name, llmInfo.LLMPrice)
	}
	return nil
}
func LLMGet(name string) (model.LLMPrice, error) {
	price, ok := llmModelCache.Get(name)
	if !ok {
		return model.LLMPrice{}, fmt.Errorf("model not found")
	}
	return price, nil
}

var versionDashPattern = regexp.MustCompile(`-(\d+)-(\d+)(-|$)`)

func LLMCleanNoPrice(ctx context.Context) (int, error) {
	all := llmModelCache.GetAll()
	toDelete := make(map[string]struct{})

	for name, p := range all {
		// 1. 清理无价格模型
		if p.Input == 0 && p.Output == 0 && p.CacheRead == 0 && p.CacheWrite == 0 {
			toDelete[name] = struct{}{}
		}

		// 2. 清理点↔杠重复：如果本条是横杠版本，且点版本也存在，删除本条
		dotted := versionDashPattern.ReplaceAllString(name, `-$1.$2$3`)
		if dotted != name {
			if _, exists := all[dotted]; exists {
				toDelete[name] = struct{}{}
			}
		}
	}

	if len(toDelete) == 0 {
		return 0, nil
	}
	names := make([]string, 0, len(toDelete))
	for n := range toDelete {
		names = append(names, n)
	}
	if err := LLMBatchDelete(names, ctx); err != nil {
		return 0, err
	}
	return len(names), nil
}

func llmRefreshCache(ctx context.Context) error {
	models := []model.LLMInfo{}
	if err := db.GetDB().WithContext(ctx).Find(&models).Error; err != nil {
		return err
	}
	for _, model := range models {
		llmModelCache.Set(model.Name, model.LLMPrice)
	}
	return nil
}
