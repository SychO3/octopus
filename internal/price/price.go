package price

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/client"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/utils/log"
)

const llmPriceUrl = "https://models.dev/api.json"

var modelVersionDotPattern = regexp.MustCompile(`-(\d+)-(\d+)(-|$)`)

var Provider = []string{
	"openai",     // GPT 系列
	"anthropic",  // Claude 系列
	"google",     // Gemini 系列
	"deepseek",   // DeepSeek 系列
	"xai",        // Grok 系列
	"alibaba",    // Qwen 系列
	"zhipuai",    // GLM 系列
	"minimax",    // MiniMax 系列
	"moonshotai", // Kimi/Moonshot
	"v0",         // v0 系列
	"openrouter", // OpenRouter 聚合
}

var lastUpdateTime time.Time

func UpdateLLMPrice(ctx context.Context) error {
	log.Debugf("update LLM price task started")
	startTime := time.Now()
	defer func() {
		log.Debugf("update LLM price task finished, update time: %s", time.Since(startTime))
	}()
	client, err := client.GetHTTPClientSystemProxy(false)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, llmPriceUrl, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to fetch LLM info: %s", resp.Status)
	}
	var rawPrice map[string]struct {
		Models map[string]struct {
			ID   string         `json:"id"`
			Cost model.LLMPrice `json:"cost"`
		} `json:"models"`
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}
	if err := json.Unmarshal(body, &rawPrice); err != nil {
		return fmt.Errorf("failed to parse LLM info: %w", err)
	}
	llmPriceLock.Lock()
	for _, provider := range Provider {
		for _, model := range rawPrice[provider].Models {
			model.ID = strings.ToLower(model.ID)
			llmPrice[model.ID] = model.Cost
		}
	}
	llmPriceLock.Unlock()
	lastUpdateTime = time.Now()
	return nil
}

func GetLastUpdateTime() time.Time {
	return lastUpdateTime
}

func GetLLMPrice(modelName string) *model.LLMPrice {
	_, price := GetLLMPriceWithCanonicalName(modelName)
	return price
}

// GetLLMPriceWithCanonicalName 返回匹配到的官方模型名和价格。
// 如果在 DB 缓存中找到，canonicalName 为传入的 modelName（已 lowercase）；
// 如果在内存价格表中找到，canonicalName 为价格表中的 key（即官方名）。
func GetLLMPriceWithCanonicalName(modelName string) (canonicalName string, result *model.LLMPrice) {
	modelName = strings.ToLower(modelName)
	price, err := op.LLMGet(modelName)
	if err == nil {
		return modelName, &price
	}
	llmPriceLock.RLock()
	defer llmPriceLock.RUnlock()
	price, ok := llmPrice[modelName]
	if ok {
		return modelName, &price
	}
	// -thinking 后缀继承：去掉 -thinking 再查一次
	if base, found := strings.CutSuffix(modelName, "-thinking"); found {
		price, ok = llmPrice[base]
		if ok {
			return base, &price
		}
	}
	// 点↔横杠转换
	dashed := strings.ReplaceAll(modelName, ".", "-")
	if dashed != modelName {
		price, ok = llmPrice[dashed]
		if ok {
			return dashed, &price
		}
	}
	dotted := modelVersionDotPattern.ReplaceAllString(modelName, `-$1.$2$3`)
	if dotted != modelName {
		price, ok = llmPrice[dotted]
		if ok {
			return dotted, &price
		}
	}
	return "", nil
}
