package impersonate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/utils/log"
)

type gitHubRepo struct {
	Owner     string
	Repo      string
	TagPrefix string
}

var repos = map[ProviderType]gitHubRepo{
	ProviderClaude: {Owner: "anthropics", Repo: "claude-code", TagPrefix: ""},
	ProviderCodex:  {Owner: "openai", Repo: "codex", TagPrefix: "rust-"},
	ProviderGemini: {Owner: "google-gemini", Repo: "gemini-cli", TagPrefix: ""},
}

type githubRelease struct {
	TagName string `json:"tag_name"`
}

func FetchLatestVersion(ctx context.Context, provider ProviderType) (string, error) {
	repo, ok := repos[provider]
	if !ok {
		return "", fmt.Errorf("unknown provider: %s", provider)
	}

	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/releases/latest", repo.Owner, repo.Repo)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Octopus-Version-Fetcher")
	req.Header.Set("Accept", "application/vnd.github+json")

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("github API returned %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("read body failed: %w", err)
	}

	var release githubRelease
	if err := json.Unmarshal(body, &release); err != nil {
		return "", fmt.Errorf("parse response failed: %w", err)
	}

	version := parseVersion(release.TagName, repo.TagPrefix)
	if version == "" {
		return "", fmt.Errorf("cannot parse version from tag: %s", release.TagName)
	}

	return version, nil
}

func parseVersion(tag, prefix string) string {
	v := tag
	if prefix != "" {
		v = strings.TrimPrefix(v, prefix)
	}
	v = strings.TrimPrefix(v, "v")
	v = strings.TrimSpace(v)
	if v == "" {
		return ""
	}
	return v
}

func RefreshAllVersions(ctx context.Context) error {
	providers := []ProviderType{ProviderClaude, ProviderCodex, ProviderGemini}
	var lastErr error

	for _, provider := range providers {
		version, err := FetchLatestVersion(ctx, provider)
		if err != nil {
			log.Warnf("fetch CLI version failed for %s: %v", provider, err)
			lastErr = err
			continue
		}

		key := versionLatestKeys[provider]
		if err := op.SettingSetString(key, version); err != nil {
			log.Warnf("save CLI version failed for %s: %v", provider, err)
			lastErr = err
			continue
		}

		log.Infof("CLI version updated: %s = %s", provider, version)
	}

	_ = op.SettingSetString(model.SettingKeyCLIVersionsUpdatedAt, time.Now().Format(time.RFC3339))
	return lastErr
}
