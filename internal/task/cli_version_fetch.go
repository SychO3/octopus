package task

import (
	"context"
	"time"

	"github.com/bestruirui/octopus/internal/impersonate"
	"github.com/bestruirui/octopus/internal/utils/log"
)

func FetchCLIVersions() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if err := impersonate.RefreshAllVersions(ctx); err != nil {
		log.Errorf("fetch CLI versions failed: %v", err)
	}
}
