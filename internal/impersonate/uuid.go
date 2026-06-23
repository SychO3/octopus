package impersonate

import (
	"crypto/sha256"
	"fmt"

	"github.com/google/uuid"
)

func deterministicUUID(seed string) string {
	hash := sha256.Sum256([]byte(seed))
	uuidBytes := make([]byte, 16)
	copy(uuidBytes, hash[:16])

	uuidBytes[6] = (uuidBytes[6] & 0x0f) | 0x50
	uuidBytes[8] = (uuidBytes[8] & 0x3f) | 0x80

	return formatUUID(uuidBytes)
}

func formatUUID(b []byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func randomUUID() string {
	return uuid.New().String()
}
