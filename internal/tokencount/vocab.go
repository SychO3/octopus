package tokencount

import (
	_ "embed"
	"encoding/json"
	"sync"
)

//go:embed vocab.json
var vocabJSON []byte

var (
	globalTrie *Trie
	trieOnce   sync.Once
)

// GetTrie returns the singleton Trie built from the embedded vocabulary.
func GetTrie() *Trie {
	trieOnce.Do(func() {
		var tokens []string
		if err := json.Unmarshal(vocabJSON, &tokens); err != nil {
			// Should never happen with embedded data
			panic("tokencount: failed to parse vocab.json: " + err.Error())
		}
		globalTrie = NewTrie()
		for _, token := range tokens {
			globalTrie.Insert(token)
		}
	})
	return globalTrie
}
