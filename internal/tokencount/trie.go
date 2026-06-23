package tokencount

// Trie implements a prefix trie for greedy longest-match tokenization.
// Ported from ctoc (github.com/rohangpta/ctoc).

type trieNode struct {
	children   [256]*trieNode
	isTerminal bool
}

// Trie holds the root of a byte-level prefix trie.
type Trie struct {
	root *trieNode
}

// NewTrie creates an empty Trie.
func NewTrie() *Trie {
	return &Trie{root: &trieNode{}}
}

// Insert adds a token (raw bytes) into the trie.
func (t *Trie) Insert(token string) {
	node := t.root
	for i := 0; i < len(token); i++ {
		b := token[i]
		if node.children[b] == nil {
			node.children[b] = &trieNode{}
		}
		node = node.children[b]
	}
	node.isTerminal = true
}

// LongestMatch returns the length of the longest token match starting at data[pos].
// Returns 0 if no match is found.
func (t *Trie) LongestMatch(data string, pos int) int {
	node := t.root
	best := 0
	for i := pos; i < len(data); i++ {
		child := node.children[data[i]]
		if child == nil {
			break
		}
		node = child
		if node.isTerminal {
			best = i - pos + 1
		}
	}
	return best
}

// CountTokens counts the number of tokens in text using greedy longest-match.
// Unknown bytes (no match) count as 1 token each.
func (t *Trie) CountTokens(text string) int {
	count := 0
	pos := 0
	for pos < len(text) {
		matchLen := t.LongestMatch(text, pos)
		if matchLen == 0 {
			pos++
		} else {
			pos += matchLen
		}
		count++
	}
	return count
}
