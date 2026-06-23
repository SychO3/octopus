package tokencount

import (
	"encoding/json"
)

// CountAll counts total input tokens from an Anthropic count_tokens request.
// Extracts text from system, messages, and tools fields.
func CountAll(system, messages, tools interface{}) int {
	trie := GetTrie()
	total := 0

	// System
	total += countSystemTokens(trie, system)

	// Messages
	total += countMessagesTokens(trie, messages)

	// Tools
	total += countToolsTokens(trie, tools)

	if total < 1 {
		return 1
	}
	return total
}

func countSystemTokens(trie *Trie, system interface{}) int {
	if system == nil {
		return 0
	}
	switch v := system.(type) {
	case string:
		return trie.CountTokens(v)
	case []interface{}:
		total := 0
		for _, item := range v {
			if m, ok := item.(map[string]interface{}); ok {
				if text, ok := m["text"].(string); ok {
					total += trie.CountTokens(text)
				}
			}
		}
		return total
	}
	return 0
}

func countMessagesTokens(trie *Trie, messages interface{}) int {
	arr, ok := messages.([]interface{})
	if !ok {
		return 0
	}
	total := 0
	for _, msg := range arr {
		m, ok := msg.(map[string]interface{})
		if !ok {
			continue
		}
		total += countContentTokens(trie, m["content"])
	}
	return total
}

// countContentTokens recursively extracts text from content values.
// Handles: string, array, object with text/thinking/input/content fields.
// Mirrors xkiro.rs count_message_content_tokens logic.
func countContentTokens(trie *Trie, value interface{}) int {
	if value == nil {
		return 0
	}
	switch v := value.(type) {
	case string:
		return trie.CountTokens(v)
	case []interface{}:
		total := 0
		for _, item := range v {
			total += countContentTokens(trie, item)
		}
		return total
	case map[string]interface{}:
		// text field
		if text, ok := v["text"].(string); ok {
			return trie.CountTokens(text)
		}
		// thinking field
		if thinking, ok := v["thinking"].(string); ok {
			return trie.CountTokens(thinking)
		}
		// input field (tool_use blocks) → JSON stringify
		if input, exists := v["input"]; exists && input != nil {
			j, _ := json.Marshal(input)
			return trie.CountTokens(string(j))
		}
		// nested content field
		if content, exists := v["content"]; exists {
			return countContentTokens(trie, content)
		}
	}
	return 0
}

func countToolsTokens(trie *Trie, tools interface{}) int {
	arr, ok := tools.([]interface{})
	if !ok {
		return 0
	}
	total := 0
	for _, tool := range arr {
		t, ok := tool.(map[string]interface{})
		if !ok {
			continue
		}
		if name, ok := t["name"].(string); ok {
			total += trie.CountTokens(name)
		}
		if desc, ok := t["description"].(string); ok {
			total += trie.CountTokens(desc)
		}
		if schema, exists := t["input_schema"]; exists && schema != nil {
			j, _ := json.Marshal(schema)
			total += trie.CountTokens(string(j))
		}
	}
	return total
}
