package transformation

import (
	"fmt"
	"strings"
	"unicode"
)

// applyStringNormalization applies trim, lower, upper, or title string transformations.
func applyStringNormalization(normalization string, val any) string {
	s := fmt.Sprint(val)
	switch strings.ToLower(normalization) {
	case "lower":
		return strings.ToLower(strings.TrimSpace(s))
	case "upper":
		return strings.ToUpper(strings.TrimSpace(s))
	case "trim":
		return strings.TrimSpace(s)
	case "title":
		return toTitleCase(strings.TrimSpace(s))
	default:
		return s
	}
}

// toTitleCase capitalizes the first rune of each whitespace-delimited word.
func toTitleCase(s string) string {
	words := strings.Fields(s)
	for i, w := range words {
		r := []rune(strings.ToLower(w))
		if len(r) > 0 {
			r[0] = unicode.ToUpper(r[0])
		}
		words[i] = string(r)
	}
	return strings.Join(words, " ")
}
