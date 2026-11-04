package utils

import (
	"bytes"
	"encoding/json"
)

// ToJSON serializes any value to a JSON byte slice.
func ToJSON(v any) ([]byte, error) {
	return json.Marshal(v)
}

// ToPrettyJSON serializes any value to indented JSON bytes.
func ToPrettyJSON(v any) ([]byte, error) {
	return json.MarshalIndent(v, "", "  ")
}

// FromJSON parses a JSON byte slice into target pointer v.
func FromJSON(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// CompactJSON strips extraneous whitespace from a JSON payload.
func CompactJSON(src []byte) ([]byte, error) {
	var dst bytes.Buffer
	if err := json.Compact(&dst, src); err != nil {
		return nil, err
	}
	return dst.Bytes(), nil
}
