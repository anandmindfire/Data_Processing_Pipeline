package utils

import "time"

// NowUTC returns the current UTC time.
func NowUTC() time.Time {
	return time.Now().UTC()
}

// FormatISO8601 formats a time.Time into RFC3339 / ISO8601 string.
func FormatISO8601(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

// ElapsedMilliseconds calculates duration in milliseconds from start to end.
func ElapsedMilliseconds(start, end time.Time) float64 {
	return float64(end.Sub(start).Microseconds()) / 1000.0
}
