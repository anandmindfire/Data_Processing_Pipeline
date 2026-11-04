package enums

// SourceType represents supported input source types.
type SourceType string

const (
	SourceCSV  SourceType = "csv"
	SourceJSON SourceType = "json"
	SourceHTTP SourceType = "http"
	SourceAPI  SourceType = "api"
)
