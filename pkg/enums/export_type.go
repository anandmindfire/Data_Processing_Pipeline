package enums

// ExportType represents supported export destination types.
type ExportType string

const (
	ExportJSON   ExportType = "json"
	ExportCSV    ExportType = "csv"
	ExportSQLite ExportType = "sqlite"
	ExportSQL    ExportType = "sql"
)
