package utils

import (
	"encoding/csv"
	"io"
	"os"
)

// ReadCSVFile reads all records from a given CSV file path.
func ReadCSVFile(filePath string) ([][]string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = f.Close()
	}()

	reader := csv.NewReader(f)
	reader.TrimLeadingSpace = true
	return reader.ReadAll()
}

// WriteCSVFile writes rows of records to a CSV file.
func WriteCSVFile(filePath string, records [][]string) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()

	writer := csv.NewWriter(f)
	if err := writer.WriteAll(records); err != nil {
		return err
	}
	writer.Flush()
	return writer.Error()
}

// ReadCSVFromReader reads all records from an io.Reader.
func ReadCSVFromReader(r io.Reader) ([][]string, error) {
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true
	return reader.ReadAll()
}
