package utils

import (
	"os"
	"path/filepath"
)

// EnsureDir checks if a directory exists and creates it if not.
func EnsureDir(path string) error {
	if err := os.MkdirAll(path, 0o750); err != nil {
		return err
	}
	return nil
}

// FileExists returns true if the file exists and is not a directory.
func FileExists(path string) bool {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

// DirExists returns true if the directory exists.
func DirExists(path string) bool {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return info.IsDir()
}

// CleanPath returns the cleaned, absolute path.
func CleanPath(path string) (string, error) {
	return filepath.Abs(filepath.Clean(path))
}
