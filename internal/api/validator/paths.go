package validator

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ErrPathNotAllowed is returned when a job references a file outside the permitted directories.
var ErrPathNotAllowed = errors.New("path is outside the permitted directories")

// canonicalDir returns the absolute, symlink-resolved form of dir (dir need not exist).
func canonicalDir(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	return resolveExisting(abs), nil
}

// resolveExisting resolves symlinks for the longest existing prefix of an absolute path and
// re-appends the non-existent remainder. This prevents escaping a sandbox through a symlinked
// directory even when the final file does not exist yet.
func resolveExisting(abs string) string {
	abs = filepath.Clean(abs)
	rest := ""
	cur := abs
	for {
		if resolved, err := filepath.EvalSymlinks(cur); err == nil {
			return filepath.Join(resolved, rest)
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return abs
		}
		rest = filepath.Join(filepath.Base(cur), rest)
		cur = parent
	}
}

// within reports whether path equals root or is nested inside it. Both must be canonical.
func within(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)))
}

// resolveInput canonicalizes a source path and checks that it lies inside one of the allowed
// input directories. Relative paths are resolved against the working directory.
func resolveInput(path string, allowedDirs []string) (string, error) {
	if len(allowedDirs) == 0 {
		return "", fmt.Errorf("%w: local file sources are disabled (INPUT_DIRS is empty)", ErrPathNotAllowed)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	canonical := resolveExisting(abs)
	for _, dir := range allowedDirs {
		root, err := canonicalDir(dir)
		if err != nil {
			continue
		}
		if within(root, canonical) && canonical != root {
			return canonical, nil
		}
	}
	return "", fmt.Errorf("%w: %q (allowed: %s)", ErrPathNotAllowed, path, strings.Join(allowedDirs, ", "))
}

// resolveOutput maps an export/DLQ path into the export directory.
//
// Absolute paths must already be inside exportDir. Relative paths are accepted either relative
// to the working directory (e.g. "exports/out.json" when exportDir is "exports") or relative to
// exportDir itself (e.g. "out.json").
func resolveOutput(path, exportDir string) (string, error) {
	root, err := canonicalDir(exportDir)
	if err != nil {
		return "", err
	}

	var candidates []string
	if filepath.IsAbs(path) {
		candidates = []string{path}
	} else {
		if abs, err := filepath.Abs(path); err == nil {
			candidates = append(candidates, abs)
		}
		candidates = append(candidates, filepath.Join(root, path))
	}

	for _, c := range candidates {
		canonical := resolveExisting(c)
		if within(root, canonical) && canonical != root {
			return canonical, nil
		}
	}
	return "", fmt.Errorf("%w: %q must be inside the export directory %q", ErrPathNotAllowed, path, exportDir)
}
