// Package validator checks job specifications at submission time so invalid or unsafe jobs are
// rejected with 400 before any work starts.
package validator

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/anandmindfire/data-processing-pipeline/internal/export"
	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// Policy holds the security boundaries and resource limits applied to every job.
type Policy struct {
	InputDirs  []string // directories local sources may read from
	ExportDir  string   // every export and DLQ file must live here
	MaxWorkers int      // per stage
	MaxBuffer  int
	MaxBatch   int
	MaxTimeout int // seconds
	MaxSources int
	MaxExports int
}

// DefaultPolicy is a conservative policy used when none is supplied (mainly by tests).
func DefaultPolicy(exportDir string, inputDirs ...string) Policy {
	return Policy{
		InputDirs:  inputDirs,
		ExportDir:  exportDir,
		MaxWorkers: 64,
		MaxBuffer:  10000,
		MaxBatch:   10000,
		MaxTimeout: 3600,
		MaxSources: 16,
		MaxExports: 8,
	}
}

var (
	sourceTypes    = set("csv", "json", "http", "api")
	sourceFormats  = set("", "csv", "json", "ndjson")
	ruleTypes      = set("", "string", "number", "bool", "boolean")
	normalizations = set("", "trim", "lower", "upper", "title")
	aggregationOps = set("count", "sum", "avg", "average", "min", "max")
)

// RequestValidator performs structural, semantic, and security validation of job specs.
type RequestValidator struct {
	policy Policy
}

// NewRequestValidator constructs a RequestValidator enforcing policy.
func NewRequestValidator(policy Policy) *RequestValidator {
	return &RequestValidator{policy: policy}
}

// Validate checks cfg and canonicalizes its file paths in place (sources, exports, DLQ) so the
// pipeline only ever touches the resolved, sandboxed locations. All problems are reported together.
func (v *RequestValidator) Validate(cfg *models.Config) error {
	if cfg == nil {
		return errors.New("request configuration is required")
	}
	var errs []error
	errs = append(errs, v.validateLimits(cfg)...)
	errs = append(errs, v.validateSources(cfg.Sources)...)
	errs = append(errs, v.validateExports(cfg)...)
	errs = append(errs, validateRules(cfg.Rules)...)
	errs = append(errs, validateAggregations(cfg.Aggregations)...)
	return errors.Join(errs...)
}

// validateLimits verifies pipeline concurrency and buffer settings against policy boundaries.
func (v *RequestValidator) validateLimits(cfg *models.Config) []error {
	p := v.policy
	var errs []error
	check := func(name string, val, maxVal int) {
		if val < 0 {
			errs = append(errs, fmt.Errorf("%s must not be negative", name))
		} else if maxVal > 0 && val > maxVal {
			errs = append(errs, fmt.Errorf("%s must be <= %d (got %d)", name, maxVal, val))
		}
	}
	check("validation_workers", cfg.ValidationWorkers, p.MaxWorkers)
	check("transform_workers", cfg.TransformWorkers, p.MaxWorkers)
	check("buffer_size", cfg.BufferSize, p.MaxBuffer)
	check("batch_size", cfg.BatchSize, p.MaxBatch)
	check("timeout_seconds", cfg.TimeoutSeconds, p.MaxTimeout)
	check("max_records", cfg.MaxRecords, 0)
	if p.MaxSources > 0 && len(cfg.Sources) > p.MaxSources {
		errs = append(errs, fmt.Errorf("at most %d sources are allowed (got %d)", p.MaxSources, len(cfg.Sources)))
	}
	if p.MaxExports > 0 && len(cfg.Exports) > p.MaxExports {
		errs = append(errs, fmt.Errorf("at most %d exports are allowed (got %d)", p.MaxExports, len(cfg.Exports)))
	}
	return errs
}

// validateSources validates data source types, names, formats, and canonical paths or URLs.
func (v *RequestValidator) validateSources(sources []models.Source) []error {
	if len(sources) == 0 {
		return []error{errors.New("at least one source is required")}
	}
	var errs []error
	names := make(map[string]bool, len(sources))
	for i := range sources {
		s := &sources[i]
		prefix := fmt.Sprintf("source[%d]", i)
		s.Type = strings.ToLower(strings.TrimSpace(s.Type))
		s.Format = strings.ToLower(strings.TrimSpace(s.Format))

		if strings.TrimSpace(s.Name) == "" {
			errs = append(errs, fmt.Errorf("%s: name is required", prefix))
		} else if names[s.Name] {
			errs = append(errs, fmt.Errorf("%s: duplicate source name %q", prefix, s.Name))
		}
		names[s.Name] = true

		if !sourceTypes[s.Type] {
			errs = append(errs, fmt.Errorf("%s: type must be one of csv, json, http, api (got %q)", prefix, s.Type))
		}
		if !sourceFormats[s.Format] {
			errs = append(errs, fmt.Errorf("%s: format must be csv, json or ndjson (got %q)", prefix, s.Format))
		}

		switch {
		case s.Path == "" && s.URL == "":
			errs = append(errs, fmt.Errorf("%s: either path or url is required", prefix))
		case s.Path != "" && s.URL != "":
			errs = append(errs, fmt.Errorf("%s: path and url are mutually exclusive", prefix))
		case s.URL != "":
			if !strings.HasPrefix(strings.ToLower(s.URL), "http://") && !strings.HasPrefix(strings.ToLower(s.URL), "https://") {
				errs = append(errs, fmt.Errorf("%s: url must use http or https", prefix))
			}
		default:
			if s.Type == "http" || s.Type == "api" {
				errs = append(errs, fmt.Errorf("%s: %s sources require a url", prefix, s.Type))
				continue
			}
			resolved, err := resolveInput(s.Path, v.policy.InputDirs)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", prefix, err))
				continue
			}
			s.Path = resolved
		}
	}
	return errs
}

// validateExports checks export destinations, supported types, DLQ uniqueness, and sandboxed paths.
func (v *RequestValidator) validateExports(cfg *models.Config) []error {
	if len(cfg.Exports) == 0 {
		return []error{errors.New("at least one export destination is required")}
	}
	var errs []error
	seen := make(map[string]int)
	for i := range cfg.Exports {
		e := &cfg.Exports[i]
		prefix := fmt.Sprintf("export[%d]", i)
		e.Type = strings.ToLower(strings.TrimSpace(e.Type))
		if !export.IsSupportedType(e.Type) {
			errs = append(errs, fmt.Errorf("%s: type must be one of %s (got %q)", prefix, strings.Join(export.SupportedTypes, ", "), e.Type))
			continue
		}
		if e.Path == "" {
			continue // the exporter writes <export_dir>/<job_id>.<ext>
		}
		resolved, err := resolveOutput(e.Path, v.policy.ExportDir)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", prefix, err))
			continue
		}
		if j, dup := seen[resolved]; dup {
			errs = append(errs, fmt.Errorf("%s: path duplicates export[%d]", prefix, j))
		}
		seen[resolved] = i
		e.Path = resolved
	}
	if cfg.DLQPath != "" {
		resolved, err := resolveOutput(cfg.DLQPath, v.policy.ExportDir)
		if err != nil {
			errs = append(errs, fmt.Errorf("dlq_path: %w", err))
		} else if _, dup := seen[resolved]; dup {
			errs = append(errs, errors.New("dlq_path: must differ from every export path"))
		} else {
			cfg.DLQPath = resolved
		}
	}
	return errs
}

// validateRules ensures fields, data types, normalizations, and regex patterns in rules are valid.
func validateRules(rules []models.Rule) []error {
	var errs []error
	for i, r := range rules {
		prefix := fmt.Sprintf("rule[%d]", i)
		if strings.TrimSpace(r.Field) == "" {
			errs = append(errs, fmt.Errorf("%s: field is required", prefix))
		}
		if !ruleTypes[strings.ToLower(r.Type)] {
			errs = append(errs, fmt.Errorf("%s: type must be string, number or boolean (got %q)", prefix, r.Type))
		}
		if !normalizations[strings.ToLower(r.Normalize)] {
			errs = append(errs, fmt.Errorf("%s: normalize must be trim, lower, upper or title (got %q)", prefix, r.Normalize))
		}
		if r.Min != nil && r.Max != nil && *r.Min > *r.Max {
			errs = append(errs, fmt.Errorf("%s: min cannot be greater than max", prefix))
		}
		if r.Pattern != "" {
			if _, err := regexp.Compile(r.Pattern); err != nil {
				errs = append(errs, fmt.Errorf("%s: invalid pattern: %w", prefix, err))
			}
		}
	}
	return errs
}

// validateAggregations validates aggregation fields and supported reduction operations.
func validateAggregations(aggs []models.Aggregation) []error {
	var errs []error
	for i, a := range aggs {
		prefix := fmt.Sprintf("aggregation[%d]", i)
		op := strings.ToLower(strings.TrimSpace(a.Operation))
		if strings.TrimSpace(a.Field) == "" {
			errs = append(errs, fmt.Errorf("%s: field is required", prefix))
		}
		if !aggregationOps[op] {
			errs = append(errs, fmt.Errorf("%s: operation must be one of count, sum, avg, min, max (got %q)", prefix, a.Operation))
		}
	}
	return errs
}

// set converts a list of strings into a lookup set map.
func set(vals ...string) map[string]bool {
	m := make(map[string]bool, len(vals))
	for _, v := range vals {
		m[v] = true
	}
	return m
}
