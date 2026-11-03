package pipeline

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/anandkr47/data-processing-pipeline/internal/domain"
	"github.com/anandkr47/data-processing-pipeline/internal/sources"
	"github.com/anandkr47/data-processing-pipeline/internal/storage"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Engine struct {
	Store  *storage.Store
	Client *http.Client
}
type result struct {
	item   sources.Item
	record domain.Record
}

func defaults(c *domain.Config) {
	if c.ValidationWorkers <= 0 {
		c.ValidationWorkers = 4
	}
	if c.TransformWorkers <= 0 {
		c.TransformWorkers = 4
	}
	if c.BufferSize <= 0 {
		c.BufferSize = 128
	}
	if c.TimeoutSeconds <= 0 {
		c.TimeoutSeconds = 300
	}
}
func (e *Engine) Run(parent context.Context, j *domain.Job, update func(*domain.Job)) {
	cfg := j.Config
	defaults(&cfg)
	j.Config = cfg
	ctx, cancel := context.WithTimeout(parent, time.Duration(cfg.TimeoutSeconds)*time.Second)
	defer cancel()
	runMu := sync.Mutex{}
	ingestFailures := 0
	j.Status = domain.Running
	j.Metrics.StartedAt = time.Now()
	j.Metrics.StageLatency = map[string]time.Duration{}
	_ = e.Store.PutJob(j)
	update(j)
	raw := make(chan sources.Item, cfg.BufferSize)
	valid := make(chan sources.Item, cfg.BufferSize)
	transformed := make(chan result, cfg.BufferSize)
	errCh := make(chan domain.RecordError, cfg.BufferSize)
	var sourceWG sync.WaitGroup
	for _, src := range cfg.Sources {
		src := src
		sourceWG.Add(1)
		go func() {
			defer sourceWG.Done()
			start := time.Now()
			err := sources.Read(ctx, src, e.Client, cfg.MaxRecords, raw, func() { runMu.Lock(); j.Metrics.Discovered++; runMu.Unlock() })
			if err != nil && ctx.Err() == nil {
				runMu.Lock()
				ingestFailures++
				runMu.Unlock()
				errCh <- domain.RecordError{Source: src.Name, Stage: "ingest", Message: err.Error(), At: time.Now()}
			}
			runMu.Lock()
			j.Metrics.StageLatency["ingestion"] += time.Since(start)
			runMu.Unlock()
		}()
	}
	go func() { sourceWG.Wait(); close(raw) }()
	var validatorWG sync.WaitGroup
	for n := 0; n < cfg.ValidationWorkers; n++ {
		validatorWG.Add(1)
		go func() {
			defer validatorWG.Done()
			for item := range raw {
				stageStart := time.Now()
				if ctx.Err() != nil {
					return
				}
				if err := validate(item.Record, cfg.Rules); err != nil {
					runMu.Lock()
					j.Metrics.StageLatency["validation"] += time.Since(stageStart)
					runMu.Unlock()
					errCh <- domain.RecordError{Source: item.Source, Index: item.Index, Stage: "validation", Message: err.Error(), Record: item.Record, At: time.Now()}
					continue
				}
				runMu.Lock()
				j.Metrics.StageLatency["validation"] += time.Since(stageStart)
				runMu.Unlock()
				select {
				case valid <- item:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() { validatorWG.Wait(); close(valid) }()
	var transformWG sync.WaitGroup
	for n := 0; n < cfg.TransformWorkers; n++ {
		transformWG.Add(1)
		go func() {
			defer transformWG.Done()
			for item := range valid {
				if ctx.Err() != nil {
					return
				}
				start := time.Now()
				r, err := transform(item.Record, cfg.Rules)
				runMu.Lock()
				j.Metrics.StageLatency["transform"] += time.Since(start)
				runMu.Unlock()
				if err != nil {
					errCh <- domain.RecordError{Source: item.Source, Index: item.Index, Stage: "transform", Message: err.Error(), Record: item.Record, At: time.Now()}
					continue
				}
				select {
				case transformed <- result{item: item, record: r}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() { transformWG.Wait(); close(transformed) }()
	// Collector is the single owner of aggregation state and durable result writes.
	aggregates := map[string]map[string]float64{}
	counts := map[string]int64{}
	for _, a := range cfg.Aggregations {
		key := aggKey(a)
		aggregates[key] = map[string]float64{}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for r := range transformed {
			stageStart := time.Now()
			_ = e.Store.AddRecord(j.ID, r.record)
			runMu.Lock()
			j.Metrics.Processed++
			j.Metrics.Valid++
			runMu.Unlock()
			for _, a := range cfg.Aggregations {
				key := aggKey(a)
				group := "_all"
				if a.GroupBy != "" {
					group = fmt.Sprint(r.record[a.GroupBy])
				}
				v, ok := number(r.record[a.Field])
				if !ok && a.Operation != "count" {
					continue
				}
				switch strings.ToLower(a.Operation) {
				case "count":
					aggregates[key][group]++
				case "sum", "avg", "average":
					aggregates[key][group] += v
				}
				counts[key+"|"+group]++
			}
			runMu.Lock()
			j.Metrics.StageLatency["aggregation"] += time.Since(stageStart)
			runMu.Unlock()
		}
	}()
	errDone := make(chan struct{})
	go func() {
		defer close(errDone)
		for er := range errCh {
			_ = e.Store.AddError(j.ID, er)
			runMu.Lock()
			j.Metrics.Errors++
			if er.Stage == "validation" || er.Stage == "transform" {
				j.Metrics.Rejected++
			}
			runMu.Unlock()
		}
	}()
	<-done
	close(errCh)
	<-errDone // wait for centralized error persistence
	if parent.Err() != nil {
		j.Status = domain.Cancelled
	} else if ctx.Err() != nil {
		j.Status = domain.Failed
	} else {
		j.Status = domain.Completed
	}
	runMu.Lock()
	noRecordsAfterIngestFailure := j.Metrics.Discovered == 0 && ingestFailures > 0
	runMu.Unlock()
	if noRecordsAfterIngestFailure {
		j.Status = domain.Failed
	}
	for _, a := range cfg.Aggregations {
		key := aggKey(a)
		for group, v := range aggregates[key] {
			if strings.EqualFold(a.Operation, "avg") || strings.EqualFold(a.Operation, "average") {
				if counts[key+"|"+group] > 0 {
					aggregates[key][group] = v / float64(counts[key+"|"+group])
				}
			}
		}
	}
	j.Aggregates = map[string]any{}
	for k, v := range aggregates {
		j.Aggregates[k] = v
	}
	exportStart := time.Now()
	if err := e.export(j, cfg); err != nil {
		j.Status = domain.Failed
		j.Metrics.Errors++
		_ = e.Store.AddError(j.ID, domain.RecordError{Stage: "export", Message: err.Error(), At: time.Now()})
	} else {
		j.Metrics.Exported = j.Metrics.Processed
	}
	j.Metrics.StageLatency["export"] = time.Since(exportStart)
	now := time.Now()
	j.Metrics.EndedAt = &now
	elapsed := now.Sub(j.Metrics.StartedAt).Seconds()
	if elapsed > 0 {
		j.Metrics.RecordsPerSecond = float64(j.Metrics.Processed) / elapsed
	}
	j.UpdatedAt = now
	_ = e.Store.PutJob(j)
	update(j)
}
func aggKey(a domain.Aggregation) string {
	if a.Name != "" {
		return a.Name
	}
	return a.Operation + "_" + a.Field
}
func number(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case string:
		f, e := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f, e == nil
	}
	return 0, false
}
func validate(r domain.Record, rules []domain.Rule) error {
	for _, rule := range rules {
		v, ok := r[rule.Field]
		if rule.Required && (!ok || v == nil || strings.TrimSpace(fmt.Sprint(v)) == "") {
			return fmt.Errorf("field %q is required", rule.Field)
		}
		if !ok || v == nil {
			continue
		}
		if rule.Type == "number" {
			n, yes := number(v)
			if !yes {
				return fmt.Errorf("field %q must be numeric", rule.Field)
			}
			if rule.Min != nil && n < *rule.Min {
				return fmt.Errorf("field %q must be >= %v", rule.Field, *rule.Min)
			}
			if rule.Max != nil && n > *rule.Max {
				return fmt.Errorf("field %q must be <= %v", rule.Field, *rule.Max)
			}
		} else if rule.Type == "string" {
			if _, yes := v.(string); !yes {
				return fmt.Errorf("field %q must be a string", rule.Field)
			}
		}
	}
	return nil
}
func transform(r domain.Record, rules []domain.Rule) (domain.Record, error) {
	out := domain.Record{}
	for k, v := range r {
		out[k] = v
	}
	for _, rule := range rules {
		v, ok := out[rule.Field]
		if !ok || v == nil {
			continue
		}
		if rule.Type == "number" {
			n, yes := number(v)
			if !yes {
				return nil, fmt.Errorf("field %q cannot convert to number", rule.Field)
			}
			if rule.Multiply != 0 {
				n *= rule.Multiply
			}
			out[rule.Field] = n
		}
		if rule.Normalize != "" {
			s := fmt.Sprint(out[rule.Field])
			switch rule.Normalize {
			case "lower":
				out[rule.Field] = strings.ToLower(strings.TrimSpace(s))
			case "upper":
				out[rule.Field] = strings.ToUpper(strings.TrimSpace(s))
			case "trim":
				out[rule.Field] = strings.TrimSpace(s)
			}
		}
	}
	return out, nil
}
func (e *Engine) export(j *domain.Job, c domain.Config) error {
	records := e.Store.Records(j.ID)
	for _, target := range c.Exports {
		if target.Type == "" {
			continue
		}
		path := target.Path
		if path == "" {
			path = filepath.Join("exports", j.ID+"."+target.Type)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return err
		}
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		switch strings.ToLower(target.Type) {
		case "json":
			enc := json.NewEncoder(f)
			enc.SetIndent("", "  ")
			err = enc.Encode(records)
		case "csv":
			if len(records) > 0 {
				headers := make([]string, 0, len(records[0]))
				for k := range records[0] {
					headers = append(headers, k)
				}
				sort.Strings(headers)
				w := csv.NewWriter(f)
				if err = w.Write(headers); err == nil {
					for _, r := range records {
						row := make([]string, len(headers))
						for i, k := range headers {
							row[i] = fmt.Sprint(r[k])
						}
						if err = w.Write(row); err != nil {
							break
						}
					}
				}
				w.Flush()
				if err == nil {
					err = w.Error()
				}
			}
		default:
			err = fmt.Errorf("unsupported export type %q", target.Type)
		}
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		j.ResultsPath = path
	}
	return nil
}
