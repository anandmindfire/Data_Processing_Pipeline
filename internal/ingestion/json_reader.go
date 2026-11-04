package ingestion

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/anandmindfire/data-processing-pipeline/internal/models"
)

// readJSON decodes JSON records with streaming memory efficiency (arrays, objects, NDJSON).
func readJSON(ctx context.Context, r io.Reader, max int, emit func(int, models.Record) error) error {
	br := bufio.NewReader(r)
	firstByte, err := peekFirstNonWhitespace(br)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidFormat, err)
	}

	dec := json.NewDecoder(br)
	if firstByte == '[' {
		return streamJSONArray(ctx, dec, max, emit)
	}
	return streamJSONObjects(ctx, dec, max, emit)
}

func streamJSONArray(ctx context.Context, dec *json.Decoder, max int, emit func(int, models.Record) error) error {
	// Consume opening '['
	_, err := dec.Token()
	if err != nil {
		return fmt.Errorf("%w: read opening bracket: %w", ErrInvalidFormat, err)
	}

	idx := 0
	for dec.More() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if max > 0 && idx >= max {
			break
		}

		var rec map[string]any
		if err := dec.Decode(&rec); err != nil {
			return fmt.Errorf("%w: decode array item %d: %w", ErrInvalidFormat, idx+1, err)
		}
		idx++
		if err := emit(idx, models.Record(rec)); err != nil {
			return err
		}
	}

	// Consume closing ']'
	_, _ = dec.Token()
	return nil
}

func streamJSONObjects(ctx context.Context, dec *json.Decoder, max int, emit func(int, models.Record) error) error {
	idx := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if max > 0 && idx >= max {
			break
		}

		var rec map[string]any
		if err := dec.Decode(&rec); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("%w: decode object %d: %w", ErrInvalidFormat, idx+1, err)
		}
		idx++
		if err := emit(idx, models.Record(rec)); err != nil {
			return err
		}
	}
	return nil
}

func peekFirstNonWhitespace(br *bufio.Reader) (byte, error) {
	for {
		b, err := br.ReadByte()
		if err != nil {
			return 0, err
		}
		if b != ' ' && b != '\t' && b != '\r' && b != '\n' {
			_ = br.UnreadByte()
			return b, nil
		}
	}
}
