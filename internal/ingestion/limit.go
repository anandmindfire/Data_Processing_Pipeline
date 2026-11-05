package ingestion

import (
	"errors"
	"fmt"
	"io"
)

// ErrSourceTooLarge is returned when a source exceeds the configured byte limit.
var ErrSourceTooLarge = errors.New("source exceeds the maximum allowed size")

// limitedReader fails (rather than silently truncating) once more than max bytes are read, so a
// huge file or an endless HTTP response cannot exhaust disk, memory, or job time.
type limitedReader struct {
	r    io.Reader
	left int64
	max  int64
}

func newLimitedReader(r io.Reader, max int64) io.Reader {
	if max <= 0 {
		return r
	}
	return &limitedReader{r: r, left: max + 1, max: max}
}

func (l *limitedReader) Read(p []byte) (int, error) {
	if l.left <= 0 {
		return 0, fmt.Errorf("%w (%d bytes)", ErrSourceTooLarge, l.max)
	}
	if int64(len(p)) > l.left {
		p = p[:l.left]
	}
	n, err := l.r.Read(p)
	l.left -= int64(n)
	if l.left <= 0 {
		return n - 1, fmt.Errorf("%w (%d bytes)", ErrSourceTooLarge, l.max)
	}
	return n, err
}
