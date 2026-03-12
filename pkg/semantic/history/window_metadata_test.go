package history

import (
	"testing"
	"time"
)

func TestWriteAndReadWindowMetadata(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	start := mustParseMetadataDate(t, "2026-03-01")
	end := start.AddDate(0, 0, 7)
	if err := WriteWindowMetadata(dataDir, "2026-03-01", start, end); err != nil {
		t.Fatalf("WriteWindowMetadata returned error: %v", err)
	}

	metadata, exists, err := ReadWindowMetadata(dataDir, "2026-03-01")
	if err != nil {
		t.Fatalf("ReadWindowMetadata returned error: %v", err)
	}
	if !exists {
		t.Fatalf("expected metadata to exist")
	}
	if metadata.WindowDays != 7 {
		t.Fatalf("expected WindowDays=7, got %d", metadata.WindowDays)
	}
	if !isCanonicalSevenDayWindow(metadata) {
		t.Fatalf("expected metadata to be canonical seven-day window")
	}
}

func TestReadWindowMetadataMissingReturnsNotExists(t *testing.T) {
	t.Parallel()

	metadata, exists, err := ReadWindowMetadata(t.TempDir(), "2026-03-01")
	if err != nil {
		t.Fatalf("ReadWindowMetadata returned error: %v", err)
	}
	if exists {
		t.Fatalf("expected missing metadata to return exists=false, got metadata=%#v", metadata)
	}
}

func mustParseMetadataDate(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse("2006-01-02", value)
	if err != nil {
		t.Fatalf("parse metadata date %q: %v", value, err)
	}
	return parsed.UTC()
}
