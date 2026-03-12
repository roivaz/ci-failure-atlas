package history

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const windowMetadataFilename = "window_metadata.json"

type WindowMetadata struct {
	WindowStart string `json:"window_start"`
	WindowEnd   string `json:"window_end"`
	WindowDays  int    `json:"window_days"`
	GeneratedAt string `json:"generated_at"`
}

func WriteWindowMetadata(dataDirectory string, semanticSubdirectory string, windowStart time.Time, windowEndExclusive time.Time) error {
	dataDirectory = strings.TrimSpace(dataDirectory)
	semanticSubdirectory = strings.TrimSpace(semanticSubdirectory)
	if dataDirectory == "" || semanticSubdirectory == "" {
		return nil
	}
	start := windowStart.UTC()
	end := windowEndExclusive.UTC()
	if !start.Before(end) {
		return fmt.Errorf("window start must be before window end (start=%s end=%s)", start.Format(time.RFC3339), end.Format(time.RFC3339))
	}
	windowDays := int(end.Sub(start).Hours() / 24)
	if windowDays <= 0 {
		return fmt.Errorf("window days must be positive (got %d)", windowDays)
	}
	metadata := WindowMetadata{
		WindowStart: start.Format(time.RFC3339),
		WindowEnd:   end.Format(time.RFC3339),
		WindowDays:  windowDays,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
	}
	raw, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal window metadata: %w", err)
	}
	path := windowMetadataPath(dataDirectory, semanticSubdirectory)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create metadata directory: %w", err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		return fmt.Errorf("write window metadata %q: %w", path, err)
	}
	return nil
}

func ReadWindowMetadata(dataDirectory string, semanticSubdirectory string) (WindowMetadata, bool, error) {
	dataDirectory = strings.TrimSpace(dataDirectory)
	semanticSubdirectory = strings.TrimSpace(semanticSubdirectory)
	if dataDirectory == "" || semanticSubdirectory == "" {
		return WindowMetadata{}, false, nil
	}
	path := windowMetadataPath(dataDirectory, semanticSubdirectory)
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return WindowMetadata{}, false, nil
		}
		return WindowMetadata{}, false, fmt.Errorf("read window metadata %q: %w", path, err)
	}
	var metadata WindowMetadata
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return WindowMetadata{}, false, fmt.Errorf("decode window metadata %q: %w", path, err)
	}
	return metadata, true, nil
}

func windowMetadataPath(dataDirectory string, semanticSubdirectory string) string {
	return filepath.Join(dataDirectory, "semantic", semanticSubdirectory, windowMetadataFilename)
}

func isCanonicalSevenDayWindow(metadata WindowMetadata) bool {
	return metadata.WindowDays == 7
}
