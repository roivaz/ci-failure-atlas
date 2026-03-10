package ndjsonoptions

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

func DefaultOptions() *RawOptions {
	return &RawOptions{
		DataDirectory:        "data",
		SemanticSubdirectory: "",
	}
}

func BindNDJSONOptions(opts *RawOptions, cmd *cobra.Command) error {
	cmd.Flags().StringVar(&opts.DataDirectory, "storage.ndjson.data-dir", opts.DataDirectory, "Root directory for NDJSON facts/state/reports.")
	cmd.Flags().StringVar(&opts.SemanticSubdirectory, "storage.ndjson.semantic-subdir", opts.SemanticSubdirectory, "Optional subdirectory under semantic/ for semantic workflow/report artifacts (for example 2026-03-01).")
	return nil
}

type RawOptions struct {
	DataDirectory        string
	SemanticSubdirectory string
}

type validatedOptions struct {
	*RawOptions
	DataDirectory        string
	SemanticSubdirectory string
}

type ValidatedOptions struct {
	*validatedOptions
}

type completedOptions struct {
	DataDirectory        string
	SemanticSubdirectory string
}

type Options struct {
	*completedOptions
}

func (o *RawOptions) Validate() (*ValidatedOptions, error) {
	dataDir := strings.TrimSpace(o.DataDirectory)
	if dataDir == "" {
		return nil, fmt.Errorf("the NDJSON data directory must be provided with --storage.ndjson.data-dir")
	}
	semanticSubdirectory, err := normalizeSemanticSubdirectory(o.SemanticSubdirectory)
	if err != nil {
		return nil, fmt.Errorf("invalid --storage.ndjson.semantic-subdir: %w", err)
	}
	return &ValidatedOptions{
		validatedOptions: &validatedOptions{
			RawOptions:           o,
			DataDirectory:        dataDir,
			SemanticSubdirectory: semanticSubdirectory,
		},
	}, nil
}

func (o *ValidatedOptions) Complete(_ context.Context) (*Options, error) {
	semanticRoot := filepath.Join(o.DataDirectory, "semantic")
	dirs := []string{
		filepath.Join(o.DataDirectory, "facts"),
		semanticRoot,
		filepath.Join(o.DataDirectory, "state"),
		filepath.Join(o.DataDirectory, "reports"),
	}
	if o.SemanticSubdirectory != "" {
		dirs = append(dirs, filepath.Join(semanticRoot, o.SemanticSubdirectory))
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, fmt.Errorf("create NDJSON data directory %q: %w", d, err)
		}
	}
	return &Options{
		completedOptions: &completedOptions{
			DataDirectory:        o.DataDirectory,
			SemanticSubdirectory: o.SemanticSubdirectory,
		},
	}, nil
}

func normalizeSemanticSubdirectory(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", nil
	}
	cleaned := filepath.Clean(trimmed)
	if cleaned == "." {
		return "", nil
	}
	if filepath.IsAbs(cleaned) {
		return "", fmt.Errorf("must be a relative path")
	}
	parts := strings.Split(cleaned, string(filepath.Separator))
	for _, part := range parts {
		switch part {
		case "", ".":
			continue
		case "..":
			return "", fmt.Errorf("must not contain '..'")
		}
	}
	return cleaned, nil
}
