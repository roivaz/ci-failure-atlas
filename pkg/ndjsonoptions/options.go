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
		DataDirectory: "data",
	}
}

func BindNDJSONOptions(opts *RawOptions, cmd *cobra.Command) error {
	cmd.Flags().StringVar(&opts.DataDirectory, "storage.ndjson.data-dir", opts.DataDirectory, "Root directory for NDJSON facts/state/reports.")
	return nil
}

type RawOptions struct {
	DataDirectory string
}

type validatedOptions struct {
	*RawOptions
	DataDirectory string
}

type ValidatedOptions struct {
	*validatedOptions
}

type completedOptions struct {
	DataDirectory string
}

type Options struct {
	*completedOptions
}

func (o *RawOptions) Validate() (*ValidatedOptions, error) {
	dataDir := strings.TrimSpace(o.DataDirectory)
	if dataDir == "" {
		return nil, fmt.Errorf("the NDJSON data directory must be provided with --storage.ndjson.data-dir")
	}
	return &ValidatedOptions{
		validatedOptions: &validatedOptions{
			RawOptions:    o,
			DataDirectory: dataDir,
		},
	}, nil
}

func (o *ValidatedOptions) Complete(_ context.Context) (*Options, error) {
	dirs := []string{
		filepath.Join(o.DataDirectory, "facts"),
		filepath.Join(o.DataDirectory, "semantic"),
		filepath.Join(o.DataDirectory, "state"),
		filepath.Join(o.DataDirectory, "reports"),
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, fmt.Errorf("create NDJSON data directory %q: %w", d, err)
		}
	}
	return &Options{
		completedOptions: &completedOptions{
			DataDirectory: o.DataDirectory,
		},
	}, nil
}
