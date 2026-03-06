package sourceoptions

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/spf13/cobra"
)

var supportedEnvironments = []string{"dev", "int", "stg", "prod"}

func DefaultOptions() *RawOptions {
	return &RawOptions{
		SippyBaseURL:         "https://sippy.dptools.openshift.org",
		ProwArtifactsBaseURL: "https://gcsweb-ci.apps.ci.l2s4.p1.openshiftapps.com/gcs",
		Environments:         []string{"dev"},
	}
}

func BindSourceOptions(opts *RawOptions, cmd *cobra.Command) error {
	cmd.Flags().StringVar(&opts.SippyBaseURL, "source.sippy.base-url", opts.SippyBaseURL, "Base URL for the Sippy API.")
	cmd.Flags().StringVar(&opts.ProwArtifactsBaseURL, "source.prow-artifacts.base-url", opts.ProwArtifactsBaseURL, "Base URL for Prow/GCS artifacts.")
	cmd.Flags().StringSliceVar(&opts.Environments, "source.envs", opts.Environments, "Environments to ingest from (allowed: dev,int,stg,prod).")
	return nil
}

type RawOptions struct {
	SippyBaseURL         string
	ProwArtifactsBaseURL string
	Environments         []string
}

type validatedOptions struct {
	*RawOptions
	SippyBaseURL         string
	ProwArtifactsBaseURL string
	Environments         []string
}

type ValidatedOptions struct {
	*validatedOptions
}

type completedOptions struct {
	SippyBaseURL         string
	ProwArtifactsBaseURL string
	Environments         []string
}

type Options struct {
	*completedOptions
}

func (o *RawOptions) Validate() (*ValidatedOptions, error) {
	sippyURL := strings.TrimSpace(o.SippyBaseURL)
	artifactsURL := strings.TrimSpace(o.ProwArtifactsBaseURL)
	if sippyURL == "" {
		return nil, fmt.Errorf("the sippy base URL must be provided with --source.sippy.base-url")
	}
	if artifactsURL == "" {
		return nil, fmt.Errorf("the prow artifacts base URL must be provided with --source.prow-artifacts.base-url")
	}
	envs := normalizeEnvironments(o.Environments)
	if len(envs) == 0 {
		return nil, fmt.Errorf("at least one environment must be provided with --source.envs (allowed: %s)", strings.Join(supportedEnvironments, ","))
	}
	for _, env := range envs {
		if !slices.Contains(supportedEnvironments, env) {
			return nil, fmt.Errorf("unsupported environment %q for --source.envs (allowed: %s)", env, strings.Join(supportedEnvironments, ","))
		}
	}
	return &ValidatedOptions{
		validatedOptions: &validatedOptions{
			RawOptions:           o,
			SippyBaseURL:         sippyURL,
			ProwArtifactsBaseURL: artifactsURL,
			Environments:         envs,
		},
	}, nil
}

func (o *ValidatedOptions) Complete(_ context.Context) (*Options, error) {
	return &Options{
		completedOptions: &completedOptions{
			SippyBaseURL:         o.SippyBaseURL,
			ProwArtifactsBaseURL: o.ProwArtifactsBaseURL,
			Environments:         append([]string(nil), o.Environments...),
		},
	}, nil
}

func normalizeEnvironments(raw []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(raw))
	for _, value := range raw {
		normalized := strings.ToLower(strings.TrimSpace(value))
		if normalized == "" {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out
}
