package sourceoptions

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var supportedEnvironments = []string{"dev", "int", "stg", "prod"}

func DefaultOptions() *RawOptions {
	return &RawOptions{
		SippyBaseURL:         "https://sippy.dptools.openshift.org",
		ProwArtifactsBaseURL: "https://gcsweb-ci.apps.ci.l2s4.p1.openshiftapps.com/gcs",
		Environments:         []string{"dev"},
		SippyOrg:             "Azure",
		SippyRepo:            "ARO-HCP",
		SippyReleaseDev:      "Presubmits",
		SippyLookback:        "7d",
	}
}

func BindSourceOptions(opts *RawOptions, cmd *cobra.Command) error {
	cmd.Flags().StringVar(&opts.SippyBaseURL, "source.sippy.base-url", opts.SippyBaseURL, "Base URL for the Sippy API.")
	cmd.Flags().StringVar(&opts.SippyOrg, "source.sippy.org", opts.SippyOrg, "Git organization filter used in Sippy queries.")
	cmd.Flags().StringVar(&opts.SippyRepo, "source.sippy.repo", opts.SippyRepo, "Git repository filter used in Sippy queries.")
	cmd.Flags().StringVar(&opts.SippyReleaseDev, "source.sippy.release.dev", opts.SippyReleaseDev, "Sippy release value for the dev environment.")
	cmd.Flags().StringVar(&opts.SippyReleaseInt, "source.sippy.release.int", opts.SippyReleaseInt, "Sippy release value for the int environment.")
	cmd.Flags().StringVar(&opts.SippyReleaseStg, "source.sippy.release.stg", opts.SippyReleaseStg, "Sippy release value for the stg environment.")
	cmd.Flags().StringVar(&opts.SippyReleaseProd, "source.sippy.release.prod", opts.SippyReleaseProd, "Sippy release value for the prod environment.")
	cmd.Flags().StringVar(&opts.SippyLookback, "source.sippy.lookback", opts.SippyLookback, "Lookback window for job-run discovery (for example 24h, 7d, 2w).")
	cmd.Flags().StringVar(&opts.ProwArtifactsBaseURL, "source.prow-artifacts.base-url", opts.ProwArtifactsBaseURL, "Base URL for Prow/GCS artifacts.")
	cmd.Flags().StringSliceVar(&opts.Environments, "source.envs", opts.Environments, "Environments to ingest from (allowed: dev,int,stg,prod).")
	return nil
}

type RawOptions struct {
	SippyBaseURL         string
	SippyOrg             string
	SippyRepo            string
	SippyReleaseDev      string
	SippyReleaseInt      string
	SippyReleaseStg      string
	SippyReleaseProd     string
	SippyLookback        string
	ProwArtifactsBaseURL string
	Environments         []string
}

type validatedOptions struct {
	*RawOptions
	SippyBaseURL         string
	SippyOrg             string
	SippyRepo            string
	SippyReleaseByEnv    map[string]string
	SippyLookback        time.Duration
	ProwArtifactsBaseURL string
	Environments         []string
}

type ValidatedOptions struct {
	*validatedOptions
}

type completedOptions struct {
	SippyBaseURL         string
	SippyOrg             string
	SippyRepo            string
	SippyReleaseByEnv    map[string]string
	SippyLookback        time.Duration
	ProwArtifactsBaseURL string
	Environments         []string
}

type Options struct {
	*completedOptions
}

func (o *RawOptions) Validate() (*ValidatedOptions, error) {
	sippyURL := strings.TrimSpace(o.SippyBaseURL)
	sippyOrg := strings.TrimSpace(o.SippyOrg)
	sippyRepo := strings.TrimSpace(o.SippyRepo)
	artifactsURL := strings.TrimSpace(o.ProwArtifactsBaseURL)
	if sippyURL == "" {
		return nil, fmt.Errorf("the sippy base URL must be provided with --source.sippy.base-url")
	}
	if sippyOrg == "" {
		return nil, fmt.Errorf("the sippy org must be provided with --source.sippy.org")
	}
	if sippyRepo == "" {
		return nil, fmt.Errorf("the sippy repo must be provided with --source.sippy.repo")
	}
	if artifactsURL == "" {
		return nil, fmt.Errorf("the prow artifacts base URL must be provided with --source.prow-artifacts.base-url")
	}
	lookback, err := parseLookback(strings.TrimSpace(o.SippyLookback))
	if err != nil {
		return nil, fmt.Errorf("invalid --source.sippy.lookback value: %w", err)
	}

	envs := normalizeEnvironments(o.Environments)
	if len(envs) == 0 {
		return nil, fmt.Errorf("at least one environment must be provided with --source.envs (allowed: %s)", strings.Join(supportedEnvironments, ","))
	}

	releasesByEnv := map[string]string{
		"dev":  strings.TrimSpace(o.SippyReleaseDev),
		"int":  strings.TrimSpace(o.SippyReleaseInt),
		"stg":  strings.TrimSpace(o.SippyReleaseStg),
		"prod": strings.TrimSpace(o.SippyReleaseProd),
	}

	for _, env := range envs {
		if !slices.Contains(supportedEnvironments, env) {
			return nil, fmt.Errorf("unsupported environment %q for --source.envs (allowed: %s)", env, strings.Join(supportedEnvironments, ","))
		}
		if strings.TrimSpace(releasesByEnv[env]) == "" {
			return nil, fmt.Errorf("missing Sippy release for environment %q (set --source.sippy.release.%s)", env, env)
		}
	}

	return &ValidatedOptions{
		validatedOptions: &validatedOptions{
			RawOptions:           o,
			SippyBaseURL:         sippyURL,
			SippyOrg:             sippyOrg,
			SippyRepo:            sippyRepo,
			SippyReleaseByEnv:    releasesByEnv,
			SippyLookback:        lookback,
			ProwArtifactsBaseURL: artifactsURL,
			Environments:         envs,
		},
	}, nil
}

func (o *ValidatedOptions) Complete(_ context.Context) (*Options, error) {
	return &Options{
		completedOptions: &completedOptions{
			SippyBaseURL:         o.SippyBaseURL,
			SippyOrg:             o.SippyOrg,
			SippyRepo:            o.SippyRepo,
			SippyReleaseByEnv:    copyStringMap(o.SippyReleaseByEnv),
			SippyLookback:        o.SippyLookback,
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

func parseLookback(raw string) (time.Duration, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 7 * 24 * time.Hour, nil
	}

	if d, err := time.ParseDuration(trimmed); err == nil {
		if d <= 0 {
			return 0, fmt.Errorf("lookback duration must be > 0")
		}
		return d, nil
	}

	if strings.HasSuffix(trimmed, "d") {
		n, err := strconv.Atoi(strings.TrimSuffix(trimmed, "d"))
		if err != nil || n <= 0 {
			return 0, fmt.Errorf("invalid day lookback %q", raw)
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	if strings.HasSuffix(trimmed, "w") {
		n, err := strconv.Atoi(strings.TrimSuffix(trimmed, "w"))
		if err != nil || n <= 0 {
			return 0, fmt.Errorf("invalid week lookback %q", raw)
		}
		return time.Duration(n) * 7 * 24 * time.Hour, nil
	}

	return 0, fmt.Errorf("unsupported lookback format %q", raw)
}

func copyStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
