package sourceoptions

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/spf13/cobra"
)

var supportedEnvironments = []string{"dev", "int", "stg", "prod"}

const defaultHistoryHorizonWeeks = 4

func DefaultOptions() *RawOptions {
	return &RawOptions{
		SippyBaseURL:         "https://sippy.dptools.openshift.org",
		ProwArtifactsBaseURL: "https://gcsweb-ci.apps.ci.l2s4.p1.openshiftapps.com/gcs",
		Environments:         []string{"dev"},
		SippyOrg:             "Azure",
		SippyRepo:            "ARO-HCP",
		SippyReleaseDev:      "Presubmits",
		SippyReleaseInt:      "aro-integration",
		SippyReleaseStg:      "aro-stage",
		SippyReleaseProd:     "aro-production",
		HistoryHorizonWeeks:  defaultHistoryHorizonWeeks,
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
	cmd.Flags().IntVar(&opts.HistoryHorizonWeeks, "history.weeks", opts.HistoryHorizonWeeks, "Number of weeks to look back for ingestion, reconciliation, and report history.")
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
	HistoryHorizonWeeks  int
	ProwArtifactsBaseURL string
	Environments         []string
}

type validatedOptions struct {
	*RawOptions
	SippyBaseURL         string
	SippyOrg             string
	SippyRepo            string
	SippyReleaseByEnv    map[string]string
	HistoryHorizonWeeks  int
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
	HistoryHorizonWeeks  int
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
	historyHorizonWeeks := o.HistoryHorizonWeeks
	if historyHorizonWeeks <= 0 {
		return nil, fmt.Errorf("the history horizon must be > 0 weeks (set --history.weeks)")
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
			HistoryHorizonWeeks:  historyHorizonWeeks,
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
			HistoryHorizonWeeks:  o.HistoryHorizonWeeks,
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

func copyStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
