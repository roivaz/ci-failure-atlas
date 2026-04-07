package options

import "strings"

type EnvironmentDefaults struct {
	SippyRelease            string
	SippyJobName            string
	DeterministicJUnitPaths []string
	SupportsPRLookup        bool
}

type RuntimeDefaults struct {
	SippyBaseURL         string
	ProwArtifactsBaseURL string
	SippyOrg             string
	SippyRepo            string
	GitHubRepoOwner      string
	GitHubRepoName       string
	HistoryHorizonWeeks  int
	DefaultEnvironments  []string
	DefaultJUnitPaths    []string
	Environments         map[string]EnvironmentDefaults
}

var supportedEnvironmentOrder = []string{"dev", "int", "stg", "prod"}

var defaultRuntimeDefaults = RuntimeDefaults{
	SippyBaseURL:         "https://sippy.dptools.openshift.org",
	ProwArtifactsBaseURL: "https://gcsweb-ci.apps.ci.l2s4.p1.openshiftapps.com/gcs",
	SippyOrg:             "Azure",
	SippyRepo:            "ARO-HCP",
	GitHubRepoOwner:      "Azure",
	GitHubRepoName:       "ARO-HCP",
	HistoryHorizonWeeks:  4,
	DefaultEnvironments:  []string{"dev"},
	DefaultJUnitPaths: []string{
		"prowjob_junit.xml",
	},
	Environments: map[string]EnvironmentDefaults{
		"dev": {
			SippyRelease: "Presubmits",
			SippyJobName: "pull-ci-Azure-ARO-HCP-main-e2e-parallel",
			DeterministicJUnitPaths: []string{
				"artifacts/e2e-parallel/aro-hcp-provision-environment/artifacts/junit_entrypoint.xml",
				"prowjob_junit.xml",
				"artifacts/e2e-parallel/aro-hcp-test-local/artifacts/junit.xml",
			},
			SupportsPRLookup: true,
		},
		"int": {
			SippyRelease: "aro-integration",
			SippyJobName: "periodic-ci-Azure-ARO-HCP-main-periodic-integration-e2e-parallel",
			DeterministicJUnitPaths: []string{
				"artifacts/integration-e2e-parallel/aro-hcp-test-persistent/artifacts/junit.xml",
				"prowjob_junit.xml",
			},
		},
		"stg": {
			SippyRelease: "aro-stage",
			SippyJobName: "periodic-ci-Azure-ARO-HCP-main-periodic-stage-e2e-parallel",
			DeterministicJUnitPaths: []string{
				"artifacts/stage-e2e-parallel/aro-hcp-test-persistent/artifacts/junit.xml",
				"prowjob_junit.xml",
			},
		},
		"prod": {
			SippyRelease: "aro-production",
			SippyJobName: "periodic-ci-Azure-ARO-HCP-main-periodic-prod-e2e-parallel",
			DeterministicJUnitPaths: []string{
				"artifacts/prod-e2e-parallel/aro-hcp-test-persistent/artifacts/junit.xml",
				"prowjob_junit.xml",
			},
		},
	},
}

func DefaultRuntimeDefaults() RuntimeDefaults {
	return cloneRuntimeDefaults(defaultRuntimeDefaults)
}

func SupportedEnvironments() []string {
	return append([]string(nil), supportedEnvironmentOrder...)
}

func EnvironmentDefaultsFor(environment string) (EnvironmentDefaults, bool) {
	defaults, ok := defaultRuntimeDefaults.Environments[normalizeEnvironmentName(environment)]
	if !ok {
		return EnvironmentDefaults{}, false
	}
	return cloneEnvironmentDefaults(defaults), true
}

func SippyJobNameForEnvironment(environment string) (string, bool) {
	defaults, ok := EnvironmentDefaultsFor(environment)
	if !ok {
		return "", false
	}
	return strings.TrimSpace(defaults.SippyJobName), strings.TrimSpace(defaults.SippyJobName) != ""
}

func SupportsPRLookupForEnvironment(environment string) bool {
	defaults, ok := EnvironmentDefaultsFor(environment)
	if !ok {
		return false
	}
	return defaults.SupportsPRLookup
}

func DefaultJUnitPaths() []string {
	return append([]string(nil), defaultRuntimeDefaults.DefaultJUnitPaths...)
}

func DeterministicJUnitPathsByEnvironment() map[string][]string {
	out := make(map[string][]string, len(defaultRuntimeDefaults.Environments))
	for environment, defaults := range defaultRuntimeDefaults.Environments {
		if len(defaults.DeterministicJUnitPaths) == 0 {
			continue
		}
		out[environment] = append([]string(nil), defaults.DeterministicJUnitPaths...)
	}
	return out
}

func DefaultGitHubRepoOwner() string {
	return defaultRuntimeDefaults.GitHubRepoOwner
}

func DefaultGitHubRepoName() string {
	return defaultRuntimeDefaults.GitHubRepoName
}

func normalizeEnvironmentName(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func cloneRuntimeDefaults(in RuntimeDefaults) RuntimeDefaults {
	out := in
	out.DefaultEnvironments = append([]string(nil), in.DefaultEnvironments...)
	out.DefaultJUnitPaths = append([]string(nil), in.DefaultJUnitPaths...)
	out.Environments = make(map[string]EnvironmentDefaults, len(in.Environments))
	for environment, defaults := range in.Environments {
		out.Environments[environment] = cloneEnvironmentDefaults(defaults)
	}
	return out
}

func cloneEnvironmentDefaults(in EnvironmentDefaults) EnvironmentDefaults {
	out := in
	out.DeterministicJUnitPaths = append([]string(nil), in.DeterministicJUnitPaths...)
	return out
}
