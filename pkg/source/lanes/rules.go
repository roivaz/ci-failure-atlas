package lanes

import (
	"regexp"
	"strings"
)

type Lane string

const (
	LaneUnknown   Lane = "unknown"
	LaneProvision Lane = "provision"
	LaneE2E       Lane = "e2e"
)

type TestFilter struct {
	TestSuite     string
	TestNameRegex string
}

type Rule struct {
	Filter TestFilter
	Lane   Lane
}

type compiledTestRule struct {
	testSuite     string
	testNameRegex *regexp.Regexp
	lane          Lane
}

// defaultRulesByEnvironment is the built-in lane/filter rule set shared by
// ingestion, metrics rollups, and semantic/report consumers.
var defaultRulesByEnvironment = map[string][]Rule{
	"dev": {
		{
			Filter: TestFilter{
				TestSuite: "rp-api-compat-all/parallel",
			},
			Lane: LaneE2E,
		},
		{
			Filter: TestFilter{
				TestSuite:     "step graph",
				TestNameRegex: `Microsoft\.Azure\.ARO\.HCP`,
			},
			Lane: LaneProvision,
		},
	},
	"int": {
		{
			Filter: TestFilter{
				TestSuite: "integration/parallel",
			},
			Lane: LaneE2E,
		},
	},
	"stg": {
		{
			Filter: TestFilter{
				TestSuite: "stage/parallel",
			},
			Lane: LaneE2E,
		},
	},
	"prod": {
		{
			Filter: TestFilter{
				TestSuite: "prod/parallel",
			},
			Lane: LaneE2E,
		},
	},
}

var compiledRulesByEnvironment = compileRulesByEnvironment(defaultRulesByEnvironment)

func DefaultRulesByEnvironment() map[string][]Rule {
	out := make(map[string][]Rule, len(defaultRulesByEnvironment))
	for environment, rules := range defaultRulesByEnvironment {
		cloned := make([]Rule, 0, len(rules))
		for _, rule := range rules {
			cloned = append(cloned, Rule{
				Filter: TestFilter{
					TestSuite:     strings.TrimSpace(rule.Filter.TestSuite),
					TestNameRegex: strings.TrimSpace(rule.Filter.TestNameRegex),
				},
				Lane: normalizeLane(rule.Lane),
			})
		}
		out[environment] = cloned
	}
	return out
}

func FiltersForEnvironment(environment string) ([]TestFilter, bool) {
	rules, ok := defaultRulesByEnvironment[normalizeEnvironment(environment)]
	if !ok || len(rules) == 0 {
		return nil, false
	}
	out := make([]TestFilter, 0, len(rules))
	for _, rule := range rules {
		out = append(out, TestFilter{
			TestSuite:     strings.TrimSpace(rule.Filter.TestSuite),
			TestNameRegex: strings.TrimSpace(rule.Filter.TestNameRegex),
		})
	}
	return out, true
}

func ClassifyLane(environment string, testSuite string, testName string) Lane {
	rules, ok := compiledRulesByEnvironment[normalizeEnvironment(environment)]
	if !ok || len(rules) == 0 {
		return LaneUnknown
	}
	suite := strings.TrimSpace(testSuite)
	if suite == "" {
		return LaneUnknown
	}
	name := strings.TrimSpace(testName)
	for _, rule := range rules {
		if rule.testSuite != suite {
			continue
		}
		if rule.testNameRegex == nil {
			return rule.lane
		}
		if rule.testNameRegex.MatchString(name) {
			return rule.lane
		}
	}
	return LaneUnknown
}

func compileRulesByEnvironment(raw map[string][]Rule) map[string][]compiledTestRule {
	out := map[string][]compiledTestRule{}
	for environment, rules := range raw {
		normalizedEnvironment := normalizeEnvironment(environment)
		if normalizedEnvironment == "" {
			continue
		}
		compiled := make([]compiledTestRule, 0, len(rules))
		for _, rule := range rules {
			suite := strings.TrimSpace(rule.Filter.TestSuite)
			if suite == "" {
				continue
			}
			normalizedLane := normalizeLane(rule.Lane)
			if normalizedLane == LaneUnknown {
				continue
			}

			var testNameRegex *regexp.Regexp
			if pattern := strings.TrimSpace(rule.Filter.TestNameRegex); pattern != "" {
				testNameRegex = regexp.MustCompile(pattern)
			}
			compiled = append(compiled, compiledTestRule{
				testSuite:     suite,
				testNameRegex: testNameRegex,
				lane:          normalizedLane,
			})
		}
		if len(compiled) > 0 {
			out[normalizedEnvironment] = compiled
		}
	}
	return out
}

func normalizeEnvironment(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeLane(value Lane) Lane {
	switch strings.TrimSpace(string(value)) {
	case string(LaneProvision):
		return LaneProvision
	case string(LaneE2E):
		return LaneE2E
	default:
		return LaneUnknown
	}
}
