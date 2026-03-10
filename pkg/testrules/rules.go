package testrules

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

type testRule struct {
	filter TestFilter
	lane   Lane
}

type compiledTestRule struct {
	testSuite     string
	testNameRegex *regexp.Regexp
	lane          Lane
}

// TODO: Wire these rules via CLI flags/config file.
var testRulesByEnvironment = map[string][]testRule{
	"dev": {
		{
			filter: TestFilter{
				TestSuite: "rp-api-compat-all/parallel",
			},
			lane: LaneE2E,
		},
		{
			filter: TestFilter{
				TestSuite:     "step graph",
				TestNameRegex: `Microsoft\.Azure\.ARO\.HCP`,
			},
			lane: LaneProvision,
		},
	},
	"int": {
		{
			filter: TestFilter{
				TestSuite: "integration/parallel",
			},
			lane: LaneE2E,
		},
	},
	"stg": {
		{
			filter: TestFilter{
				TestSuite: "stage/parallel",
			},
			lane: LaneE2E,
		},
	},
	"prod": {
		{
			filter: TestFilter{
				TestSuite: "prod/parallel",
			},
			lane: LaneE2E,
		},
	},
}

var compiledRulesByEnvironment = compileRulesByEnvironment(testRulesByEnvironment)

func FiltersForEnvironment(environment string) ([]TestFilter, bool) {
	rules, ok := testRulesByEnvironment[normalizeEnvironment(environment)]
	if !ok || len(rules) == 0 {
		return nil, false
	}
	out := make([]TestFilter, 0, len(rules))
	for _, rule := range rules {
		out = append(out, TestFilter{
			TestSuite:     strings.TrimSpace(rule.filter.TestSuite),
			TestNameRegex: strings.TrimSpace(rule.filter.TestNameRegex),
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

func compileRulesByEnvironment(raw map[string][]testRule) map[string][]compiledTestRule {
	out := map[string][]compiledTestRule{}
	for environment, rules := range raw {
		normalizedEnvironment := normalizeEnvironment(environment)
		if normalizedEnvironment == "" {
			continue
		}
		compiled := make([]compiledTestRule, 0, len(rules))
		for _, rule := range rules {
			suite := strings.TrimSpace(rule.filter.TestSuite)
			if suite == "" {
				continue
			}
			normalizedLane := normalizeLane(rule.lane)
			if normalizedLane == LaneUnknown {
				continue
			}

			var testNameRegex *regexp.Regexp
			if pattern := strings.TrimSpace(rule.filter.TestNameRegex); pattern != "" {
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
