package contracts

import (
	"fmt"
	"strings"
)

const (
	SchemaVersionV1 = "v1"
	SchemaVersionV2 = "v2"

	// CurrentSchemaVersion is the semantic identity contract emitted by new
	// materialization and phase3 write paths.
	CurrentSchemaVersion = SchemaVersionV2
)

func NormalizeSchemaVersion(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", SchemaVersionV1:
		return SchemaVersionV1
	case SchemaVersionV2:
		return SchemaVersionV2
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

func SupportedSchemaVersion(value string) bool {
	switch NormalizeSchemaVersion(value) {
	case SchemaVersionV1, SchemaVersionV2:
		return true
	default:
		return false
	}
}

func InferWeekSchemaVersion(
	failurePatterns []FailurePatternRecord,
	reviewQueue []ReviewItemRecord,
) (string, error) {
	version := ""
	apply := func(raw string, source string) error {
		normalized := NormalizeSchemaVersion(raw)
		if !SupportedSchemaVersion(normalized) {
			return fmt.Errorf("unsupported semantic schema version %q in %s", strings.TrimSpace(raw), source)
		}
		if version == "" {
			version = normalized
			return nil
		}
		if version != normalized {
			return fmt.Errorf(
				"mixed semantic schema versions within one materialized week: %s and %s",
				version,
				normalized,
			)
		}
		return nil
	}
	for index, row := range failurePatterns {
		if err := apply(row.SchemaVersion, fmt.Sprintf("failure_patterns[%d]", index)); err != nil {
			return "", err
		}
	}
	for index, row := range reviewQueue {
		if err := apply(row.SchemaVersion, fmt.Sprintf("review_queue[%d]", index)); err != nil {
			return "", err
		}
	}
	return version, nil
}

func IsCurrentOrUnsetSchemaVersion(value string) bool {
	if strings.TrimSpace(value) == "" {
		return true
	}
	return NormalizeSchemaVersion(value) == CurrentSchemaVersion
}

func RequireCurrentSchemaVersion(value string, context string) error {
	trimmedValue := strings.TrimSpace(value)
	if trimmedValue == "" {
		return nil
	}
	normalized := NormalizeSchemaVersion(trimmedValue)
	if !SupportedSchemaVersion(normalized) {
		if strings.TrimSpace(context) == "" {
			context = "semantic week loading"
		}
		return fmt.Errorf("%s uses unsupported semantic schema %q", context, trimmedValue)
	}
	if normalized == CurrentSchemaVersion {
		return nil
	}
	if strings.TrimSpace(context) == "" {
		context = "semantic week loading"
	}
	return fmt.Errorf(
		"%s uses legacy semantic schema %s; rematerialize/backfill this week before loading it",
		context,
		normalized,
	)
}

func RequireCompatibleWeekSchemas(expected string, actual string, context string) error {
	if strings.TrimSpace(expected) == "" || strings.TrimSpace(actual) == "" {
		return nil
	}
	normalizedExpected := NormalizeSchemaVersion(expected)
	normalizedActual := NormalizeSchemaVersion(actual)
	if normalizedExpected == "" || normalizedActual == "" || normalizedExpected == normalizedActual {
		return nil
	}
	if strings.TrimSpace(context) == "" {
		context = "semantic history/window loading"
	}
	return fmt.Errorf(
		"%s mixes semantic schema %s with %s; rematerialize/backfill the affected weeks before combining them",
		context,
		normalizedExpected,
		normalizedActual,
	)
}
