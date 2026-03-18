package history

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	semanticcontracts "ci-failure-atlas/pkg/semantic/contracts"
	"ci-failure-atlas/pkg/store/ndjson"
)

func TestBuildGlobalSignatureResolverCollectsPriorWeeksWithinLookback(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()

	seedGlobalClusters(
		t,
		ctx,
		dataDir,
		"2026-03-01",
		[]semanticcontracts.GlobalClusterRecord{
			{
				SchemaVersion:           semanticcontracts.SchemaVersionV1,
				Environment:             "dev",
				Phase2ClusterID:         "cluster-a-1",
				CanonicalEvidencePhrase: "ERROR CODE: Conflict",
				SearchQueryPhrase:       "ERROR CODE: Conflict",
				References: []semanticcontracts.ReferenceRecord{
					{RunURL: "https://prow.example/job/1", OccurredAt: "2026-03-01T12:00:00Z"},
					{RunURL: "https://prow.example/job/2", OccurredAt: "2026-03-02T12:00:00Z"},
				},
			},
		},
	)
	seedCanonicalWeekMetadata(t, dataDir, "2026-03-01")
	seedGlobalClusters(
		t,
		ctx,
		dataDir,
		"2026-02-22",
		[]semanticcontracts.GlobalClusterRecord{
			{
				SchemaVersion:           semanticcontracts.SchemaVersionV1,
				Environment:             "dev",
				Phase2ClusterID:         "cluster-a-2",
				CanonicalEvidencePhrase: "ERROR CODE: Conflict",
				SearchQueryPhrase:       "ERROR CODE: Conflict",
				References: []semanticcontracts.ReferenceRecord{
					{RunURL: "https://prow.example/job/2", OccurredAt: "2026-02-23T12:00:00Z"},
					{RunURL: "https://prow.example/job/3", OccurredAt: "2026-02-24T12:00:00Z"},
				},
			},
		},
	)
	seedCanonicalWeekMetadata(t, dataDir, "2026-02-22")
	seedGlobalClusters(
		t,
		ctx,
		dataDir,
		"2026-01-18",
		[]semanticcontracts.GlobalClusterRecord{
			{
				SchemaVersion:           semanticcontracts.SchemaVersionV1,
				Environment:             "dev",
				Phase2ClusterID:         "cluster-a-3",
				CanonicalEvidencePhrase: "ERROR CODE: Conflict",
				SearchQueryPhrase:       "ERROR CODE: Conflict",
				References: []semanticcontracts.ReferenceRecord{
					{RunURL: "https://prow.example/job/999", OccurredAt: "2026-01-18T12:00:00Z"},
				},
			},
		},
	)
	seedCanonicalWeekMetadata(t, dataDir, "2026-01-18")

	resolver, err := BuildGlobalSignatureResolver(ctx, BuildOptions{
		DataDirectory:                dataDir,
		CurrentSemanticSubdir:        "2026-03-08",
		GlobalSignatureLookbackWeeks: 4,
	})
	if err != nil {
		t.Fatalf("BuildGlobalSignatureResolver returned error: %v", err)
	}

	presence := resolver.PresenceFor(SignatureKey{
		Environment: "dev",
		Phrase:      "ERROR CODE: Conflict",
		SearchQuery: "ERROR CODE: Conflict",
	})
	if presence.PriorWeeksPresent != 2 {
		t.Fatalf("expected PriorWeeksPresent=2, got %d", presence.PriorWeeksPresent)
	}
	if got := presence.PriorWeekStarts; len(got) != 2 || got[0] != "2026-02-22" || got[1] != "2026-03-01" {
		t.Fatalf("unexpected PriorWeekStarts: %#v", got)
	}
	if presence.PriorJobsAffected != 3 {
		t.Fatalf("expected PriorJobsAffected=3 (dedup by run url), got %d", presence.PriorJobsAffected)
	}
	expectedLastSeen := mustParseRFC3339(t, "2026-03-02T12:00:00Z")
	if !presence.PriorLastSeenAt.Equal(expectedLastSeen) {
		t.Fatalf("expected PriorLastSeenAt=%s, got %s", expectedLastSeen, presence.PriorLastSeenAt)
	}
}

func TestBuildGlobalSignatureResolverCollectsPhase3ClusterPresence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()

	seedGlobalClusters(
		t,
		ctx,
		dataDir,
		"2026-03-01",
		[]semanticcontracts.GlobalClusterRecord{
			{
				SchemaVersion:           semanticcontracts.SchemaVersionV1,
				Environment:             "dev",
				Phase2ClusterID:         "cluster-dev-a",
				CanonicalEvidencePhrase: "context deadline exceeded",
				SearchQueryPhrase:       "context deadline exceeded",
				References: []semanticcontracts.ReferenceRecord{
					{RowID: "row-a", RunURL: "https://prow.example/run/a", OccurredAt: "2026-03-01T12:00:00Z"},
					{RowID: "row-b", RunURL: "https://prow.example/run/b", OccurredAt: "2026-03-02T12:00:00Z"},
				},
			},
			{
				SchemaVersion:           semanticcontracts.SchemaVersionV1,
				Environment:             "dev",
				Phase2ClusterID:         "cluster-dev-b",
				CanonicalEvidencePhrase: "context deadline exceeded",
				SearchQueryPhrase:       "context deadline exceeded",
				References: []semanticcontracts.ReferenceRecord{
					{RowID: "row-c", RunURL: "https://prow.example/run/c", OccurredAt: "2026-03-01T18:00:00Z"},
				},
			},
		},
	)
	seedPhase3Links(
		t,
		ctx,
		dataDir,
		"2026-03-01",
		[]semanticcontracts.Phase3LinkRecord{
			{
				SchemaVersion: semanticcontracts.SchemaVersionV1,
				IssueID:       "p3c-shared",
				Environment:   "dev",
				RunURL:        "https://prow.example/run/a",
				RowID:         "row-a",
			},
			{
				SchemaVersion: semanticcontracts.SchemaVersionV1,
				IssueID:       "p3c-shared",
				Environment:   "dev",
				RunURL:        "https://prow.example/run/c",
				RowID:         "row-c",
			},
		},
	)
	seedCanonicalWeekMetadata(t, dataDir, "2026-03-01")

	seedGlobalClusters(
		t,
		ctx,
		dataDir,
		"2026-02-22",
		[]semanticcontracts.GlobalClusterRecord{
			{
				SchemaVersion:           semanticcontracts.SchemaVersionV1,
				Environment:             "dev",
				Phase2ClusterID:         "cluster-dev-c",
				CanonicalEvidencePhrase: "context deadline exceeded",
				SearchQueryPhrase:       "context deadline exceeded",
				References: []semanticcontracts.ReferenceRecord{
					{RowID: "row-d", RunURL: "https://prow.example/run/d", OccurredAt: "2026-02-23T12:00:00Z"},
				},
			},
		},
	)
	seedPhase3Links(
		t,
		ctx,
		dataDir,
		"2026-02-22",
		[]semanticcontracts.Phase3LinkRecord{
			{
				SchemaVersion: semanticcontracts.SchemaVersionV1,
				IssueID:       "p3c-shared",
				Environment:   "dev",
				RunURL:        "https://prow.example/run/d",
				RowID:         "row-d",
			},
		},
	)
	seedCanonicalWeekMetadata(t, dataDir, "2026-02-22")

	resolver, err := BuildGlobalSignatureResolver(ctx, BuildOptions{
		DataDirectory:                dataDir,
		CurrentSemanticSubdir:        "2026-03-08",
		GlobalSignatureLookbackWeeks: 4,
	})
	if err != nil {
		t.Fatalf("BuildGlobalSignatureResolver returned error: %v", err)
	}

	presence := resolver.PresenceForPhase3Cluster("dev", "p3c-shared")
	if presence.PriorWeeksPresent != 2 {
		t.Fatalf("expected PriorWeeksPresent=2, got %d", presence.PriorWeeksPresent)
	}
	if got := presence.PriorWeekStarts; len(got) != 2 || got[0] != "2026-02-22" || got[1] != "2026-03-01" {
		t.Fatalf("unexpected PriorWeekStarts: %#v", got)
	}
	if presence.PriorJobsAffected != 4 {
		t.Fatalf("expected PriorJobsAffected=4, got %d", presence.PriorJobsAffected)
	}
	expectedLastSeen := mustParseRFC3339(t, "2026-03-02T12:00:00Z")
	if !presence.PriorLastSeenAt.Equal(expectedLastSeen) {
		t.Fatalf("expected PriorLastSeenAt=%s, got %s", expectedLastSeen, presence.PriorLastSeenAt)
	}
}

func TestBuildGlobalSignatureResolverReturnsEmptyForNonDateSemanticSubdir(t *testing.T) {
	t.Parallel()

	resolver, err := BuildGlobalSignatureResolver(context.Background(), BuildOptions{
		DataDirectory:                t.TempDir(),
		CurrentSemanticSubdir:        "latest",
		GlobalSignatureLookbackWeeks: 4,
	})
	if err != nil {
		t.Fatalf("BuildGlobalSignatureResolver returned error: %v", err)
	}
	presence := resolver.PresenceFor(SignatureKey{
		Environment: "dev",
		Phrase:      "something",
	})
	if presence.PriorWeeksPresent != 0 || presence.PriorJobsAffected != 0 || len(presence.PriorWeekStarts) != 0 || !presence.PriorLastSeenAt.IsZero() {
		t.Fatalf("expected empty presence for non-date subdir, got %#v", presence)
	}
}

func seedGlobalClusters(
	t *testing.T,
	ctx context.Context,
	dataDir string,
	semanticSubdir string,
	rows []semanticcontracts.GlobalClusterRecord,
) {
	t.Helper()
	store, err := ndjson.NewWithOptions(dataDir, ndjson.Options{
		SemanticSubdirectory: semanticSubdir,
	})
	if err != nil {
		t.Fatalf("new store for semantic subdir %q: %v", semanticSubdir, err)
	}
	if err := store.UpsertGlobalClusters(ctx, rows); err != nil {
		t.Fatalf("upsert global clusters for semantic subdir %q: %v", semanticSubdir, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store for semantic subdir %q: %v", semanticSubdir, err)
	}
}

func seedPhase3Links(
	t *testing.T,
	ctx context.Context,
	dataDir string,
	semanticSubdir string,
	rows []semanticcontracts.Phase3LinkRecord,
) {
	t.Helper()
	store, err := ndjson.NewWithOptions(dataDir, ndjson.Options{
		SemanticSubdirectory: semanticSubdir,
	})
	if err != nil {
		t.Fatalf("new store for semantic subdir %q: %v", semanticSubdir, err)
	}
	if err := store.UpsertPhase3Links(ctx, rows); err != nil {
		t.Fatalf("upsert phase3 links for semantic subdir %q: %v", semanticSubdir, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store for semantic subdir %q: %v", semanticSubdir, err)
	}
}

func mustParseRFC3339(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("parse RFC3339 %q: %v", value, err)
	}
	return parsed.UTC()
}

func mustParseDate(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse("2006-01-02", value)
	if err != nil {
		t.Fatalf("parse date %q: %v", value, err)
	}
	return parsed.UTC()
}

func seedCanonicalWeekMetadata(t *testing.T, dataDir string, weekStart string) {
	t.Helper()
	start := mustParseDate(t, weekStart)
	if err := WriteWindowMetadata(dataDir, weekStart, start, start.AddDate(0, 0, 7)); err != nil {
		t.Fatalf("seed canonical window metadata for %q: %v", weekStart, err)
	}
}

func TestBuildGlobalSignatureResolverFallsBackToSearchQueryWhenPhraseMissing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()
	seedGlobalClusters(
		t,
		ctx,
		dataDir,
		"2026-03-01",
		[]semanticcontracts.GlobalClusterRecord{
			{
				SchemaVersion:           semanticcontracts.SchemaVersionV1,
				Environment:             "dev",
				Phase2ClusterID:         "cluster-query-only",
				CanonicalEvidencePhrase: " ",
				SearchQueryPhrase:       "connection reset by peer",
				References: []semanticcontracts.ReferenceRecord{
					{RunURL: "https://prow.example/job/query-only", OccurredAt: "2026-03-01T00:00:00Z"},
				},
			},
		},
	)
	seedCanonicalWeekMetadata(t, dataDir, "2026-03-01")

	resolver, err := BuildGlobalSignatureResolver(ctx, BuildOptions{
		DataDirectory:         dataDir,
		CurrentSemanticSubdir: "2026-03-08",
	})
	if err != nil {
		t.Fatalf("BuildGlobalSignatureResolver returned error: %v", err)
	}

	presence := resolver.PresenceFor(SignatureKey{
		Environment: "dev",
		Phrase:      "",
		SearchQuery: "connection reset by peer",
	})
	if presence.PriorWeeksPresent != 1 {
		t.Fatalf("expected PriorWeeksPresent=1, got %d", presence.PriorWeeksPresent)
	}
}

func TestBuildGlobalSignatureResolverIgnoresCurrentWeekAndFutureWeeks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()
	seedGlobalClusters(
		t,
		ctx,
		dataDir,
		"2026-03-08",
		[]semanticcontracts.GlobalClusterRecord{
			{
				SchemaVersion:           semanticcontracts.SchemaVersionV1,
				Environment:             "dev",
				Phase2ClusterID:         "cluster-current",
				CanonicalEvidencePhrase: "current week only",
				SearchQueryPhrase:       "current week only",
			},
		},
	)
	seedCanonicalWeekMetadata(t, dataDir, "2026-03-01")
	seedGlobalClusters(
		t,
		ctx,
		dataDir,
		"2026-03-15",
		[]semanticcontracts.GlobalClusterRecord{
			{
				SchemaVersion:           semanticcontracts.SchemaVersionV1,
				Environment:             "dev",
				Phase2ClusterID:         "cluster-future",
				CanonicalEvidencePhrase: "future week only",
				SearchQueryPhrase:       "future week only",
			},
		},
	)

	resolver, err := BuildGlobalSignatureResolver(ctx, BuildOptions{
		DataDirectory:         dataDir,
		CurrentSemanticSubdir: "2026-03-08",
	})
	if err != nil {
		t.Fatalf("BuildGlobalSignatureResolver returned error: %v", err)
	}

	currentPresence := resolver.PresenceFor(SignatureKey{
		Environment: "dev",
		Phrase:      "current week only",
	})
	if currentPresence.PriorWeeksPresent != 0 {
		t.Fatalf("expected current-week signature to have no prior presence, got %d", currentPresence.PriorWeeksPresent)
	}
	futurePresence := resolver.PresenceFor(SignatureKey{
		Environment: "dev",
		Phrase:      "future week only",
	})
	if futurePresence.PriorWeeksPresent != 0 {
		t.Fatalf("expected future-week signature to have no prior presence, got %d", futurePresence.PriorWeeksPresent)
	}
}

func TestBuildGlobalSignatureResolverWithEmptyInputReturnsNoPresence(t *testing.T) {
	t.Parallel()

	resolver, err := BuildGlobalSignatureResolver(context.Background(), BuildOptions{})
	if err != nil {
		t.Fatalf("BuildGlobalSignatureResolver returned error: %v", err)
	}
	presence := resolver.PresenceFor(SignatureKey{
		Environment: "dev",
		Phrase:      "whatever",
	})
	if presence.PriorWeeksPresent != 0 || presence.PriorJobsAffected != 0 || len(presence.PriorWeekStarts) != 0 || !presence.PriorLastSeenAt.IsZero() {
		t.Fatalf("expected empty presence for empty inputs, got %#v", presence)
	}
}

func TestBuildGlobalSignatureResolverWithPathLikeSubdirStillHandlesDateDirs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()
	seedGlobalClusters(
		t,
		ctx,
		dataDir,
		filepath.Join("2026-03-01"),
		[]semanticcontracts.GlobalClusterRecord{
			{
				SchemaVersion:           semanticcontracts.SchemaVersionV1,
				Environment:             "dev",
				Phase2ClusterID:         "cluster",
				CanonicalEvidencePhrase: "phrase",
				SearchQueryPhrase:       "phrase",
			},
		},
	)
	seedCanonicalWeekMetadata(t, dataDir, "2026-03-01")

	resolver, err := BuildGlobalSignatureResolver(ctx, BuildOptions{
		DataDirectory:         dataDir,
		CurrentSemanticSubdir: "2026-03-08",
	})
	if err != nil {
		t.Fatalf("BuildGlobalSignatureResolver returned error: %v", err)
	}
	presence := resolver.PresenceFor(SignatureKey{
		Environment: "dev",
		Phrase:      "phrase",
	})
	if presence.PriorWeeksPresent != 1 {
		t.Fatalf("expected PriorWeeksPresent=1, got %d", presence.PriorWeeksPresent)
	}
}

func TestBuildGlobalSignatureResolverSkipsNonCanonicalWindowMetadata(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dataDir := t.TempDir()
	seedGlobalClusters(
		t,
		ctx,
		dataDir,
		"2026-03-01",
		[]semanticcontracts.GlobalClusterRecord{
			{
				SchemaVersion:           semanticcontracts.SchemaVersionV1,
				Environment:             "dev",
				Phase2ClusterID:         "cluster-short-window",
				CanonicalEvidencePhrase: "short window phrase",
				SearchQueryPhrase:       "short window phrase",
			},
		},
	)

	start := mustParseDate(t, "2026-03-01")
	if err := WriteWindowMetadata(dataDir, "2026-03-01", start, start.AddDate(0, 0, 3)); err != nil {
		t.Fatalf("write non-canonical window metadata: %v", err)
	}

	resolver, err := BuildGlobalSignatureResolver(ctx, BuildOptions{
		DataDirectory:         dataDir,
		CurrentSemanticSubdir: "2026-03-08",
	})
	if err != nil {
		t.Fatalf("BuildGlobalSignatureResolver returned error: %v", err)
	}
	presence := resolver.PresenceFor(SignatureKey{
		Environment: "dev",
		Phrase:      "short window phrase",
	})
	if presence.PriorWeeksPresent != 0 {
		t.Fatalf("expected non-canonical week metadata to be skipped, got PriorWeeksPresent=%d", presence.PriorWeeksPresent)
	}
}
