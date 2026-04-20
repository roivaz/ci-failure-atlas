package controllers

import (
	"context"
	"testing"
	"time"

	"ci-failure-atlas/pkg/store/contracts"
)

func TestShouldWriteMissingArtifactMarkerWaitsForRetryWindow(t *testing.T) {
	t.Parallel()

	store := &fakeCheckpointStore{
		checkpoints: map[string]contracts.CheckpointRecord{},
	}
	environment := "dev"
	runURL := "https://prow.ci.openshift.org/view/gs/test-platform-results/pr-logs/pull/Azure_ARO-HCP/4313/pull-ci-Azure-ARO-HCP-main-e2e-parallel/2029578186907455488"
	retryWindow := 10 * time.Minute
	start := mustParseRFC3339(t, "2026-04-20T10:00:00Z")

	shouldWrite, err := shouldWriteMissingArtifactMarker(context.Background(), store, retryWindow, environment, runURL, start)
	if err != nil {
		t.Fatalf("first retry decision returned error: %v", err)
	}
	if shouldWrite {
		t.Fatalf("expected first empty artifact fetch to defer marker write")
	}

	checkpointName := artifactRetryCheckpointName(environment, runURL)
	checkpoint, found, err := store.GetCheckpoint(context.Background(), checkpointName)
	if err != nil {
		t.Fatalf("get checkpoint after first decision: %v", err)
	}
	if !found {
		t.Fatalf("expected retry checkpoint to be stored")
	}
	if checkpoint.Value != start.Format(time.RFC3339Nano) {
		t.Fatalf("unexpected first-seen checkpoint value: got=%q want=%q", checkpoint.Value, start.Format(time.RFC3339Nano))
	}

	shouldWrite, err = shouldWriteMissingArtifactMarker(context.Background(), store, retryWindow, environment, runURL, start.Add(5*time.Minute))
	if err != nil {
		t.Fatalf("second retry decision returned error: %v", err)
	}
	if shouldWrite {
		t.Fatalf("expected retry window to keep deferring marker write")
	}

	shouldWrite, err = shouldWriteMissingArtifactMarker(context.Background(), store, retryWindow, environment, runURL, start.Add(11*time.Minute))
	if err != nil {
		t.Fatalf("third retry decision returned error: %v", err)
	}
	if !shouldWrite {
		t.Fatalf("expected marker write once retry window has elapsed")
	}
}

type fakeCheckpointStore struct {
	checkpoints map[string]contracts.CheckpointRecord
}

func (f *fakeCheckpointStore) UpsertCheckpoints(_ context.Context, rows []contracts.CheckpointRecord) error {
	for _, row := range rows {
		f.checkpoints[row.Name] = row
	}
	return nil
}

func (f *fakeCheckpointStore) GetCheckpoint(_ context.Context, name string) (contracts.CheckpointRecord, bool, error) {
	row, found := f.checkpoints[name]
	return row, found, nil
}
