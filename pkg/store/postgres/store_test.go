package postgres

import (
	"context"
	"errors"
	"testing"
)

func TestNewRequiresPool(t *testing.T) {
	t.Parallel()

	if _, err := New(nil, Options{}); err == nil {
		t.Fatalf("expected error when creating postgres store with nil pool")
	}
}

func TestNotImplementedMethodsReturnSentinelError(t *testing.T) {
	t.Parallel()

	store := &Store{}
	err := store.UpsertPhase1Normalized(context.Background(), nil)
	if !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("expected ErrNotImplemented, got %v", err)
	}
}

func TestMethodsRequireContext(t *testing.T) {
	t.Parallel()

	store := &Store{}
	if err := store.UpsertPhase1Normalized(nil, nil); err == nil {
		t.Fatalf("expected context validation error")
	}
}
