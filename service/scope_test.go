package workflowservice

import (
	"context"
	"path/filepath"
	"testing"

	"gochen/errors"
)

func TestCurrentManagedScopeRejectsNilContext(t *testing.T) {
	if _, err := currentManagedScope(nil); err == nil || !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected nil context to return InvalidInput, got %v", err)
	}
}

func TestStoresRejectNilContextForScopeAwareOperations(t *testing.T) {
	ctx := context.Background()
	stores := []struct {
		name  string
		store Store
	}{
		{name: "memory", store: NewMemoryStore()},
		{
			name: "sqlite",
			store: func() Store {
				store, err := OpenSQLiteStore(ctx, filepath.Join(t.TempDir(), "workflow.db"))
				if err != nil {
					t.Fatalf("open sqlite store: %v", err)
				}
				return store
			}(),
		},
	}

	for _, tt := range stores {
		t.Run(tt.name, func(t *testing.T) {
			if closer, ok := tt.store.(interface{ Close() error }); ok {
				t.Cleanup(func() { _ = closer.Close() })
			}
			if _, err := tt.store.ListDesigns(nil); err == nil || !errors.Is(err, errors.InvalidInput) {
				t.Fatalf("expected nil context list designs to return InvalidInput, got %v", err)
			}
			if _, err := tt.store.ListInstances(nil); err == nil || !errors.Is(err, errors.InvalidInput) {
				t.Fatalf("expected nil context list instances to return InvalidInput, got %v", err)
			}
			if _, err := tt.store.ListStartedInstances(nil); err == nil || !errors.Is(err, errors.InvalidInput) {
				t.Fatalf("expected nil context list started instances to return InvalidInput, got %v", err)
			}
		})
	}
}

func TestOpenSQLiteStoreRejectsNilContext(t *testing.T) {
	if _, err := OpenSQLiteStore(nil, filepath.Join(t.TempDir(), "workflow.db")); err == nil || !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected nil context open to return InvalidInput, got %v", err)
	}
}

func TestStrictScopeRequiresExplicitScope(t *testing.T) {
	service := newTestService(NewMemoryStore())
	if _, err := service.ListDefinitions(WithStrictScope(context.Background())); err == nil || !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected strict scope without explicit scope to return InvalidInput, got %v", err)
	}
	ctx := WithStrictScope(WithScope(context.Background(), Scope{Kind: ScopeKindPlatform}))
	if _, err := service.ListDefinitions(ctx); err != nil {
		t.Fatalf("expected explicit platform scope to satisfy strict scope, got %v", err)
	}
}
