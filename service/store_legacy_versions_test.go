package workflowservice

import (
	"context"
	"path/filepath"
	"testing"

	"gochen/errors"
	runtime "gochen/process/workflow"
)

func TestLegacyScopeMigrationMovesDefinitionHistory(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLiteStore(ctx, filepath.Join(t.TempDir(), "legacy-versions.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	engine := runtime.NewEngine(store)
	v1 := &runtime.Definition{ID: "legacy", Name: "Version 1", Version: 1, StartNodeID: "first", Nodes: []runtime.Node{{ID: "first"}}}
	if err := engine.SaveDefinition(ctx, v1); err != nil {
		t.Fatal(err)
	}
	if err := engine.CreateInstance(ctx, "pinned", v1.ID, runtime.WithDefinitionVersion(1)); err != nil {
		t.Fatal(err)
	}
	target := Scope{Kind: ScopeKindTenant, ID: 77}
	migrated, err := NewSQLiteStore(ctx, store.db, WithLegacyDataScope(target))
	if err != nil {
		t.Fatal(err)
	}
	tenantCtx := scopedContext(t, target.ID)
	engine = runtime.NewEngine(migrated)
	v2 := &runtime.Definition{ID: v1.ID, Name: "Version 2", Version: 2, StartNodeID: "second", Nodes: []runtime.Node{{ID: "second"}}}
	if err := engine.SaveDefinition(tenantCtx, v2); err != nil {
		t.Fatal(err)
	}
	got, err := migrated.GetDefinition(tenantCtx, v1.ID, 1)
	if err != nil || got == nil || got.StartNodeID != "first" {
		t.Fatalf("migrated version 1: %+v, %v", got, err)
	}
	if err := engine.StartInstance(tenantCtx, "pinned"); err != nil {
		t.Fatalf("instance lost its pinned definition: %v", err)
	}
	if err := migrated.Migrate(ctx); err != nil {
		t.Fatalf("scope migration is not idempotent: %v", err)
	}
	var count int
	if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM workflow_definition_versions WHERE managed_scope_kind = 'platform' AND managed_scope_id = 0").Scan(&count); err != nil || count != 0 {
		t.Fatalf("legacy versions remain: %d, %v", count, err)
	}
}

func TestLegacyScopeMigrationRejectsVersionCollisionAtomically(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLiteStore(ctx, filepath.Join(t.TempDir(), "version-collision.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	def := &runtime.Definition{ID: "legacy", Name: "Version 1", Version: 1, StartNodeID: "task", Nodes: []runtime.Node{{ID: "task"}}}
	if err := runtime.NewEngine(store).SaveDefinition(ctx, def); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `INSERT INTO workflow_definition_versions
		(managed_scope_kind, managed_scope_id, id, version, runtime_definition_json, graph_json, created_at)
		SELECT 'tenant', 77, id, version, runtime_definition_json, graph_json, created_at FROM workflow_definition_versions`); err != nil {
		t.Fatal(err)
	}
	if _, err := NewSQLiteStore(ctx, store.db, WithLegacyDataScope(Scope{Kind: ScopeKindTenant, ID: 77})); !errors.Is(err, errors.Conflict) {
		t.Fatalf("version collision returned %v", err)
	}
	var count int
	if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM workflow_definitions WHERE managed_scope_kind = 'platform' AND managed_scope_id = 0").Scan(&count); err != nil || count != 1 {
		t.Fatalf("failed migration moved definitions: count=%d, err=%v", count, err)
	}
}
