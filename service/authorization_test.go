package workflowservice

import (
	"context"
	"path/filepath"
	"testing"

	hostauthz "gochen-runtime/host/authz"
	"gochen-workflow/permissions"
	workflowtypes "gochen-workflow/types"
	"gochen/errors"
)

func workflowTestContext() context.Context {
	ctx, err := hostauthz.WithPrincipal(context.Background(), hostauthz.Principal{SubjectID: "system", IsSystem: true})
	if err != nil {
		panic(err)
	}
	return WithStrictScope(WithScope(ctx, Scope{Kind: ScopeKindPlatform, ID: 1}))
}

func ordinaryServiceContext(t *testing.T, permission string, scopeID int64) context.Context {
	t.Helper()
	ctx, err := hostauthz.WithPrincipal(context.Background(), hostauthz.Principal{SubjectID: "reader", ActiveScopeID: scopeID,
		Permissions: []string{permission}, PermissionScopes: []string{ScopeKindTenant}})
	if err != nil {
		t.Fatal(err)
	}
	ctx = hostauthz.BindActiveScopeContext(ctx, scopeID, ScopeKindTenant)
	return WithStrictScope(WithScope(ctx, Scope{Kind: ScopeKindTenant, ID: scopeID}))
}

func TestServiceRejectsBeforeAnyStoreAccess(t *testing.T) {
	// An unimplemented Store panics on access, proving every rejected path stops before IO.
	s := NewService(&systemExecutionProbe{}, nil, nil)
	for _, ctx := range []context.Context{context.Background(), ordinaryServiceContext(t, "other:api:read", 3),
		WithScope(ordinaryServiceContext(t, "workflow:api:*", 3), Scope{Kind: ScopeKindTenant, ID: 4}),
		WithScope(ordinaryServiceContext(t, "workflow:api:*", 3), Scope{Kind: ScopeKindPlatform, ID: 3})} {
		for _, call := range []func() error{
			func() error { _, e := s.ListDefinitions(ctx); return e },
			func() error { _, e := s.GetDefinition(ctx, "x"); return e },
			func() error { _, e := s.SaveDefinition(ctx, sampleDesign()); return e },
			func() error { return s.DeleteDefinition(ctx, "x") },
			func() error { _, e := s.PublishDefinition(ctx, "x"); return e },
			func() error { _, e := s.ArchiveDefinition(ctx, "x"); return e },
			func() error { _, e := s.CreateInstance(ctx, "x", "x", nil); return e },
			func() error { _, e := s.StartInstance(ctx, "x"); return e },
			func() error { _, e := s.GetInstance(ctx, "x"); return e },
			func() error { _, e := s.ClaimTask(ctx, "x", "start", workflowtypes.TaskActionInput{}); return e },
			func() error { _, e := s.ListInstances(ctx, workflowtypes.InstanceQuery{}); return e },
		} {
			if err := call(); err == nil {
				t.Fatal("unauthorized service call accepted")
			}
		}
	}
}

func TestServiceSeparatesReadDesignAndRunPermissions(t *testing.T) {
	for _, driver := range []string{"memory", "sqlite"} {
		t.Run(driver, func(t *testing.T) {
			var store Store = NewMemoryStore()
			if driver == "sqlite" {
				sqlite, err := OpenSQLiteStore(t.Context(), filepath.Join(t.TempDir(), "authorization.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = sqlite.Close() })
				store = sqlite
			}
			s := NewService(store, nil, nil)
			design := ordinaryServiceContext(t, permissions.WorkflowDesignCode, 3)
			run := ordinaryServiceContext(t, permissions.WorkflowRunCode, 3)
			read := ordinaryServiceContext(t, permissions.WorkflowReadCode, 3)
			definition, err := s.SaveDefinition(design, sampleDesign())
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.PublishDefinition(design, definition.ID); err != nil {
				t.Fatal(err)
			}
			if _, err = s.GetDefinition(design, definition.ID); !errors.Is(err, errors.Forbidden) {
				t.Fatalf("design permission read: %v", err)
			}
			instance, err := s.CreateInstance(run, "ordinary", definition.ID, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.StartInstance(run, instance.ID); err != nil {
				t.Fatal(err)
			}
			if _, err = s.GetInstance(read, instance.ID); err != nil {
				t.Fatal(err)
			}
			if _, err = s.GetInstance(ordinaryServiceContext(t, permissions.WorkflowReadCode, 4), instance.ID); !errors.Is(err, errors.NotFound) {
				t.Fatalf("cross-scope read: %v", err)
			}
			if _, err = s.StartInstance(read, instance.ID); !errors.Is(err, errors.Forbidden) {
				t.Fatalf("read permission write: %v", err)
			}
			if _, err = s.CreateInstanceWithOptions(run, workflowtypes.CreateInstanceInput{ID: "forged-initiator", DefinitionID: definition.ID, InitiatorID: "other"}); !errors.Is(err, errors.Forbidden) {
				t.Fatalf("forged initiator: %v", err)
			}
		})
	}
}
