package workflowservice

import (
	"context"
	"testing"

	workflowtypes "gochen-workflow/types"
	"gochen/auth"
	"gochen/contextx"
)

type systemExecutionProbe struct {
	Store
	reads int
}

func (s *systemExecutionProbe) GetDesign(ctx context.Context, id string) (*workflowtypes.DesignerDefinition, error) {
	s.reads++
	return s.Store.GetDesign(ctx, id)
}
func TestSystemExecutionRejectsBeforeStoreAccess(t *testing.T) {
	store := &systemExecutionProbe{Store: NewMemoryStore()}
	execution, err := NewSystemExecution(NewService(store, nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	tenant, _ := contextx.WithTenantID(context.Background(), "tenant-a")
	system, _ := auth.WithPrincipal(tenant, auth.Principal{System: true})
	ordinary, _ := auth.WithPrincipal(tenant, auth.Principal{SubjectID: "ordinary"})
	for _, ctx := range []context.Context{context.Background(), WithStrictScope(WithScope(ordinary, Scope{Kind: ScopeKindTenant, ID: 3})), WithScope(system, Scope{Kind: ScopeKindTenant, ID: 3}), WithStrictScope(WithScope(system, Scope{Kind: ScopeKindPlatform})), WithStrictScope(WithScope(auth.AsSystem(context.Background(), "test"), Scope{Kind: ScopeKindTenant, ID: 3}))} {
		if _, err = execution.GetDefinition(ctx, "missing"); err == nil {
			t.Fatal("invalid context accepted")
		}
	}
	if store.reads != 0 {
		t.Fatalf("rejected calls reached store: %d", store.reads)
	}
	ctx := WithStrictScope(WithScope(system, Scope{Kind: ScopeKindTenant, ID: 3}))
	if _, err = execution.GetDefinition(ctx, "missing"); err == nil {
		t.Fatal("missing design accepted")
	}
	if store.reads != 1 {
		t.Fatalf("authorized scope did not execute real service: %d", store.reads)
	}
	binding, err := execution.AuthorizationBinding("get_definition")
	if err != nil {
		t.Fatal(err)
	}
	if binding.Plan == nil || binding.Plan.Validate() != nil {
		t.Fatal("missing real compiled plan")
	}
}
