package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	hostauthz "gochen-runtime/host/authz"
	runtimehttp "gochen-runtime/http"
	"gochen-runtime/http/nethttp"
	workflowpermissions "gochen-workflow/permissions"
	workflowservice "gochen-workflow/service"
	workflowtypes "gochen-workflow/types"
	"gochen/auth"
	"gochen/contextx"
	"gochen/errors"
)

func declaredWorkflowContext(t *testing.T, user, tenant string, scopeID int64, permissions ...string) context.Context {
	t.Helper()
	ctx, err := contextx.WithTenantID(t.Context(), tenant)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err = hostauthz.WithPrincipal(ctx, hostauthz.Principal{SubjectID: user, ActiveScopeID: scopeID, Permissions: permissions, PermissionScopes: []string{workflowservice.ScopeKindTenant}})
	if err != nil {
		t.Fatal(err)
	}
	return hostauthz.BindActiveScopeContext(ctx, scopeID, workflowservice.ScopeKindTenant)
}

func TestDeclaredWorkflowActualRoutesEnforcePartitionPermissionsAndActor(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	registrar := NewRegistrar(service)
	server := nethttp.NewServer(nil)
	registry := hostauthz.NewEntryRegistry()
	group, err := runtimehttp.WithEntryRegistry(server.Group("/workflow"), registry, "workflow")
	if err != nil {
		t.Fatal(err)
	}
	if err := registrar.RegisterRoutes(group); err != nil {
		t.Fatal(err)
	}
	if err := hostauthz.NewEntryRegistry().ValidateEntries(t.Context(), server); err == nil {
		t.Fatal("actual workflow routes without their execution bindings passed startup validation")
	}
	if err := registry.ValidateEntries(t.Context(), server); err != nil {
		t.Fatal(err)
	}
	declared := 0
	for _, entry := range registry.Report() {
		if entry.Class == auth.EntryDeclared {
			declared++
			if entry.Plan.Descriptor.Resource != workflowResource || !strings.Contains(entry.ConditionPath, "WithStrictScope") {
				t.Fatalf("unexpected workflow binding: %+v", entry)
			}
		} else if entry.Class != auth.EntryAllowed {
			t.Fatalf("invalid workflow entry: %+v", entry)
		}
	}
	if declared < 27 { // HTTP drivers may also expose bound HEAD aliases.
		t.Fatalf("only %d declared entries for the complete workflow API", declared)
	}
	if err := registry.ActivateEntries(); err != nil {
		t.Fatal(err)
	}

	all := []string{workflowpermissions.WorkflowReadCode, workflowpermissions.WorkflowDesignCode, workflowpermissions.WorkflowRunCode}
	owner := declaredWorkflowContext(t, "1001", "tenant-a", 1, all...)
	otherScope := declaredWorkflowContext(t, "1001", "tenant-b", 2, all...)
	otherActor := declaredWorkflowContext(t, "3003", "tenant-a", 1, all...)
	readOnly := declaredWorkflowContext(t, "1001", "tenant-a", 1, workflowpermissions.WorkflowReadCode)
	request := func(ctx context.Context, method, path, body string, status int) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, "/workflow"+path, strings.NewReader(body)).WithContext(ctx)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		server.Handler().ServeHTTP(rec, req)
		if rec.Code != status {
			t.Fatalf("%s %s: status=%d want=%d body=%s", method, path, rec.Code, status, rec.Body.String())
		}
		return rec
	}
	payload := `{"id":"declared","name":"Declared","start_node_id":"start","nodes":[{"id":"start","name":"Start","next":["end"]},{"id":"end","name":"End"}],"edges":[{"id":"edge","source":"start","target":"end"}]}`
	request(readOnly, "POST", "/definitions", payload, http.StatusForbidden)
	checkCtx := workflowservice.WithScope(owner, workflowservice.Scope{Kind: "tenant", ID: 1})
	if _, err := service.GetDefinition(checkCtx, "declared"); !errors.Is(err, errors.NotFound) {
		t.Fatalf("permission rejection wrote definition: %v", err)
	}
	request(owner, "POST", "/definitions", payload, http.StatusCreated)
	request(owner, "POST", "/definitions/declared/publish", "", http.StatusOK)
	request(otherScope, "GET", "/definitions/declared", "", http.StatusNotFound)
	request(otherScope, "DELETE", "/definitions/declared", "", http.StatusNotFound)
	if _, err := service.GetDefinition(checkCtx, "declared"); err != nil {
		t.Fatalf("cross-scope deletion changed definition: %v", err)
	}
	request(otherScope, "POST", "/instances", `{"id":"foreign","definition_id":"declared"}`, http.StatusNotFound)
	foreignCtx := workflowservice.WithScope(otherScope, workflowservice.Scope{Kind: "tenant", ID: 2})
	if instances, err := service.ListInstances(foreignCtx, workflowtypes.InstanceQuery{}); err != nil || instances.Total != 0 {
		t.Fatalf("cross-scope creation changed instances: %+v, %v", instances, err)
	}
	request(owner, "POST", "/instances", `{"id":"instance","definition_id":"declared","assignee_id":"1001","initiator_id":"forged"}`, http.StatusCreated)
	request(owner, "POST", "/instances/instance/start", "", http.StatusOK)
	request(otherActor, "POST", "/instances/instance/nodes/start/complete", `{}`, http.StatusForbidden)
	state, err := service.GetInstance(checkCtx, "instance")
	if err != nil || len(state.ActiveNodeIDs) != 1 || state.ActiveNodeIDs[0] != "start" || len(state.CompletedNodeIDs) != 0 || workflowtypes.InstanceStateFromRuntime(state).InitiatorID != "1001" {
		t.Fatalf("wrong actor changed instance or forged initiator accepted: %+v, %v", state, err)
	}
	request(owner, "POST", "/instances/instance/nodes/start/complete", `{}`, http.StatusOK)

	changedTenant, err := contextx.WithTenantID(owner, "tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	request(changedTenant, "DELETE", "/definitions/declared", "", http.StatusForbidden)
	if _, err := service.GetDefinition(checkCtx, "declared"); err != nil {
		t.Fatalf("stale tenant binding changed definition: %v", err)
	}
}
