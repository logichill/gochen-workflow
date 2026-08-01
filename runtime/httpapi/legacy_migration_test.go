package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	workflowservice "gochen-workflow/service"
	workflowtypes "gochen-workflow/types"

	_ "modernc.org/sqlite"
)

func TestLegacyScopeMigrationMakesDataReachableOnlyInExplicitHTTPScope(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "workflow.db")
	ctx := context.Background()
	legacyStore, err := workflowservice.OpenSQLiteStore(ctx, dbPath)
	if err != nil {
		t.Fatalf("open legacy store: %v", err)
	}
	legacyService := workflowservice.NewService(legacyStore, nil, nil)
	design := &workflowtypes.DesignerDefinition{
		ID:          "legacy-http-definition",
		Name:        "Legacy HTTP Definition",
		StartNodeID: "start",
		Nodes: []workflowtypes.DesignerNode{
			{ID: "start", Name: "Start", Type: workflowtypes.NodeKindTask, Next: []string{"end"}},
			{ID: "end", Name: "End", Type: workflowtypes.NodeKindTask},
		},
		Edges: []workflowtypes.DesignerEdge{{ID: "legacy-edge", Source: "start", Target: "end"}},
	}
	saved, err := legacyService.SaveDefinition(ctx, design)
	if err != nil {
		_ = legacyStore.Close()
		t.Fatalf("save legacy definition: %v", err)
	}
	if _, err := legacyService.PublishDefinition(ctx, saved.ID); err != nil {
		_ = legacyStore.Close()
		t.Fatalf("publish legacy definition: %v", err)
	}
	if _, err := legacyService.CreateInstance(ctx, "legacy-http-instance", saved.ID, nil); err != nil {
		_ = legacyStore.Close()
		t.Fatalf("create legacy instance: %v", err)
	}
	if err := legacyStore.Close(); err != nil {
		t.Fatalf("close legacy store: %v", err)
	}

	store, err := workflowservice.OpenSQLiteStore(ctx, dbPath,
		workflowservice.WithLegacyDataScope(workflowservice.Scope{Kind: workflowservice.ScopeKindTenant, ID: 77}))
	if err != nil {
		t.Fatalf("migrate legacy scope: %v", err)
	}
	defer store.Close()
	handlers := registerTestRoutes(t, NewRegistrar(workflowservice.NewService(store, nil, nil)))

	definitionReq := httptest.NewRequest(http.MethodGet, "/", nil)
	definitionReq = definitionReq.WithContext(ctx)
	definitionResp := doRequestWithScope(t, handlers["GET /definitions/:id"], map[string]string{"id": saved.ID}, definitionReq, 77, workflowservice.ScopeKindTenant)
	if definitionResp.Code != http.StatusOK {
		t.Fatalf("definition status = %d body=%s", definitionResp.Code, definitionResp.Body.String())
	}

	otherReq := httptest.NewRequest(http.MethodGet, "/", nil)
	otherResp := doRequestWithScope(t, handlers["GET /definitions/:id"], map[string]string{"id": saved.ID}, otherReq, 88, workflowservice.ScopeKindTenant)
	if otherResp.Code != http.StatusNotFound {
		t.Fatalf("other scope status = %d body=%s", otherResp.Code, otherResp.Body.String())
	}

	instanceReq := httptest.NewRequest(http.MethodGet, "/", nil)
	instanceResp := doRequestWithScope(t, handlers["GET /instances/:id"], map[string]string{"id": "legacy-http-instance"}, instanceReq, 77, workflowservice.ScopeKindTenant)
	if instanceResp.Code != http.StatusOK {
		t.Fatalf("instance status = %d body=%s", instanceResp.Code, instanceResp.Body.String())
	}
}
