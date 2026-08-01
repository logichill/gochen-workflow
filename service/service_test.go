package workflowservice

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	workflowtypes "gochen-workflow/types"

	"gochen/auth"
	"gochen/errors"
	runtime "gochen/process/workflow"
)

func TestServiceSavesPublishesAndRunsGraphWorkflow(t *testing.T) {
	ctx := context.Background()
	service := newTestService(NewMemoryStore())

	saved, err := service.SaveDefinition(ctx, sampleDesign())
	if err != nil {
		t.Fatalf("save definition: %v", err)
	}
	if saved.ID != "onboarding" || saved.RuntimeDefinition == nil {
		t.Fatalf("unexpected saved definition: %#v", saved)
	}
	if saved.Description != "Employee entry approval and setup flow" {
		t.Fatalf("description was not kept: %q", saved.Description)
	}
	if len(saved.RuntimeDefinition.Nodes) != 5 {
		t.Fatalf("runtime node count = %d", len(saved.RuntimeDefinition.Nodes))
	}
	if got := saved.Nodes[0].Next; len(got) != 1 || got[0] != "decision" {
		t.Fatalf("saved frontend next = %#v", got)
	}
	if got := saved.RuntimeDefinition.Nodes[0].Next; len(got) != 1 || got[0] != "decision" {
		t.Fatalf("runtime next = %#v", got)
	}
	if saved.Nodes[1].Type != workflowtypes.NodeKindBranch || saved.RuntimeDefinition.Nodes[1].Type != workflowtypes.NodeKindBranch {
		t.Fatalf("branch node type was not preserved: design=%s runtime=%s", saved.Nodes[1].Type, saved.RuntimeDefinition.Nodes[1].Type)
	}

	published, err := service.PublishDefinition(ctx, saved.ID)
	if err != nil {
		t.Fatalf("publish definition: %v", err)
	}
	if published.Status != workflowtypes.StatusPublished || published.PublishedAt == nil {
		t.Fatalf("definition was not published: %#v", published)
	}
	created, err := service.CreateInstance(ctx, "instance-1", saved.ID, map[string]any{"request_id": "REQ-1"})
	if err != nil {
		t.Fatalf("create instance: %v", err)
	}
	if created.Status != runtime.InstanceStatusPending || created.Data["request_id"] != "REQ-1" {
		t.Fatalf("unexpected created instance: %#v", created)
	}
	archived, err := service.ArchiveDefinition(ctx, saved.ID)
	if err != nil {
		t.Fatalf("archive definition: %v", err)
	}
	if archived.Status != workflowtypes.StatusArchived {
		t.Fatalf("definition was not archived: %#v", archived)
	}
	if _, err := service.CreateInstance(ctx, "instance-archived", saved.ID, nil); err == nil {
		t.Fatal("expected archived definition to reject instance creation")
	}

	started, err := service.StartInstance(ctx, created.ID)
	if err != nil {
		t.Fatalf("start instance: %v", err)
	}
	if got := started.ActiveNodeIDs; len(got) != 1 || got[0] != "start" {
		t.Fatalf("active after start = %#v", got)
	}

	state, err := service.AdvanceNode(ctx, created.ID, "start")
	if err != nil {
		t.Fatalf("advance start: %v", err)
	}
	if got := state.ActiveNodeIDs; len(got) != 1 || got[0] != "decision" {
		t.Fatalf("active after start advance = %#v", got)
	}

	state, err = service.AdvanceNode(ctx, created.ID, "decision")
	if err != nil {
		t.Fatalf("advance decision: %v", err)
	}
	if got := state.ActiveNodeIDs; len(got) != 2 {
		t.Fatalf("active after branch activation = %#v", got)
	}
}

func TestServiceRejectsPublishedDefinitionMutationAndDelete(t *testing.T) {
	ctx := context.Background()
	service := newTestService(NewMemoryStore())

	saved, err := service.SaveDefinition(ctx, sampleDesign())
	if err != nil {
		t.Fatalf("save definition: %v", err)
	}
	published, err := service.PublishDefinition(ctx, saved.ID)
	if err != nil {
		t.Fatalf("publish definition: %v", err)
	}

	published.Name = "mutated"
	if _, err := service.SaveDefinition(ctx, published); err == nil || !errors.Is(err, errors.Conflict) {
		t.Fatalf("expected immutable published definition conflict, got %v", err)
	}
	if err := service.DeleteDefinition(ctx, published.ID); err == nil || !errors.Is(err, errors.Conflict) {
		t.Fatalf("expected published delete conflict, got %v", err)
	}
}

func TestServiceDeleteMissingDefinitionReturnsNotFound(t *testing.T) {
	ctx := context.Background()
	service := newTestService(NewMemoryStore())

	err := service.DeleteDefinition(ctx, "missing")
	if err == nil || !errors.Is(err, errors.NotFound) {
		t.Fatalf("expected missing definition to return NotFound, got %v", err)
	}
}

func TestServiceRejectsEmptyDefinitionID(t *testing.T) {
	ctx := context.Background()
	service := newTestService(NewMemoryStore())

	if _, err := service.GetDefinition(ctx, " "); err == nil || !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected empty get definition id to return InvalidInput, got %v", err)
	}
	if _, err := service.CreateInstance(ctx, "instance-empty-definition", " ", nil); err == nil || !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected empty create instance definition id to return InvalidInput, got %v", err)
	}
}

func TestServiceRejectsEmptyInstanceID(t *testing.T) {
	ctx := context.Background()
	service := newTestService(NewMemoryStore())

	if _, err := service.GetInstance(ctx, " "); err == nil || !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected empty get instance id to return InvalidInput, got %v", err)
	}
	if _, err := service.StartInstance(ctx, " "); err == nil || !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected empty start instance id to return InvalidInput, got %v", err)
	}
	if _, err := service.AdvanceNode(ctx, " ", "start"); err == nil || !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected empty advance instance id to return InvalidInput, got %v", err)
	}
	if _, err := service.AdvanceNodeTo(ctx, " ", "start", "end"); err == nil || !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected empty selected advance instance id to return InvalidInput, got %v", err)
	}
	if _, err := service.AdvanceNode(ctx, "instance", " "); err == nil || !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected empty advance node id to return InvalidInput, got %v", err)
	}
	if _, err := service.AdvanceNodeTo(ctx, "instance", " ", "end"); err == nil || !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected empty selected advance node id to return InvalidInput, got %v", err)
	}
	if _, err := service.AdvanceNodeTo(ctx, "instance", "start", " "); err == nil || !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected empty selected next node id to return InvalidInput, got %v", err)
	}
}

func TestServiceRejectsNilContextAndStore(t *testing.T) {
	service := newTestService(NewMemoryStore())
	if _, err := service.ListDefinitions(nil); err == nil || !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected nil context to return InvalidInput, got %v", err)
	}

	misconfigured := NewService(nil, nil, nil)
	if _, err := misconfigured.ListDefinitions(context.Background()); err == nil || !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected nil store to return InvalidInput, got %v", err)
	}
}

func TestServiceListDefinitionsSortsByUpdatedAtThenIDAndClonesResults(t *testing.T) {
	ctx := context.Background()
	updatedAt := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	store := &listOnlyStore{designs: []*workflowtypes.DesignerDefinition{
		{ID: "b", UpdatedAt: updatedAt},
		{ID: "a", UpdatedAt: updatedAt},
		{ID: "c", UpdatedAt: updatedAt.Add(-time.Hour)},
	}}
	service := newTestService(store)

	items, err := service.ListDefinitions(ctx)
	if err != nil {
		t.Fatalf("list definitions: %v", err)
	}
	got := []string{items[0].ID, items[1].ID, items[2].ID}
	want := []string{"a", "b", "c"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("definition order = %#v, want %#v", got, want)
		}
	}

	items[0].ID = "mutated"
	items, err = service.ListDefinitions(ctx)
	if err != nil {
		t.Fatalf("list definitions again: %v", err)
	}
	if items[0].ID != "a" {
		t.Fatalf("listed definition shared storage state, got first id %q", items[0].ID)
	}
}

func TestServiceBuildDefinitionListResponseCountsStatuses(t *testing.T) {
	ctx := context.Background()
	store := &listOnlyStore{designs: []*workflowtypes.DesignerDefinition{
		{ID: "draft", Status: workflowtypes.StatusDraft},
		{ID: "published", Status: workflowtypes.StatusPublished},
		{ID: "archived", Status: workflowtypes.StatusArchived},
	}}
	service := newTestService(store)

	response, err := service.BuildDefinitionListResponse(ctx)
	if err != nil {
		t.Fatalf("BuildDefinitionListResponse returned error: %v", err)
	}
	if response.Total != 3 || len(response.Items) != 3 {
		t.Fatalf("response total/items = %d/%d, want 3/3", response.Total, len(response.Items))
	}
	if response.Stats.Draft != 1 || response.Stats.Published != 1 || response.Stats.Archived != 1 {
		t.Fatalf("stats = %#v, want one per status", response.Stats)
	}
	if response.StatusCounts != response.Stats {
		t.Fatalf("status counts = %#v, want %#v", response.StatusCounts, response.Stats)
	}
}

func TestServiceBuildDefinitionListResponseKeepsFullListSemantics(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	service := newTestService(store)
	for i := 0; i < 21; i++ {
		design := sampleDesign()
		design.ID = fmt.Sprintf("definition-%02d", i)
		if _, err := service.SaveDefinition(ctx, design); err != nil {
			t.Fatalf("save definition %d: %v", i, err)
		}
	}

	response, err := service.BuildDefinitionListResponse(ctx)
	if err != nil {
		t.Fatalf("BuildDefinitionListResponse: %v", err)
	}
	if response.Total != 21 || len(response.Items) != 21 {
		t.Fatalf("full list total/items = %d/%d, want 21/21", response.Total, len(response.Items))
	}
}

func TestServiceBuildDefinitionListResponseFiltersAndPaginates(t *testing.T) {
	ctx := context.Background()
	store := &listOnlyStore{designs: []*workflowtypes.DesignerDefinition{
		{ID: "draft-a", Name: "Expense approval", Status: workflowtypes.StatusDraft, UpdatedAt: time.Now()},
		{ID: "draft-b", Name: "Expense review", Status: workflowtypes.StatusDraft, UpdatedAt: time.Now().Add(-time.Minute)},
		{ID: "published", Name: "Expense published", Status: workflowtypes.StatusPublished, UpdatedAt: time.Now().Add(-2 * time.Minute)},
		{ID: "other", Name: "Onboarding", Status: workflowtypes.StatusDraft, UpdatedAt: time.Now().Add(-3 * time.Minute)},
	}}
	service := newTestService(store)

	response, err := service.BuildDefinitionListResponseWithQuery(ctx, workflowtypes.DefinitionQuery{
		Page: 1, Size: 1, Keyword: "expense", Status: workflowtypes.StatusDraft,
	})
	if err != nil {
		t.Fatalf("BuildDefinitionListResponseWithQuery: %v", err)
	}
	if response.Total != 2 || len(response.Items) != 1 || response.Page != 1 || response.Size != 1 {
		t.Fatalf("unexpected filtered page: %#v", response)
	}
	if response.Stats.Total != 3 || response.Stats.Draft != 2 || response.Stats.Published != 1 {
		t.Fatalf("keyword-scoped stats = %#v", response.Stats)
	}

	if _, err := service.BuildDefinitionListResponseWithQuery(ctx, workflowtypes.DefinitionQuery{Status: "unknown"}); !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("invalid status error = %v, want InvalidInput", err)
	}
}

func TestServiceBuildDefinitionListResponseQueriesSQLitePage(t *testing.T) {
	ctx := scopedContext(t, 1)
	store, err := OpenSQLiteStore(t.Context(), filepath.Join(t.TempDir(), "workflow.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()
	service := newTestService(store)

	for _, input := range []struct {
		id          string
		name        string
		description string
		published   bool
	}{
		{id: "draft-a", name: "Expense approval", description: "first expense draft"},
		{id: "draft-b", name: "Review", description: "second EXPENSE draft"},
		{id: "published", name: "Expense published", published: true},
		{id: "other", name: "Onboarding"},
	} {
		design := sampleDesign()
		design.ID = input.id
		design.Name = input.name
		design.Description = input.description
		if _, err := service.SaveDefinition(ctx, design); err != nil {
			t.Fatalf("save definition %s: %v", input.id, err)
		}
		if input.published {
			if _, err := service.PublishDefinition(ctx, input.id); err != nil {
				t.Fatalf("publish definition %s: %v", input.id, err)
			}
		}
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE workflow_definitions
		SET updated_at = CASE id
			WHEN 'draft-b' THEN '2026-05-24T10:03:00.1Z'
			WHEN 'draft-a' THEN '2026-05-24T10:03:00Z'
			WHEN 'published' THEN '2026-05-24T10:02:00Z'
			ELSE '2026-05-24T10:01:00Z'
		END`); err != nil {
		t.Fatalf("set deterministic definition ordering: %v", err)
	}

	response, err := service.BuildDefinitionListResponseWithQuery(ctx, workflowtypes.DefinitionQuery{
		Page: 2, Size: 1, Keyword: "expense", Status: workflowtypes.StatusDraft,
	})
	if err != nil {
		t.Fatalf("BuildDefinitionListResponseWithQuery: %v", err)
	}
	if response.Total != 2 || len(response.Items) != 1 {
		t.Fatalf("sqlite filtered page total/items = %d/%d, want 2/1", response.Total, len(response.Items))
	}
	if response.Page != 2 || response.Items[0].ID != "draft-a" {
		t.Fatalf("sqlite second page = page %d item %q, want page 2 item draft-a", response.Page, response.Items[0].ID)
	}
	if response.Stats.Total != 3 || response.Stats.Draft != 2 || response.Stats.Published != 1 {
		t.Fatalf("sqlite keyword-scoped stats = %#v", response.Stats)
	}
}

func TestServiceBuildDefinitionListResponseRejectsOffsetOverflow(t *testing.T) {
	service := newTestService(&listOnlyStore{})
	_, err := service.BuildDefinitionListResponseWithQuery(context.Background(), workflowtypes.DefinitionQuery{
		Page: math.MaxInt,
		Size: 100,
	})
	if !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected invalid input for overflowing offset, got %v", err)
	}
}

func TestServiceInstanceAndWorkItemListsRejectOffsetOverflow(t *testing.T) {
	service := newTestService(NewMemoryStore())
	tests := []struct {
		name string
		list func() error
	}{
		{
			name: "instances",
			list: func() error {
				_, err := service.ListInstances(context.Background(), workflowtypes.InstanceQuery{Page: math.MaxInt, Size: 2})
				return err
			},
		},
		{
			name: "work items",
			list: func() error {
				_, err := service.ListTodoItemsByQuery(context.Background(), workflowtypes.WorkItemQuery{Page: math.MaxInt, Size: 2})
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.list(); !errors.Is(err, errors.InvalidInput) {
				t.Fatalf("expected invalid input for overflowing offset, got %v", err)
			}
		})
	}
}

func TestServiceGetDefinitionClonesStoreResult(t *testing.T) {
	ctx := context.Background()
	store := &nonCloningDesignStore{design: &workflowtypes.DesignerDefinition{
		ID:          "stored",
		Name:        "Stored",
		Status:      workflowtypes.StatusDraft,
		StartNodeID: "start",
		Nodes: []workflowtypes.DesignerNode{
			{
				ID:   "start",
				Name: "Start",
				Type: workflowtypes.NodeKindTask,
				UI: map[string]any{
					"items": []map[string]any{{"label": "original"}},
				},
			},
		},
	}}
	service := newTestService(store)

	first, err := service.GetDefinition(ctx, "stored")
	if err != nil {
		t.Fatalf("get definition: %v", err)
	}
	first.Name = "mutated"
	first.Nodes[0].UI["items"].([]map[string]any)[0]["label"] = "changed"

	second, err := service.GetDefinition(ctx, "stored")
	if err != nil {
		t.Fatalf("get definition again: %v", err)
	}
	if second.Name != "Stored" {
		t.Fatalf("definition name = %q, want Stored", second.Name)
	}
	if got := second.Nodes[0].UI["items"].([]map[string]any)[0]["label"]; got != "original" {
		t.Fatalf("definition UI item label = %v, want original", got)
	}
}

func TestServiceRejectsStaleDefinitionVersion(t *testing.T) {
	ctx := context.Background()
	service := newTestService(NewMemoryStore())

	saved, err := service.SaveDefinition(ctx, sampleDesign())
	if err != nil {
		t.Fatalf("save definition: %v", err)
	}
	stale := cloneDesign(saved)
	current := cloneDesign(saved)
	current.Name = "current update"
	if _, err := service.SaveDefinition(ctx, current); err != nil {
		t.Fatalf("save current definition: %v", err)
	}
	stale.Name = "stale update"
	if _, err := service.SaveDefinition(ctx, stale); err == nil || !errors.Is(err, errors.Conflict) {
		t.Fatalf("expected stale definition conflict, got %v", err)
	}
}

func TestServiceConcurrentDraftSavesRejectOneStaleVersion(t *testing.T) {
	ctx := context.Background()
	service := newTestService(NewMemoryStore())

	saved, err := service.SaveDefinition(ctx, sampleDesign())
	if err != nil {
		t.Fatalf("save definition: %v", err)
	}
	first := cloneDesign(saved)
	second := cloneDesign(saved)
	first.Name = "first update"
	second.Name = "second update"

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, input := range []*workflowtypes.DesignerDefinition{first, second} {
		wg.Add(1)
		go func(input *workflowtypes.DesignerDefinition) {
			defer wg.Done()
			<-start
			_, err := service.SaveDefinition(ctx, input)
			errs <- err
		}(input)
	}

	close(start)
	wg.Wait()
	close(errs)

	successes := 0
	conflicts := 0
	for err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, errors.Conflict):
			conflicts++
		default:
			t.Fatalf("unexpected concurrent save error: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("concurrent save results: successes=%d conflicts=%d, want 1/1", successes, conflicts)
	}
}

func TestServiceNormalizesExternalIDs(t *testing.T) {
	ctx := context.Background()
	service := newTestService(NewMemoryStore())
	input := sampleDesign()
	input.ID = " onboarding-spaced "

	saved, err := service.SaveDefinition(ctx, input)
	if err != nil {
		t.Fatalf("save definition: %v", err)
	}
	if saved.ID != "onboarding-spaced" {
		t.Fatalf("saved definition id = %q, want trimmed id", saved.ID)
	}
	if _, err := service.GetDefinition(ctx, " onboarding-spaced "); err != nil {
		t.Fatalf("get definition with spaced id: %v", err)
	}
	if _, err := service.PublishDefinition(ctx, " onboarding-spaced "); err != nil {
		t.Fatalf("publish definition with spaced id: %v", err)
	}

	state, err := service.CreateInstance(ctx, " instance-spaced ", " onboarding-spaced ", nil)
	if err != nil {
		t.Fatalf("create instance with spaced ids: %v", err)
	}
	if state.ID != "instance-spaced" {
		t.Fatalf("created instance id = %q, want trimmed id", state.ID)
	}
	if _, err := service.StartInstance(ctx, " instance-spaced "); err != nil {
		t.Fatalf("start instance with spaced id: %v", err)
	}
}

func TestServiceReturnedDesignAndStateDoNotShareNestedSliceMaps(t *testing.T) {
	ctx := context.Background()
	service := newTestService(NewMemoryStore())
	design := sampleDesign()
	design.Nodes[0].UI = map[string]any{
		"items": []map[string]any{
			{"label": "original"},
		},
	}

	saved, err := service.SaveDefinition(ctx, design)
	if err != nil {
		t.Fatalf("save definition: %v", err)
	}
	saved.Nodes[0].UI["items"].([]map[string]any)[0]["label"] = "changed"
	stored, err := service.GetDefinition(ctx, saved.ID)
	if err != nil {
		t.Fatalf("get definition: %v", err)
	}
	if got := stored.Nodes[0].UI["items"].([]map[string]any)[0]["label"]; got != "original" {
		t.Fatalf("stored UI item label = %v, want original", got)
	}

	if _, err := service.PublishDefinition(ctx, saved.ID); err != nil {
		t.Fatalf("publish definition: %v", err)
	}
	state, err := service.CreateInstance(ctx, "instance-clone", saved.ID, map[string]any{
		"items": []map[string]any{
			{"value": "original"},
		},
	})
	if err != nil {
		t.Fatalf("create instance: %v", err)
	}
	state.Data["items"].([]map[string]any)[0]["value"] = "changed"
	storedState, err := service.GetInstance(ctx, "instance-clone")
	if err != nil {
		t.Fatalf("get instance: %v", err)
	}
	if got := storedState.Data["items"].([]map[string]any)[0]["value"]; got != "original" {
		t.Fatalf("stored data item value = %v, want original", got)
	}
}

func TestSQLiteStorePersistsDesignAndState(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLiteStore(ctx, filepath.Join(t.TempDir(), "workflow.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()

	service := newTestService(store)
	if _, err := service.SaveDefinition(ctx, sampleDesign()); err != nil {
		t.Fatalf("save definition: %v", err)
	}
	if _, err := service.PublishDefinition(ctx, "onboarding"); err != nil {
		t.Fatalf("publish definition: %v", err)
	}
	got, err := service.GetDefinition(ctx, "onboarding")
	if err != nil {
		t.Fatalf("get definition: %v", err)
	}
	if got.RuntimeDefinition == nil || got.RuntimeDefinition.StartNodeID != "start" {
		t.Fatalf("runtime projection was not stored: %#v", got.RuntimeDefinition)
	}
	if got.Description != "Employee entry approval and setup flow" {
		t.Fatalf("description was not persisted: %q", got.Description)
	}
	if _, err := service.CreateInstance(ctx, "instance-sql", "onboarding", nil); err != nil {
		t.Fatalf("create instance: %v", err)
	}
	if _, err := service.StartInstance(ctx, "instance-sql"); err != nil {
		t.Fatalf("start instance: %v", err)
	}
	state, err := service.GetInstance(ctx, "instance-sql")
	if err != nil {
		t.Fatalf("get instance: %v", err)
	}
	if state.Status != runtime.InstanceStatusRunning {
		t.Fatalf("state status = %s", state.Status)
	}
}

func TestServiceIsolatesDefinitionsAndInstancesByActiveScope(t *testing.T) {
	ctxScope1 := scopedContext(t, 101)
	ctxScope2 := scopedContext(t, 202)

	tests := []struct {
		name  string
		store Store
	}{
		{name: "memory", store: NewMemoryStore()},
		{
			name: "sqlite",
			store: func() Store {
				store, err := OpenSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "workflow.db"))
				if err != nil {
					t.Fatalf("open sqlite store: %v", err)
				}
				return store
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if closer, ok := tt.store.(interface{ Close() error }); ok {
				t.Cleanup(func() { _ = closer.Close() })
			}
			service := newTestService(tt.store)
			designScope1 := sampleDesign()
			designScope1.ID = "onboarding-scope-1"
			designScope1.Name = "Scope 1"

			savedScope1, err := service.SaveDefinition(ctxScope1, designScope1)
			if err != nil {
				t.Fatalf("save scope 1 definition: %v", err)
			}
			if _, err := service.GetDefinition(ctxScope2, savedScope1.ID); err == nil || !errors.Is(err, errors.NotFound) {
				t.Fatalf("expected scope 2 to miss scope 1 definition, got %v", err)
			}
			itemsScope2, err := service.ListDefinitions(ctxScope2)
			if err != nil {
				t.Fatalf("list scope 2 definitions: %v", err)
			}
			if len(itemsScope2) != 0 {
				t.Fatalf("scope 2 definitions = %d, want 0", len(itemsScope2))
			}
			designScope2 := sampleDesign()
			designScope2.ID = savedScope1.ID
			designScope2.Name = "Scope 2"
			savedScope2, err := service.SaveDefinition(ctxScope2, designScope2)
			if err != nil {
				t.Fatalf("save same id in scope 2: %v", err)
			}
			if savedScope2.ID != savedScope1.ID {
				t.Fatalf("scope 2 definition id = %q, want %q", savedScope2.ID, savedScope1.ID)
			}

			if _, err := service.PublishDefinition(ctxScope1, savedScope1.ID); err != nil {
				t.Fatalf("publish scope 1 definition: %v", err)
			}
			state, err := service.CreateInstance(ctxScope1, "instance-scope-1", savedScope1.ID, nil)
			if err != nil {
				t.Fatalf("create scope 1 instance: %v", err)
			}
			if _, err := service.GetInstance(ctxScope2, state.ID); err == nil || !errors.Is(err, errors.NotFound) {
				t.Fatalf("expected scope 2 to miss scope 1 instance, got %v", err)
			}
			if _, err := service.StartInstance(ctxScope2, state.ID); err == nil || !errors.Is(err, errors.NotFound) {
				t.Fatalf("expected scope 2 to fail starting scope 1 instance, got %v", err)
			}

			itemsScope1, err := service.ListDefinitions(ctxScope1)
			if err != nil {
				t.Fatalf("list scope 1 definitions: %v", err)
			}
			if len(itemsScope1) != 1 || itemsScope1[0].ID != savedScope1.ID {
				t.Fatalf("scope 1 definitions = %#v, want %s only", itemsScope1, savedScope1.ID)
			}
			if _, err := service.PublishDefinition(ctxScope2, savedScope2.ID); err != nil {
				t.Fatalf("publish scope 2 definition: %v", err)
			}
			if _, err := service.CreateInstance(ctxScope2, state.ID, savedScope2.ID, nil); err != nil {
				t.Fatalf("create same instance id in scope 2: %v", err)
			}
		})
	}
}

func TestServiceSeparatesPlatformAndTenantScopes(t *testing.T) {
	tenantCtx := scopedContext(t, 101)
	platformCtx := WithScope(context.Background(), Scope{Kind: ScopeKindPlatform, ID: 101})
	tests := []struct {
		name  string
		store Store
	}{
		{name: "memory", store: NewMemoryStore()},
		{
			name: "sqlite",
			store: func() Store {
				store, err := OpenSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "workflow.db"))
				if err != nil {
					t.Fatalf("open sqlite store: %v", err)
				}
				return store
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if closer, ok := tt.store.(interface{ Close() error }); ok {
				t.Cleanup(func() { _ = closer.Close() })
			}
			service := newTestService(tt.store)
			design := sampleDesign()
			design.ID = "shared-platform-tenant"
			if _, err := service.SaveDefinition(tenantCtx, design); err != nil {
				t.Fatalf("save tenant definition: %v", err)
			}
			if _, err := service.GetDefinition(platformCtx, design.ID); err == nil || !errors.Is(err, errors.NotFound) {
				t.Fatalf("expected platform scope to miss tenant definition, got %v", err)
			}
			platformDesign := sampleDesign()
			platformDesign.ID = design.ID
			platformDesign.Name = "Platform Workflow"
			if _, err := service.SaveDefinition(platformCtx, platformDesign); err != nil {
				t.Fatalf("save same id in platform scope: %v", err)
			}
			got, err := service.GetDefinition(tenantCtx, design.ID)
			if err != nil {
				t.Fatalf("get tenant definition: %v", err)
			}
			if got.Name == platformDesign.Name {
				t.Fatalf("tenant definition was overwritten by platform scope")
			}
		})
	}
}

func TestServiceRejectsUnsupportedScopeKindAcrossStores(t *testing.T) {
	ctx := WithScope(context.Background(), Scope{Kind: "department", ID: 101})
	tests := []struct {
		name  string
		store Store
	}{
		{name: "memory", store: NewMemoryStore()},
		{
			name: "sqlite",
			store: func() Store {
				store, err := OpenSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "workflow.db"))
				if err != nil {
					t.Fatalf("open sqlite store: %v", err)
				}
				return store
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if closer, ok := tt.store.(interface{ Close() error }); ok {
				t.Cleanup(func() { _ = closer.Close() })
			}
			service := newTestService(tt.store)
			if _, err := service.SaveDefinition(ctx, sampleDesign()); err == nil || !errors.Is(err, errors.InvalidInput) {
				t.Fatalf("expected unsupported scope to return InvalidInput, got %v", err)
			}
			if _, err := service.ListDefinitions(ctx); err == nil || !errors.Is(err, errors.InvalidInput) {
				t.Fatalf("expected unsupported scope list to return InvalidInput, got %v", err)
			}
		})
	}
}

func TestServiceRejectsTenantScopeWithoutID(t *testing.T) {
	ctx := WithScope(context.Background(), Scope{Kind: ScopeKindTenant})
	tests := []struct {
		name  string
		store Store
	}{
		{name: "memory", store: NewMemoryStore()},
		{
			name: "sqlite",
			store: func() Store {
				store, err := OpenSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "workflow.db"))
				if err != nil {
					t.Fatalf("open sqlite store: %v", err)
				}
				return store
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if closer, ok := tt.store.(interface{ Close() error }); ok {
				t.Cleanup(func() { _ = closer.Close() })
			}
			service := newTestService(tt.store)
			if _, err := service.ListDefinitions(ctx); err == nil || !errors.Is(err, errors.InvalidInput) {
				t.Fatalf("expected missing tenant scope id to return InvalidInput, got %v", err)
			}
			if _, err := service.SaveDefinition(ctx, sampleDesign()); err == nil || !errors.Is(err, errors.InvalidInput) {
				t.Fatalf("expected save with missing tenant scope id to return InvalidInput, got %v", err)
			}
		})
	}
}

func TestStoreSaveDesignRejectsStaleDefinitionVersion(t *testing.T) {
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
			base := normalizedTestDesign(t, sampleDesign(), 1)
			runtimeDef := runtimeDefinitionForTest(t, base)
			if err := tt.store.SaveDesign(ctx, base, runtimeDef); err != nil {
				t.Fatalf("save base design: %v", err)
			}

			first := cloneDesign(base)
			first.Name = "first update"
			first.Version = 2
			first.UpdatedAt = first.UpdatedAt.Add(time.Minute)
			if err := tt.store.SaveDesign(ctx, first, runtimeDefinitionForTest(t, first)); err != nil {
				t.Fatalf("save first update: %v", err)
			}

			stale := cloneDesign(base)
			stale.Name = "stale update"
			stale.Version = 2
			stale.UpdatedAt = stale.UpdatedAt.Add(2 * time.Minute)
			err := tt.store.SaveDesign(ctx, stale, runtimeDefinitionForTest(t, stale))
			if err == nil || !errors.Is(err, errors.Conflict) {
				t.Fatalf("expected stale definition conflict, got %v", err)
			}
		})
	}
}

func TestStoreSaveDesignRejectsMismatchedRuntimeDefinitionID(t *testing.T) {
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
			design := normalizedTestDesign(t, sampleDesign(), 1)
			runtimeDef := runtimeDefinitionForTest(t, design)
			runtimeDef.ID = "different-runtime-id"

			err := tt.store.SaveDesign(ctx, design, runtimeDef)
			if err == nil || !errors.Is(err, errors.InvalidInput) {
				t.Fatalf("expected mismatched runtime id to return InvalidInput, got %v", err)
			}
			if got, getErr := tt.store.GetDesign(ctx, design.ID); getErr != nil || got != nil {
				t.Fatalf("design after rejected save = %#v, err=%v; want nil, nil", got, getErr)
			}
		})
	}
}

func TestMemoryStoreRuntimeSaveDefinitionSynthesizesDesign(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	def := &runtime.Definition{
		ID:          "memory-runtime",
		Name:        "Memory Runtime",
		StartNodeID: "start",
		Nodes: []runtime.Node{
			{ID: "start", Name: "Start", Kind: runtime.NodeKindTask, Next: []string{"end"}},
			{ID: "end", Name: "End", Kind: runtime.NodeKindTask},
		},
	}

	if err := store.SaveDefinition(ctx, def); err != nil {
		t.Fatalf("save runtime definition: %v", err)
	}
	design, err := store.GetDesign(ctx, def.ID)
	if err != nil {
		t.Fatalf("get synthesized design: %v", err)
	}
	if design == nil {
		t.Fatal("expected synthesized design")
	}
	if design.RuntimeDefinition == nil || design.RuntimeDefinition.StartNodeID != "start" {
		t.Fatalf("runtime preview = %#v, want synthesized projection", design.RuntimeDefinition)
	}
	if design.PublishedAt == nil {
		t.Fatal("runtime-synthesized published definition should set published_at")
	}
}

func TestSQLiteStoreRuntimeSaveDefinitionPreservesExplicitNodeKinds(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLiteStore(ctx, filepath.Join(t.TempDir(), "workflow.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()

	def := &runtime.Definition{
		ID:          "choice-merge-runtime",
		Name:        "Choice Merge Runtime",
		StartNodeID: "review",
		Nodes: []runtime.Node{
			{ID: "review", Name: "Review", Kind: runtime.NodeKindBranch, Next: []string{"approved", "manager_review"}},
			{ID: "manager_review", Name: "Manager Review", Kind: runtime.NodeKindTask, Next: []string{"approved"}},
			{ID: "approved", Name: "Approved", Kind: runtime.NodeKindTask},
		},
	}
	if err := store.SaveDefinition(ctx, def); err != nil {
		t.Fatalf("save runtime definition: %v", err)
	}

	design, err := store.GetDesign(ctx, def.ID)
	if err != nil {
		t.Fatalf("get synthesized design: %v", err)
	}
	if design == nil {
		t.Fatal("expected synthesized design")
	}
	foundDesignNode := false
	for _, node := range design.Nodes {
		if node.ID == "approved" && node.Type != workflowtypes.NodeKindTask {
			t.Fatalf("approved node type = %s, want task", node.Type)
		}
		foundDesignNode = foundDesignNode || node.ID == "approved"
	}
	if !foundDesignNode {
		t.Fatal("expected approved node in synthesized design")
	}
	foundRuntimeNode := false
	for _, node := range design.RuntimeDefinition.Nodes {
		if node.ID == "approved" && node.Type != workflowtypes.NodeKindTask {
			t.Fatalf("runtime preview approved type = %s, want task", node.Type)
		}
		foundRuntimeNode = foundRuntimeNode || node.ID == "approved"
	}
	if !foundRuntimeNode {
		t.Fatal("expected approved node in runtime preview")
	}
	if design.PublishedAt == nil {
		t.Fatal("runtime-synthesized published definition should set published_at")
	}
}

func TestSQLiteStoreRuntimeSaveDefinitionRefreshesDesignSnapshot(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLiteStore(ctx, filepath.Join(t.TempDir(), "workflow.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()

	createdAt := time.Date(2026, 5, 24, 9, 0, 0, 0, time.UTC)
	if err := store.SaveDefinition(ctx, &runtime.Definition{
		ID:          "runtime-refresh",
		Name:        "Runtime Refresh",
		StartNodeID: "start",
		CreatedAt:   createdAt,
		UpdatedAt:   createdAt,
		Nodes: []runtime.Node{
			{ID: "start", Name: "Start", Kind: runtime.NodeKindTask, Next: []string{"old"}},
			{ID: "old", Name: "Old", Kind: runtime.NodeKindTask},
		},
	}); err != nil {
		t.Fatalf("save initial runtime definition: %v", err)
	}

	if err := store.SaveDefinition(ctx, &runtime.Definition{
		ID:          "runtime-refresh",
		Name:        "Runtime Refresh Updated",
		StartNodeID: "start",
		UpdatedAt:   createdAt.Add(time.Hour),
		Nodes: []runtime.Node{
			{ID: "start", Name: "Start", Kind: runtime.NodeKindTask, Next: []string{"new"}},
			{ID: "new", Name: "New", Kind: runtime.NodeKindTask},
		},
	}); err != nil {
		t.Fatalf("save updated runtime definition: %v", err)
	}

	design, err := store.GetDesign(ctx, "runtime-refresh")
	if err != nil {
		t.Fatalf("get design: %v", err)
	}
	if design.Name != "Runtime Refresh Updated" {
		t.Fatalf("design name = %q, want updated", design.Name)
	}
	if !design.CreatedAt.Equal(createdAt) {
		t.Fatalf("created_at = %s, want %s", design.CreatedAt, createdAt)
	}
	if len(design.Nodes) != 2 || design.Nodes[1].ID != "new" {
		t.Fatalf("design nodes were not refreshed: %#v", design.Nodes)
	}
	if got := design.RuntimeDefinition.Nodes[0].Next; len(got) != 1 || got[0] != "new" {
		t.Fatalf("runtime preview next = %#v, want new", got)
	}
}

func TestSQLiteStoreRuntimeSaveDefinitionPreservesDraftPublishedAtNil(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLiteStore(ctx, filepath.Join(t.TempDir(), "workflow.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()

	service := newTestService(store)
	saved, err := service.SaveDefinition(ctx, sampleDesign())
	if err != nil {
		t.Fatalf("save draft definition: %v", err)
	}
	if saved.PublishedAt != nil {
		t.Fatalf("draft published_at = %v, want nil", saved.PublishedAt)
	}

	if err := store.SaveDefinition(ctx, &runtime.Definition{
		ID:          saved.ID,
		Name:        "Runtime Refresh Draft",
		StartNodeID: "start",
		Nodes: []runtime.Node{
			{ID: "start", Name: "Start", Kind: runtime.NodeKindTask},
		},
	}); err != nil {
		t.Fatalf("save runtime definition over draft: %v", err)
	}

	design, err := store.GetDesign(ctx, saved.ID)
	if err != nil {
		t.Fatalf("get design: %v", err)
	}
	if design.Status != workflowtypes.StatusDraft {
		t.Fatalf("status = %s, want draft", design.Status)
	}
	if design.PublishedAt != nil {
		t.Fatalf("draft published_at = %v, want nil", design.PublishedAt)
	}
}

func TestMemoryStoreRuntimeSaveDefinitionPreservesDraftPublishedAtNil(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	service := newTestService(store)
	saved, err := service.SaveDefinition(ctx, sampleDesign())
	if err != nil {
		t.Fatalf("save draft definition: %v", err)
	}
	if saved.PublishedAt != nil {
		t.Fatalf("draft published_at = %v, want nil", saved.PublishedAt)
	}

	if err := store.SaveDefinition(ctx, &runtime.Definition{
		ID:          saved.ID,
		Name:        "Runtime Refresh Draft",
		StartNodeID: "start",
		Nodes: []runtime.Node{
			{ID: "start", Name: "Start", Kind: runtime.NodeKindTask},
		},
	}); err != nil {
		t.Fatalf("save runtime definition over draft: %v", err)
	}

	design, err := store.GetDesign(ctx, saved.ID)
	if err != nil {
		t.Fatalf("get design: %v", err)
	}
	if design.Status != workflowtypes.StatusDraft {
		t.Fatalf("status = %s, want draft", design.Status)
	}
	if design.PublishedAt != nil {
		t.Fatalf("draft published_at = %v, want nil", design.PublishedAt)
	}
}

func TestSQLiteStoreRuntimeSaveDefinitionGeneratesUniqueEdgeIDs(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLiteStore(ctx, filepath.Join(t.TempDir(), "workflow.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()

	if err := store.SaveDefinition(ctx, &runtime.Definition{
		ID:          "runtime-edge-ids",
		Name:        "Runtime Edge IDs",
		StartNodeID: "start",
		Nodes: []runtime.Node{
			{ID: "start", Name: "Start", Kind: runtime.NodeKindBranch, Next: []string{"a-b", "a"}},
			{ID: "a-b", Name: "A B", Kind: runtime.NodeKindTask, Next: []string{"c"}},
			{ID: "a", Name: "A", Kind: runtime.NodeKindTask, Next: []string{"b-c"}},
			{ID: "c", Name: "C", Kind: runtime.NodeKindTask},
			{ID: "b-c", Name: "B C", Kind: runtime.NodeKindTask},
		},
	}); err != nil {
		t.Fatalf("save runtime definition: %v", err)
	}

	design, err := store.GetDesign(ctx, "runtime-edge-ids")
	if err != nil {
		t.Fatalf("get design: %v", err)
	}
	seen := map[string]bool{}
	for _, edge := range design.Edges {
		if seen[edge.ID] {
			t.Fatalf("duplicate edge id %q in %#v", edge.ID, design.Edges)
		}
		seen[edge.ID] = true
	}
	if len(seen) != 4 {
		t.Fatalf("edge count = %d, want 4", len(seen))
	}
}

func TestSQLiteStoreRejectsStaleInstanceVersion(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLiteStore(ctx, filepath.Join(t.TempDir(), "workflow.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()

	service := newTestService(store)
	if _, err := service.SaveDefinition(ctx, sampleDesign()); err != nil {
		t.Fatalf("save definition: %v", err)
	}
	if _, err := service.PublishDefinition(ctx, "onboarding"); err != nil {
		t.Fatalf("publish definition: %v", err)
	}
	state, err := service.CreateInstance(ctx, "instance-version", "onboarding", nil)
	if err != nil {
		t.Fatalf("create instance: %v", err)
	}

	first := cloneState(state)
	second := cloneState(state)
	first.Data = map[string]any{"attempt": "first"}
	second.Data = map[string]any{"attempt": "second"}

	if err := store.SaveIfVersion(ctx, first, first.Version); err != nil {
		t.Fatalf("first versioned save: %v", err)
	}
	err = store.SaveIfVersion(ctx, second, second.Version)
	if err == nil {
		t.Fatal("expected stale version conflict")
	}
	if !errors.Is(err, errors.Conflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
}

func TestSQLiteStoreSaveIfVersionMissingInstanceReturnsDoesNotExist(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLiteStore(ctx, filepath.Join(t.TempDir(), "workflow.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()

	err = store.SaveIfVersion(ctx, &runtime.State{
		ID:           "missing-instance",
		DefinitionID: "missing-definition",
		Status:       runtime.InstanceStatusRunning,
	}, 1)
	if err == nil {
		t.Fatal("expected missing instance conflict")
	}
	if !errors.Is(err, errors.Conflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("expected missing instance error, got %v", err)
	}
}

func TestSQLiteStoreUsesSingleConnectionForForeignKeyPragma(t *testing.T) {
	store, err := OpenSQLiteStore(t.Context(), filepath.Join(t.TempDir(), "workflow.db"))
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()

	if got := store.db.Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("max open connections = %d, want 1", got)
	}
}

func TestSQLiteStoreMigratesManagedScopeColumnsOnLegacySchema(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "workflow.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open legacy sqlite: %v", err)
	}
	_, err = db.ExecContext(ctx, `CREATE TABLE workflow_definitions (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		version INTEGER NOT NULL,
		status TEXT NOT NULL,
		graph_json TEXT NOT NULL,
		runtime_definition_json TEXT NOT NULL,
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		published_at TEXT
	)`)
	if err != nil {
		t.Fatalf("create legacy definitions table: %v", err)
	}
	_, err = db.ExecContext(ctx, `CREATE TABLE workflow_instances (
		id TEXT PRIMARY KEY,
		definition_id TEXT NOT NULL,
		status TEXT NOT NULL,
		version INTEGER NOT NULL DEFAULT 1,
		state_json TEXT NOT NULL,
		data_json TEXT NOT NULL,
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		started_at TEXT,
		completed_at TEXT
	)`)
	if err != nil {
		t.Fatalf("create legacy instances table: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close legacy sqlite: %v", err)
	}

	store, err := OpenSQLiteStore(ctx, dbPath)
	if err != nil {
		t.Fatalf("migrate legacy sqlite store: %v", err)
	}
	defer store.Close()

	if !sqliteColumnExists(t, store.db, "workflow_definitions", "managed_scope_id") {
		t.Fatal("workflow_definitions.managed_scope_id was not migrated")
	}
	if !sqliteColumnExists(t, store.db, "workflow_definitions", "managed_scope_kind") {
		t.Fatal("workflow_definitions.managed_scope_kind was not migrated")
	}
	if !sqliteColumnExists(t, store.db, "workflow_instances", "managed_scope_id") {
		t.Fatal("workflow_instances.managed_scope_id was not migrated")
	}
	if !sqliteColumnExists(t, store.db, "workflow_instances", "managed_scope_kind") {
		t.Fatal("workflow_instances.managed_scope_kind was not migrated")
	}
	assertSQLitePrimaryKey(t, store.db, "workflow_definitions", []string{"managed_scope_kind", "managed_scope_id", "id"})
	assertSQLitePrimaryKey(t, store.db, "workflow_instances", []string{"managed_scope_kind", "managed_scope_id", "id"})
}

func TestSQLiteStoreMigratesLegacyScopePrimaryKeys(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw sqlite: %v", err)
	}
	legacyDesign := normalizedTestDesign(t, sampleDesign(), 1)
	legacyRuntime := runtimeDefinitionForTest(t, legacyDesign)
	graphJSON, err := json.Marshal(legacyDesign)
	if err != nil {
		t.Fatalf("marshal legacy design: %v", err)
	}
	runtimeJSON, err := json.Marshal(legacyRuntime)
	if err != nil {
		t.Fatalf("marshal legacy runtime: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		CREATE TABLE workflow_definitions (
			id TEXT PRIMARY KEY,
			managed_scope_id INTEGER NOT NULL DEFAULT 0,
			name TEXT NOT NULL,
			version INTEGER NOT NULL,
			status TEXT NOT NULL,
			graph_json TEXT NOT NULL,
			runtime_definition_json TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			published_at TEXT
		);
		CREATE TABLE workflow_instances (
			id TEXT PRIMARY KEY,
			managed_scope_id INTEGER NOT NULL DEFAULT 0,
			definition_id TEXT NOT NULL,
			status TEXT NOT NULL,
			version INTEGER NOT NULL DEFAULT 1,
			state_json TEXT NOT NULL,
			data_json TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			started_at TEXT,
			completed_at TEXT,
			FOREIGN KEY(definition_id) REFERENCES workflow_definitions(id) ON DELETE RESTRICT
		);
		INSERT INTO workflow_definitions
			(id, managed_scope_id, name, version, status, graph_json, runtime_definition_json, created_at, updated_at, published_at)
			VALUES (?, 0, ?, 1, ?, ?, ?, ?, ?, NULL);
	`, legacyDesign.ID, legacyDesign.Name, string(legacyDesign.Status), string(graphJSON), string(runtimeJSON), formatTime(legacyDesign.CreatedAt), formatTime(legacyDesign.UpdatedAt))
	if closeErr := db.Close(); closeErr != nil {
		t.Fatalf("close raw sqlite: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("seed legacy schema: %v", err)
	}

	store, err := OpenSQLiteStore(ctx, dbPath)
	if err != nil {
		t.Fatalf("migrate legacy store: %v", err)
	}
	defer store.Close()
	service := newTestService(store)
	if _, err := service.GetDefinition(context.Background(), legacyDesign.ID); err != nil {
		t.Fatalf("legacy platform definition should remain readable: %v", err)
	}
	tenantDesign := sampleDesign()
	tenantDesign.ID = legacyDesign.ID
	if _, err := service.SaveDefinition(scopedContext(t, 303), tenantDesign); err != nil {
		t.Fatalf("save same id after legacy migration: %v", err)
	}
	assertSQLitePrimaryKey(t, store.db, "workflow_definitions", []string{"managed_scope_kind", "managed_scope_id", "id"})
	assertSQLitePrimaryKey(t, store.db, "workflow_instances", []string{"managed_scope_kind", "managed_scope_id", "id"})
}

func TestSQLiteStoreMigratesLegacyScopePrimaryKeysWithInstances(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "legacy-with-instances.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw sqlite: %v", err)
	}
	legacyDesign := normalizedTestDesign(t, sampleDesign(), 1)
	legacyRuntime := runtimeDefinitionForTest(t, legacyDesign)
	graphJSON, err := json.Marshal(legacyDesign)
	if err != nil {
		t.Fatalf("marshal legacy design: %v", err)
	}
	runtimeJSON, err := json.Marshal(legacyRuntime)
	if err != nil {
		t.Fatalf("marshal legacy runtime: %v", err)
	}
	legacyState := &runtime.State{
		ID:           "legacy-instance",
		DefinitionID: legacyDesign.ID,
		Status:       runtime.InstanceStatusPending,
		Version:      1,
		CreatedAt:    legacyDesign.CreatedAt,
		UpdatedAt:    legacyDesign.UpdatedAt,
	}
	stateJSON, err := json.Marshal(legacyState)
	if err != nil {
		t.Fatalf("marshal legacy state: %v", err)
	}
	dataJSON, err := json.Marshal(legacyState.Data)
	if err != nil {
		t.Fatalf("marshal legacy data: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		CREATE TABLE workflow_definitions (
			id TEXT PRIMARY KEY,
			managed_scope_id INTEGER NOT NULL DEFAULT 0,
			name TEXT NOT NULL,
			version INTEGER NOT NULL,
			status TEXT NOT NULL,
			graph_json TEXT NOT NULL,
			runtime_definition_json TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			published_at TEXT
		);
		CREATE TABLE workflow_instances (
			id TEXT PRIMARY KEY,
			managed_scope_id INTEGER NOT NULL DEFAULT 0,
			definition_id TEXT NOT NULL,
			status TEXT NOT NULL,
			version INTEGER NOT NULL DEFAULT 1,
			state_json TEXT NOT NULL,
			data_json TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			started_at TEXT,
			completed_at TEXT,
			FOREIGN KEY(definition_id) REFERENCES workflow_definitions(id) ON DELETE RESTRICT
		);
	`)
	if err == nil {
		_, err = db.ExecContext(ctx, `INSERT INTO workflow_definitions
			(id, managed_scope_id, name, version, status, graph_json, runtime_definition_json, created_at, updated_at, published_at)
			VALUES (?, 0, ?, 1, ?, ?, ?, ?, ?, NULL)`,
			legacyDesign.ID, legacyDesign.Name, string(legacyDesign.Status), string(graphJSON), string(runtimeJSON), formatTime(legacyDesign.CreatedAt), formatTime(legacyDesign.UpdatedAt))
	}
	if err == nil {
		_, err = db.ExecContext(ctx, `INSERT INTO workflow_instances
			(id, managed_scope_id, definition_id, status, version, state_json, data_json, created_at, updated_at, started_at, completed_at)
			VALUES (?, 0, ?, ?, 1, ?, ?, ?, ?, NULL, NULL)`,
			string(legacyState.ID), legacyState.DefinitionID, string(legacyState.Status), string(stateJSON), string(dataJSON), formatTime(legacyState.CreatedAt), formatTime(legacyState.UpdatedAt))
	}
	if closeErr := db.Close(); closeErr != nil {
		t.Fatalf("close raw sqlite: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("seed legacy schema: %v", err)
	}

	store, err := OpenSQLiteStore(ctx, dbPath,
		WithLegacyDataScope(Scope{Kind: ScopeKindTenant, ID: 909}))
	if err != nil {
		t.Fatalf("migrate legacy store with instances: %v", err)
	}
	defer store.Close()
	service := newTestService(store)
	state, err := service.GetInstance(scopedContext(t, 909), legacyState.ID)
	if err != nil {
		t.Fatalf("legacy instance should be readable in assigned scope: %v", err)
	}
	if state.DefinitionID != legacyDesign.ID {
		t.Fatalf("legacy instance definition = %q, want %q", state.DefinitionID, legacyDesign.ID)
	}
	if _, err := service.GetInstance(scopedContext(t, 910), legacyState.ID); err == nil || !errors.Is(err, errors.NotFound) {
		t.Fatalf("legacy instance should remain isolated from other scopes, got %v", err)
	}
}

func TestSQLiteStoreMigratedLegacyInstancesStillRestrictDefinitionDelete(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "legacy-delete-restrict.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw sqlite: %v", err)
	}
	legacyDesign := normalizedTestDesign(t, sampleDesign(), 1)
	legacyRuntime := runtimeDefinitionForTest(t, legacyDesign)
	graphJSON, err := json.Marshal(legacyDesign)
	if err != nil {
		t.Fatalf("marshal legacy design: %v", err)
	}
	runtimeJSON, err := json.Marshal(legacyRuntime)
	if err != nil {
		t.Fatalf("marshal legacy runtime: %v", err)
	}
	legacyState := &runtime.State{
		ID:           "legacy-delete-instance",
		DefinitionID: legacyDesign.ID,
		Status:       runtime.InstanceStatusPending,
		Version:      1,
		CreatedAt:    legacyDesign.CreatedAt,
		UpdatedAt:    legacyDesign.UpdatedAt,
	}
	stateJSON, err := json.Marshal(legacyState)
	if err != nil {
		t.Fatalf("marshal legacy state: %v", err)
	}
	dataJSON, err := json.Marshal(legacyState.Data)
	if err != nil {
		t.Fatalf("marshal legacy data: %v", err)
	}
	_, err = db.ExecContext(ctx, `
		CREATE TABLE workflow_definitions (
			id TEXT PRIMARY KEY,
			managed_scope_id INTEGER NOT NULL DEFAULT 0,
			name TEXT NOT NULL,
			version INTEGER NOT NULL,
			status TEXT NOT NULL,
			graph_json TEXT NOT NULL,
			runtime_definition_json TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			published_at TEXT
		);
		CREATE TABLE workflow_instances (
			id TEXT PRIMARY KEY,
			managed_scope_id INTEGER NOT NULL DEFAULT 0,
			definition_id TEXT NOT NULL,
			status TEXT NOT NULL,
			version INTEGER NOT NULL DEFAULT 1,
			state_json TEXT NOT NULL,
			data_json TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			started_at TEXT,
			completed_at TEXT,
			FOREIGN KEY(definition_id) REFERENCES workflow_definitions(id) ON DELETE RESTRICT
		);
	`)
	if err == nil {
		_, err = db.ExecContext(ctx, `INSERT INTO workflow_definitions
			(id, managed_scope_id, name, version, status, graph_json, runtime_definition_json, created_at, updated_at, published_at)
			VALUES (?, 0, ?, 1, ?, ?, ?, ?, ?, NULL)`,
			legacyDesign.ID, legacyDesign.Name, string(legacyDesign.Status), string(graphJSON), string(runtimeJSON), formatTime(legacyDesign.CreatedAt), formatTime(legacyDesign.UpdatedAt))
	}
	if err == nil {
		_, err = db.ExecContext(ctx, `INSERT INTO workflow_instances
			(id, managed_scope_id, definition_id, status, version, state_json, data_json, created_at, updated_at, started_at, completed_at)
			VALUES (?, 0, ?, ?, 1, ?, ?, ?, ?, NULL, NULL)`,
			string(legacyState.ID), legacyState.DefinitionID, string(legacyState.Status), string(stateJSON), string(dataJSON), formatTime(legacyState.CreatedAt), formatTime(legacyState.UpdatedAt))
	}
	if closeErr := db.Close(); closeErr != nil {
		t.Fatalf("close raw sqlite: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("seed legacy schema: %v", err)
	}

	store, err := OpenSQLiteStore(ctx, dbPath)
	if err != nil {
		t.Fatalf("migrate legacy store with instances: %v", err)
	}
	defer store.Close()
	if err := store.DeleteDesign(context.Background(), legacyDesign.ID); err == nil {
		t.Fatal("expected migrated foreign key to restrict deleting a referenced definition")
	}
}

func TestSQLiteStoreLegacyScopeMigrationRejectsCollisionsAtomically(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "legacy-collision.db")
	ctx := context.Background()
	store, err := OpenSQLiteStore(ctx, dbPath)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	service := newTestService(store)
	if _, err := service.SaveDefinition(ctx, sampleDesign()); err != nil {
		t.Fatalf("save platform definition: %v", err)
	}
	tenantDesign := sampleDesign()
	tenantDesign.ID = "onboarding"
	if _, err := service.SaveDefinition(scopedContext(t, 77), tenantDesign); err != nil {
		t.Fatalf("save tenant definition: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close sqlite store: %v", err)
	}

	if _, err := OpenSQLiteStore(ctx, dbPath, WithLegacyDataScope(Scope{Kind: ScopeKindTenant, ID: 77})); err == nil || !errors.Is(err, errors.Conflict) {
		t.Fatalf("legacy scope collision error = %v, want conflict", err)
	}

	store, err = OpenSQLiteStore(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen sqlite store after rejected migration: %v", err)
	}
	defer store.Close()
	service = newTestService(store)
	if _, err := service.GetDefinition(ctx, "onboarding"); err != nil {
		t.Fatalf("platform definition should remain after rejected migration: %v", err)
	}
	if _, err := service.GetDefinition(scopedContext(t, 77), "onboarding"); err != nil {
		t.Fatalf("tenant definition should remain after rejected migration: %v", err)
	}
}

func TestRejectsInvalidRuntimeGraph(t *testing.T) {
	ctx := context.Background()
	service := newTestService(NewMemoryStore())
	input := sampleDesign()
	input.Edges = input.Edges[:1]
	if _, err := service.SaveDefinition(ctx, input); err == nil {
		t.Fatal("expected invalid graph error")
	}
}

func TestServiceRejectsDuplicateNodeNextBeforeNormalize(t *testing.T) {
	ctx := context.Background()
	service := newTestService(NewMemoryStore())
	input := &workflowtypes.DesignerDefinition{
		ID:          "duplicate-node-next",
		Name:        "Duplicate Node Next",
		StartNodeID: "start",
		Nodes: []workflowtypes.DesignerNode{
			{ID: "start", Name: "Start", Type: workflowtypes.NodeKindTask, Next: []string{"end", "end"}},
			{ID: "end", Name: "End", Type: workflowtypes.NodeKindTask},
		},
	}

	if _, err := service.SaveDefinition(ctx, input); err == nil || !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected duplicate node next to return InvalidInput, got %v", err)
	}
}

func TestServiceListsTodoAndDoneWorkItems(t *testing.T) {
	ctx := context.Background()
	service := newTestService(NewMemoryStore())
	saved, err := service.SaveDefinition(ctx, sampleDesign())
	if err != nil {
		t.Fatalf("save definition: %v", err)
	}
	if _, err := service.PublishDefinition(ctx, saved.ID); err != nil {
		t.Fatalf("publish definition: %v", err)
	}
	created, err := service.CreateInstance(ctx, "instance-work-items", saved.ID, map[string]any{"title": "入职申请"})
	if err != nil {
		t.Fatalf("create instance: %v", err)
	}
	if _, err := service.StartInstance(ctx, created.ID); err != nil {
		t.Fatalf("start instance: %v", err)
	}

	todos, err := service.ListTodoItems(ctx, 1, 20)
	if err != nil {
		t.Fatalf("list todos: %v", err)
	}
	if todos.Total != 1 || len(todos.Items) != 1 {
		t.Fatalf("todo page = %#v", todos)
	}
	if item := todos.Items[0]; item.Type != "todo" || item.InstanceID != "instance-work-items" || item.NodeID != "start" || item.NodeName != "Start" || item.DefinitionName != "Employee onboarding" {
		t.Fatalf("unexpected todo item: %#v", item)
	}

	if _, err := service.AdvanceNode(ctx, created.ID, "start"); err != nil {
		t.Fatalf("advance start: %v", err)
	}
	todos, err = service.ListTodoItems(ctx, 1, 20)
	if err != nil {
		t.Fatalf("list todos after advance: %v", err)
	}
	if todos.Total != 1 || todos.Items[0].NodeID != "decision" {
		t.Fatalf("todo after advance = %#v", todos)
	}
	done, err := service.ListDoneItems(ctx, 1, 20)
	if err != nil {
		t.Fatalf("list done: %v", err)
	}
	if done.Total != 1 || done.Items[0].Type != "done" || done.Items[0].NodeID != "start" || done.Items[0].CompletedAt == nil {
		t.Fatalf("done page = %#v", done)
	}
}

func TestServiceWorkItemPagination(t *testing.T) {
	ctx := context.Background()
	service := newTestService(NewMemoryStore())
	saved, err := service.SaveDefinition(ctx, sampleDesign())
	if err != nil {
		t.Fatalf("save definition: %v", err)
	}
	if _, err := service.PublishDefinition(ctx, saved.ID); err != nil {
		t.Fatalf("publish definition: %v", err)
	}
	for _, id := range []runtime.ID{"instance-page-1", "instance-page-2"} {
		created, err := service.CreateInstance(ctx, id, saved.ID, nil)
		if err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
		if _, err := service.StartInstance(ctx, created.ID); err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
	}

	page, err := service.ListTodoItems(ctx, 2, 1)
	if err != nil {
		t.Fatalf("list paged todos: %v", err)
	}
	if page.Total != 2 || page.Page != 2 || page.Size != 1 || len(page.Items) != 1 {
		t.Fatalf("paged todos = %#v", page)
	}
	if _, err := service.ListTodoItems(ctx, 1, 101); err == nil || !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected invalid size error, got %v", err)
	}
}

func TestServiceWorkflowCenterPersonalListsAndActions(t *testing.T) {
	service := newTestService(NewMemoryStore())
	baseCtx := context.Background()
	saved, err := service.SaveDefinition(baseCtx, sampleDesign())
	if err != nil {
		t.Fatalf("save definition: %v", err)
	}
	if _, err := service.PublishDefinition(baseCtx, saved.ID); err != nil {
		t.Fatalf("publish definition: %v", err)
	}
	initiatorCtx := principalContext(t, 1001)
	approverCtx := principalContext(t, 2002)
	otherCtx := principalContext(t, 3003)

	created, err := service.CreateInstanceWithOptions(initiatorCtx, workflowtypes.CreateInstanceInput{
		ID:            "center-1",
		DefinitionID:  saved.ID,
		BusinessKey:   "REQ-100",
		Title:         "入职申请",
		AssigneeID:    "2002",
		AssigneeName:  "审批人",
		InitiatorName: "发起人",
		Data:          map[string]any{"form_id": "F-1"},
	})
	if err != nil {
		t.Fatalf("create instance: %v", err)
	}
	if _, err := service.StartInstance(initiatorCtx, created.ID); err != nil {
		t.Fatalf("start instance: %v", err)
	}

	started, err := service.ListMyStartedInstances(initiatorCtx, workflowtypes.InstanceQuery{Page: 1, Size: 20})
	if err != nil {
		t.Fatalf("list started: %v", err)
	}
	if started.Total != 1 || started.Items[0].InitiatorID != "1001" || started.Items[0].BusinessKey != "REQ-100" {
		t.Fatalf("started page = %#v", started)
	}
	if otherStarted, err := service.ListMyStartedInstances(otherCtx, workflowtypes.InstanceQuery{Page: 1, Size: 20}); err != nil || otherStarted.Total != 0 {
		t.Fatalf("other started page = %#v err=%v", otherStarted, err)
	}

	todos, err := service.ListMyTodoItems(approverCtx, workflowtypes.WorkItemQuery{Page: 1, Size: 20})
	if err != nil {
		t.Fatalf("list my todos: %v", err)
	}
	if todos.Total != 1 || todos.Items[0].AssigneeID != "2002" || todos.Items[0].Title != "入职申请" {
		t.Fatalf("todo page = %#v", todos)
	}
	if otherTodos, err := service.ListMyTodoItems(otherCtx, workflowtypes.WorkItemQuery{Page: 1, Size: 20}); err != nil || otherTodos.Total != 0 {
		t.Fatalf("other todo page = %#v err=%v", otherTodos, err)
	}

	if _, err := service.TransferTask(approverCtx, created.ID, "start", workflowtypes.TaskActionInput{AssigneeID: "3003", AssigneeName: "转办人"}); err != nil {
		t.Fatalf("transfer task: %v", err)
	}
	otherTodos, err := service.ListMyTodoItems(otherCtx, workflowtypes.WorkItemQuery{Page: 1, Size: 20})
	if err != nil {
		t.Fatalf("list transferred todo: %v", err)
	}
	if otherTodos.Total != 1 || otherTodos.Items[0].AssigneeID != "3003" {
		t.Fatalf("transferred todo page = %#v", otherTodos)
	}

	if _, err := service.CompleteNode(otherCtx, created.ID, "start", workflowtypes.AdvanceNodeInput{Comment: "同意", AssigneeID: "2002"}); err != nil {
		t.Fatalf("complete start: %v", err)
	}
	done, err := service.ListMyDoneItems(otherCtx, workflowtypes.WorkItemQuery{Page: 1, Size: 20})
	if err != nil {
		t.Fatalf("list my done: %v", err)
	}
	if done.Total != 1 || done.Items[0].ActorID != "3003" || done.Items[0].Comment != "同意" {
		t.Fatalf("done page = %#v", done)
	}
}

func TestServiceListMyStartedInstancesExcludesPendingInstances(t *testing.T) {
	service := newTestService(NewMemoryStore())
	baseCtx := context.Background()
	saved, err := service.SaveDefinition(baseCtx, sampleDesign())
	if err != nil {
		t.Fatalf("save definition: %v", err)
	}
	if _, err := service.PublishDefinition(baseCtx, saved.ID); err != nil {
		t.Fatalf("publish definition: %v", err)
	}
	initiatorCtx := principalContext(t, 1001)
	otherCtx := principalContext(t, 2002)
	pending, err := service.CreateInstanceWithOptions(initiatorCtx, workflowtypes.CreateInstanceInput{
		ID:           "started-pending",
		DefinitionID: saved.ID,
		Title:        "pending instance",
	})
	if err != nil {
		t.Fatalf("create pending instance: %v", err)
	}
	started, err := service.CreateInstanceWithOptions(initiatorCtx, workflowtypes.CreateInstanceInput{
		ID:           "started-running",
		DefinitionID: saved.ID,
		Title:        "started instance",
	})
	if err != nil {
		t.Fatalf("create started instance: %v", err)
	}
	if _, err := service.StartInstance(initiatorCtx, started.ID); err != nil {
		t.Fatalf("start started instance: %v", err)
	}

	page, err := service.ListMyStartedInstances(initiatorCtx, workflowtypes.InstanceQuery{Page: 1, Size: 20})
	if err != nil {
		t.Fatalf("list my started: %v", err)
	}
	if page.Total != 1 || len(page.Items) != 1 {
		t.Fatalf("started page = %#v", page)
	}
	if item := page.Items[0]; item.ID != "started-running" || item.StartedAt == nil {
		t.Fatalf("started item = %#v", item)
	}

	instances, err := service.ListInstances(initiatorCtx, workflowtypes.InstanceQuery{Page: 1, Size: 20})
	if err != nil {
		t.Fatalf("list instances: %v", err)
	}
	if instances.Total != 2 {
		t.Fatalf("instances page = %#v, want both pending and started", instances)
	}
	if pending.Status != runtime.InstanceStatusPending {
		t.Fatalf("pending instance status = %s, want pending", pending.Status)
	}
	if otherPage, err := service.ListMyStartedInstances(otherCtx, workflowtypes.InstanceQuery{Page: 1, Size: 20}); err != nil || otherPage.Total != 0 {
		t.Fatalf("other started page = %#v err=%v", otherPage, err)
	}
}

func TestServiceListMyTodoItemsIncludesCandidateRoles(t *testing.T) {
	service := newTestService(NewMemoryStore())
	baseCtx := context.Background()
	saved, err := service.SaveDefinition(baseCtx, sampleDesign())
	if err != nil {
		t.Fatalf("save definition: %v", err)
	}
	if _, err := service.PublishDefinition(baseCtx, saved.ID); err != nil {
		t.Fatalf("publish definition: %v", err)
	}
	roleCtx := principalContextWithRoles(t, 2002, []string{"workflow-approver"})
	otherCtx := principalContextWithRoles(t, 3003, []string{"workflow-observer"})
	created, err := service.CreateInstanceWithOptions(baseCtx, workflowtypes.CreateInstanceInput{
		ID:               "candidate-role-1",
		DefinitionID:     saved.ID,
		CandidateRoleIDs: []string{"workflow-approver"},
	})
	if err != nil {
		t.Fatalf("create instance: %v", err)
	}
	if _, err := service.StartInstance(baseCtx, created.ID); err != nil {
		t.Fatalf("start instance: %v", err)
	}

	todos, err := service.ListMyTodoItems(roleCtx, workflowtypes.WorkItemQuery{Page: 1, Size: 20})
	if err != nil {
		t.Fatalf("list my todos by role: %v", err)
	}
	if todos.Total != 1 || len(todos.Items[0].CandidateRoleIDs) != 1 || todos.Items[0].CandidateRoleIDs[0] != "workflow-approver" {
		t.Fatalf("role todo page = %#v", todos)
	}

	otherTodos, err := service.ListMyTodoItems(otherCtx, workflowtypes.WorkItemQuery{Page: 1, Size: 20})
	if err != nil {
		t.Fatalf("list other todos: %v", err)
	}
	if otherTodos.Total != 0 {
		t.Fatalf("other role todo page = %#v", otherTodos)
	}
}

func TestServiceWorkflowCenterRejectsMissingPrincipalForMine(t *testing.T) {
	service := newTestService(NewMemoryStore())
	if _, err := service.ListMyTodoItems(context.Background(), workflowtypes.WorkItemQuery{}); err == nil || !errors.Is(err, errors.Unauthorized) {
		t.Fatalf("expected missing principal to return Unauthorized, got %v", err)
	}
}

func TestServiceWorkflowActionsRequireAuthorizedPrincipal(t *testing.T) {
	service := newTestService(NewMemoryStore())
	baseCtx := context.Background()
	saved, err := service.SaveDefinition(baseCtx, sampleDesign())
	if err != nil {
		t.Fatalf("save definition: %v", err)
	}
	if _, err := service.PublishDefinition(baseCtx, saved.ID); err != nil {
		t.Fatalf("publish definition: %v", err)
	}
	initiatorCtx := principalContext(t, 1001)
	approverCtx := principalContext(t, 2002)
	otherCtx := principalContext(t, 3003)
	created, err := service.CreateInstanceWithOptions(initiatorCtx, workflowtypes.CreateInstanceInput{
		ID:           "guarded-actions",
		DefinitionID: saved.ID,
		AssigneeID:   "2002",
	})
	if err != nil {
		t.Fatalf("create instance: %v", err)
	}
	if _, err := service.StartInstance(initiatorCtx, created.ID); err != nil {
		t.Fatalf("start instance: %v", err)
	}

	if _, err := service.CompleteNode(context.Background(), created.ID, "start", workflowtypes.AdvanceNodeInput{}); err == nil || !errors.Is(err, errors.Unauthorized) {
		t.Fatalf("expected missing principal complete to return Unauthorized, got %v", err)
	}
	state, err := service.GetInstance(baseCtx, created.ID)
	if err != nil {
		t.Fatalf("get instance after rejected complete: %v", err)
	}
	if len(state.ActiveNodeIDs) != 1 || state.ActiveNodeIDs[0] != "start" {
		t.Fatalf("active nodes after rejected complete = %#v", state.ActiveNodeIDs)
	}
	if _, err := service.CompleteNode(otherCtx, created.ID, "start", workflowtypes.AdvanceNodeInput{}); err == nil || !errors.Is(err, errors.Forbidden) {
		t.Fatalf("expected non-assignee complete to return Forbidden, got %v", err)
	}
	if _, err := service.TransferTask(otherCtx, created.ID, "start", workflowtypes.TaskActionInput{AssigneeID: "3003"}); err == nil || !errors.Is(err, errors.Forbidden) {
		t.Fatalf("expected non-assignee transfer to return Forbidden, got %v", err)
	}
	if _, err := service.TransferTask(approverCtx, created.ID, "start", workflowtypes.TaskActionInput{AssigneeID: "2002"}); err == nil || !errors.Is(err, errors.Conflict) {
		t.Fatalf("expected same-assignee transfer to return Conflict, got %v", err)
	}
	if _, err := service.DelegateTask(approverCtx, created.ID, "start", workflowtypes.TaskActionInput{AssigneeID: "2002"}); err == nil || !errors.Is(err, errors.Conflict) {
		t.Fatalf("expected same-assignee delegate to return Conflict, got %v", err)
	}
	if _, err := service.UnclaimTask(context.Background(), created.ID, "start", workflowtypes.TaskActionInput{}); err == nil || !errors.Is(err, errors.Unauthorized) {
		t.Fatalf("expected missing principal unclaim to return Unauthorized, got %v", err)
	}
	if _, err := service.WithdrawInstance(context.Background(), created.ID, workflowtypes.TaskActionInput{}); err == nil || !errors.Is(err, errors.Unauthorized) {
		t.Fatalf("expected missing principal withdraw to return Unauthorized, got %v", err)
	}
	if _, err := service.CompleteNode(approverCtx, created.ID, "start", workflowtypes.AdvanceNodeInput{}); err != nil {
		t.Fatalf("authorized complete: %v", err)
	}
}

func TestServiceRejectNodeReactivatesTargetAndRecordsMetadata(t *testing.T) {
	service := newTestService(NewMemoryStore())
	baseCtx := context.Background()
	saved, err := service.SaveDefinition(baseCtx, rejectableLinearDesign())
	if err != nil {
		t.Fatalf("save definition: %v", err)
	}
	if _, err := service.PublishDefinition(baseCtx, saved.ID); err != nil {
		t.Fatalf("publish definition: %v", err)
	}
	approverCtx := principalContext(t, 2002)
	created, err := service.CreateInstanceWithOptions(approverCtx, workflowtypes.CreateInstanceInput{
		ID:           "reject-linear-1",
		DefinitionID: saved.ID,
		AssigneeID:   "2002",
	})
	if err != nil {
		t.Fatalf("create instance: %v", err)
	}
	if _, err := service.StartInstance(approverCtx, created.ID); err != nil {
		t.Fatalf("start instance: %v", err)
	}
	if _, err := service.CompleteNode(approverCtx, created.ID, "submit", workflowtypes.AdvanceNodeInput{AssigneeID: "2002"}); err != nil {
		t.Fatalf("complete submit: %v", err)
	}
	if _, err := service.CompleteNode(approverCtx, created.ID, "review", workflowtypes.AdvanceNodeInput{AssigneeID: "2002"}); err != nil {
		t.Fatalf("complete review: %v", err)
	}

	state, err := service.RejectNode(approverCtx, created.ID, "approve", workflowtypes.RejectNodeInput{
		TargetNodeID: "review",
		Reason:       "请补充材料",
		AssigneeID:   "3003",
		AssigneeName: "复核人",
	})
	if err != nil {
		t.Fatalf("reject approve: %v", err)
	}
	if len(state.ActiveNodeIDs) != 1 || state.ActiveNodeIDs[0] != "review" {
		t.Fatalf("active nodes after reject = %#v, want review", state.ActiveNodeIDs)
	}
	if len(state.CompletedNodeIDs) != 1 || state.CompletedNodeIDs[0] != "submit" {
		t.Fatalf("completed nodes after reject = %#v, want submit only", state.CompletedNodeIDs)
	}
	lastHistory := state.History[len(state.History)-1]
	if lastHistory.Action != "reject" || lastHistory.NodeID != "review" || lastHistory.FromNodeID != "approve" {
		t.Fatalf("last history = %#v, want reject approve -> review", lastHistory)
	}
	meta := metadataFromState(state)
	lastAction := meta.Actions[len(meta.Actions)-1]
	if lastAction.Action != "reject" || lastAction.NodeID != "approve" || lastAction.TargetNodeID != "review" || lastAction.ActorID != "2002" || lastAction.Comment != "请补充材料" {
		t.Fatalf("last action = %#v, want reject metadata", lastAction)
	}
	if _, ok := meta.TaskAssignments["approve"]; ok {
		t.Fatalf("approve assignment should be removed after reject: %#v", meta.TaskAssignments)
	}
	reviewAssignment := meta.TaskAssignments["review"]
	if reviewAssignment.AssigneeID != "3003" || reviewAssignment.AssigneeName != "复核人" {
		t.Fatalf("review assignment = %#v, want assignee 3003", reviewAssignment)
	}

	if _, err := service.CompleteNode(principalContext(t, 3003), created.ID, "review", workflowtypes.AdvanceNodeInput{}); err != nil {
		t.Fatalf("complete rejected target again: %v", err)
	}
	state, err = service.GetInstance(baseCtx, created.ID)
	if err != nil {
		t.Fatalf("get instance: %v", err)
	}
	if len(state.ActiveNodeIDs) != 1 || state.ActiveNodeIDs[0] != "approve" {
		t.Fatalf("active nodes after resubmit = %#v, want approve", state.ActiveNodeIDs)
	}
}

func TestServiceRejectNodeRequiresAuthorizedPrincipal(t *testing.T) {
	service := newTestService(NewMemoryStore())
	baseCtx := context.Background()
	saved, err := service.SaveDefinition(baseCtx, rejectableLinearDesign())
	if err != nil {
		t.Fatalf("save definition: %v", err)
	}
	if _, err := service.PublishDefinition(baseCtx, saved.ID); err != nil {
		t.Fatalf("publish definition: %v", err)
	}
	approverCtx := principalContext(t, 2002)
	otherCtx := principalContext(t, 3003)
	created, err := service.CreateInstanceWithOptions(approverCtx, workflowtypes.CreateInstanceInput{
		ID:           "reject-guarded-1",
		DefinitionID: saved.ID,
		AssigneeID:   "2002",
	})
	if err != nil {
		t.Fatalf("create instance: %v", err)
	}
	if _, err := service.StartInstance(approverCtx, created.ID); err != nil {
		t.Fatalf("start instance: %v", err)
	}
	if _, err := service.CompleteNode(approverCtx, created.ID, "submit", workflowtypes.AdvanceNodeInput{AssigneeID: "2002"}); err != nil {
		t.Fatalf("complete submit: %v", err)
	}
	if _, err := service.CompleteNode(approverCtx, created.ID, "review", workflowtypes.AdvanceNodeInput{AssigneeID: "2002"}); err != nil {
		t.Fatalf("complete review: %v", err)
	}

	if _, err := service.RejectNode(context.Background(), created.ID, "approve", workflowtypes.RejectNodeInput{TargetNodeID: "review"}); err == nil || !errors.Is(err, errors.Unauthorized) {
		t.Fatalf("expected missing principal reject to return Unauthorized, got %v", err)
	}
	if _, err := service.RejectNode(otherCtx, created.ID, "approve", workflowtypes.RejectNodeInput{TargetNodeID: "review"}); err == nil || !errors.Is(err, errors.Forbidden) {
		t.Fatalf("expected non-assignee reject to return Forbidden, got %v", err)
	}
	if _, err := service.RejectNode(approverCtx, created.ID, "approve", workflowtypes.RejectNodeInput{TargetNodeID: "submit"}); err == nil || !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected disallowed target reject to return InvalidInput, got %v", err)
	}
	state, err := service.GetInstance(baseCtx, created.ID)
	if err != nil {
		t.Fatalf("get instance: %v", err)
	}
	if len(state.ActiveNodeIDs) != 1 || state.ActiveNodeIDs[0] != "approve" {
		t.Fatalf("active nodes after rejected operations = %#v, want approve", state.ActiveNodeIDs)
	}
}

func TestServiceWithdrawInstanceByInitiator(t *testing.T) {
	service := newTestService(NewMemoryStore())
	baseCtx := context.Background()
	saved, err := service.SaveDefinition(baseCtx, sampleDesign())
	if err != nil {
		t.Fatalf("save definition: %v", err)
	}
	if _, err := service.PublishDefinition(baseCtx, saved.ID); err != nil {
		t.Fatalf("publish definition: %v", err)
	}
	initiatorCtx := principalContext(t, 1001)
	otherCtx := principalContext(t, 2002)
	created, err := service.CreateInstanceWithOptions(initiatorCtx, workflowtypes.CreateInstanceInput{ID: "withdraw-1", DefinitionID: saved.ID})
	if err != nil {
		t.Fatalf("create instance: %v", err)
	}
	if _, err := service.StartInstance(initiatorCtx, created.ID); err != nil {
		t.Fatalf("start instance: %v", err)
	}
	if _, err := service.WithdrawInstance(otherCtx, created.ID, workflowtypes.TaskActionInput{}); err == nil || !errors.Is(err, errors.Forbidden) {
		t.Fatalf("expected non-initiator withdraw forbidden, got %v", err)
	}
	state, err := service.WithdrawInstance(initiatorCtx, created.ID, workflowtypes.TaskActionInput{Comment: "撤回"})
	if err != nil {
		t.Fatalf("withdraw instance: %v", err)
	}
	if state.Status != "withdrawn" || len(state.ActiveNodeIDs) != 0 {
		t.Fatalf("withdrawn state = %#v", state)
	}
}

func TestServiceWorkflowCenterSLADelegationReminderAndCC(t *testing.T) {
	service := newTestService(NewMemoryStore())
	baseCtx := context.Background()
	saved, err := service.SaveDefinition(baseCtx, sampleDesign())
	if err != nil {
		t.Fatalf("save definition: %v", err)
	}
	if _, err := service.PublishDefinition(baseCtx, saved.ID); err != nil {
		t.Fatalf("publish definition: %v", err)
	}
	initiatorCtx := principalContext(t, 1001)
	approverCtx := principalContext(t, 2002)
	delegateCtx := principalContext(t, 3003)
	dueAt := time.Now().Add(-time.Hour).UTC()

	created, err := service.CreateInstanceWithOptions(initiatorCtx, workflowtypes.CreateInstanceInput{
		ID:           "center-p2",
		DefinitionID: saved.ID,
		Title:        "超时审批",
		AssigneeID:   "2002",
		CCUserIDs:    []string{"4004"},
		DueAt:        &dueAt,
	})
	if err != nil {
		t.Fatalf("create instance: %v", err)
	}
	if _, err := service.StartInstance(initiatorCtx, created.ID); err != nil {
		t.Fatalf("start instance: %v", err)
	}

	todos, err := service.ListMyTodoItems(approverCtx, workflowtypes.WorkItemQuery{Page: 1, Size: 20})
	if err != nil {
		t.Fatalf("list my todos: %v", err)
	}
	if todos.Total != 1 || todos.Items[0].DueAt == nil || !todos.Items[0].Overdue || len(todos.Items[0].CCUserIDs) != 1 {
		t.Fatalf("todo with sla/cc = %#v", todos)
	}

	if _, err := service.RemindTask(initiatorCtx, created.ID, "start", workflowtypes.TaskActionInput{Comment: "请尽快处理"}); err != nil {
		t.Fatalf("remind task: %v", err)
	}
	if _, err := service.AddCCUsers(initiatorCtx, created.ID, workflowtypes.TaskActionInput{CCUserIDs: []string{"4004", "5005"}, Comment: "补充抄送"}); err != nil {
		t.Fatalf("add cc users: %v", err)
	}
	if _, err := service.DelegateTask(approverCtx, created.ID, "start", workflowtypes.TaskActionInput{AssigneeID: "3003", AssigneeName: "代理人", Comment: "委托处理"}); err != nil {
		t.Fatalf("delegate task: %v", err)
	}

	delegatedTodos, err := service.ListMyTodoItems(delegateCtx, workflowtypes.WorkItemQuery{Page: 1, Size: 20})
	if err != nil {
		t.Fatalf("list delegated todos: %v", err)
	}
	if delegatedTodos.Total != 1 {
		t.Fatalf("delegated todo page = %#v", delegatedTodos)
	}
	item := delegatedTodos.Items[0]
	if item.AssigneeID != "3003" || item.DelegatorID != "2002" || len(item.CCUserIDs) != 2 {
		t.Fatalf("delegated todo item = %#v", item)
	}

	instances, err := service.ListInstances(initiatorCtx, workflowtypes.InstanceQuery{Page: 1, Size: 20})
	if err != nil {
		t.Fatalf("list instances: %v", err)
	}
	if instances.Total != 1 || instances.Items[0].EarliestDueAt == nil || instances.Items[0].OverdueCount != 1 {
		t.Fatalf("instance summary = %#v", instances)
	}
}

func TestServiceTransferAndDelegateRecordOperatorNameInMetadata(t *testing.T) {
	service := newTestService(NewMemoryStore())
	baseCtx := context.Background()
	saved, err := service.SaveDefinition(baseCtx, sampleDesign())
	if err != nil {
		t.Fatalf("save definition: %v", err)
	}
	if _, err := service.PublishDefinition(baseCtx, saved.ID); err != nil {
		t.Fatalf("publish definition: %v", err)
	}
	approverCtx := principalContext(t, 2002)
	delegateCtx := principalContext(t, 3003)
	created, err := service.CreateInstanceWithOptions(baseCtx, workflowtypes.CreateInstanceInput{
		ID:           "action-actor-name",
		DefinitionID: saved.ID,
		AssigneeID:   "2002",
		AssigneeName: "审批人",
	})
	if err != nil {
		t.Fatalf("create instance: %v", err)
	}
	if _, err := service.StartInstance(baseCtx, created.ID); err != nil {
		t.Fatalf("start instance: %v", err)
	}

	state, err := service.TransferTask(approverCtx, created.ID, "start", workflowtypes.TaskActionInput{
		AssigneeID:   "3003",
		AssigneeName: "转办人",
		Comment:      "转办给转办人",
	})
	if err != nil {
		t.Fatalf("transfer task: %v", err)
	}
	meta := workflowtypes.InstanceMetadataFromData(state.Data)
	if meta == nil || len(meta.Actions) == 0 {
		t.Fatalf("transfer metadata = %#v", meta)
	}
	transfer := meta.Actions[len(meta.Actions)-1]
	if transfer.Action != "transfer" || transfer.ActorID != "2002" || transfer.ActorName != "审批人" {
		t.Fatalf("transfer action = %#v, want actor 2002/审批人", transfer)
	}

	state, err = service.DelegateTask(delegateCtx, created.ID, "start", workflowtypes.TaskActionInput{
		AssigneeID:   "4004",
		AssigneeName: "代理人",
		Comment:      "继续委托",
	})
	if err != nil {
		t.Fatalf("delegate task: %v", err)
	}
	meta = workflowtypes.InstanceMetadataFromData(state.Data)
	if meta == nil || len(meta.Actions) == 0 {
		t.Fatalf("delegate metadata = %#v", meta)
	}
	delegate := meta.Actions[len(meta.Actions)-1]
	if delegate.Action != "delegate" || delegate.ActorID != "3003" || delegate.ActorName != "转办人" {
		t.Fatalf("delegate action = %#v, want actor 3003/转办人", delegate)
	}
}

func sampleDesign() *workflowtypes.DesignerDefinition {
	return &workflowtypes.DesignerDefinition{
		ID:          "onboarding",
		Name:        "Employee onboarding",
		Description: "Employee entry approval and setup flow",
		StartNodeID: "start",
		Nodes: []workflowtypes.DesignerNode{
			{ID: "start", Name: "Start", Type: workflowtypes.NodeKindTask, Next: []string{"decision"}, Position: workflowtypes.Position{X: 0, Y: 0}},
			{ID: "decision", Name: "Decide", Type: workflowtypes.NodeKindBranch, Next: []string{"it", "hr"}, Position: workflowtypes.Position{X: 180, Y: 0}},
			{ID: "it", Name: "IT setup", Type: workflowtypes.NodeKindTask, Next: []string{"join"}, Position: workflowtypes.Position{X: 360, Y: -80}},
			{ID: "hr", Name: "HR setup", Type: workflowtypes.NodeKindTask, Next: []string{"join"}, Position: workflowtypes.Position{X: 360, Y: 80}},
			{ID: "join", Name: "Join", Type: workflowtypes.NodeKindJoin, Config: map[string]any{"joinMode": "all"}, Position: workflowtypes.Position{X: 540, Y: 0}},
		},
		Edges: []workflowtypes.DesignerEdge{
			{ID: "e1", Source: "start", Target: "decision"},
			{ID: "e2", Source: "decision", Target: "it"},
			{ID: "e3", Source: "decision", Target: "hr"},
			{ID: "e4", Source: "it", Target: "join"},
			{ID: "e5", Source: "hr", Target: "join"},
		},
	}
}

func rejectableLinearDesign() *workflowtypes.DesignerDefinition {
	return &workflowtypes.DesignerDefinition{
		ID:          "rejectable-linear",
		Name:        "Rejectable Linear",
		StartNodeID: "submit",
		Nodes: []workflowtypes.DesignerNode{
			{ID: "submit", Name: "Submit", Type: workflowtypes.NodeKindTask, Position: workflowtypes.Position{X: 0, Y: 0}},
			{ID: "review", Name: "Review", Type: workflowtypes.NodeKindTask, Position: workflowtypes.Position{X: 180, Y: 0}},
			{ID: "approve", Name: "Approve", Type: workflowtypes.NodeKindTask, RejectTo: []string{"review"}, Position: workflowtypes.Position{X: 360, Y: 0}},
		},
		Edges: []workflowtypes.DesignerEdge{
			{ID: "submit-review", Source: "submit", Target: "review"},
			{ID: "review-approve", Source: "review", Target: "approve"},
		},
	}
}

func newTestService(store Store) *Service {
	return NewService(store, nil, nil)
}

func normalizedTestDesign(t *testing.T, design *workflowtypes.DesignerDefinition, version int) *workflowtypes.DesignerDefinition {
	t.Helper()
	now := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	design.Version = version
	design.Status = workflowtypes.StatusDraft
	design.Normalize(now)
	return design
}

func runtimeDefinitionForTest(t *testing.T, design *workflowtypes.DesignerDefinition) *runtime.Definition {
	t.Helper()
	runtimeDef, err := design.ToRuntimeDefinition()
	if err != nil {
		t.Fatalf("project runtime definition: %v", err)
	}
	return runtimeDef
}

func scopedContext(t *testing.T, scopeID int64) context.Context {
	t.Helper()
	return WithScope(context.Background(), Scope{Kind: ScopeKindTenant, ID: scopeID})
}

func principalContext(t *testing.T, userID int64) context.Context {
	return principalContextWithRoles(t, userID, nil)
}

func principalContextWithRoles(t *testing.T, userID int64, roles []string) context.Context {
	t.Helper()
	ctx, err := auth.WithPrincipal(context.Background(), auth.Principal{SubjectID: userID, ActiveScopeID: 1, Roles: roles})
	if err != nil {
		t.Fatalf("with principal: %v", err)
	}
	return ctx
}

func sqliteColumnExists(t *testing.T, db *sql.DB, table string, column string) bool {
	t.Helper()
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		t.Fatalf("inspect %s columns: %v", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			t.Fatalf("scan %s column: %v", table, err)
		}
		if name == column {
			return true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %s columns: %v", table, err)
	}
	return false
}

func assertSQLitePrimaryKey(t *testing.T, db *sql.DB, table string, want []string) {
	t.Helper()
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		t.Fatalf("inspect %s primary key: %v", table, err)
	}
	defer rows.Close()
	got := make([]string, len(want))
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			t.Fatalf("scan %s primary key: %v", table, err)
		}
		if pk > 0 && pk <= len(got) {
			got[pk-1] = name
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %s primary key: %v", table, err)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s primary key = %#v, want %#v", table, got, want)
		}
	}
}

type listOnlyStore struct {
	Store
	designs []*workflowtypes.DesignerDefinition
}

func (s *listOnlyStore) ListDesigns(ctx context.Context) ([]*workflowtypes.DesignerDefinition, error) {
	return s.designs, nil
}

type nonCloningDesignStore struct {
	Store
	design *workflowtypes.DesignerDefinition
}

func (s *nonCloningDesignStore) GetDesign(ctx context.Context, id string) (*workflowtypes.DesignerDefinition, error) {
	if s.design == nil || s.design.ID != id {
		return nil, nil
	}
	return s.design, nil
}
