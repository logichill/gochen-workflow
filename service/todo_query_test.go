package workflowservice

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	workflowtypes "gochen-workflow/types"
	runtime "gochen/process/workflow"
)

func TestSQLiteTodoQuerySkipsFinishedHistoryAndOtherScopes(t *testing.T) {
	ctx := scopedContext(t, 1)
	store, err := OpenSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "todos.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := NewService(store, nil, nil)
	for _, scopeCtx := range []context.Context{ctx, scopedContext(t, 2)} {
		if _, err := service.SaveDefinition(scopeCtx, sampleDesign()); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC()
	for _, input := range []struct {
		ctx    context.Context
		id     runtime.ID
		status runtime.InstanceStatus
	}{
		{ctx, "active", runtime.InstanceStatusRunning},
		{ctx, "finished", runtime.InstanceStatusCompleted},
		{scopedContext(t, 2), "foreign", runtime.InstanceStatusRunning},
	} {
		if err := store.Save(input.ctx, &runtime.State{ID: input.id, DefinitionID: sampleDesign().ID, Status: input.status, ActiveNodeIDs: []string{"review"}, CreatedAt: now, UpdatedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	// Unreadable historical/foreign payloads prove they are excluded before decoding.
	if _, err := store.db.ExecContext(ctx, `UPDATE workflow_instances SET state_json = 'invalid-json' WHERE id IN ('finished', 'foreign')`); err != nil {
		t.Fatal(err)
	}
	page, err := service.ListTodoItems(ctx, 1, 10)
	if err != nil {
		t.Fatalf("todo query decoded unrelated history: %v", err)
	}
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].InstanceID != "active" {
		t.Fatalf("unexpected todos: %+v", page)
	}
	if _, err := store.ListInstances(ctx); err == nil {
		t.Fatal("unfiltered scan should still expose invalid history")
	}
}

func TestListDoneItemsByQuery_SkipsUnstartedInstances(t *testing.T) {
	ctx := scopedContext(t, 1)
	store, err := OpenSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "done.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := NewService(store, nil, nil)
	if _, err := service.SaveDefinition(ctx, sampleDesign()); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()

	// 1. 创建一个正常已完成节点（已启动）的实例
	startedState := &runtime.State{
		ID:               "started-inst",
		DefinitionID:     sampleDesign().ID,
		Status:           runtime.InstanceStatusRunning,
		ActiveNodeIDs:    []string{"node2"},
		CompletedNodeIDs: []string{"node1"},
		StartedAt:        now,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := store.Save(ctx, startedState); err != nil {
		t.Fatal(err)
	}

	// 2. 创建一个草稿未启动实例（started_at 为空），将其 state_json 设为非法 JSON 验证不会被扫描解析 (F17)
	unstartedState := &runtime.State{
		ID:           "unstarted-inst",
		DefinitionID: sampleDesign().ID,
		Status:       runtime.InstanceStatusPending,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := store.Save(ctx, unstartedState); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE workflow_instances SET state_json = 'corrupted-json' WHERE id = 'unstarted-inst'`); err != nil {
		t.Fatal(err)
	}

	// 3. 查询已办列表：应该只扫描 started 实例并成功返回，未启动实例被下推排除不触发 JSON 解析崩溃
	page, err := service.ListDoneItemsByQuery(ctx, workflowtypes.WorkItemQuery{Page: 1, Size: 10})
	if err != nil {
		t.Fatalf("ListDoneItemsByQuery failed on corrupted unstarted instance: %v", err)
	}
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].InstanceID != "started-inst" {
		t.Fatalf("unexpected done items: %+v", page)
	}
}
