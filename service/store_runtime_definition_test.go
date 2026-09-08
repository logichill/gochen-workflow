package workflowservice

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	workflowtypes "gochen-workflow/types"
	"gochen/clock"
	"gochen/errors"
	runtime "gochen/process/workflow"
)

func TestStore_ExplicitRuntimeVersionsPreserveHistoryAndTimeout(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			var store Store = NewMemoryStore()
			if kind == "sqlite" {
				sqlStore, err := OpenSQLiteStore(ctx, filepath.Join(t.TempDir(), "versions.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = sqlStore.Close() })
				store = sqlStore
			}
			clk := clock.NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
			engine := runtime.NewEngine(store).WithClock(clk)
			def := &runtime.Definition{ID: "explicit", Name: "Version 7", Version: 7, StartNodeID: "task", Nodes: []runtime.Node{{ID: "task", Timeout: time.Minute}}}
			if err := engine.SaveDefinition(ctx, def); err != nil {
				t.Fatal(err)
			}
			saved, err := store.GetDefinition(ctx, def.ID, 7)
			if err != nil || saved == nil || saved.Version != 7 || saved.Nodes[0].Timeout != time.Minute {
				t.Fatalf("version 7 round trip: %+v %v", saved, err)
			}
			saved.Nodes[0].Timeout = 0
			saved, err = store.GetDefinition(ctx, def.ID, 7)
			if err != nil || saved.Nodes[0].Timeout != time.Minute {
				t.Fatal("read modified stored timeout")
			}
			if err := engine.CreateInstance(ctx, "instance", def.ID, runtime.WithDefinitionVersion(7)); err != nil {
				t.Fatal(err)
			}
			next := *def
			next.Version = 11
			next.Name = "Version 11"
			if err := engine.SaveDefinition(ctx, &next); err != nil {
				t.Fatal(err)
			}
			if err := store.SaveDefinition(ctx, &next); !errors.Is(err, errors.Conflict) {
				t.Fatalf("existing version overwrite error=%v", err)
			}
			if err := engine.StartInstance(ctx, "instance"); err != nil {
				t.Fatalf("instance pinned to 7 failed after publishing 11: %v", err)
			}
			clk.Advance(2 * time.Minute)
			timedOut, err := engine.CheckTimeouts(ctx, "instance")
			if err != nil || len(timedOut) != 1 || timedOut[0] != "task" {
				t.Fatalf("persisted node timeout was not enforced: %v %v", timedOut, err)
			}
			if err := store.DeleteDefinition(ctx, def.ID, 11); err != nil {
				t.Fatal(err)
			}
			design, err := store.GetDesign(ctx, def.ID)
			if err != nil || design == nil || design.Version != 7 || design.Name != "Version 7" {
				t.Fatalf("design rollback: %+v %v", design, err)
			}
			automatic := *def
			automatic.Version = 0
			if err := engine.SaveDefinition(ctx, &automatic); err != nil {
				t.Fatal(err)
			}
			if automatic.Version != 8 {
				t.Fatalf("automatic version=%d, want 8", automatic.Version)
			}
			badDesign := sampleDesign()
			badDesign.ID = "mismatch"
			badDesign.Version = 1
			badDesign.Status = workflowtypes.StatusPublished
			badRuntime := runtimeDefinitionForTest(t, badDesign)
			badRuntime.Version = 2
			if err := store.SaveDesign(ctx, badDesign, badRuntime); !errors.Is(err, errors.InvalidInput) {
				t.Fatalf("mismatched versions accepted: %v", err)
			}
			if got, err := store.GetDesign(ctx, badDesign.ID); err != nil || got != nil {
				t.Fatalf("invalid write persisted: %+v %v", got, err)
			}
		})
	}
}
