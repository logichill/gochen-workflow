package workflowservice

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	workflowtypes "gochen-workflow/types"
	"gochen/clock"
)

func TestDoneHistoryKeepsActorsAtIdenticalTimes(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			var store Store = NewMemoryStore()
			if kind == "sqlite" {
				db, e := OpenSQLiteStore(ctx, filepath.Join(t.TempDir(), "done.db"))
				if e != nil {
					t.Fatal(e)
				}
				defer db.Close()
				store = db
			}
			clk := clock.NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
			svc := NewService(store, nil, clk)
			def, e := svc.SaveDefinition(ctx, rejectableLinearDesign())
			if e != nil {
				t.Fatal(e)
			}
			if _, e = svc.PublishDefinition(ctx, def.ID); e != nil {
				t.Fatal(e)
			}
			a, b, c := principalContext(t, 1001), principalContext(t, 2002), principalContext(t, 3003)
			st, e := svc.CreateInstanceWithOptions(a, workflowtypes.CreateInstanceInput{ID: "same-time", DefinitionID: def.ID, AssigneeID: "1001"})
			if e != nil {
				t.Fatal(e)
			}
			if _, e = svc.StartInstance(a, st.ID); e != nil {
				t.Fatal(e)
			}
			if _, e = svc.CompleteNode(a, st.ID, "submit", workflowtypes.AdvanceNodeInput{AssigneeID: "1001"}); e != nil {
				t.Fatal(e)
			}
			if _, e = svc.CompleteNode(a, st.ID, "review", workflowtypes.AdvanceNodeInput{AssigneeID: "2002", Comment: "first review"}); e != nil {
				t.Fatal(e)
			}
			if _, e = svc.RejectNode(b, st.ID, "approve", workflowtypes.RejectNodeInput{TargetNodeID: "review", AssigneeID: "3003"}); e != nil {
				t.Fatal(e)
			}
			if _, e = svc.CompleteNode(c, st.ID, "review", workflowtypes.AdvanceNodeInput{Comment: "second review"}); e != nil {
				t.Fatal(e)
			}
			svc = NewService(store, nil, clk)
			for _, tc := range []struct {
				ctx            context.Context
				actor, comment string
			}{{a, "1001", "first review"}, {c, "3003", "second review"}} {
				page, e := svc.ListMyDoneItems(tc.ctx, workflowtypes.WorkItemQuery{Page: 1, Size: 20})
				if e != nil {
					t.Fatal(e)
				}
				count := 0
				for _, item := range page.Items {
					if item.NodeID == "review" {
						count++
						if item.ActorID != tc.actor || item.Comment != tc.comment {
							t.Errorf("wrong historical actor/comment: %+v", item)
						}
					}
				}
				if count != 1 {
					t.Errorf("actor %s review count=%d want1; items=%+v", tc.actor, count, page.Items)
				}
			}
			// A legacy state missing one operation cannot safely attribute either equal-time occurrence.
			persisted, err := store.Get(ctx, st.ID)
			if err != nil {
				t.Fatal(err)
			}
			meta := metadataFromState(persisted)
			for i, action := range meta.Actions {
				if action.NodeID == "review" && action.ActorID == "1001" {
					meta.Actions = append(meta.Actions[:i], meta.Actions[i+1:]...)
					break
				}
			}
			saveMetadata(persisted, meta, clk.Now())
			if err := store.SaveIfVersion(ctx, persisted, persisted.Version); err != nil {
				t.Fatal(err)
			}
			svc = NewService(store, nil, clk)
			page, err := svc.ListDoneItems(ctx, 1, 20)
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, item := range page.Items {
				if item.NodeID == "review" {
					count++
					if item.ActorID != "" || item.Comment != "" {
						t.Errorf("ambiguous history attributed: %+v", item)
					}
				}
			}
			if count != 2 {
				t.Errorf("lost un-attributed history: count=%d", count)
			}

		})
	}
}
