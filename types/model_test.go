package workflowtypes

import (
	"testing"
	"time"

	runtime "gochen/process/workflow"
)

func TestValidateDesignerDefinitionAllowsTaskChoiceMerge(t *testing.T) {
	definition := &DesignerDefinition{
		ID:          "choice-merge",
		Name:        "Choice Merge",
		Status:      StatusDraft,
		StartNodeID: "review",
		Nodes: []DesignerNode{
			{ID: "review", Name: "Review", Type: NodeKindBranch, Position: Position{X: 0, Y: 0}},
			{ID: "manager_review", Name: "Manager Review", Type: NodeKindTask, Position: Position{X: 120, Y: 80}},
			{ID: "approved", Name: "Approved", Type: NodeKindTask, Position: Position{X: 240, Y: 0}},
		},
		Edges: []DesignerEdge{
			{ID: "direct", Source: "review", Target: "approved"},
			{ID: "manager", Source: "review", Target: "manager_review"},
			{ID: "manager-approved", Source: "manager_review", Target: "approved"},
		},
	}

	if err := ValidateDesignerDefinition(definition); err != nil {
		t.Fatalf("ValidateDesignerDefinition returned error: %v", err)
	}
}

func TestValidateDesignerDefinitionRejectsDuplicateEdgePair(t *testing.T) {
	definition := &DesignerDefinition{
		ID:          "duplicate-edge-pair",
		Name:        "Duplicate Edge Pair",
		Status:      StatusDraft,
		StartNodeID: "start",
		Nodes: []DesignerNode{
			{ID: "start", Name: "Start", Type: NodeKindTask, Position: Position{X: 0, Y: 0}},
			{ID: "end", Name: "End", Type: NodeKindTask, Position: Position{X: 120, Y: 0}},
		},
		Edges: []DesignerEdge{
			{ID: "edge-1", Source: "start", Target: "end"},
			{ID: "edge-2", Source: "start", Target: "end"},
		},
	}

	if err := ValidateDesignerDefinition(definition); err == nil {
		t.Fatal("expected duplicate edge pair to be rejected")
	}
}

func TestValidateDesignerDefinitionRejectsDuplicateNodeNextTarget(t *testing.T) {
	definition := &DesignerDefinition{
		ID:          "duplicate-node-next",
		Name:        "Duplicate Node Next",
		Status:      StatusDraft,
		StartNodeID: "start",
		Nodes: []DesignerNode{
			{ID: "start", Name: "Start", Type: NodeKindTask, Next: []string{"end", "end"}, Position: Position{X: 0, Y: 0}},
			{ID: "end", Name: "End", Type: NodeKindTask, Position: Position{X: 120, Y: 0}},
		},
	}

	if err := ValidateDesignerDefinition(definition); err == nil {
		t.Fatal("expected duplicate node next target to be rejected")
	}
}

func TestValidateDesignerDefinitionRejectsMissingNodeNextTarget(t *testing.T) {
	definition := &DesignerDefinition{
		ID:          "missing-node-next",
		Name:        "Missing Node Next",
		Status:      StatusDraft,
		StartNodeID: "start",
		Nodes: []DesignerNode{
			{ID: "start", Name: "Start", Type: NodeKindTask, Next: []string{"missing"}, Position: Position{X: 0, Y: 0}},
		},
	}

	if err := ValidateDesignerDefinition(definition); err == nil {
		t.Fatal("expected missing node next target to be rejected")
	}
	if _, err := definition.ToRuntimeDefinition(); err == nil {
		t.Fatal("expected runtime projection to reject missing node next target")
	}
}

func TestToRuntimeDefinitionProjectsRejectTo(t *testing.T) {
	definition := &DesignerDefinition{
		ID:          "reject-projection",
		Name:        "Reject Projection",
		Status:      StatusDraft,
		StartNodeID: "submit",
		Nodes: []DesignerNode{
			{ID: "submit", Name: "Submit", Type: NodeKindTask, Position: Position{X: 0, Y: 0}},
			{ID: "review", Name: "Review", Type: NodeKindTask, Position: Position{X: 120, Y: 0}},
			{ID: "approve", Name: "Approve", Type: NodeKindTask, RejectTo: []string{"review"}, Position: Position{X: 240, Y: 0}},
		},
		Edges: []DesignerEdge{
			{ID: "submit-review", Source: "submit", Target: "review"},
			{ID: "review-approve", Source: "review", Target: "approve"},
		},
	}

	runtimeDef, err := definition.ToRuntimeDefinition()
	if err != nil {
		t.Fatalf("ToRuntimeDefinition returned error: %v", err)
	}
	if len(runtimeDef.Nodes[2].RejectTo) != 1 || runtimeDef.Nodes[2].RejectTo[0] != "review" {
		t.Fatalf("runtime reject_to = %#v, want review", runtimeDef.Nodes[2].RejectTo)
	}
	preview := RuntimeDefinitionFromRuntime(runtimeDef, definition)
	if len(preview.Nodes[2].RejectTo) != 1 || preview.Nodes[2].RejectTo[0] != "review" {
		t.Fatalf("preview reject_to = %#v, want review", preview.Nodes[2].RejectTo)
	}
}

func TestToRuntimeDefinitionRejectsNonDominatingRejectTarget(t *testing.T) {
	definition := &DesignerDefinition{
		ID:          "invalid-reject-target",
		Name:        "Invalid Reject Target",
		Status:      StatusDraft,
		StartNodeID: "start",
		Nodes: []DesignerNode{
			{ID: "start", Name: "Start", Type: NodeKindBranch, Position: Position{X: 0, Y: 0}},
			{ID: "left", Name: "Left", Type: NodeKindTask, Position: Position{X: 120, Y: -80}},
			{ID: "right", Name: "Right", Type: NodeKindTask, Position: Position{X: 120, Y: 80}},
			{ID: "join", Name: "Join", Type: NodeKindJoin, RejectTo: []string{"left"}, Position: Position{X: 240, Y: 0}},
		},
		Edges: []DesignerEdge{
			{ID: "start-left", Source: "start", Target: "left"},
			{ID: "start-right", Source: "start", Target: "right"},
			{ID: "left-join", Source: "left", Target: "join"},
			{ID: "right-join", Source: "right", Target: "join"},
		},
	}

	if err := ValidateDesignerDefinition(definition); err != nil {
		t.Fatalf("designer validation should leave DAG dominance checks to runtime, got %v", err)
	}
	if _, err := definition.ToRuntimeDefinition(); err == nil {
		t.Fatal("expected runtime projection to reject non-dominating reject target")
	}
}

func TestValidateDesignerDefinitionRejectsInvalidRejectToReferences(t *testing.T) {
	tests := []struct {
		name     string
		rejectTo []string
	}{
		{name: "missing", rejectTo: []string{"missing"}},
		{name: "blank", rejectTo: []string{" "}},
		{name: "duplicate", rejectTo: []string{"submit", "submit"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			definition := &DesignerDefinition{
				ID:          "invalid-reject-reference-" + tt.name,
				Name:        "Invalid Reject Reference",
				Status:      StatusDraft,
				StartNodeID: "submit",
				Nodes: []DesignerNode{
					{ID: "submit", Name: "Submit", Type: NodeKindTask, Position: Position{X: 0, Y: 0}},
					{ID: "review", Name: "Review", Type: NodeKindTask, RejectTo: tt.rejectTo, Position: Position{X: 120, Y: 0}},
				},
				Edges: []DesignerEdge{{ID: "submit-review", Source: "submit", Target: "review"}},
			}
			if err := ValidateDesignerDefinition(definition); err == nil {
				t.Fatal("expected invalid reject_to reference to be rejected")
			}
		})
	}
}

func TestToRuntimeDefinitionRejectsCyclicGraph(t *testing.T) {
	definition := &DesignerDefinition{
		ID:          "cyclic-runtime",
		Name:        "Cyclic Runtime",
		Status:      StatusDraft,
		StartNodeID: "start",
		Nodes: []DesignerNode{
			{ID: "start", Name: "Start", Type: NodeKindTask, Next: []string{"review"}, Position: Position{X: 0, Y: 0}},
			{ID: "review", Name: "Review", Type: NodeKindTask, Next: []string{"approve"}, Position: Position{X: 120, Y: 0}},
			{ID: "approve", Name: "Approve", Type: NodeKindTask, Next: []string{"review"}, Position: Position{X: 240, Y: 0}},
		},
	}

	if err := ValidateDesignerDefinition(definition); err != nil {
		t.Fatalf("designer validation should leave DAG checks to runtime, got %v", err)
	}
	if _, err := definition.ToRuntimeDefinition(); err == nil {
		t.Fatal("expected runtime projection to reject cyclic graph")
	}
}

func TestValidateDesignerDefinitionRejectsIncomingStartNode(t *testing.T) {
	definition := &DesignerDefinition{
		ID:          "incoming-start",
		Name:        "Incoming Start",
		Status:      StatusDraft,
		StartNodeID: "start",
		Nodes: []DesignerNode{
			{ID: "start", Name: "Start", Type: NodeKindTask, Position: Position{X: 0, Y: 0}},
			{ID: "loop", Name: "Loop", Type: NodeKindTask, Position: Position{X: 120, Y: 0}},
		},
		Edges: []DesignerEdge{{ID: "back", Source: "loop", Target: "start"}},
	}

	if err := ValidateDesignerDefinition(definition); err == nil {
		t.Fatal("expected incoming start edge to be rejected")
	}
}

func TestValidateDesignerDefinitionRejectsSurroundingWhitespaceInIDs(t *testing.T) {
	definition := &DesignerDefinition{
		ID:          "surrounding-whitespace",
		Name:        "Surrounding Whitespace",
		Status:      StatusDraft,
		StartNodeID: "start",
		Nodes: []DesignerNode{
			{ID: "start", Name: "Start", Type: NodeKindTask, Position: Position{X: 0, Y: 0}},
			{ID: " end", Name: "End", Type: NodeKindTask, Position: Position{X: 120, Y: 0}},
		},
		Edges: []DesignerEdge{
			{ID: "edge-1", Source: "start", Target: " end"},
		},
	}

	if err := ValidateDesignerDefinition(definition); err == nil {
		t.Fatal("expected surrounding whitespace in node id to be rejected")
	}
}

func TestTaskChoiceMergeProjectsAsExclusiveMerge(t *testing.T) {
	ctx := t.Context()
	definition := &DesignerDefinition{
		ID:          "choice-merge-runtime",
		Name:        "Choice Merge Runtime",
		Status:      StatusDraft,
		StartNodeID: "review",
		Nodes: []DesignerNode{
			{ID: "review", Name: "Review", Type: NodeKindBranch, Position: Position{X: 0, Y: 0}},
			{ID: "manager_review", Name: "Manager Review", Type: NodeKindTask, Position: Position{X: 120, Y: 80}},
			{ID: "approved", Name: "Approved", Type: NodeKindTask, Position: Position{X: 240, Y: 0}},
		},
		Edges: []DesignerEdge{
			{ID: "direct", Source: "review", Target: "approved"},
			{ID: "manager", Source: "review", Target: "manager_review"},
			{ID: "manager-approved", Source: "manager_review", Target: "approved"},
		},
	}

	definition.Normalize(time.Now())
	runtimeDef, err := definition.ToRuntimeDefinition()
	if err != nil {
		t.Fatalf("ToRuntimeDefinition returned error: %v", err)
	}
	store := runtime.NewMemoryStore()
	engine := runtime.NewEngine(store)
	if err := engine.SaveDefinition(ctx, runtimeDef); err != nil {
		t.Fatalf("SaveDefinition returned error: %v", err)
	}
	if err := engine.CreateInstance(ctx, "choice-merge-1", definition.ID); err != nil {
		t.Fatalf("CreateInstance returned error: %v", err)
	}
	if err := engine.StartInstance(ctx, "choice-merge-1"); err != nil {
		t.Fatalf("StartInstance returned error: %v", err)
	}
	if err := engine.AdvanceNodeTo(ctx, "choice-merge-1", "review", "approved"); err != nil {
		t.Fatalf("AdvanceNodeTo returned error: %v", err)
	}
	state, err := store.Get(ctx, "choice-merge-1")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if len(state.ActiveNodeIDs) != 1 || state.ActiveNodeIDs[0] != "approved" {
		t.Fatalf("active nodes = %#v, want approved", state.ActiveNodeIDs)
	}
	if len(state.PendingJoins) != 0 {
		t.Fatalf("pending joins = %#v, want empty", state.PendingJoins)
	}
}

func TestJoinNodeCanFanOutAfterAllIncomingBranchesArrive(t *testing.T) {
	ctx := t.Context()
	definition := &DesignerDefinition{
		ID:          "join-fanout-runtime",
		Name:        "Join Fanout Runtime",
		Status:      StatusDraft,
		StartNodeID: "start",
		Nodes: []DesignerNode{
			{ID: "start", Name: "Start", Type: NodeKindBranch, Position: Position{X: 0, Y: 0}},
			{ID: "left", Name: "Left", Type: NodeKindTask, Position: Position{X: 120, Y: -80}},
			{ID: "right", Name: "Right", Type: NodeKindTask, Position: Position{X: 120, Y: 80}},
			{ID: "join", Name: "Join", Type: NodeKindJoin, Position: Position{X: 240, Y: 0}},
			{ID: "archive", Name: "Archive", Type: NodeKindTask, Position: Position{X: 360, Y: -80}},
			{ID: "notify", Name: "Notify", Type: NodeKindTask, Position: Position{X: 360, Y: 80}},
		},
		Edges: []DesignerEdge{
			{ID: "start-left", Source: "start", Target: "left"},
			{ID: "start-right", Source: "start", Target: "right"},
			{ID: "left-join", Source: "left", Target: "join"},
			{ID: "right-join", Source: "right", Target: "join"},
			{ID: "join-archive", Source: "join", Target: "archive"},
			{ID: "join-notify", Source: "join", Target: "notify"},
		},
	}

	runtimeDef, err := definition.ToRuntimeDefinition()
	if err != nil {
		t.Fatalf("ToRuntimeDefinition returned error: %v", err)
	}
	kindByNode := map[string]runtime.NodeKind{}
	for _, node := range runtimeDef.Nodes {
		kindByNode[node.ID] = node.Kind
	}
	if kindByNode["join"] != runtime.NodeKindJoin {
		t.Fatalf("join kind = %s, want join", kindByNode["join"])
	}

	store := runtime.NewMemoryStore()
	engine := runtime.NewEngine(store)
	if err := engine.SaveDefinition(ctx, runtimeDef); err != nil {
		t.Fatalf("SaveDefinition returned error: %v", err)
	}
	if err := engine.CreateInstance(ctx, "join-fanout-1", definition.ID); err != nil {
		t.Fatalf("CreateInstance returned error: %v", err)
	}
	if err := engine.StartInstance(ctx, "join-fanout-1"); err != nil {
		t.Fatalf("StartInstance returned error: %v", err)
	}
	if err := engine.AdvanceNode(ctx, "join-fanout-1", "start"); err != nil {
		t.Fatalf("AdvanceNode start returned error: %v", err)
	}
	if err := engine.AdvanceNode(ctx, "join-fanout-1", "left"); err != nil {
		t.Fatalf("AdvanceNode left returned error: %v", err)
	}
	state, err := store.Get(ctx, "join-fanout-1")
	if err != nil {
		t.Fatalf("Get after left returned error: %v", err)
	}
	if len(state.PendingJoins) != 1 || state.PendingJoins[0].NodeID != "join" {
		t.Fatalf("pending joins after left = %#v, want join wait", state.PendingJoins)
	}
	if err := engine.AdvanceNode(ctx, "join-fanout-1", "right"); err != nil {
		t.Fatalf("AdvanceNode right returned error: %v", err)
	}
	if err := engine.AdvanceNode(ctx, "join-fanout-1", "join"); err != nil {
		t.Fatalf("AdvanceNode join returned error: %v", err)
	}
	state, err = store.Get(ctx, "join-fanout-1")
	if err != nil {
		t.Fatalf("Get after join returned error: %v", err)
	}
	if !sameStringSet(state.ActiveNodeIDs, []string{"archive", "notify"}) {
		t.Fatalf("active nodes after join fanout = %#v, want archive and notify", state.ActiveNodeIDs)
	}
}

func TestToRuntimeDefinitionInfersNodeKindWithoutNormalize(t *testing.T) {
	definition := &DesignerDefinition{
		ID:          "implicit-kind-choice",
		Name:        "Implicit Kind Choice",
		Status:      StatusDraft,
		StartNodeID: "review",
		Nodes: []DesignerNode{
			{ID: "review", Name: "Review", Position: Position{X: 0, Y: 0}},
			{ID: "manager_review", Name: "Manager Review", Position: Position{X: 120, Y: 80}},
			{ID: "approved", Name: "Approved", Position: Position{X: 240, Y: 0}},
		},
		Edges: []DesignerEdge{
			{ID: "direct", Source: "review", Target: "approved"},
			{ID: "manager", Source: "review", Target: "manager_review"},
			{ID: "manager-approved", Source: "manager_review", Target: "approved"},
		},
	}

	runtimeDef, err := definition.ToRuntimeDefinition()
	if err != nil {
		t.Fatalf("ToRuntimeDefinition returned error: %v", err)
	}
	kinds := map[string]runtime.NodeKind{}
	for _, node := range runtimeDef.Nodes {
		kinds[node.ID] = node.Kind
	}
	if kinds["review"] != runtime.NodeKindBranch {
		t.Fatalf("review kind = %s, want branch", kinds["review"])
	}
	if kinds["approved"] != runtime.NodeKindJoin {
		t.Fatalf("approved kind = %s, want join", kinds["approved"])
	}
}

func sameStringSet(got []string, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	counts := make(map[string]int, len(got))
	for _, value := range got {
		counts[value]++
	}
	for _, value := range want {
		if counts[value] == 0 {
			return false
		}
		counts[value]--
	}
	return true
}

func TestToRuntimeDefinitionDoesNotShareNextSlices(t *testing.T) {
	definition := &DesignerDefinition{
		ID:          "slice-isolation",
		Name:        "Slice Isolation",
		Status:      StatusDraft,
		StartNodeID: "start",
		Nodes: []DesignerNode{
			{ID: "start", Name: "Start", Type: NodeKindTask, Next: []string{"end"}, Position: Position{X: 0, Y: 0}},
			{ID: "end", Name: "End", Type: NodeKindTask, Position: Position{X: 120, Y: 0}},
		},
	}

	definition.Normalize(time.Now())
	runtimeDef, err := definition.ToRuntimeDefinition()
	if err != nil {
		t.Fatalf("ToRuntimeDefinition returned error: %v", err)
	}
	runtimeDef.Nodes[0].Edges[0].Target = "mutated"

	if got := definition.Nodes[0].Next[0]; got != "end" {
		t.Fatalf("designer next was mutated through runtime projection: %q", got)
	}
}

func TestRuntimeDefinitionFromRuntimePreservesRuntimeNodeKindWithoutDesign(t *testing.T) {
	definition := &runtime.Definition{
		ID:          "runtime-kind",
		Name:        "Runtime Kind",
		StartNodeID: "review",
		Nodes: []runtime.Node{
			{ID: "review", Name: "Review", Kind: runtime.NodeKindBranch, Edges: []runtime.Edge{{Target: "approved"}, {Target: "manager_review"}}},
			{ID: "manager_review", Name: "Manager Review", Kind: runtime.NodeKindTask, Edges: []runtime.Edge{{Target: "approved"}}},
			{ID: "approved", Name: "Approved", Kind: runtime.NodeKindTask},
		},
	}

	preview := RuntimeDefinitionFromRuntime(definition, nil)
	for _, node := range preview.Nodes {
		if node.ID == "approved" && node.Type != NodeKindTask {
			t.Fatalf("approved node type = %s, want task", node.Type)
		}
	}
}

func TestInstanceStateFromRuntimeClonesData(t *testing.T) {
	state := &runtime.State{
		ID:           "clone-data",
		DefinitionID: "definition",
		Data: map[string]any{
			"items": []map[string]any{
				{"value": "original"},
			},
			"labels": map[string]string{"state": "original"},
			"counts": []int{1, 2},
		},
	}

	dto := InstanceStateFromRuntime(state)
	dto.Data["items"].([]map[string]any)[0]["value"] = "changed"
	dto.Data["labels"].(map[string]string)["state"] = "changed"
	dto.Data["counts"].([]int)[0] = 99

	if got := state.Data["items"].([]map[string]any)[0]["value"]; got != "original" {
		t.Fatalf("runtime state data value = %v, want original", got)
	}
	if got := state.Data["labels"].(map[string]string)["state"]; got != "original" {
		t.Fatalf("runtime state data label = %v, want original", got)
	}
	if got := state.Data["counts"].([]int)[0]; got != 1 {
		t.Fatalf("runtime state data count = %v, want 1", got)
	}
}

func TestInstanceMetadataFromDataPreservesTypedMetadataShape(t *testing.T) {
	location := time.FixedZone("UTC+8", 8*60*60)
	now := time.Date(2026, 5, 28, 9, 30, 0, 0, location)
	raw := &InstanceMetadata{
		CCUserIDs: []string{},
		TaskAssignments: map[string]TaskAssignment{
			"review": {
				CandidateUserIDs: []string{},
				CandidateRoleIDs: []string{},
				ClaimedAt:        &now,
				DueAt:            &now,
				UpdatedAt:        &now,
			},
		},
		Actions: []WorkflowAction{},
	}

	meta := InstanceMetadataFromData(map[string]any{
		WorkflowMetadataDataKey: raw,
	})
	if meta == nil {
		t.Fatal("InstanceMetadataFromData returned nil")
	}
	if meta == raw {
		t.Fatal("InstanceMetadataFromData reused original metadata pointer")
	}
	if meta.CCUserIDs == nil {
		t.Fatal("CCUserIDs = nil, want empty slice preserved")
	}
	if meta.Actions == nil {
		t.Fatal("Actions = nil, want empty slice preserved")
	}
	assignment := meta.TaskAssignments["review"]
	if assignment.CandidateUserIDs == nil {
		t.Fatal("CandidateUserIDs = nil, want empty slice preserved")
	}
	if assignment.CandidateRoleIDs == nil {
		t.Fatal("CandidateRoleIDs = nil, want empty slice preserved")
	}
	if assignment.ClaimedAt == raw.TaskAssignments["review"].ClaimedAt {
		t.Fatal("ClaimedAt pointer was not cloned")
	}
	if assignment.DueAt == raw.TaskAssignments["review"].DueAt {
		t.Fatal("DueAt pointer was not cloned")
	}
	if assignment.UpdatedAt == raw.TaskAssignments["review"].UpdatedAt {
		t.Fatal("UpdatedAt pointer was not cloned")
	}
	if assignment.ClaimedAt.Location() != time.UTC {
		t.Fatalf("ClaimedAt location = %v, want UTC", assignment.ClaimedAt.Location())
	}
	if !assignment.ClaimedAt.Equal(now) {
		t.Fatalf("ClaimedAt = %v, want instant %v", assignment.ClaimedAt, now)
	}

	meta.CCUserIDs = append(meta.CCUserIDs, "u-1")
	assignment.CandidateUserIDs = append(assignment.CandidateUserIDs, "candidate-1")
	assignment.CandidateRoleIDs = append(assignment.CandidateRoleIDs, "role-1")
	claimedAt := assignment.ClaimedAt.Add(time.Hour)
	*assignment.ClaimedAt = claimedAt
	meta.TaskAssignments["review"] = assignment
	meta.Actions = append(meta.Actions, WorkflowAction{Action: "complete", At: now})

	if got := len(raw.CCUserIDs); got != 0 {
		t.Fatalf("raw CCUserIDs length = %d, want 0", got)
	}
	rawAssignment := raw.TaskAssignments["review"]
	if got := len(rawAssignment.CandidateUserIDs); got != 0 {
		t.Fatalf("raw CandidateUserIDs length = %d, want 0", got)
	}
	if got := len(rawAssignment.CandidateRoleIDs); got != 0 {
		t.Fatalf("raw CandidateRoleIDs length = %d, want 0", got)
	}
	if got := len(raw.Actions); got != 0 {
		t.Fatalf("raw Actions length = %d, want 0", got)
	}
	if rawAssignment.ClaimedAt == nil || !rawAssignment.ClaimedAt.Equal(now) {
		t.Fatalf("raw ClaimedAt = %v, want %v", rawAssignment.ClaimedAt, now)
	}
}

func TestPublicInstanceDataSkipsInternalWorkflowMetadata(t *testing.T) {
	data := map[string]any{
		WorkflowMetadataDataKey: &InstanceMetadata{
			CCUserIDs: []string{"internal"},
		},
		"items": []map[string]any{
			{"value": "original"},
		},
	}

	public := PublicInstanceData(data)
	if _, ok := public[WorkflowMetadataDataKey]; ok {
		t.Fatal("public data leaked internal workflow metadata")
	}

	public["items"].([]map[string]any)[0]["value"] = "changed"
	if got := data["items"].([]map[string]any)[0]["value"]; got != "original" {
		t.Fatalf("public data mutation leaked back to source: %v", got)
	}
}
