package workflowservice

import (
	"gochen-workflow/internal/jsonclone"
	workflowtypes "gochen-workflow/types"

	runtime "gochen/process/workflow"
)

func cloneDesign(def *workflowtypes.DesignerDefinition) *workflowtypes.DesignerDefinition {
	if def == nil {
		return nil
	}
	clone := *def
	if def.PublishedAt != nil {
		publishedAt := *def.PublishedAt
		clone.PublishedAt = &publishedAt
	}
	if def.Nodes != nil {
		clone.Nodes = make([]workflowtypes.DesignerNode, len(def.Nodes))
		for i := range def.Nodes {
			clone.Nodes[i] = def.Nodes[i]
			clone.Nodes[i].Next = cloneStrings(def.Nodes[i].Next)
			clone.Nodes[i].RejectTo = cloneStrings(def.Nodes[i].RejectTo)
			clone.Nodes[i].UI = cloneMap(def.Nodes[i].UI)
			clone.Nodes[i].Config = cloneMap(def.Nodes[i].Config)
		}
	}
	if def.Edges != nil {
		clone.Edges = make([]workflowtypes.DesignerEdge, len(def.Edges))
		for i := range def.Edges {
			clone.Edges[i] = def.Edges[i]
			clone.Edges[i].Condition = cloneMap(def.Edges[i].Condition)
		}
	}
	clone.RuntimeDefinition = cloneRuntimeDefinitionPreview(def.RuntimeDefinition)
	return &clone
}

func cloneDesigns(items []*workflowtypes.DesignerDefinition) []*workflowtypes.DesignerDefinition {
	if items == nil {
		return nil
	}
	out := make([]*workflowtypes.DesignerDefinition, len(items))
	for i := range items {
		out[i] = cloneDesign(items[i])
	}
	return out
}

func cloneRuntimeDefinitionPreview(def *workflowtypes.RuntimeDefinition) *workflowtypes.RuntimeDefinition {
	if def == nil {
		return nil
	}
	clone := &workflowtypes.RuntimeDefinition{
		ID:          def.ID,
		Name:        def.Name,
		StartNodeID: def.StartNodeID,
	}
	if def.Nodes != nil {
		clone.Nodes = make([]workflowtypes.RuntimeNode, len(def.Nodes))
		for i := range def.Nodes {
			clone.Nodes[i] = def.Nodes[i]
			clone.Nodes[i].Next = cloneStrings(def.Nodes[i].Next)
			clone.Nodes[i].RejectTo = cloneStrings(def.Nodes[i].RejectTo)
		}
	}
	return clone
}

func cloneRuntimeDefinition(def *runtime.Definition) *runtime.Definition {
	if def == nil {
		return nil
	}
	clone := &runtime.Definition{
		ID:          def.ID,
		Name:        def.Name,
		StartNodeID: def.StartNodeID,
		CreatedAt:   def.CreatedAt,
		UpdatedAt:   def.UpdatedAt,
	}
	if def.Nodes != nil {
		clone.Nodes = make([]runtime.Node, len(def.Nodes))
		for i := range def.Nodes {
			clone.Nodes[i] = runtime.Node{ID: def.Nodes[i].ID, Name: def.Nodes[i].Name, Kind: def.Nodes[i].Kind}
			clone.Nodes[i].Next = cloneStrings(def.Nodes[i].Next)
			clone.Nodes[i].RejectTo = cloneStrings(def.Nodes[i].RejectTo)
		}
	}
	return clone
}

func cloneState(st *runtime.State) *runtime.State {
	if st == nil {
		return nil
	}
	clone := &runtime.State{
		ID:            st.ID,
		DefinitionID:  st.DefinitionID,
		Version:       st.Version,
		Status:        st.Status,
		CurrentNodeID: st.CurrentNodeID,
		StartedAt:     st.StartedAt,
		CompletedAt:   st.CompletedAt,
		CreatedAt:     st.CreatedAt,
		UpdatedAt:     st.UpdatedAt,
	}
	clone.ActiveNodeIDs = cloneStrings(st.ActiveNodeIDs)
	clone.CompletedNodeIDs = cloneStrings(st.CompletedNodeIDs)
	if st.PendingJoins != nil {
		clone.PendingJoins = make([]runtime.PendingJoin, len(st.PendingJoins))
		for i := range st.PendingJoins {
			clone.PendingJoins[i] = st.PendingJoins[i]
			clone.PendingJoins[i].ArrivedFrom = cloneStrings(st.PendingJoins[i].ArrivedFrom)
		}
	}
	if st.History != nil {
		clone.History = make([]runtime.HistoryEntry, len(st.History))
		copy(clone.History, st.History)
	}
	clone.Data = cloneMap(st.Data)
	return clone
}

func cloneMap(values map[string]any) map[string]any {
	return jsonclone.Map(values)
}

func cloneStrings(values []string) []string {
	if values == nil {
		return nil
	}
	out := make([]string, len(values))
	copy(out, values)
	return out
}
