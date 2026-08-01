package workflowtypes

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"gochen-workflow/internal/jsonclone"

	"gochen/errors"
	runtime "gochen/process/workflow"
)

const (
	WorkflowMetadataDataKey = "_workflow"

	// StatusDraft 表示流程定义仍可编辑。
	StatusDraft DefinitionStatus = "draft"
	// StatusPublished 表示流程定义已发布，可创建运行实例。
	StatusPublished DefinitionStatus = "published"
	// StatusArchived 表示流程定义已归档，不再允许创建新实例。
	StatusArchived DefinitionStatus = "archived"

	// NodeKindTask 表示普通任务节点。
	NodeKindTask NodeKind = "task"
	// NodeKindBranch 表示并行分支节点。
	NodeKindBranch NodeKind = "branch"
	// NodeKindJoin 表示等待多个前驱到达的汇聚节点。
	NodeKindJoin NodeKind = "join"
)

// DefinitionStatus 表示流程定义在设计、发布与归档之间的生命周期状态。
type DefinitionStatus string

// NodeKind 表示设计器节点的执行语义，并会投影到运行时 Node.Kind。
type NodeKind string

// Position 表示设计器画布中的节点坐标。
type Position struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

// Viewport 表示设计器画布视口状态。
type Viewport struct {
	X    float64 `json:"x"`
	Y    float64 `json:"y"`
	Zoom float64 `json:"zoom"`
}

// DesignerNode 表示设计态节点，包含 UI 与配置元数据。
type DesignerNode struct {
	ID       string         `json:"id"`
	Name     string         `json:"name"`
	Type     NodeKind       `json:"type"`
	Next     []string       `json:"next,omitempty"`
	RejectTo []string       `json:"reject_to,omitempty"`
	Position Position       `json:"position"`
	UI       map[string]any `json:"ui,omitempty"`
	Config   map[string]any `json:"config,omitempty"`
}

// DesignerEdge 表示设计态边，运行时会投影为节点 Next 关系。
type DesignerEdge struct {
	ID        string         `json:"id"`
	Source    string         `json:"source"`
	Target    string         `json:"target"`
	Label     string         `json:"label,omitempty"`
	Condition map[string]any `json:"condition,omitempty"`
}

// DesignerDefinition 表示前端设计器保存的完整流程定义。
type DesignerDefinition struct {
	ID                string             `json:"id"`
	Name              string             `json:"name"`
	Description       string             `json:"description,omitempty"`
	Version           int                `json:"version"`
	Status            DefinitionStatus   `json:"status"`
	StartNodeID       string             `json:"start_node_id"`
	Nodes             []DesignerNode     `json:"nodes"`
	Edges             []DesignerEdge     `json:"edges"`
	Viewport          Viewport           `json:"viewport"`
	RuntimeDefinition *RuntimeDefinition `json:"runtime_definition,omitempty"`
	CreatedAt         time.Time          `json:"created_at"`
	UpdatedAt         time.Time          `json:"updated_at"`
	PublishedAt       *time.Time         `json:"published_at,omitempty"`
}

// DefinitionStats 表示流程定义列表的全量状态统计。
type DefinitionStats struct {
	Total     int `json:"total"`
	Draft     int `json:"draft"`
	Published int `json:"published"`
	Archived  int `json:"archived"`
}

// DefinitionListResponse 是定义列表接口响应体，items 保留原列表语义。
type DefinitionListResponse struct {
	Items        []*DesignerDefinition `json:"items"`
	Total        int                   `json:"total"`
	Page         int                   `json:"page"`
	Size         int                   `json:"size"`
	Stats        DefinitionStats       `json:"stats"`
	StatusCounts DefinitionStats       `json:"status_counts"`
}

// DefinitionQuery 表示流程定义列表的筛选与分页条件。
type DefinitionQuery struct {
	Page    int
	Size    int
	Keyword string
	Status  DefinitionStatus
}

// RuntimeNode 是 HTTP 响应中用于预览 runtime 投影的节点 DTO。
type RuntimeNode struct {
	ID       string   `json:"id"`
	Name     string   `json:"name"`
	Type     NodeKind `json:"type"`
	Next     []string `json:"next"`
	RejectTo []string `json:"reject_to,omitempty"`
}

// RuntimeDefinition 是 HTTP 响应中稳定的 runtime 投影 DTO，避免直接暴露核心包结构体。
type RuntimeDefinition struct {
	ID          string        `json:"id"`
	Name        string        `json:"name"`
	StartNodeID string        `json:"start_node_id"`
	Nodes       []RuntimeNode `json:"nodes"`
}

// InstanceHistoryEntry 是 API 层暴露的实例历史记录，字段命名保持 JSON snake_case。
type InstanceHistoryEntry struct {
	Action     string    `json:"action"`
	NodeID     string    `json:"node_id,omitempty"`
	FromNodeID string    `json:"from_node_id,omitempty"`
	At         time.Time `json:"at"`
}

// InstancePendingJoin 是 API 层暴露的汇聚等待状态。
type InstancePendingJoin struct {
	NodeID        string   `json:"node_id"`
	ExpectedCount int      `json:"expected_count"`
	ArrivedFrom   []string `json:"arrived_from"`
}

// InstanceState 是工作流实例响应 DTO，避免把核心 runtime 结构体字段名泄漏到 HTTP API。
type InstanceState struct {
	ID               string                 `json:"id"`
	DefinitionID     string                 `json:"definition_id"`
	DefinitionName   string                 `json:"definition_name,omitempty"`
	BusinessKey      string                 `json:"business_key,omitempty"`
	Title            string                 `json:"title,omitempty"`
	InitiatorID      string                 `json:"initiator_id,omitempty"`
	InitiatorName    string                 `json:"initiator_name,omitempty"`
	Version          uint64                 `json:"version"`
	Status           runtime.InstanceStatus `json:"status"`
	CurrentNodeID    string                 `json:"current_node_id,omitempty"`
	ActiveNodeIDs    []string               `json:"active_node_ids"`
	PendingJoins     []InstancePendingJoin  `json:"pending_joins"`
	CompletedNodeIDs []string               `json:"completed_node_ids"`
	History          []InstanceHistoryEntry `json:"history"`
	Data             map[string]any         `json:"data"`
	StartedAt        *time.Time             `json:"started_at,omitempty"`
	CompletedAt      *time.Time             `json:"completed_at,omitempty"`
	CreatedAt        time.Time              `json:"created_at"`
	UpdatedAt        time.Time              `json:"updated_at"`
}

type TaskAssignment struct {
	AssigneeID       string     `json:"assignee_id,omitempty"`
	AssigneeName     string     `json:"assignee_name,omitempty"`
	CandidateUserIDs []string   `json:"candidate_user_ids,omitempty"`
	CandidateRoleIDs []string   `json:"candidate_role_ids,omitempty"`
	DelegatorID      string     `json:"delegator_id,omitempty"`
	DelegatorName    string     `json:"delegator_name,omitempty"`
	ClaimedAt        *time.Time `json:"claimed_at,omitempty"`
	DueAt            *time.Time `json:"due_at,omitempty"`
	UpdatedAt        *time.Time `json:"updated_at,omitempty"`
}

type WorkflowAction struct {
	Action       string    `json:"action"`
	NodeID       string    `json:"node_id,omitempty"`
	TargetNodeID string    `json:"target_node_id,omitempty"`
	ActorID      string    `json:"actor_id,omitempty"`
	ActorName    string    `json:"actor_name,omitempty"`
	Comment      string    `json:"comment,omitempty"`
	At           time.Time `json:"at"`
}

type InstanceMetadata struct {
	BusinessKey     string                    `json:"business_key,omitempty"`
	Title           string                    `json:"title,omitempty"`
	InitiatorID     string                    `json:"initiator_id,omitempty"`
	InitiatorName   string                    `json:"initiator_name,omitempty"`
	CCUserIDs       []string                  `json:"cc_user_ids,omitempty"`
	TaskAssignments map[string]TaskAssignment `json:"task_assignments,omitempty"`
	Actions         []WorkflowAction          `json:"actions,omitempty"`
}

// WorkItem 表示 workflow 运行时投影出的工作项；todo 来自活动节点，done 来自已推进节点。
type WorkItem struct {
	ID               string                 `json:"id"`
	Type             string                 `json:"type"`
	InstanceID       string                 `json:"instance_id"`
	DefinitionID     string                 `json:"definition_id"`
	DefinitionName   string                 `json:"definition_name,omitempty"`
	BusinessKey      string                 `json:"business_key,omitempty"`
	Title            string                 `json:"title,omitempty"`
	NodeID           string                 `json:"node_id"`
	NodeName         string                 `json:"node_name,omitempty"`
	NodeType         NodeKind               `json:"node_type,omitempty"`
	Status           runtime.InstanceStatus `json:"status"`
	InitiatorID      string                 `json:"initiator_id,omitempty"`
	InitiatorName    string                 `json:"initiator_name,omitempty"`
	AssigneeID       string                 `json:"assignee_id,omitempty"`
	AssigneeName     string                 `json:"assignee_name,omitempty"`
	CandidateUserIDs []string               `json:"candidate_user_ids,omitempty"`
	CandidateRoleIDs []string               `json:"candidate_role_ids,omitempty"`
	DelegatorID      string                 `json:"delegator_id,omitempty"`
	DelegatorName    string                 `json:"delegator_name,omitempty"`
	CCUserIDs        []string               `json:"cc_user_ids,omitempty"`
	ActorID          string                 `json:"actor_id,omitempty"`
	ActorName        string                 `json:"actor_name,omitempty"`
	Action           string                 `json:"action,omitempty"`
	Comment          string                 `json:"comment,omitempty"`
	Data             map[string]any         `json:"data,omitempty"`
	DueAt            *time.Time             `json:"due_at,omitempty"`
	Overdue          bool                   `json:"overdue,omitempty"`
	CreatedAt        time.Time              `json:"created_at"`
	UpdatedAt        time.Time              `json:"updated_at"`
	StartedAt        *time.Time             `json:"started_at,omitempty"`
	CompletedAt      *time.Time             `json:"completed_at,omitempty"`
}

// WorkItemPage 是 workflow 工作项分页响应。
type WorkItemPage struct {
	Items []WorkItem `json:"items"`
	Total int        `json:"total"`
	Page  int        `json:"page"`
	Size  int        `json:"size"`
}

type InstanceSummary struct {
	ID             string                 `json:"id"`
	DefinitionID   string                 `json:"definition_id"`
	DefinitionName string                 `json:"definition_name,omitempty"`
	BusinessKey    string                 `json:"business_key,omitempty"`
	Title          string                 `json:"title,omitempty"`
	Status         runtime.InstanceStatus `json:"status"`
	ActiveNodeIDs  []string               `json:"active_node_ids,omitempty"`
	CurrentNodeID  string                 `json:"current_node_id,omitempty"`
	InitiatorID    string                 `json:"initiator_id,omitempty"`
	InitiatorName  string                 `json:"initiator_name,omitempty"`
	AssigneeIDs    []string               `json:"assignee_ids,omitempty"`
	CCUserIDs      []string               `json:"cc_user_ids,omitempty"`
	EarliestDueAt  *time.Time             `json:"earliest_due_at,omitempty"`
	OverdueCount   int                    `json:"overdue_count,omitempty"`
	CreatedAt      time.Time              `json:"created_at"`
	UpdatedAt      time.Time              `json:"updated_at"`
	StartedAt      *time.Time             `json:"started_at,omitempty"`
	CompletedAt    *time.Time             `json:"completed_at,omitempty"`
}

type InstancePage struct {
	Items []InstanceSummary `json:"items"`
	Total int               `json:"total"`
	Page  int               `json:"page"`
	Size  int               `json:"size"`
}

type CreateInstanceInput struct {
	ID               string         `json:"id"`
	DefinitionID     string         `json:"definition_id"`
	BusinessKey      string         `json:"business_key,omitempty"`
	Title            string         `json:"title,omitempty"`
	InitiatorID      string         `json:"initiator_id,omitempty"`
	InitiatorName    string         `json:"initiator_name,omitempty"`
	AssigneeID       string         `json:"assignee_id,omitempty"`
	AssigneeName     string         `json:"assignee_name,omitempty"`
	CandidateUserIDs []string       `json:"candidate_user_ids,omitempty"`
	CandidateRoleIDs []string       `json:"candidate_role_ids,omitempty"`
	CCUserIDs        []string       `json:"cc_user_ids,omitempty"`
	DueAt            *time.Time     `json:"due_at,omitempty"`
	Data             map[string]any `json:"data,omitempty"`
}

type AdvanceNodeInput struct {
	NextNodeID       string     `json:"next_node_id,omitempty"`
	NextNodeIDs      []string   `json:"next_node_ids,omitempty"`
	AssigneeID       string     `json:"assignee_id,omitempty"`
	AssigneeName     string     `json:"assignee_name,omitempty"`
	CandidateUserIDs []string   `json:"candidate_user_ids,omitempty"`
	CandidateRoleIDs []string   `json:"candidate_role_ids,omitempty"`
	CCUserIDs        []string   `json:"cc_user_ids,omitempty"`
	DueAt            *time.Time `json:"due_at,omitempty"`
	Comment          string     `json:"comment,omitempty"`
}

type TaskActionInput struct {
	AssigneeID       string     `json:"assignee_id,omitempty"`
	AssigneeName     string     `json:"assignee_name,omitempty"`
	CandidateUserIDs []string   `json:"candidate_user_ids,omitempty"`
	CandidateRoleIDs []string   `json:"candidate_role_ids,omitempty"`
	CCUserIDs        []string   `json:"cc_user_ids,omitempty"`
	DueAt            *time.Time `json:"due_at,omitempty"`
	Comment          string     `json:"comment,omitempty"`
}

type RejectNodeInput struct {
	TargetNodeID     string     `json:"target_node_id"`
	Reason           string     `json:"reason,omitempty"`
	AssigneeID       string     `json:"assignee_id,omitempty"`
	AssigneeName     string     `json:"assignee_name,omitempty"`
	CandidateUserIDs []string   `json:"candidate_user_ids,omitempty"`
	CandidateRoleIDs []string   `json:"candidate_role_ids,omitempty"`
	CCUserIDs        []string   `json:"cc_user_ids,omitempty"`
	DueAt            *time.Time `json:"due_at,omitempty"`
	Comment          string     `json:"comment,omitempty"`
}

type WorkItemQuery struct {
	Page         int
	Size         int
	UserID       string
	DefinitionID string
	Keyword      string
}

type InstanceQuery struct {
	Page         int
	Size         int
	Status       string
	DefinitionID string
	ActiveNodeID string
	InitiatorID  string
	AssigneeID   string
	Keyword      string
}

// InstanceStateFromRuntime 将核心实例状态映射为稳定的 HTTP DTO。
func InstanceStateFromRuntime(st *runtime.State) *InstanceState {
	if st == nil {
		return nil
	}
	out := &InstanceState{
		ID:               string(st.ID),
		DefinitionID:     st.DefinitionID,
		Version:          st.Version,
		Status:           st.Status,
		CurrentNodeID:    st.CurrentNodeID,
		ActiveNodeIDs:    append([]string(nil), st.ActiveNodeIDs...),
		CompletedNodeIDs: append([]string(nil), st.CompletedNodeIDs...),
		Data:             PublicInstanceData(st.Data),
		CreatedAt:        st.CreatedAt,
		UpdatedAt:        st.UpdatedAt,
	}
	if meta := InstanceMetadataFromData(st.Data); meta != nil {
		out.BusinessKey = meta.BusinessKey
		out.Title = meta.Title
		out.InitiatorID = meta.InitiatorID
		out.InitiatorName = meta.InitiatorName
	}
	if !st.StartedAt.IsZero() {
		startedAt := st.StartedAt
		out.StartedAt = &startedAt
	}
	if !st.CompletedAt.IsZero() {
		completedAt := st.CompletedAt
		out.CompletedAt = &completedAt
	}
	if st.PendingJoins != nil {
		out.PendingJoins = make([]InstancePendingJoin, len(st.PendingJoins))
		for i, join := range st.PendingJoins {
			out.PendingJoins[i] = InstancePendingJoin{
				NodeID:        join.NodeID,
				ExpectedCount: join.ExpectedCount,
				ArrivedFrom:   append([]string(nil), join.ArrivedFrom...),
			}
		}
	}
	if st.History != nil {
		out.History = make([]InstanceHistoryEntry, len(st.History))
		for i, entry := range st.History {
			out.History[i] = InstanceHistoryEntry{
				Action:     entry.Action,
				NodeID:     entry.NodeID,
				FromNodeID: entry.FromNodeID,
				At:         entry.At,
			}
		}
	}
	return out
}

func InstanceMetadataFromData(data map[string]any) *InstanceMetadata {
	if len(data) == 0 {
		return nil
	}
	raw, ok := data[WorkflowMetadataDataKey]
	if !ok || raw == nil {
		return nil
	}
	switch meta := raw.(type) {
	case InstanceMetadata:
		cloned := cloneInstanceMetadata(meta)
		if cloned.TaskAssignments == nil {
			cloned.TaskAssignments = map[string]TaskAssignment{}
		}
		return &cloned
	case *InstanceMetadata:
		if meta == nil {
			return nil
		}
		cloned := cloneInstanceMetadata(*meta)
		if cloned.TaskAssignments == nil {
			cloned.TaskAssignments = map[string]TaskAssignment{}
		}
		return &cloned
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var meta InstanceMetadata
	if err := json.Unmarshal(encoded, &meta); err != nil {
		return nil
	}
	if meta.TaskAssignments == nil {
		meta.TaskAssignments = map[string]TaskAssignment{}
	}
	return &meta
}

func PublicInstanceData(data map[string]any) map[string]any {
	if data == nil {
		return nil
	}
	out := make(map[string]any, len(data))
	for key, value := range data {
		if key == WorkflowMetadataDataKey {
			continue
		}
		out[key] = jsonclone.Any(value)
	}
	return out
}

func cloneInstanceMetadata(meta InstanceMetadata) InstanceMetadata {
	meta.CCUserIDs = jsonclone.Strings(meta.CCUserIDs)
	meta.TaskAssignments = cloneTaskAssignments(meta.TaskAssignments)
	meta.Actions = cloneWorkflowActions(meta.Actions)
	return meta
}

func cloneTaskAssignments(assignments map[string]TaskAssignment) map[string]TaskAssignment {
	if assignments == nil {
		return nil
	}
	out := make(map[string]TaskAssignment, len(assignments))
	for nodeID, assignment := range assignments {
		out[nodeID] = cloneTaskAssignment(assignment)
	}
	return out
}

func cloneTaskAssignment(assignment TaskAssignment) TaskAssignment {
	assignment.CandidateUserIDs = jsonclone.Strings(assignment.CandidateUserIDs)
	assignment.CandidateRoleIDs = jsonclone.Strings(assignment.CandidateRoleIDs)
	assignment.ClaimedAt = cloneTimePtr(assignment.ClaimedAt)
	assignment.DueAt = cloneTimePtr(assignment.DueAt)
	assignment.UpdatedAt = cloneTimePtr(assignment.UpdatedAt)
	return assignment
}

func cloneWorkflowActions(actions []WorkflowAction) []WorkflowAction {
	if actions == nil {
		return nil
	}
	out := make([]WorkflowAction, len(actions))
	copy(out, actions)
	return out
}

func cloneTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := value.UTC()
	return &cloned
}

func (d *DesignerDefinition) Normalize(now time.Time) {
	if d.Version <= 0 {
		d.Version = 1
	}
	if d.Status == "" {
		d.Status = StatusDraft
	}
	if d.CreatedAt.IsZero() {
		d.CreatedAt = now
	}
	d.UpdatedAt = now
	if d.Viewport.Zoom == 0 {
		d.Viewport.Zoom = 1
	}
	nextByNode := d.nextByNode()
	incomingByNode := incomingCounts(nextByNode)
	for i := range d.Nodes {
		if len(d.Edges) > 0 {
			d.Nodes[i].Next = dedupeStrings(nextByNode[d.Nodes[i].ID])
		} else {
			d.Nodes[i].Next = dedupeStrings(d.Nodes[i].Next)
		}
		d.Nodes[i].RejectTo = dedupeStrings(d.Nodes[i].RejectTo)
		if d.Nodes[i].Type == "" {
			d.Nodes[i].Type = inferNodeKind(len(d.Nodes[i].Next), incomingByNode[d.Nodes[i].ID])
		}
	}
}

func (d *DesignerDefinition) ToRuntimeDefinition() (*runtime.Definition, error) {
	if d == nil {
		return nil, errors.NewCode(errors.InvalidInput, "workflow definition is nil")
	}
	if strings.TrimSpace(d.ID) == "" {
		return nil, errors.NewCode(errors.InvalidInput, "workflow definition id is empty")
	}
	if strings.TrimSpace(d.Name) == "" {
		return nil, errors.NewCode(errors.InvalidInput, "workflow definition name is empty")
	}
	if len(d.Nodes) == 0 {
		return nil, errors.NewCode(errors.InvalidInput, "workflow definition has no nodes").WithContext("definition_id", d.ID)
	}
	if err := ValidateDesignerDefinition(d); err != nil {
		return nil, err
	}

	nextByNode := d.nextByNode()
	incomingByNode := incomingCounts(nextByNode)
	if len(d.Edges) > 0 {
		for _, edge := range d.Edges {
			if strings.TrimSpace(edge.Source) == "" || strings.TrimSpace(edge.Target) == "" {
				return nil, errors.NewCode(errors.InvalidInput, "workflow edge source and target are required").WithContext("edge_id", edge.ID)
			}
		}
	}

	nodes := make([]runtime.Node, 0, len(d.Nodes))
	for _, node := range d.Nodes {
		kind := node.Type
		if kind == "" {
			kind = inferNodeKind(len(dedupeStrings(nextByNode[node.ID])), incomingByNode[node.ID])
		}
		nodes = append(nodes, runtime.Node{
			ID:       node.ID,
			Name:     node.Name,
			Kind:     runtime.NodeKind(kind),
			Next:     dedupeStrings(nextByNode[node.ID]),
			RejectTo: dedupeStrings(node.RejectTo),
		})
	}
	runtimeDef := &runtime.Definition{
		ID:          d.ID,
		Name:        d.Name,
		Nodes:       nodes,
		StartNodeID: d.StartNodeID,
		CreatedAt:   d.CreatedAt,
		UpdatedAt:   d.UpdatedAt,
	}
	if err := runtime.ValidateDefinition(runtimeDef); err != nil {
		return nil, err
	}
	return runtimeDef, nil
}

// RuntimeDefinitionFromRuntime 将核心运行时定义转换为 API DTO，并尽量保留设计态节点类型。
func RuntimeDefinitionFromRuntime(def *runtime.Definition, design *DesignerDefinition) *RuntimeDefinition {
	if def == nil {
		return nil
	}
	kindByNode := map[string]NodeKind{}
	if design != nil {
		for _, node := range design.Nodes {
			kindByNode[node.ID] = node.Type
		}
	}
	nextByNode := make(map[string][]string, len(def.Nodes))
	for _, node := range def.Nodes {
		nextByNode[node.ID] = node.Next
	}
	incomingByNode := incomingCounts(nextByNode)
	out := &RuntimeDefinition{
		ID:          def.ID,
		Name:        def.Name,
		StartNodeID: def.StartNodeID,
		Nodes:       make([]RuntimeNode, 0, len(def.Nodes)),
	}
	for _, node := range def.Nodes {
		kind := NodeKind(node.Kind)
		if designKind := kindByNode[node.ID]; designKind != "" {
			kind = designKind
		}
		if kind == "" {
			kind = inferNodeKind(len(node.Next), incomingByNode[node.ID])
		}
		out.Nodes = append(out.Nodes, RuntimeNode{
			ID:       node.ID,
			Name:     node.Name,
			Type:     kind,
			Next:     dedupeStrings(node.Next),
			RejectTo: dedupeStrings(node.RejectTo),
		})
	}
	return out
}

func (d *DesignerDefinition) nextByNode() map[string][]string {
	nextByNode := make(map[string][]string, len(d.Nodes))
	if len(d.Edges) > 0 {
		for _, edge := range d.Edges {
			nextByNode[edge.Source] = append(nextByNode[edge.Source], edge.Target)
		}
		return nextByNode
	}
	for _, node := range d.Nodes {
		nextByNode[node.ID] = append(nextByNode[node.ID], node.Next...)
	}
	return nextByNode
}

// ValidateDesignerDefinition 校验设计态契约，再由核心 runtime 校验可执行 DAG。
func ValidateDesignerDefinition(def *DesignerDefinition) error {
	if def == nil {
		return errors.NewCode(errors.InvalidInput, "workflow definition is nil")
	}
	if strings.TrimSpace(def.ID) == "" {
		return errors.NewCode(errors.InvalidInput, "workflow definition id is empty")
	}
	if strings.TrimSpace(def.Name) == "" {
		return errors.NewCode(errors.InvalidInput, "workflow definition name is empty")
	}
	if hasSurroundingWhitespace(def.ID) {
		return errors.NewCode(errors.InvalidInput, "workflow definition id has surrounding whitespace").
			WithContext("definition_id", def.ID)
	}
	if hasSurroundingWhitespace(def.StartNodeID) {
		return errors.NewCode(errors.InvalidInput, "workflow start node id has surrounding whitespace").
			WithContext("definition_id", def.ID).
			WithContext("start_node_id", def.StartNodeID)
	}
	if !isValidStatus(def.Status) {
		return errors.NewCode(errors.InvalidInput, "workflow definition status is invalid").
			WithContext("status", string(def.Status))
	}
	if len(def.Nodes) == 0 {
		return errors.NewCode(errors.InvalidInput, "workflow definition has no nodes").
			WithContext("definition_id", def.ID)
	}

	nodes := make(map[string]DesignerNode, len(def.Nodes))
	for i, node := range def.Nodes {
		if strings.TrimSpace(node.ID) == "" {
			return errors.NewCode(errors.InvalidInput, "workflow node id is empty").
				WithContext("definition_id", def.ID).
				WithContext("node_index", i)
		}
		if hasSurroundingWhitespace(node.ID) {
			return errors.NewCode(errors.InvalidInput, "workflow node id has surrounding whitespace").
				WithContext("definition_id", def.ID).
				WithContext("node_id", node.ID)
		}
		if _, exists := nodes[node.ID]; exists {
			return errors.NewCode(errors.InvalidInput, "workflow node id must be unique").
				WithContext("definition_id", def.ID).
				WithContext("node_id", node.ID)
		}
		nodes[node.ID] = node
	}
	if _, ok := nodes[def.StartNodeID]; !ok {
		return errors.NewCode(errors.InvalidInput, "workflow start node does not exist").
			WithContext("definition_id", def.ID).
			WithContext("start_node_id", def.StartNodeID)
	}

	nextByNode := def.nextByNode()
	if len(def.Edges) > 0 {
		type edgePair struct {
			source string
			target string
		}
		seenEdges := make(map[string]struct{}, len(def.Edges))
		seenEdgePairs := make(map[edgePair]struct{}, len(def.Edges))
		for i, edge := range def.Edges {
			if strings.TrimSpace(edge.ID) == "" {
				return errors.NewCode(errors.InvalidInput, "workflow edge id is empty").
					WithContext("definition_id", def.ID).
					WithContext("edge_index", i)
			}
			if hasSurroundingWhitespace(edge.ID) {
				return errors.NewCode(errors.InvalidInput, "workflow edge id has surrounding whitespace").
					WithContext("definition_id", def.ID).
					WithContext("edge_id", edge.ID)
			}
			if _, exists := seenEdges[edge.ID]; exists {
				return errors.NewCode(errors.InvalidInput, "workflow edge id must be unique").
					WithContext("definition_id", def.ID).
					WithContext("edge_id", edge.ID)
			}
			seenEdges[edge.ID] = struct{}{}
			if strings.TrimSpace(edge.Source) == "" || strings.TrimSpace(edge.Target) == "" {
				return errors.NewCode(errors.InvalidInput, "workflow edge source and target are required").
					WithContext("definition_id", def.ID).
					WithContext("edge_id", edge.ID)
			}
			if hasSurroundingWhitespace(edge.Source) || hasSurroundingWhitespace(edge.Target) {
				return errors.NewCode(errors.InvalidInput, "workflow edge source and target must not have surrounding whitespace").
					WithContext("definition_id", def.ID).
					WithContext("edge_id", edge.ID).
					WithContext("source", edge.Source).
					WithContext("target", edge.Target)
			}
			if _, ok := nodes[edge.Source]; !ok {
				return errors.NewCode(errors.InvalidInput, "workflow edge source node does not exist").
					WithContext("definition_id", def.ID).
					WithContext("edge_id", edge.ID).
					WithContext("source", edge.Source)
			}
			if _, ok := nodes[edge.Target]; !ok {
				return errors.NewCode(errors.InvalidInput, "workflow edge target node does not exist").
					WithContext("definition_id", def.ID).
					WithContext("edge_id", edge.ID).
					WithContext("target", edge.Target)
			}
			pairKey := edgePair{source: edge.Source, target: edge.Target}
			if _, exists := seenEdgePairs[pairKey]; exists {
				return errors.NewCode(errors.InvalidInput, "workflow edge source and target must be unique").
					WithContext("definition_id", def.ID).
					WithContext("edge_id", edge.ID).
					WithContext("source", edge.Source).
					WithContext("target", edge.Target)
			}
			seenEdgePairs[pairKey] = struct{}{}
		}
	}
	for _, node := range def.Nodes {
		seenNext := make(map[string]struct{}, len(nextByNode[node.ID]))
		for _, nextNodeID := range nextByNode[node.ID] {
			if strings.TrimSpace(nextNodeID) == "" {
				return errors.NewCode(errors.InvalidInput, "workflow next node id is empty").
					WithContext("definition_id", def.ID).
					WithContext("node_id", node.ID)
			}
			if hasSurroundingWhitespace(nextNodeID) {
				return errors.NewCode(errors.InvalidInput, "workflow next node id has surrounding whitespace").
					WithContext("definition_id", def.ID).
					WithContext("node_id", node.ID).
					WithContext("next_node_id", nextNodeID)
			}
			if _, ok := nodes[nextNodeID]; !ok {
				return errors.NewCode(errors.InvalidInput, "workflow next node does not exist").
					WithContext("definition_id", def.ID).
					WithContext("node_id", node.ID).
					WithContext("next_node_id", nextNodeID)
			}
			if _, exists := seenNext[nextNodeID]; exists {
				return errors.NewCode(errors.InvalidInput, "workflow node next targets must be unique").
					WithContext("definition_id", def.ID).
					WithContext("node_id", node.ID).
					WithContext("next_node_id", nextNodeID)
			}
			seenNext[nextNodeID] = struct{}{}
		}
		seenRejectTo := make(map[string]struct{}, len(node.RejectTo))
		for _, targetNodeID := range node.RejectTo {
			if strings.TrimSpace(targetNodeID) == "" {
				return errors.NewCode(errors.InvalidInput, "workflow reject target node id is empty").
					WithContext("definition_id", def.ID).
					WithContext("node_id", node.ID)
			}
			if hasSurroundingWhitespace(targetNodeID) {
				return errors.NewCode(errors.InvalidInput, "workflow reject target node id has surrounding whitespace").
					WithContext("definition_id", def.ID).
					WithContext("node_id", node.ID).
					WithContext("reject_to", targetNodeID)
			}
			if _, ok := nodes[targetNodeID]; !ok {
				return errors.NewCode(errors.InvalidInput, "workflow reject target node does not exist").
					WithContext("definition_id", def.ID).
					WithContext("node_id", node.ID).
					WithContext("reject_to", targetNodeID)
			}
			if _, exists := seenRejectTo[targetNodeID]; exists {
				return errors.NewCode(errors.InvalidInput, "workflow reject targets must be unique").
					WithContext("definition_id", def.ID).
					WithContext("node_id", node.ID).
					WithContext("reject_to", targetNodeID)
			}
			seenRejectTo[targetNodeID] = struct{}{}
		}
	}

	incomingByNode := incomingCounts(nextByNode)
	if incomingByNode[def.StartNodeID] > 0 {
		return errors.NewCode(errors.InvalidInput, "workflow start node cannot have incoming edges").
			WithContext("definition_id", def.ID).
			WithContext("start_node_id", def.StartNodeID).
			WithContext("incoming_count", incomingByNode[def.StartNodeID])
	}
	for _, node := range def.Nodes {
		outgoingCount := len(dedupeStrings(nextByNode[node.ID]))
		incomingCount := incomingByNode[node.ID]
		nodeType := node.Type
		if nodeType == "" {
			nodeType = inferNodeKind(outgoingCount, incomingCount)
		}
		if !isValidNodeKind(nodeType) {
			return errors.NewCode(errors.InvalidInput, "workflow node type is invalid").
				WithContext("definition_id", def.ID).
				WithContext("node_id", node.ID).
				WithContext("node_type", string(node.Type))
		}
		if nodeType == NodeKindBranch && outgoingCount < 2 {
			return errors.NewCode(errors.InvalidInput, "workflow branch node requires at least two outgoing edges").
				WithContext("definition_id", def.ID).
				WithContext("node_id", node.ID)
		}
		if nodeType == NodeKindTask && outgoingCount > 1 {
			return errors.NewCode(errors.InvalidInput, "workflow task node with multiple outgoing edges must be branch or join").
				WithContext("definition_id", def.ID).
				WithContext("node_id", node.ID)
		}
		if nodeType == NodeKindJoin && incomingCount < 2 {
			return errors.NewCode(errors.InvalidInput, "workflow join node requires at least two incoming edges").
				WithContext("definition_id", def.ID).
				WithContext("node_id", node.ID)
		}
		if nodeType == NodeKindJoin {
			if joinMode, ok := node.Config["joinMode"].(string); ok && joinMode != "" && joinMode != "all" {
				return errors.NewCode(errors.InvalidInput, "workflow join mode is unsupported").
					WithContext("definition_id", def.ID).
					WithContext("node_id", node.ID).
					WithContext("join_mode", joinMode)
			}
		}
	}
	return nil
}

// ValidateRuntimeDefinition 校验投影后的核心 runtime 定义是否可执行。
func ValidateRuntimeDefinition(ctx context.Context, def *runtime.Definition) error {
	if ctx == nil {
		return errors.NewCode(errors.InvalidInput, "workflow context is nil")
	}
	return runtime.ValidateDefinition(def)
}

func dedupeStrings(values []string) []string {
	if values == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func incomingCounts(nextByNode map[string][]string) map[string]int {
	counts := make(map[string]int, len(nextByNode))
	for _, nextIDs := range nextByNode {
		for _, nextID := range dedupeStrings(nextIDs) {
			counts[nextID]++
		}
	}
	return counts
}

func inferNodeKind(outgoingCount int, incomingCount int) NodeKind {
	switch {
	case incomingCount > 1:
		return NodeKindJoin
	case outgoingCount > 1:
		return NodeKindBranch
	default:
		return NodeKindTask
	}
}

func isValidStatus(status DefinitionStatus) bool {
	return status == StatusDraft || status == StatusPublished || status == StatusArchived
}

func isValidNodeKind(kind NodeKind) bool {
	return kind == NodeKindTask || kind == NodeKindBranch || kind == NodeKindJoin
}

func hasSurroundingWhitespace(value string) bool {
	return value != strings.TrimSpace(value)
}
