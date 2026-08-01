package workflowservice

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	workflowtypes "gochen-workflow/types"

	"gochen/auth"
	"gochen/clock"
	"gochen/errors"
	"gochen/ident"
	runtime "gochen/process/workflow"
)

const withdrawnInstanceStatus runtime.InstanceStatus = "withdrawn"
const systemInitiatorID = "system"

// Service 编排设计态定义、运行时投影与实例推进。
type Service struct {
	store       Store
	engine      *runtime.Engine
	idGenerator ident.IGenerator[string]
	clock       clock.IClock

	definitionMu sync.Mutex
}

// NewService 创建 workflow 应用服务。
func NewService(store Store, idGenerator ident.IGenerator[string], clk clock.IClock) *Service {
	if idGenerator == nil {
		idGenerator = ident.DefaultStringGenerator()
	}
	if clk == nil {
		clk = clock.NewRealClock()
	}
	return &Service{
		store:       store,
		engine:      runtime.NewEngine(store).WithClock(clk),
		idGenerator: idGenerator,
		clock:       clk,
	}
}

// ListDefinitions 返回按更新时间倒序排列的设计态定义列表。
func (s *Service) ListDefinitions(ctx context.Context) ([]*workflowtypes.DesignerDefinition, error) {
	if err := s.validateReady(ctx); err != nil {
		return nil, err
	}
	items, err := s.store.ListDesigns(ctx)
	if err != nil {
		return nil, err
	}
	items = cloneDesigns(items)
	sort.SliceStable(items, func(i, j int) bool {
		if !items[i].UpdatedAt.Equal(items[j].UpdatedAt) {
			return items[i].UpdatedAt.After(items[j].UpdatedAt)
		}
		return items[i].ID < items[j].ID
	})
	return items, nil
}

// BuildDefinitionListResponse 生成定义列表及全量状态统计。
func (s *Service) BuildDefinitionListResponse(ctx context.Context) (*workflowtypes.DefinitionListResponse, error) {
	items, err := s.ListDefinitions(ctx)
	if err != nil {
		return nil, err
	}
	stats := countDefinitionStats(items)
	return &workflowtypes.DefinitionListResponse{
		Items:        items,
		Total:        len(items),
		Stats:        stats,
		StatusCounts: stats,
	}, nil
}

// BuildDefinitionListResponseWithQuery 生成经过筛选和分页的定义列表及全量状态统计。
func (s *Service) BuildDefinitionListResponseWithQuery(ctx context.Context, query workflowtypes.DefinitionQuery) (*workflowtypes.DefinitionListResponse, error) {
	if err := s.validateReady(ctx); err != nil {
		return nil, err
	}
	page, size, err := normalizeDefinitionPage(query.Page, query.Size)
	if err != nil {
		return nil, err
	}
	if err := validateDefinitionStatus(query.Status); err != nil {
		return nil, err
	}
	normalizedQuery := definitionListQuery{
		page:    page,
		size:    size,
		offset:  (page - 1) * size,
		keyword: strings.ToLower(strings.TrimSpace(query.Keyword)),
		status:  query.Status,
	}
	if queryStore, ok := s.store.(definitionQueryStore); ok {
		return queryStore.queryDesigns(ctx, normalizedQuery)
	}

	items, err := s.ListDefinitions(ctx)
	if err != nil {
		return nil, err
	}
	keywordItems := filterDefinitionsByKeyword(items, query.Keyword)
	stats := countDefinitionStats(keywordItems)
	filtered, err := filterDefinitionsByStatus(keywordItems, query.Status)
	if err != nil {
		return nil, err
	}
	total := len(filtered)
	start := normalizedQuery.offset
	if start >= total {
		filtered = []*workflowtypes.DesignerDefinition{}
	} else {
		end := start + size
		if end > total {
			end = total
		}
		filtered = filtered[start:end]
	}
	return &workflowtypes.DefinitionListResponse{
		Items:        filtered,
		Total:        total,
		Page:         page,
		Size:         size,
		Stats:        stats,
		StatusCounts: stats,
	}, nil
}

func normalizeDefinitionPage(page, size int) (int, int, error) {
	if page == 0 {
		page = 1
	}
	if size == 0 {
		size = 20
	}
	if page < 1 {
		return 0, 0, errors.NewCode(errors.InvalidInput, "workflow definition page must be greater than 0")
	}
	if size < 1 || size > 100 {
		return 0, 0, errors.NewCode(errors.InvalidInput, "workflow definition size must be between 1 and 100")
	}
	if page > 1 && page-1 > math.MaxInt/size {
		return 0, 0, errors.NewCode(errors.InvalidInput, "workflow definition pagination offset is too large")
	}
	return page, size, nil
}

func filterDefinitionsByKeyword(items []*workflowtypes.DesignerDefinition, rawKeyword string) []*workflowtypes.DesignerDefinition {
	keyword := strings.ToLower(strings.TrimSpace(rawKeyword))
	if keyword == "" {
		return items
	}
	filtered := make([]*workflowtypes.DesignerDefinition, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		haystack := strings.ToLower(strings.Join([]string{item.ID, item.Name, item.Description}, " "))
		if strings.Contains(haystack, keyword) {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func filterDefinitionsByStatus(items []*workflowtypes.DesignerDefinition, status workflowtypes.DefinitionStatus) ([]*workflowtypes.DesignerDefinition, error) {
	if err := validateDefinitionStatus(status); err != nil {
		return nil, err
	}
	if status == "" {
		return items, nil
	}
	filtered := make([]*workflowtypes.DesignerDefinition, 0, len(items))
	for _, item := range items {
		if item != nil && item.Status == status {
			filtered = append(filtered, item)
		}
	}
	return filtered, nil
}

func validateDefinitionStatus(status workflowtypes.DefinitionStatus) error {
	switch status {
	case "", workflowtypes.StatusDraft, workflowtypes.StatusPublished, workflowtypes.StatusArchived:
		return nil
	default:
		return errors.NewCode(errors.InvalidInput, "workflow definition status is invalid").WithContext("status", status)
	}
}

func countDefinitionStats(items []*workflowtypes.DesignerDefinition) workflowtypes.DefinitionStats {
	stats := workflowtypes.DefinitionStats{Total: len(items)}
	for _, item := range items {
		if item == nil {
			continue
		}
		switch item.Status {
		case workflowtypes.StatusDraft:
			stats.Draft++
		case workflowtypes.StatusPublished:
			stats.Published++
		case workflowtypes.StatusArchived:
			stats.Archived++
		}
	}
	return stats
}

// GetDefinition 按 ID 查询设计态定义。
func (s *Service) GetDefinition(ctx context.Context, id string) (*workflowtypes.DesignerDefinition, error) {
	if err := s.validateReady(ctx); err != nil {
		return nil, err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, errors.NewCode(errors.InvalidInput, "workflow definition id is empty")
	}
	design, err := s.store.GetDesign(ctx, id)
	if err != nil {
		return nil, err
	}
	if design == nil {
		return nil, errors.NewCode(errors.NotFound, "workflow definition not found").WithContext("definition_id", id)
	}
	return cloneDesign(design), nil
}

// SaveDefinition 创建或更新草稿定义；已发布/归档定义保持不可变。
func (s *Service) SaveDefinition(ctx context.Context, input *workflowtypes.DesignerDefinition) (*workflowtypes.DesignerDefinition, error) {
	if err := s.validateReady(ctx); err != nil {
		return nil, err
	}
	if input == nil {
		return nil, errors.NewCode(errors.InvalidInput, "workflow definition payload is required")
	}
	design := cloneDesign(input)
	design.ID = strings.TrimSpace(design.ID)
	if design.ID == "" {
		id, err := s.idGenerator.Next()
		if err != nil {
			return nil, errors.Wrap(err, errors.Internal, "generate workflow definition id")
		}
		design.ID = id
	}
	if strings.TrimSpace(design.StartNodeID) == "" && len(design.Nodes) > 0 {
		design.StartNodeID = design.Nodes[0].ID
	}

	s.definitionMu.Lock()
	defer s.definitionMu.Unlock()

	previous, err := s.store.GetDesign(ctx, design.ID)
	if err != nil {
		return nil, err
	}
	if previous != nil && previous.Status != workflowtypes.StatusDraft {
		return nil, errors.NewCode(errors.Conflict, "workflow definition is immutable after publish").
			WithContext("definition_id", design.ID).
			WithContext("status", string(previous.Status))
	}
	if previous != nil {
		if design.Status != "" && design.Status != workflowtypes.StatusDraft {
			return nil, errors.NewCode(errors.InvalidInput, "workflow definition status transition must use lifecycle API").
				WithContext("definition_id", design.ID).
				WithContext("status", string(design.Status))
		}
		if design.Version != previous.Version {
			return nil, errors.NewCode(errors.Conflict, "workflow definition version mismatch").
				WithContext("definition_id", design.ID).
				WithContext("expected_version", previous.Version).
				WithContext("actual_version", design.Version)
		}
		design.CreatedAt = previous.CreatedAt
		design.Version = previous.Version + 1
	} else {
		if design.Status != "" && design.Status != workflowtypes.StatusDraft {
			return nil, errors.NewCode(errors.InvalidInput, "workflow definition must be saved as draft before publish").
				WithContext("definition_id", design.ID).
				WithContext("status", string(design.Status))
		}
		design.Version = 1
	}
	design.Status = workflowtypes.StatusDraft
	design.PublishedAt = nil

	return s.persistDefinition(ctx, design)
}

// DeleteDefinition 只允许删除未被实例引用的草稿定义。
func (s *Service) DeleteDefinition(ctx context.Context, id string) error {
	if err := s.validateReady(ctx); err != nil {
		return err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.NewCode(errors.InvalidInput, "workflow definition id is empty")
	}
	s.definitionMu.Lock()
	defer s.definitionMu.Unlock()

	design, err := s.store.GetDesign(ctx, id)
	if err != nil {
		return err
	}
	if design == nil {
		return errors.NewCode(errors.NotFound, "workflow definition not found").WithContext("definition_id", id)
	}
	if design.Status != workflowtypes.StatusDraft {
		return errors.NewCode(errors.Conflict, "only draft workflow definitions can be deleted").
			WithContext("definition_id", id).
			WithContext("status", string(design.Status))
	}
	count, err := s.store.CountInstancesByDefinition(ctx, id)
	if err != nil {
		return err
	}
	if count > 0 {
		return errors.NewCode(errors.Conflict, "workflow definition is used by instances").
			WithContext("definition_id", id).
			WithContext("instance_count", count)
	}
	return s.store.DeleteDesign(ctx, id)
}

// PublishDefinition 将当前草稿固化为可运行定义；发布后不再允许原地编辑图结构。
func (s *Service) PublishDefinition(ctx context.Context, id string) (*workflowtypes.DesignerDefinition, error) {
	if err := s.validateReady(ctx); err != nil {
		return nil, err
	}
	id = strings.TrimSpace(id)
	s.definitionMu.Lock()
	defer s.definitionMu.Unlock()

	design, err := s.GetDefinition(ctx, id)
	if err != nil {
		return nil, err
	}
	if design.Status != workflowtypes.StatusDraft {
		return nil, errors.NewCode(errors.Conflict, "only draft workflow definitions can be published").
			WithContext("definition_id", id).
			WithContext("status", string(design.Status))
	}
	now := s.clock.Now().UTC()
	design.Status = workflowtypes.StatusPublished
	design.Version++
	design.PublishedAt = &now
	return s.persistDefinition(ctx, design)
}

// ArchiveDefinition 将已发布定义标记为归档；运行时快照保留给既有实例继续推进。
func (s *Service) ArchiveDefinition(ctx context.Context, id string) (*workflowtypes.DesignerDefinition, error) {
	if err := s.validateReady(ctx); err != nil {
		return nil, err
	}
	id = strings.TrimSpace(id)
	s.definitionMu.Lock()
	defer s.definitionMu.Unlock()

	design, err := s.GetDefinition(ctx, id)
	if err != nil {
		return nil, err
	}
	if design.Status == workflowtypes.StatusArchived {
		return design, nil
	}
	if design.Status != workflowtypes.StatusPublished {
		return nil, errors.NewCode(errors.Conflict, "only published workflow definitions can be archived").
			WithContext("definition_id", id).
			WithContext("status", string(design.Status))
	}
	design.Status = workflowtypes.StatusArchived
	design.Version++
	return s.persistDefinition(ctx, design)
}

// persistDefinition 统一归一化、runtime 投影、核心校验与落库，避免状态切换绕过校验。
func (s *Service) persistDefinition(ctx context.Context, design *workflowtypes.DesignerDefinition) (*workflowtypes.DesignerDefinition, error) {
	if err := workflowtypes.ValidateDesignerDefinition(design); err != nil {
		return nil, err
	}
	now := s.clock.Now().UTC()
	design.Normalize(now)
	runtimeDef, err := design.ToRuntimeDefinition()
	if err != nil {
		return nil, err
	}
	if err := workflowtypes.ValidateRuntimeDefinition(ctx, runtimeDef); err != nil {
		return nil, err
	}
	design.RuntimeDefinition = workflowtypes.RuntimeDefinitionFromRuntime(runtimeDef, design)
	if err := s.store.SaveDesign(ctx, design, runtimeDef); err != nil {
		return nil, err
	}
	return cloneDesign(design), nil
}

func (s *Service) CreateInstance(ctx context.Context, instanceID runtime.ID, definitionID string, data map[string]any) (*runtime.State, error) {
	return s.CreateInstanceWithOptions(ctx, workflowtypes.CreateInstanceInput{
		ID:           string(instanceID),
		DefinitionID: definitionID,
		Data:         data,
	})
}

func (s *Service) CreateInstanceWithOptions(ctx context.Context, input workflowtypes.CreateInstanceInput) (*runtime.State, error) {
	if err := s.validateReady(ctx); err != nil {
		return nil, err
	}
	instanceID := runtime.ID(strings.TrimSpace(input.ID))
	definitionID := strings.TrimSpace(input.DefinitionID)
	if instanceID == "" {
		id, err := s.idGenerator.Next()
		if err != nil {
			return nil, errors.Wrap(err, errors.Internal, "generate workflow instance id")
		}
		instanceID = runtime.ID(id)
	}
	if definitionID == "" {
		return nil, errors.NewCode(errors.InvalidInput, "workflow definition id is empty")
	}
	meta := workflowtypes.InstanceMetadata{
		BusinessKey:     strings.TrimSpace(input.BusinessKey),
		Title:           strings.TrimSpace(input.Title),
		InitiatorID:     strings.TrimSpace(input.InitiatorID),
		InitiatorName:   strings.TrimSpace(input.InitiatorName),
		CCUserIDs:       normalizeStringSlice(input.CCUserIDs),
		TaskAssignments: map[string]workflowtypes.TaskAssignment{},
	}
	if meta.InitiatorID == "" {
		meta.InitiatorID = currentPrincipalID(ctx)
	}
	if meta.InitiatorID == "" {
		meta.InitiatorID = systemInitiatorID
	}
	if assignment := assignmentFromInput(input.AssigneeID, input.AssigneeName, input.CandidateUserIDs, input.CandidateRoleIDs, nil, input.DueAt); !emptyAssignment(assignment) {
		meta.TaskAssignments["*"] = assignment
	}
	s.definitionMu.Lock()
	defer s.definitionMu.Unlock()

	design, err := s.GetDefinition(ctx, definitionID)
	if err != nil {
		return nil, err
	}
	if design.Status != workflowtypes.StatusPublished {
		return nil, errors.NewCode(errors.Conflict, "workflow definition is not published").
			WithContext("definition_id", definitionID).
			WithContext("status", string(design.Status))
	}
	data := cloneMap(input.Data)
	if data == nil {
		data = map[string]any{}
	}
	data[workflowtypes.WorkflowMetadataDataKey] = meta
	if err := s.engine.CreateInstanceWithData(ctx, instanceID, definitionID, data); err != nil {
		return nil, err
	}
	st, err := s.store.Get(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, errors.NewCode(errors.Internal, "workflow instance was not saved").WithContext("instance_id", string(instanceID))
	}
	return cloneState(st), nil
}

func (s *Service) StartInstance(ctx context.Context, id runtime.ID) (*runtime.State, error) {
	if err := s.validateReady(ctx); err != nil {
		return nil, err
	}
	id = runtime.ID(strings.TrimSpace(string(id)))
	if id == "" {
		return nil, errors.NewCode(errors.InvalidInput, "workflow instance id is empty")
	}
	if err := s.engine.StartInstanceWithMutation(ctx, id, func(st *runtime.State, now time.Time) error {
		s.applyActiveAssignments(st, nil, now)
		return nil
	}); err != nil {
		return nil, err
	}
	return s.GetInstance(ctx, id)
}

func (s *Service) AdvanceNode(ctx context.Context, id runtime.ID, nodeID string) (*runtime.State, error) {
	return s.AdvanceNodeWithInput(ctx, id, nodeID, workflowtypes.AdvanceNodeInput{})
}

func (s *Service) AdvanceNodeWithInput(ctx context.Context, id runtime.ID, nodeID string, input workflowtypes.AdvanceNodeInput) (*runtime.State, error) {
	return s.completeNodeWithAction(ctx, id, nodeID, input, "advance")
}

func (s *Service) CompleteNode(ctx context.Context, id runtime.ID, nodeID string, input workflowtypes.AdvanceNodeInput) (*runtime.State, error) {
	return s.completeNodeWithAction(ctx, id, nodeID, input, "complete")
}

func (s *Service) completeNodeWithAction(ctx context.Context, id runtime.ID, nodeID string, input workflowtypes.AdvanceNodeInput, action string) (*runtime.State, error) {
	if err := s.validateReady(ctx); err != nil {
		return nil, err
	}
	id = runtime.ID(strings.TrimSpace(string(id)))
	nodeID = strings.TrimSpace(nodeID)
	if id == "" {
		return nil, errors.NewCode(errors.InvalidInput, "workflow instance id is empty")
	}
	if nodeID == "" {
		return nil, errors.NewCode(errors.InvalidInput, "workflow node id is empty")
	}
	if err := s.engine.AdvanceNodeWithMutation(ctx, id, nodeID, func(st *runtime.State, now time.Time) error {
		return s.applyNodeCompletedMetadata(ctx, st, nodeID, input, action, now)
	}); err != nil {
		return nil, err
	}
	return s.GetInstance(ctx, id)
}

func (s *Service) AdvanceNodeTo(ctx context.Context, id runtime.ID, nodeID string, nextNodeID string) (*runtime.State, error) {
	return s.AdvanceNodeToWithInput(ctx, id, nodeID, nextNodeID, workflowtypes.AdvanceNodeInput{})
}

func (s *Service) AdvanceNodeToWithInput(ctx context.Context, id runtime.ID, nodeID string, nextNodeID string, input workflowtypes.AdvanceNodeInput) (*runtime.State, error) {
	return s.completeNodeToWithAction(ctx, id, nodeID, nextNodeID, input, "advance")
}

func (s *Service) CompleteNodeTo(ctx context.Context, id runtime.ID, nodeID string, nextNodeID string, input workflowtypes.AdvanceNodeInput) (*runtime.State, error) {
	return s.completeNodeToWithAction(ctx, id, nodeID, nextNodeID, input, "complete")
}

func (s *Service) completeNodeToWithAction(ctx context.Context, id runtime.ID, nodeID string, nextNodeID string, input workflowtypes.AdvanceNodeInput, action string) (*runtime.State, error) {
	if err := s.validateReady(ctx); err != nil {
		return nil, err
	}
	id = runtime.ID(strings.TrimSpace(string(id)))
	nodeID = strings.TrimSpace(nodeID)
	nextNodeID = strings.TrimSpace(nextNodeID)
	if id == "" {
		return nil, errors.NewCode(errors.InvalidInput, "workflow instance id is empty")
	}
	if nodeID == "" {
		return nil, errors.NewCode(errors.InvalidInput, "workflow node id is empty")
	}
	if nextNodeID == "" {
		return nil, errors.NewCode(errors.InvalidInput, "workflow selected next node is empty")
	}
	if err := s.engine.AdvanceNodeToWithMutation(ctx, id, nodeID, nextNodeID, func(st *runtime.State, now time.Time) error {
		return s.applyNodeCompletedMetadata(ctx, st, nodeID, input, action, now)
	}); err != nil {
		return nil, err
	}
	return s.GetInstance(ctx, id)
}

func (s *Service) RejectNode(ctx context.Context, id runtime.ID, nodeID string, input workflowtypes.RejectNodeInput) (*runtime.State, error) {
	if err := s.validateReady(ctx); err != nil {
		return nil, err
	}
	id = runtime.ID(strings.TrimSpace(string(id)))
	nodeID = strings.TrimSpace(nodeID)
	targetNodeID := strings.TrimSpace(input.TargetNodeID)
	if id == "" {
		return nil, errors.NewCode(errors.InvalidInput, "workflow instance id is empty")
	}
	if nodeID == "" {
		return nil, errors.NewCode(errors.InvalidInput, "workflow node id is empty")
	}
	if targetNodeID == "" {
		return nil, errors.NewCode(errors.InvalidInput, "workflow reject target node id is empty")
	}
	if err := s.engine.RejectNodeWithMutation(ctx, id, nodeID, targetNodeID, func(st *runtime.State, now time.Time) error {
		return s.applyNodeRejectedMetadata(ctx, st, nodeID, targetNodeID, input, now)
	}); err != nil {
		return nil, err
	}
	return s.GetInstance(ctx, id)
}

func (s *Service) GetInstance(ctx context.Context, id runtime.ID) (*runtime.State, error) {
	if err := s.validateReady(ctx); err != nil {
		return nil, err
	}
	id = runtime.ID(strings.TrimSpace(string(id)))
	if id == "" {
		return nil, errors.NewCode(errors.InvalidInput, "workflow instance id is empty")
	}
	st, err := s.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, errors.NewCode(errors.NotFound, "workflow instance not found").WithContext("instance_id", string(id))
	}
	return cloneState(st), nil
}

func (s *Service) ListMyStartedInstances(ctx context.Context, query workflowtypes.InstanceQuery) (*workflowtypes.InstancePage, error) {
	if query.InitiatorID == "" {
		query.InitiatorID = currentPrincipalID(ctx)
	}
	if query.InitiatorID == "" {
		return nil, errors.NewCode(errors.Unauthorized, "workflow principal is required")
	}
	return s.listInstances(ctx, query, true)
}

func (s *Service) ListInstances(ctx context.Context, query workflowtypes.InstanceQuery) (*workflowtypes.InstancePage, error) {
	return s.listInstances(ctx, query, false)
}

func (s *Service) ListStartedInstances(ctx context.Context, query workflowtypes.InstanceQuery) (*workflowtypes.InstancePage, error) {
	return s.listInstances(ctx, query, true)
}

func (s *Service) listInstances(ctx context.Context, query workflowtypes.InstanceQuery, startedOnly bool) (*workflowtypes.InstancePage, error) {
	if err := s.validateReady(ctx); err != nil {
		return nil, err
	}
	page, size, err := normalizeWorkItemPage(query.Page, query.Size)
	if err != nil {
		return nil, err
	}
	var instances []*runtime.State
	if startedOnly {
		instances, err = s.store.ListStartedInstances(ctx)
	} else {
		instances, err = s.store.ListInstances(ctx)
	}
	if err != nil {
		return nil, err
	}
	definitions, err := s.definitionMetasForStates(ctx, instances)
	if err != nil {
		return nil, err
	}
	query = normalizeInstanceQuery(query)
	items := make([]workflowtypes.InstanceSummary, 0, len(instances))
	for _, st := range instances {
		if startedOnly && st != nil && st.StartedAt.IsZero() {
			continue
		}
		if st == nil || !instanceMatches(st, definitions, query) {
			continue
		}
		items = append(items, s.instanceSummaryFromState(st, definitions))
	}
	sort.SliceStable(items, func(i, j int) bool {
		if !items[i].UpdatedAt.Equal(items[j].UpdatedAt) {
			return items[i].UpdatedAt.After(items[j].UpdatedAt)
		}
		return items[i].ID < items[j].ID
	})
	return paginateInstances(items, page, size), nil
}

// ListTodoItems 返回当前 scope 内可推进的 workflow 活动节点。
func (s *Service) ListTodoItems(ctx context.Context, page int, size int) (*workflowtypes.WorkItemPage, error) {
	return s.ListTodoItemsByQuery(ctx, workflowtypes.WorkItemQuery{Page: page, Size: size})
}

func (s *Service) ListMyTodoItems(ctx context.Context, query workflowtypes.WorkItemQuery) (*workflowtypes.WorkItemPage, error) {
	if query.UserID == "" {
		query.UserID = currentPrincipalID(ctx)
	}
	if query.UserID == "" {
		return nil, errors.NewCode(errors.Unauthorized, "workflow principal is required")
	}
	return s.listTodoItemsByQuery(ctx, query, currentPrincipalRoles(ctx))
}

func (s *Service) ListTodoItemsByQuery(ctx context.Context, query workflowtypes.WorkItemQuery) (*workflowtypes.WorkItemPage, error) {
	return s.listTodoItemsByQuery(ctx, query, nil)
}

func (s *Service) listTodoItemsByQuery(ctx context.Context, query workflowtypes.WorkItemQuery, actorRoles []string) (*workflowtypes.WorkItemPage, error) {
	if err := s.validateReady(ctx); err != nil {
		return nil, err
	}
	page, size, err := normalizeWorkItemPage(query.Page, query.Size)
	if err != nil {
		return nil, err
	}
	instances, err := s.store.ListInstances(ctx)
	if err != nil {
		return nil, err
	}
	definitions, err := s.definitionMetasForStates(ctx, instances)
	if err != nil {
		return nil, err
	}
	items := make([]workflowtypes.WorkItem, 0)
	for _, st := range instances {
		if st == nil || st.Status != runtime.InstanceStatusRunning {
			continue
		}
		for _, nodeID := range st.ActiveNodeIDs {
			nodeID = strings.TrimSpace(nodeID)
			if nodeID == "" {
				continue
			}
			item := s.workItemFromState(st, definitions, "todo", nodeID, st.UpdatedAt)
			if workItemMatches(item, query, actorRoles) {
				items = append(items, item)
			}
		}
	}
	sort.SliceStable(items, func(i, j int) bool {
		return compareWorkItems(items[i], items[j])
	})
	return paginateWorkItems(items, page, size), nil
}

// ListDoneItems 返回当前 scope 内已完成推进的 workflow 节点。
func (s *Service) ListDoneItems(ctx context.Context, page int, size int) (*workflowtypes.WorkItemPage, error) {
	return s.ListDoneItemsByQuery(ctx, workflowtypes.WorkItemQuery{Page: page, Size: size})
}

func (s *Service) ListMyDoneItems(ctx context.Context, query workflowtypes.WorkItemQuery) (*workflowtypes.WorkItemPage, error) {
	if query.UserID == "" {
		query.UserID = currentPrincipalID(ctx)
	}
	if query.UserID == "" {
		return nil, errors.NewCode(errors.Unauthorized, "workflow principal is required")
	}
	return s.ListDoneItemsByQuery(ctx, query)
}

func (s *Service) ListDoneItemsByQuery(ctx context.Context, query workflowtypes.WorkItemQuery) (*workflowtypes.WorkItemPage, error) {
	if err := s.validateReady(ctx); err != nil {
		return nil, err
	}
	page, size, err := normalizeWorkItemPage(query.Page, query.Size)
	if err != nil {
		return nil, err
	}
	instances, err := s.store.ListInstances(ctx)
	if err != nil {
		return nil, err
	}
	definitions, err := s.definitionMetasForStates(ctx, instances)
	if err != nil {
		return nil, err
	}
	items := make([]workflowtypes.WorkItem, 0)
	for _, st := range instances {
		if st == nil {
			continue
		}
		seen := map[string]struct{}{}
		for _, entry := range st.History {
			if !isNodeCompletionAction(entry.Action) {
				continue
			}
			nodeID := strings.TrimSpace(entry.NodeID)
			if nodeID == "" {
				continue
			}
			seen[nodeID] = struct{}{}
			item := s.workItemFromState(st, definitions, "done", nodeID, entry.At)
			if workItemMatches(item, query, nil) {
				items = append(items, item)
			}
		}
		for _, nodeID := range st.CompletedNodeIDs {
			nodeID = strings.TrimSpace(nodeID)
			if nodeID == "" {
				continue
			}
			if _, ok := seen[nodeID]; ok {
				continue
			}
			item := s.workItemFromState(st, definitions, "done", nodeID, st.UpdatedAt)
			if workItemMatches(item, query, nil) {
				items = append(items, item)
			}
		}
	}
	sort.SliceStable(items, func(i, j int) bool {
		return compareWorkItems(items[i], items[j])
	})
	return paginateWorkItems(items, page, size), nil
}

func (s *Service) ClaimTask(ctx context.Context, id runtime.ID, nodeID string, input workflowtypes.TaskActionInput) (*runtime.State, error) {
	st, nodeID, err := s.getActiveTaskState(ctx, id, nodeID)
	if err != nil {
		return nil, err
	}
	actorID, actorRoles, err := requireActor(ctx)
	if err != nil {
		return nil, err
	}
	meta := metadataFromState(st)
	current := assignmentForNode(meta, nodeID)
	if current.AssigneeID != "" && current.AssigneeID != actorID {
		return nil, errors.NewCode(errors.Conflict, "workflow task is already claimed").
			WithContext("instance_id", string(id)).
			WithContext("node_id", nodeID).
			WithContext("assignee_id", current.AssigneeID)
	}
	if current.AssigneeID == "" && !actorCanTakeAssignment(current, actorID, actorRoles) {
		return nil, errors.NewCode(errors.Forbidden, "workflow task cannot be claimed by current principal").
			WithContext("instance_id", string(id)).
			WithContext("node_id", nodeID)
	}
	now := s.clock.Now().UTC()
	current.AssigneeID = actorID
	current.AssigneeName = strings.TrimSpace(input.AssigneeName)
	current.ClaimedAt = &now
	current.UpdatedAt = &now
	meta.TaskAssignments[nodeID] = current
	appendAction(&meta, "claim", nodeID, actorID, current.AssigneeName, input.Comment, now)
	saveMetadata(st, meta, now)
	if err := s.store.SaveIfVersion(ctx, st, st.Version); err != nil {
		return nil, err
	}
	return s.GetInstance(ctx, id)
}

func (s *Service) UnclaimTask(ctx context.Context, id runtime.ID, nodeID string, input workflowtypes.TaskActionInput) (*runtime.State, error) {
	st, nodeID, err := s.getActiveTaskState(ctx, id, nodeID)
	if err != nil {
		return nil, err
	}
	actorID, _, err := requireActor(ctx)
	if err != nil {
		return nil, err
	}
	meta := metadataFromState(st)
	current := assignmentForNode(meta, nodeID)
	if current.AssigneeID == "" {
		return cloneState(st), nil
	}
	if current.AssigneeID != actorID {
		return nil, errors.NewCode(errors.Forbidden, "workflow task can only be unclaimed by current assignee").
			WithContext("instance_id", string(id)).
			WithContext("node_id", nodeID)
	}
	now := s.clock.Now().UTC()
	current.AssigneeID = ""
	current.AssigneeName = ""
	current.ClaimedAt = nil
	current.UpdatedAt = &now
	meta.TaskAssignments[nodeID] = current
	appendAction(&meta, "unclaim", nodeID, actorID, "", input.Comment, now)
	saveMetadata(st, meta, now)
	if err := s.store.SaveIfVersion(ctx, st, st.Version); err != nil {
		return nil, err
	}
	return s.GetInstance(ctx, id)
}

func (s *Service) TransferTask(ctx context.Context, id runtime.ID, nodeID string, input workflowtypes.TaskActionInput) (*runtime.State, error) {
	st, nodeID, err := s.getActiveTaskState(ctx, id, nodeID)
	if err != nil {
		return nil, err
	}
	targetID := strings.TrimSpace(input.AssigneeID)
	if targetID == "" {
		return nil, errors.NewCode(errors.InvalidInput, "workflow transfer assignee is required")
	}
	actorID, actorRoles, err := requireActor(ctx)
	if err != nil {
		return nil, err
	}
	meta := metadataFromState(st)
	now := s.clock.Now().UTC()
	current := assignmentForNode(meta, nodeID)
	if err := requireTaskOperator(id, nodeID, current, actorID, actorRoles); err != nil {
		return nil, err
	}
	if strings.TrimSpace(current.AssigneeID) == targetID {
		return nil, errors.NewCode(errors.Conflict, "workflow transfer assignee is unchanged").
			WithContext("instance_id", string(id)).
			WithContext("node_id", nodeID).
			WithContext("assignee_id", targetID)
	}
	operatorName := current.AssigneeName
	assignment := assignmentFromInput(targetID, input.AssigneeName, input.CandidateUserIDs, input.CandidateRoleIDs, &now, input.DueAt)
	if len(assignment.CandidateUserIDs) == 0 {
		assignment.CandidateUserIDs = cloneStrings(current.CandidateUserIDs)
	}
	if len(assignment.CandidateRoleIDs) == 0 {
		assignment.CandidateRoleIDs = cloneStrings(current.CandidateRoleIDs)
	}
	if assignment.DueAt == nil {
		assignment.DueAt = cloneTimePtr(current.DueAt)
	}
	meta.TaskAssignments[nodeID] = assignment
	appendAction(&meta, "transfer", nodeID, actorID, operatorName, input.Comment, now)
	saveMetadata(st, meta, now)
	if err := s.store.SaveIfVersion(ctx, st, st.Version); err != nil {
		return nil, err
	}
	return s.GetInstance(ctx, id)
}

func (s *Service) DelegateTask(ctx context.Context, id runtime.ID, nodeID string, input workflowtypes.TaskActionInput) (*runtime.State, error) {
	st, nodeID, err := s.getActiveTaskState(ctx, id, nodeID)
	if err != nil {
		return nil, err
	}
	targetID := strings.TrimSpace(input.AssigneeID)
	if targetID == "" {
		return nil, errors.NewCode(errors.InvalidInput, "workflow delegate assignee is required")
	}
	actorID, actorRoles, err := requireActor(ctx)
	if err != nil {
		return nil, err
	}
	meta := metadataFromState(st)
	now := s.clock.Now().UTC()
	current := assignmentForNode(meta, nodeID)
	if err := requireTaskOperator(id, nodeID, current, actorID, actorRoles); err != nil {
		return nil, err
	}
	if strings.TrimSpace(current.AssigneeID) == targetID {
		return nil, errors.NewCode(errors.Conflict, "workflow delegate assignee is unchanged").
			WithContext("instance_id", string(id)).
			WithContext("node_id", nodeID).
			WithContext("assignee_id", targetID)
	}
	operatorName := current.AssigneeName
	assignment := assignmentFromInput(targetID, input.AssigneeName, input.CandidateUserIDs, input.CandidateRoleIDs, &now, input.DueAt)
	if len(assignment.CandidateUserIDs) == 0 {
		assignment.CandidateUserIDs = cloneStrings(current.CandidateUserIDs)
	}
	if len(assignment.CandidateRoleIDs) == 0 {
		assignment.CandidateRoleIDs = cloneStrings(current.CandidateRoleIDs)
	}
	if assignment.DueAt == nil {
		assignment.DueAt = cloneTimePtr(current.DueAt)
	}
	assignment.DelegatorID = actorID
	assignment.DelegatorName = current.AssigneeName
	meta.TaskAssignments[nodeID] = assignment
	appendAction(&meta, "delegate", nodeID, actorID, operatorName, input.Comment, now)
	saveMetadata(st, meta, now)
	if err := s.store.SaveIfVersion(ctx, st, st.Version); err != nil {
		return nil, err
	}
	return s.GetInstance(ctx, id)
}

func (s *Service) RemindTask(ctx context.Context, id runtime.ID, nodeID string, input workflowtypes.TaskActionInput) (*runtime.State, error) {
	st, nodeID, err := s.getActiveTaskState(ctx, id, nodeID)
	if err != nil {
		return nil, err
	}
	actorID, actorRoles, err := requireActor(ctx)
	if err != nil {
		return nil, err
	}
	meta := metadataFromState(st)
	if err := requireTaskCoordinator(id, nodeID, meta, actorID, actorRoles); err != nil {
		return nil, err
	}
	now := s.clock.Now().UTC()
	appendAction(&meta, "remind", nodeID, actorID, "", input.Comment, now)
	saveMetadata(st, meta, now)
	if err := s.store.SaveIfVersion(ctx, st, st.Version); err != nil {
		return nil, err
	}
	return s.GetInstance(ctx, id)
}

func (s *Service) AddCCUsers(ctx context.Context, id runtime.ID, input workflowtypes.TaskActionInput) (*runtime.State, error) {
	if err := s.validateReady(ctx); err != nil {
		return nil, err
	}
	id = runtime.ID(strings.TrimSpace(string(id)))
	if id == "" {
		return nil, errors.NewCode(errors.InvalidInput, "workflow instance id is empty")
	}
	users := normalizeStringSlice(input.CCUserIDs)
	if len(users) == 0 {
		return nil, errors.NewCode(errors.InvalidInput, "workflow cc users are required")
	}
	actorID, actorRoles, err := requireActor(ctx)
	if err != nil {
		return nil, err
	}
	st, err := s.GetInstance(ctx, id)
	if err != nil {
		return nil, err
	}
	meta := metadataFromState(st)
	if err := requireInstanceCoordinator(id, st, meta, actorID, actorRoles); err != nil {
		return nil, err
	}
	meta.CCUserIDs = mergeStrings(meta.CCUserIDs, users)
	now := s.clock.Now().UTC()
	appendAction(&meta, "cc", "", actorID, "", input.Comment, now)
	saveMetadata(st, meta, now)
	if err := s.store.SaveIfVersion(ctx, st, st.Version); err != nil {
		return nil, err
	}
	return s.GetInstance(ctx, id)
}

func (s *Service) WithdrawInstance(ctx context.Context, id runtime.ID, input workflowtypes.TaskActionInput) (*runtime.State, error) {
	if err := s.validateReady(ctx); err != nil {
		return nil, err
	}
	id = runtime.ID(strings.TrimSpace(string(id)))
	if id == "" {
		return nil, errors.NewCode(errors.InvalidInput, "workflow instance id is empty")
	}
	st, err := s.GetInstance(ctx, id)
	if err != nil {
		return nil, err
	}
	if st.Status == runtime.InstanceStatusCompleted || st.Status == withdrawnInstanceStatus {
		return nil, errors.NewCode(errors.Conflict, "workflow instance cannot be withdrawn").
			WithContext("instance_id", string(id)).
			WithContext("status", string(st.Status))
	}
	actorID, _, err := requireActor(ctx)
	if err != nil {
		return nil, err
	}
	meta := metadataFromState(st)
	if meta.InitiatorID != "" && meta.InitiatorID != actorID {
		return nil, errors.NewCode(errors.Forbidden, "workflow instance can only be withdrawn by initiator").
			WithContext("instance_id", string(id))
	}
	now := s.clock.Now().UTC()
	applyWithdrawState(st, now)
	appendAction(&meta, "withdraw", "", actorID, "", input.Comment, now)
	saveMetadata(st, meta, now)
	if err := s.store.SaveIfVersion(ctx, st, st.Version); err != nil {
		return nil, err
	}
	return s.GetInstance(ctx, id)
}

func applyWithdrawState(st *runtime.State, now time.Time) {
	st.Status = withdrawnInstanceStatus
	st.ActiveNodeIDs = []string{}
	st.CurrentNodeID = ""
	st.PendingJoins = []runtime.PendingJoin{}
	st.CompletedAt = now
	st.History = append(st.History, runtime.HistoryEntry{Action: "withdraw", At: now})
}

type definitionMeta struct {
	name  string
	nodes map[string]workflowtypes.RuntimeNode
}

func (s *Service) definitionMetasForStates(ctx context.Context, states []*runtime.State) (map[string]definitionMeta, error) {
	ids := make(map[string]struct{})
	for _, st := range states {
		if st == nil || strings.TrimSpace(st.DefinitionID) == "" {
			continue
		}
		ids[st.DefinitionID] = struct{}{}
	}
	metas := make(map[string]definitionMeta, len(ids))
	for id := range ids {
		definition, err := s.store.GetDesign(ctx, id)
		if err != nil {
			return nil, err
		}
		if definition == nil {
			continue
		}
		metas[id] = buildDefinitionMeta(definition)
	}
	return metas, nil
}

func buildDefinitionMeta(definition *workflowtypes.DesignerDefinition) definitionMeta {
	if definition == nil {
		return definitionMeta{nodes: map[string]workflowtypes.RuntimeNode{}}
	}
	meta := definitionMeta{name: definition.Name, nodes: map[string]workflowtypes.RuntimeNode{}}
	if definition.RuntimeDefinition != nil {
		for _, node := range definition.RuntimeDefinition.Nodes {
			meta.nodes[node.ID] = node
		}
	}
	if len(meta.nodes) == 0 {
		for _, node := range definition.Nodes {
			meta.nodes[node.ID] = workflowtypes.RuntimeNode{
				ID:       node.ID,
				Name:     node.Name,
				Type:     node.Type,
				Next:     append([]string(nil), node.Next...),
				RejectTo: append([]string(nil), node.RejectTo...),
			}
		}
	}
	return meta
}

func (s *Service) workItemFromState(
	st *runtime.State,
	definitions map[string]definitionMeta,
	itemType string,
	nodeID string,
	eventAt time.Time,
) workflowtypes.WorkItem {
	meta := definitions[st.DefinitionID]
	node := meta.nodes[nodeID]
	instanceMeta := metadataFromState(st)
	assignment := assignmentForNode(instanceMeta, nodeID)
	action := actionForNode(instanceMeta, itemType, nodeID, eventAt)
	if eventAt.IsZero() {
		eventAt = st.UpdatedAt
	}
	item := workflowtypes.WorkItem{
		ID:               fmt.Sprintf("%s:%s:%s", itemType, st.ID, nodeID),
		Type:             itemType,
		InstanceID:       string(st.ID),
		DefinitionID:     st.DefinitionID,
		DefinitionName:   meta.name,
		BusinessKey:      instanceMeta.BusinessKey,
		Title:            instanceMeta.Title,
		NodeID:           nodeID,
		NodeName:         node.Name,
		NodeType:         node.Type,
		Status:           st.Status,
		InitiatorID:      instanceMeta.InitiatorID,
		InitiatorName:    instanceMeta.InitiatorName,
		AssigneeID:       assignment.AssigneeID,
		AssigneeName:     assignment.AssigneeName,
		CandidateUserIDs: cloneStrings(assignment.CandidateUserIDs),
		CandidateRoleIDs: cloneStrings(assignment.CandidateRoleIDs),
		DelegatorID:      assignment.DelegatorID,
		DelegatorName:    assignment.DelegatorName,
		CCUserIDs:        cloneStrings(instanceMeta.CCUserIDs),
		ActorID:          action.ActorID,
		ActorName:        action.ActorName,
		Action:           action.Action,
		Comment:          action.Comment,
		Data:             workflowtypes.PublicInstanceData(st.Data),
		DueAt:            cloneTimePtr(assignment.DueAt),
		Overdue:          isOverdue(assignment, s.clock.Now().UTC()),
		CreatedAt:        st.CreatedAt,
		UpdatedAt:        eventAt,
	}
	if !st.StartedAt.IsZero() {
		startedAt := st.StartedAt
		item.StartedAt = &startedAt
	}
	if itemType == "done" && !eventAt.IsZero() {
		completedAt := eventAt
		item.CompletedAt = &completedAt
	}
	return item
}

func (s *Service) applyNodeCompletedMetadata(ctx context.Context, st *runtime.State, nodeID string, input workflowtypes.AdvanceNodeInput, action string, now time.Time) error {
	meta := metadataFromState(st)
	actorID := currentPrincipalID(ctx)
	var actorRoles []string
	if principal, ok := auth.PrincipalFromContext(ctx); ok {
		actorRoles = normalizeStringSlice(principal.Roles)
	}
	current := assignmentForNode(meta, nodeID)
	if !emptyAssignment(current) {
		if actorID == "" {
			return errors.NewCode(errors.Unauthorized, "workflow principal is required")
		}
		if !actorCanCompleteAssignment(current, actorID, actorRoles) {
			return errors.NewCode(errors.Forbidden, "workflow task can only be completed by assignee or candidate").
				WithContext("instance_id", string(st.ID)).
				WithContext("node_id", nodeID)
		}
	}
	if !isNodeCompletionAction(action) {
		action = "complete"
	}
	appendAction(&meta, action, nodeID, actorID, "", input.Comment, now)
	delete(meta.TaskAssignments, nodeID)
	if len(input.CCUserIDs) > 0 {
		meta.CCUserIDs = mergeStrings(meta.CCUserIDs, input.CCUserIDs)
	}
	nextAssignment := assignmentFromInput(input.AssigneeID, input.AssigneeName, input.CandidateUserIDs, input.CandidateRoleIDs, nil, input.DueAt)
	saveMetadata(st, meta, now)
	s.applyActiveAssignments(st, &nextAssignment, now)
	return nil
}

func (s *Service) applyNodeRejectedMetadata(ctx context.Context, st *runtime.State, nodeID string, targetNodeID string, input workflowtypes.RejectNodeInput, now time.Time) error {
	meta := metadataFromState(st)
	actorID := currentPrincipalID(ctx)
	var actorRoles []string
	if principal, ok := auth.PrincipalFromContext(ctx); ok {
		actorRoles = normalizeStringSlice(principal.Roles)
	}
	current := assignmentForNode(meta, nodeID)
	if !emptyAssignment(current) {
		if actorID == "" {
			return errors.NewCode(errors.Unauthorized, "workflow principal is required")
		}
		if !actorCanCompleteAssignment(current, actorID, actorRoles) {
			return errors.NewCode(errors.Forbidden, "workflow task can only be rejected by assignee or candidate").
				WithContext("instance_id", string(st.ID)).
				WithContext("node_id", nodeID)
		}
	}
	comment := strings.TrimSpace(input.Comment)
	if comment == "" {
		comment = strings.TrimSpace(input.Reason)
	}
	appendTargetAction(&meta, "reject", nodeID, targetNodeID, actorID, "", comment, now)
	removeInactiveAssignments(&meta, st.ActiveNodeIDs)
	if len(input.CCUserIDs) > 0 {
		meta.CCUserIDs = mergeStrings(meta.CCUserIDs, input.CCUserIDs)
	}
	targetAssignment := assignmentFromInput(input.AssigneeID, input.AssigneeName, input.CandidateUserIDs, input.CandidateRoleIDs, nil, input.DueAt)
	saveMetadata(st, meta, now)
	s.applyAssignmentToNode(st, targetNodeID, &targetAssignment, now)
	return nil
}

func (s *Service) applyActiveAssignments(st *runtime.State, assignment *workflowtypes.TaskAssignment, now time.Time) bool {
	if st == nil || len(st.ActiveNodeIDs) == 0 {
		return false
	}
	meta := metadataFromState(st)
	changed := false
	defaultAssignment := assignmentForNode(meta, "*")
	for _, nodeID := range st.ActiveNodeIDs {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID == "" {
			continue
		}
		if _, ok := meta.TaskAssignments[nodeID]; ok {
			continue
		}
		next := defaultAssignment
		if assignment != nil && !emptyAssignment(*assignment) {
			next = *assignment
		}
		if emptyAssignment(next) {
			continue
		}
		next.UpdatedAt = &now
		meta.TaskAssignments[nodeID] = next
		changed = true
	}
	if !changed {
		return false
	}
	saveMetadata(st, meta, now)
	return true
}

func (s *Service) applyAssignmentToNode(st *runtime.State, nodeID string, assignment *workflowtypes.TaskAssignment, now time.Time) bool {
	if st == nil || !containsString(st.ActiveNodeIDs, nodeID) {
		return false
	}
	meta := metadataFromState(st)
	if _, ok := meta.TaskAssignments[nodeID]; ok {
		return false
	}
	next := assignmentForNode(meta, "*")
	if assignment != nil && !emptyAssignment(*assignment) {
		next = *assignment
	}
	if emptyAssignment(next) {
		return false
	}
	next.UpdatedAt = &now
	meta.TaskAssignments[nodeID] = next
	saveMetadata(st, meta, now)
	return true
}

func removeInactiveAssignments(meta *workflowtypes.InstanceMetadata, activeNodeIDs []string) {
	if meta == nil || meta.TaskAssignments == nil {
		return
	}
	active := make(map[string]struct{}, len(activeNodeIDs))
	for _, nodeID := range activeNodeIDs {
		active[strings.TrimSpace(nodeID)] = struct{}{}
	}
	for nodeID := range meta.TaskAssignments {
		if nodeID == "*" {
			continue
		}
		if _, ok := active[nodeID]; !ok {
			delete(meta.TaskAssignments, nodeID)
		}
	}
}

func (s *Service) getActiveTaskState(ctx context.Context, id runtime.ID, nodeID string) (*runtime.State, string, error) {
	if err := s.validateReady(ctx); err != nil {
		return nil, "", err
	}
	id = runtime.ID(strings.TrimSpace(string(id)))
	nodeID = strings.TrimSpace(nodeID)
	if id == "" {
		return nil, "", errors.NewCode(errors.InvalidInput, "workflow instance id is empty")
	}
	if nodeID == "" {
		return nil, "", errors.NewCode(errors.InvalidInput, "workflow node id is empty")
	}
	st, err := s.GetInstance(ctx, id)
	if err != nil {
		return nil, "", err
	}
	if st.Status != runtime.InstanceStatusRunning || !containsString(st.ActiveNodeIDs, nodeID) {
		return nil, "", errors.NewCode(errors.Conflict, "workflow node is not active").
			WithContext("instance_id", string(id)).
			WithContext("node_id", nodeID)
	}
	return st, nodeID, nil
}

func metadataFromState(st *runtime.State) workflowtypes.InstanceMetadata {
	if st == nil {
		return workflowtypes.InstanceMetadata{TaskAssignments: map[string]workflowtypes.TaskAssignment{}}
	}
	if meta := workflowtypes.InstanceMetadataFromData(st.Data); meta != nil {
		if meta.TaskAssignments == nil {
			meta.TaskAssignments = map[string]workflowtypes.TaskAssignment{}
		}
		return *meta
	}
	return workflowtypes.InstanceMetadata{TaskAssignments: map[string]workflowtypes.TaskAssignment{}}
}

func saveMetadata(st *runtime.State, meta workflowtypes.InstanceMetadata, now time.Time) {
	if st.Data == nil {
		st.Data = map[string]any{}
	}
	if meta.TaskAssignments == nil {
		meta.TaskAssignments = map[string]workflowtypes.TaskAssignment{}
	}
	st.Data[workflowtypes.WorkflowMetadataDataKey] = meta
	st.UpdatedAt = now
}

func assignmentFromInput(assigneeID string, assigneeName string, candidateUserIDs []string, candidateRoleIDs []string, claimedAt *time.Time, dueAt *time.Time) workflowtypes.TaskAssignment {
	out := workflowtypes.TaskAssignment{
		AssigneeID:       strings.TrimSpace(assigneeID),
		AssigneeName:     strings.TrimSpace(assigneeName),
		CandidateUserIDs: normalizeStringSlice(candidateUserIDs),
		CandidateRoleIDs: normalizeStringSlice(candidateRoleIDs),
	}
	if claimedAt != nil {
		at := *claimedAt
		out.ClaimedAt = &at
		out.UpdatedAt = &at
	}
	if dueAt != nil && !dueAt.IsZero() {
		due := dueAt.UTC()
		out.DueAt = &due
	}
	return out
}

func assignmentForNode(meta workflowtypes.InstanceMetadata, nodeID string) workflowtypes.TaskAssignment {
	if meta.TaskAssignments == nil {
		return workflowtypes.TaskAssignment{}
	}
	if assignment, ok := meta.TaskAssignments[nodeID]; ok {
		return assignment
	}
	return meta.TaskAssignments["*"]
}

func emptyAssignment(assignment workflowtypes.TaskAssignment) bool {
	return strings.TrimSpace(assignment.AssigneeID) == "" &&
		strings.TrimSpace(assignment.AssigneeName) == "" &&
		len(assignment.CandidateUserIDs) == 0 &&
		len(assignment.CandidateRoleIDs) == 0 &&
		assignment.DueAt == nil
}

func appendAction(meta *workflowtypes.InstanceMetadata, action string, nodeID string, actorID string, actorName string, comment string, at time.Time) {
	appendTargetAction(meta, action, nodeID, "", actorID, actorName, comment, at)
}

func appendTargetAction(meta *workflowtypes.InstanceMetadata, action string, nodeID string, targetNodeID string, actorID string, actorName string, comment string, at time.Time) {
	if meta == nil {
		return
	}
	meta.Actions = append(meta.Actions, workflowtypes.WorkflowAction{
		Action:       strings.TrimSpace(action),
		NodeID:       strings.TrimSpace(nodeID),
		TargetNodeID: strings.TrimSpace(targetNodeID),
		ActorID:      strings.TrimSpace(actorID),
		ActorName:    strings.TrimSpace(actorName),
		Comment:      strings.TrimSpace(comment),
		At:           at,
	})
}

func actionForNode(meta workflowtypes.InstanceMetadata, itemType string, nodeID string, eventAt time.Time) workflowtypes.WorkflowAction {
	if itemType != "done" {
		return workflowtypes.WorkflowAction{}
	}
	for i := len(meta.Actions) - 1; i >= 0; i-- {
		action := meta.Actions[i]
		if action.NodeID == nodeID && isNodeCompletionAction(action.Action) {
			return action
		}
	}
	return workflowtypes.WorkflowAction{Action: "complete", NodeID: nodeID, At: eventAt}
}

func isNodeCompletionAction(action string) bool {
	switch strings.TrimSpace(action) {
	case "advance", "complete":
		return true
	default:
		return false
	}
}

func currentPrincipalID(ctx context.Context) string {
	principal, ok := auth.PrincipalFromContext(ctx)
	if !ok || principal.SubjectID <= 0 {
		return ""
	}
	return strconv.FormatInt(principal.SubjectID, 10)
}

func currentPrincipalRoles(ctx context.Context) []string {
	principal, ok := auth.PrincipalFromContext(ctx)
	if !ok {
		return nil
	}
	return normalizeStringSlice(principal.Roles)
}

func requireActor(ctx context.Context) (string, []string, error) {
	principal, err := auth.RequirePrincipal(ctx)
	if err != nil {
		return "", nil, errors.NewCode(errors.Unauthorized, "workflow principal is required")
	}
	if principal.SubjectID <= 0 {
		return "", nil, errors.NewCode(errors.Unauthorized, "workflow principal is required")
	}
	return strconv.FormatInt(principal.SubjectID, 10), normalizeStringSlice(principal.Roles), nil
}

func actorCanTakeAssignment(assignment workflowtypes.TaskAssignment, actorID string, actorRoles []string) bool {
	if actorID == "" {
		return false
	}
	if len(assignment.CandidateUserIDs) > 0 {
		return containsString(assignment.CandidateUserIDs, actorID)
	}
	if len(assignment.CandidateRoleIDs) > 0 {
		for _, roleID := range assignment.CandidateRoleIDs {
			if containsString(actorRoles, roleID) {
				return true
			}
		}
		return false
	}
	return true
}

func actorCanCompleteAssignment(assignment workflowtypes.TaskAssignment, actorID string, actorRoles []string) bool {
	if actorID == "" {
		return false
	}
	if assignment.AssigneeID != "" {
		return assignment.AssigneeID == actorID
	}
	return actorCanTakeAssignment(assignment, actorID, actorRoles)
}

func requireTaskOperator(instanceID runtime.ID, nodeID string, assignment workflowtypes.TaskAssignment, actorID string, actorRoles []string) error {
	if emptyAssignment(assignment) {
		return nil
	}
	if actorCanCompleteAssignment(assignment, actorID, actorRoles) {
		return nil
	}
	return errors.NewCode(errors.Forbidden, "workflow task can only be operated by assignee or candidate").
		WithContext("instance_id", string(instanceID)).
		WithContext("node_id", nodeID)
}

func requireTaskCoordinator(instanceID runtime.ID, nodeID string, meta workflowtypes.InstanceMetadata, actorID string, actorRoles []string) error {
	if meta.InitiatorID != "" && meta.InitiatorID == actorID {
		return nil
	}
	assignment := assignmentForNode(meta, nodeID)
	if !emptyAssignment(assignment) {
		return requireTaskOperator(instanceID, nodeID, assignment, actorID, actorRoles)
	}
	return errors.NewCode(errors.Forbidden, "workflow task can only be operated by initiator or current task operator").
		WithContext("instance_id", string(instanceID)).
		WithContext("node_id", nodeID)
}

func requireInstanceCoordinator(instanceID runtime.ID, st *runtime.State, meta workflowtypes.InstanceMetadata, actorID string, actorRoles []string) error {
	if meta.InitiatorID != "" && meta.InitiatorID == actorID {
		return nil
	}
	if st != nil {
		for _, nodeID := range st.ActiveNodeIDs {
			assignment := assignmentForNode(meta, nodeID)
			if !emptyAssignment(assignment) && requireTaskOperator(instanceID, nodeID, assignment, actorID, actorRoles) == nil {
				return nil
			}
		}
	}
	if meta.InitiatorID == "" && len(meta.TaskAssignments) == 0 {
		return nil
	}
	return errors.NewCode(errors.Forbidden, "workflow instance can only be operated by initiator or current task operator").
		WithContext("instance_id", string(instanceID))
}

func normalizeStringSlice(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func mergeStrings(left []string, right []string) []string {
	return normalizeStringSlice(append(cloneStrings(left), right...))
}

func containsString(values []string, target string) bool {
	target = strings.TrimSpace(target)
	if target == "" {
		return false
	}
	for _, value := range values {
		if strings.TrimSpace(value) == target {
			return true
		}
	}
	return false
}

func normalizeInstanceQuery(query workflowtypes.InstanceQuery) workflowtypes.InstanceQuery {
	query.Status = strings.TrimSpace(query.Status)
	query.DefinitionID = strings.TrimSpace(query.DefinitionID)
	query.ActiveNodeID = strings.TrimSpace(query.ActiveNodeID)
	query.InitiatorID = strings.TrimSpace(query.InitiatorID)
	query.AssigneeID = strings.TrimSpace(query.AssigneeID)
	query.Keyword = strings.ToLower(strings.TrimSpace(query.Keyword))
	return query
}

func instanceMatches(st *runtime.State, definitions map[string]definitionMeta, query workflowtypes.InstanceQuery) bool {
	if query.Status != "" && string(st.Status) != query.Status {
		return false
	}
	if query.DefinitionID != "" && st.DefinitionID != query.DefinitionID {
		return false
	}
	if query.ActiveNodeID != "" && !containsString(st.ActiveNodeIDs, query.ActiveNodeID) {
		return false
	}
	meta := metadataFromState(st)
	if query.InitiatorID != "" && meta.InitiatorID != query.InitiatorID {
		return false
	}
	if query.AssigneeID != "" && !metadataHasAssignee(meta, query.AssigneeID) {
		return false
	}
	if query.Keyword != "" && !instanceKeywordMatches(st, definitions, meta, query.Keyword) {
		return false
	}
	return true
}

func instanceKeywordMatches(st *runtime.State, definitions map[string]definitionMeta, meta workflowtypes.InstanceMetadata, keyword string) bool {
	definition := definitions[st.DefinitionID]
	haystack := strings.ToLower(strings.Join([]string{
		string(st.ID),
		st.DefinitionID,
		definition.name,
		meta.BusinessKey,
		meta.Title,
		meta.InitiatorName,
	}, " "))
	return strings.Contains(haystack, keyword)
}

func metadataHasAssignee(meta workflowtypes.InstanceMetadata, assigneeID string) bool {
	for _, assignment := range meta.TaskAssignments {
		if assignment.AssigneeID == assigneeID || containsString(assignment.CandidateUserIDs, assigneeID) {
			return true
		}
	}
	return false
}

func (s *Service) instanceSummaryFromState(st *runtime.State, definitions map[string]definitionMeta) workflowtypes.InstanceSummary {
	meta := metadataFromState(st)
	out := workflowtypes.InstanceSummary{
		ID:             string(st.ID),
		DefinitionID:   st.DefinitionID,
		DefinitionName: definitions[st.DefinitionID].name,
		BusinessKey:    meta.BusinessKey,
		Title:          meta.Title,
		Status:         st.Status,
		ActiveNodeIDs:  cloneStrings(st.ActiveNodeIDs),
		CurrentNodeID:  st.CurrentNodeID,
		InitiatorID:    meta.InitiatorID,
		InitiatorName:  meta.InitiatorName,
		AssigneeIDs:    activeAssigneeIDs(st, meta),
		CCUserIDs:      cloneStrings(meta.CCUserIDs),
		EarliestDueAt:  earliestActiveDueAt(st, meta),
		OverdueCount:   activeOverdueCount(st, meta, s.clock.Now().UTC()),
		CreatedAt:      st.CreatedAt,
		UpdatedAt:      st.UpdatedAt,
	}
	if !st.StartedAt.IsZero() {
		startedAt := st.StartedAt
		out.StartedAt = &startedAt
	}
	if !st.CompletedAt.IsZero() {
		completedAt := st.CompletedAt
		out.CompletedAt = &completedAt
	}
	return out
}

func activeAssigneeIDs(st *runtime.State, meta workflowtypes.InstanceMetadata) []string {
	if st == nil {
		return nil
	}
	seen := map[string]struct{}{}
	var out []string
	for _, nodeID := range st.ActiveNodeIDs {
		assignment := assignmentForNode(meta, nodeID)
		if assignment.AssigneeID == "" {
			continue
		}
		if _, ok := seen[assignment.AssigneeID]; ok {
			continue
		}
		seen[assignment.AssigneeID] = struct{}{}
		out = append(out, assignment.AssigneeID)
	}
	return out
}

func earliestActiveDueAt(st *runtime.State, meta workflowtypes.InstanceMetadata) *time.Time {
	if st == nil {
		return nil
	}
	var earliest *time.Time
	for _, nodeID := range st.ActiveNodeIDs {
		dueAt := assignmentForNode(meta, nodeID).DueAt
		if dueAt == nil || dueAt.IsZero() {
			continue
		}
		due := dueAt.UTC()
		if earliest == nil || due.Before(*earliest) {
			earliest = &due
		}
	}
	return earliest
}

func activeOverdueCount(st *runtime.State, meta workflowtypes.InstanceMetadata, now time.Time) int {
	if st == nil {
		return 0
	}
	count := 0
	for _, nodeID := range st.ActiveNodeIDs {
		if isOverdue(assignmentForNode(meta, nodeID), now) {
			count++
		}
	}
	return count
}

func isOverdue(assignment workflowtypes.TaskAssignment, now time.Time) bool {
	return assignment.DueAt != nil && !assignment.DueAt.IsZero() && assignment.DueAt.Before(now)
}

func cloneTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := value.UTC()
	return &cloned
}

func workItemMatches(item workflowtypes.WorkItem, query workflowtypes.WorkItemQuery, actorRoles []string) bool {
	userID := strings.TrimSpace(query.UserID)
	if userID != "" {
		switch item.Type {
		case "todo":
			if item.AssigneeID != userID && !containsString(item.CandidateUserIDs, userID) && !containsAnyString(item.CandidateRoleIDs, actorRoles) {
				return false
			}
		case "done":
			if item.ActorID != userID {
				return false
			}
		}
	}
	if query.DefinitionID != "" && item.DefinitionID != strings.TrimSpace(query.DefinitionID) {
		return false
	}
	keyword := strings.ToLower(strings.TrimSpace(query.Keyword))
	if keyword == "" {
		return true
	}
	haystack := strings.ToLower(strings.Join([]string{
		item.InstanceID,
		item.DefinitionID,
		item.DefinitionName,
		item.BusinessKey,
		item.Title,
		item.NodeID,
		item.NodeName,
		item.InitiatorName,
		item.AssigneeName,
	}, " "))
	return strings.Contains(haystack, keyword)
}

func containsAnyString(values []string, targets []string) bool {
	for _, target := range targets {
		if containsString(values, target) {
			return true
		}
	}
	return false
}

func paginateInstances(items []workflowtypes.InstanceSummary, page int, size int) *workflowtypes.InstancePage {
	total := len(items)
	start := (page - 1) * size
	if start >= total {
		return &workflowtypes.InstancePage{Items: []workflowtypes.InstanceSummary{}, Total: total, Page: page, Size: size}
	}
	end := start + size
	if end > total {
		end = total
	}
	return &workflowtypes.InstancePage{
		Items: append([]workflowtypes.InstanceSummary(nil), items[start:end]...),
		Total: total,
		Page:  page,
		Size:  size,
	}
}

func normalizeWorkItemPage(page int, size int) (int, int, error) {
	if page == 0 {
		page = 1
	}
	if size == 0 {
		size = 20
	}
	if page < 0 {
		return 0, 0, errors.NewCode(errors.InvalidInput, "workflow work item page must be greater than 0")
	}
	if size < 0 {
		return 0, 0, errors.NewCode(errors.InvalidInput, "workflow work item size must be greater than 0")
	}
	if size > 100 {
		return 0, 0, errors.NewCode(errors.InvalidInput, "workflow work item size must not exceed 100")
	}
	if page > 1 && page-1 > math.MaxInt/size {
		return 0, 0, errors.NewCode(errors.InvalidInput, "workflow pagination offset is too large")
	}
	return page, size, nil
}

func paginateWorkItems(items []workflowtypes.WorkItem, page int, size int) *workflowtypes.WorkItemPage {
	total := len(items)
	start := (page - 1) * size
	if start >= total {
		return &workflowtypes.WorkItemPage{Items: []workflowtypes.WorkItem{}, Total: total, Page: page, Size: size}
	}
	end := start + size
	if end > total {
		end = total
	}
	return &workflowtypes.WorkItemPage{
		Items: append([]workflowtypes.WorkItem(nil), items[start:end]...),
		Total: total,
		Page:  page,
		Size:  size,
	}
}

func compareWorkItems(left workflowtypes.WorkItem, right workflowtypes.WorkItem) bool {
	if !left.UpdatedAt.Equal(right.UpdatedAt) {
		return left.UpdatedAt.After(right.UpdatedAt)
	}
	if left.InstanceID != right.InstanceID {
		return left.InstanceID < right.InstanceID
	}
	return left.NodeID < right.NodeID
}

func (s *Service) validateReady(ctx context.Context) error {
	if ctx == nil {
		return errors.NewCode(errors.InvalidInput, "workflow context is nil")
	}
	if s == nil || s.store == nil {
		return errors.NewCode(errors.InvalidInput, "workflow service store is nil")
	}
	if s.engine == nil {
		return errors.NewCode(errors.InvalidInput, "workflow service engine is nil")
	}
	if s.idGenerator == nil {
		return errors.NewCode(errors.InvalidInput, "workflow service id generator is nil")
	}
	if s.clock == nil {
		return errors.NewCode(errors.InvalidInput, "workflow service clock is nil")
	}
	if strictScopeRequired(ctx) && !hasExplicitScope(ctx) {
		return errors.NewCode(errors.InvalidInput, "workflow scope is required")
	}
	return validateScope(ScopeFromContext(ctx))
}
