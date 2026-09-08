package httpapi

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"time"

	iamauth "gochen-iam/auth"
	iammw "gochen-iam/middleware"
	auth "gochen-runtime/host/authz"
	workflowpermissions "gochen-workflow/permissions"
	workflowservice "gochen-workflow/service"
	workflowtypes "gochen-workflow/types"
	"gochen/errors"
	"gochen/httpx"
	runtime "gochen/process/workflow"
)

// Registrar 将 workflow 业务接口挂载到 GoChen httpx 路由组。
type Registrar struct {
	service *workflowservice.Service
}

// NewRegistrar 创建 workflow HTTP 路由注册器。
func NewRegistrar(service *workflowservice.Service) *Registrar {
	return &Registrar{service: service}
}

// RegisterRoutes 注册 workflow definitions 与 instances API。
func (r *Registrar) RegisterRoutes(group httpx.IRouteGroup) error {
	if r == nil || r.service == nil {
		return errors.NewCode(errors.InvalidInput, "workflow route registrar requires service")
	}
	if group == nil {
		return errors.NewCode(errors.InvalidInput, "workflow route group is nil")
	}

	readDefinitions := group.Group("/definitions").Use(iammw.PermissionMiddleware(workflowpermissions.WorkflowRead))
	readDefinitions.GET("", r.listDefinitions)
	readDefinitions.GET("/:id", r.getDefinition)

	writeDefinitions := group.Group("/definitions").Use(iammw.PermissionMiddleware(workflowpermissions.WorkflowDesign))
	writeDefinitions.POST("", r.createDefinition)
	writeDefinitions.PUT("/:id", r.updateDefinition)
	writeDefinitions.DELETE("/:id", r.deleteDefinition)
	writeDefinitions.POST("/:id/publish", r.publishDefinition)
	writeDefinitions.POST("/:id/archive", r.archiveDefinition)

	readInstances := group.Group("/instances").Use(iammw.PermissionMiddleware(workflowpermissions.WorkflowRead))
	readInstances.GET("", r.listInstances)
	readInstances.GET("/:id", r.getInstance)

	readWorkItems := group.Group("").Use(iammw.PermissionMiddleware(workflowpermissions.WorkflowRead))
	readWorkItems.GET("/todos", r.listTodos)
	readWorkItems.GET("/done", r.listDone)
	readWorkItems.GET("/started", r.listStarted)
	readWorkItems.GET("/my/todos", r.listMyTodos)
	readWorkItems.GET("/my/done", r.listMyDone)
	readWorkItems.GET("/my/started", r.listMyStarted)

	runInstances := group.Group("/instances").Use(iammw.PermissionMiddleware(workflowpermissions.WorkflowRun))
	runInstances.POST("", r.createInstance)
	runInstances.POST("/:id/start", r.startInstance)
	runInstances.POST("/:id/nodes/:node_id/advance", r.advanceNode)
	runInstances.POST("/:id/nodes/:node_id/complete", r.completeNode)
	runInstances.POST("/:id/nodes/:node_id/reject", r.rejectNode)
	runInstances.POST("/:id/nodes/:node_id/claim", r.claimTask)
	runInstances.POST("/:id/nodes/:node_id/unclaim", r.unclaimTask)
	runInstances.POST("/:id/nodes/:node_id/transfer", r.transferTask)
	runInstances.POST("/:id/nodes/:node_id/delegate", r.delegateTask)
	runInstances.POST("/:id/nodes/:node_id/remind", r.remindTask)
	runInstances.POST("/:id/cc", r.addCCUsers)
	runInstances.POST("/:id/withdraw", r.withdrawInstance)
	return nil
}

func (r *Registrar) listDefinitions(ctx httpx.IContext) error {
	serviceCtx, err := r.serviceContext(ctx)
	if err != nil {
		return err
	}
	query, err := definitionQuery(ctx)
	if err != nil {
		return err
	}
	response, err := r.service.BuildDefinitionListResponseWithQuery(serviceCtx, query)
	if err != nil {
		return err
	}
	return httpx.WriteSuccess(ctx, response)
}

func definitionQuery(ctx httpx.IContext) (workflowtypes.DefinitionQuery, error) {
	page, size, err := workItemPageQuery(ctx)
	if err != nil {
		return workflowtypes.DefinitionQuery{}, err
	}
	return workflowtypes.DefinitionQuery{
		Page:    page,
		Size:    size,
		Keyword: strings.TrimSpace(ctx.Query("keyword")),
		Status:  workflowtypes.DefinitionStatus(strings.TrimSpace(ctx.Query("status"))),
	}, nil
}

func (r *Registrar) createDefinition(ctx httpx.IContext) error {
	serviceCtx, err := r.serviceContext(ctx)
	if err != nil {
		return err
	}
	var input workflowtypes.DesignerDefinition
	if err := ctx.BindJSON(&input); err != nil {
		return err
	}
	created, err := r.service.SaveDefinition(serviceCtx, &input)
	if err != nil {
		return err
	}
	return httpx.WriteCreated(ctx, created)
}

func (r *Registrar) getDefinition(ctx httpx.IContext) error {
	serviceCtx, err := r.serviceContext(ctx)
	if err != nil {
		return err
	}
	id, err := requiredParamValue(ctx, "id", "workflow definition id is empty")
	if err != nil {
		return err
	}
	item, err := r.service.GetDefinition(serviceCtx, id)
	if err != nil {
		return err
	}
	return httpx.WriteSuccess(ctx, item)
}

func (r *Registrar) updateDefinition(ctx httpx.IContext) error {
	serviceCtx, err := r.serviceContext(ctx)
	if err != nil {
		return err
	}
	var input workflowtypes.DesignerDefinition
	if err := ctx.BindJSON(&input); err != nil {
		return err
	}
	id, err := requiredParamValue(ctx, "id", "workflow definition id is empty")
	if err != nil {
		return err
	}
	input.ID = id
	saved, err := r.service.SaveDefinition(serviceCtx, &input)
	if err != nil {
		return err
	}
	return httpx.WriteSuccess(ctx, saved)
}

func (r *Registrar) deleteDefinition(ctx httpx.IContext) error {
	serviceCtx, err := r.serviceContext(ctx)
	if err != nil {
		return err
	}
	id, err := requiredParamValue(ctx, "id", "workflow definition id is empty")
	if err != nil {
		return err
	}
	if err := r.service.DeleteDefinition(serviceCtx, id); err != nil {
		return err
	}
	return httpx.WriteSuccess(ctx, map[string]any{"deleted": true})
}

func (r *Registrar) publishDefinition(ctx httpx.IContext) error {
	serviceCtx, err := r.serviceContext(ctx)
	if err != nil {
		return err
	}
	id, err := requiredParamValue(ctx, "id", "workflow definition id is empty")
	if err != nil {
		return err
	}
	published, err := r.service.PublishDefinition(serviceCtx, id)
	if err != nil {
		return err
	}
	return httpx.WriteSuccess(ctx, published)
}

func (r *Registrar) archiveDefinition(ctx httpx.IContext) error {
	serviceCtx, err := r.serviceContext(ctx)
	if err != nil {
		return err
	}
	id, err := requiredParamValue(ctx, "id", "workflow definition id is empty")
	if err != nil {
		return err
	}
	archived, err := r.service.ArchiveDefinition(serviceCtx, id)
	if err != nil {
		return err
	}
	return httpx.WriteSuccess(ctx, archived)
}

func (r *Registrar) createInstance(ctx httpx.IContext) error {
	serviceCtx, err := r.serviceContext(ctx)
	if err != nil {
		return err
	}
	var input workflowtypes.CreateInstanceInput
	if err := ctx.BindJSON(&input); err != nil {
		return err
	}
	principal, err := auth.RequirePrincipal(serviceCtx)
	if err != nil || principal.SubjectID <= 0 {
		return errors.NewCode(errors.Unauthorized, "workflow principal is required")
	}
	input.InitiatorID = strconv.FormatInt(principal.SubjectID, 10)
	input.InitiatorName = ""
	state, err := r.service.CreateInstanceWithOptions(serviceCtx, input)
	if err != nil {
		return err
	}
	return httpx.WriteCreated(ctx, workflowtypes.InstanceStateFromRuntime(state))
}

func (r *Registrar) getInstance(ctx httpx.IContext) error {
	serviceCtx, err := r.serviceContext(ctx)
	if err != nil {
		return err
	}
	id, err := requiredParamValue(ctx, "id", "workflow instance id is empty")
	if err != nil {
		return err
	}
	state, err := r.service.GetInstance(serviceCtx, runtime.ID(id))
	if err != nil {
		return err
	}
	return httpx.WriteSuccess(ctx, workflowtypes.InstanceStateFromRuntime(state))
}

func (r *Registrar) listInstances(ctx httpx.IContext) error {
	serviceCtx, err := r.serviceContext(ctx)
	if err != nil {
		return err
	}
	query, err := instanceQuery(ctx)
	if err != nil {
		return err
	}
	result, err := r.service.ListInstances(serviceCtx, query)
	if err != nil {
		return err
	}
	return httpx.WriteSuccess(ctx, result)
}

func (r *Registrar) startInstance(ctx httpx.IContext) error {
	serviceCtx, err := r.serviceContext(ctx)
	if err != nil {
		return err
	}
	id, err := requiredParamValue(ctx, "id", "workflow instance id is empty")
	if err != nil {
		return err
	}
	state, err := r.service.StartInstance(serviceCtx, runtime.ID(id))
	if err != nil {
		return err
	}
	return httpx.WriteSuccess(ctx, workflowtypes.InstanceStateFromRuntime(state))
}

func (r *Registrar) advanceNode(ctx httpx.IContext) error {
	return r.completeNodeWithAction(ctx, "advance")
}

func (r *Registrar) completeNode(ctx httpx.IContext) error {
	return r.completeNodeWithAction(ctx, "complete")
}

func (r *Registrar) completeNodeWithAction(ctx httpx.IContext, action string) error {
	serviceCtx, err := r.serviceContext(ctx)
	if err != nil {
		return err
	}
	id, err := requiredParamValue(ctx, "id", "workflow instance id is empty")
	if err != nil {
		return err
	}
	nodeID, err := requiredParamValue(ctx, "node_id", "workflow node id is empty")
	if err != nil {
		return err
	}
	var payload struct {
		NextNodeID       *string    `json:"next_node_id"`
		NextNodeIDs      *[]string  `json:"next_node_ids"`
		AssigneeID       string     `json:"assignee_id,omitempty"`
		AssigneeName     string     `json:"assignee_name,omitempty"`
		CandidateUserIDs []string   `json:"candidate_user_ids,omitempty"`
		CandidateRoleIDs []string   `json:"candidate_role_ids,omitempty"`
		CCUserIDs        []string   `json:"cc_user_ids,omitempty"`
		DueAt            *time.Time `json:"due_at,omitempty"`
		Comment          string     `json:"comment,omitempty"`
	}
	hasBody, err := bindOptionalJSON(ctx, &payload)
	if err != nil {
		return err
	}
	nextNodeID, selected, err := selectedNextNodeID(payload.NextNodeID, payload.NextNodeIDs)
	if err != nil {
		return err
	}
	input := workflowtypes.AdvanceNodeInput{
		AssigneeID:       payload.AssigneeID,
		AssigneeName:     payload.AssigneeName,
		CandidateUserIDs: payload.CandidateUserIDs,
		CandidateRoleIDs: payload.CandidateRoleIDs,
		CCUserIDs:        payload.CCUserIDs,
		DueAt:            payload.DueAt,
		Comment:          payload.Comment,
	}
	if selected {
		input.NextNodeID = nextNodeID
	}
	if !hasBody || !selected {
		var state *runtime.State
		if action == "advance" {
			state, err = r.service.AdvanceNodeWithInput(serviceCtx, runtime.ID(id), nodeID, input)
		} else {
			state, err = r.service.CompleteNode(serviceCtx, runtime.ID(id), nodeID, input)
		}
		if err != nil {
			return err
		}
		return httpx.WriteSuccess(ctx, workflowtypes.InstanceStateFromRuntime(state))
	}
	var state *runtime.State
	if action == "advance" {
		state, err = r.service.AdvanceNodeToWithInput(serviceCtx, runtime.ID(id), nodeID, nextNodeID, input)
	} else {
		state, err = r.service.CompleteNodeTo(serviceCtx, runtime.ID(id), nodeID, nextNodeID, input)
	}
	if err != nil {
		return err
	}
	return httpx.WriteSuccess(ctx, workflowtypes.InstanceStateFromRuntime(state))
}

func (r *Registrar) rejectNode(ctx httpx.IContext) error {
	serviceCtx, err := r.serviceContext(ctx)
	if err != nil {
		return err
	}
	id, err := requiredParamValue(ctx, "id", "workflow instance id is empty")
	if err != nil {
		return err
	}
	nodeID, err := requiredParamValue(ctx, "node_id", "workflow node id is empty")
	if err != nil {
		return err
	}
	var input workflowtypes.RejectNodeInput
	if _, err := bindOptionalJSON(ctx, &input); err != nil {
		return err
	}
	state, err := r.service.RejectNode(serviceCtx, runtime.ID(id), nodeID, input)
	if err != nil {
		return err
	}
	return httpx.WriteSuccess(ctx, workflowtypes.InstanceStateFromRuntime(state))
}

func (r *Registrar) claimTask(ctx httpx.IContext) error {
	return r.taskAction(ctx, "claim")
}

func (r *Registrar) unclaimTask(ctx httpx.IContext) error {
	return r.taskAction(ctx, "unclaim")
}

func (r *Registrar) transferTask(ctx httpx.IContext) error {
	return r.taskAction(ctx, "transfer")
}

func (r *Registrar) delegateTask(ctx httpx.IContext) error {
	return r.taskAction(ctx, "delegate")
}

func (r *Registrar) remindTask(ctx httpx.IContext) error {
	return r.taskAction(ctx, "remind")
}

func (r *Registrar) taskAction(ctx httpx.IContext, action string) error {
	serviceCtx, err := r.serviceContext(ctx)
	if err != nil {
		return err
	}
	id, err := requiredParamValue(ctx, "id", "workflow instance id is empty")
	if err != nil {
		return err
	}
	nodeID, err := requiredParamValue(ctx, "node_id", "workflow node id is empty")
	if err != nil {
		return err
	}
	var input workflowtypes.TaskActionInput
	if _, err := bindOptionalJSON(ctx, &input); err != nil {
		return err
	}
	var state *runtime.State
	switch action {
	case "claim":
		state, err = r.service.ClaimTask(serviceCtx, runtime.ID(id), nodeID, input)
	case "unclaim":
		state, err = r.service.UnclaimTask(serviceCtx, runtime.ID(id), nodeID, input)
	case "transfer":
		state, err = r.service.TransferTask(serviceCtx, runtime.ID(id), nodeID, input)
	case "delegate":
		state, err = r.service.DelegateTask(serviceCtx, runtime.ID(id), nodeID, input)
	case "remind":
		state, err = r.service.RemindTask(serviceCtx, runtime.ID(id), nodeID, input)
	default:
		return errors.NewCode(errors.InvalidInput, "workflow task action is unsupported").WithContext("action", action)
	}
	if err != nil {
		return err
	}
	return httpx.WriteSuccess(ctx, workflowtypes.InstanceStateFromRuntime(state))
}

func (r *Registrar) addCCUsers(ctx httpx.IContext) error {
	serviceCtx, err := r.serviceContext(ctx)
	if err != nil {
		return err
	}
	id, err := requiredParamValue(ctx, "id", "workflow instance id is empty")
	if err != nil {
		return err
	}
	var input workflowtypes.TaskActionInput
	if _, err := bindOptionalJSON(ctx, &input); err != nil {
		return err
	}
	state, err := r.service.AddCCUsers(serviceCtx, runtime.ID(id), input)
	if err != nil {
		return err
	}
	return httpx.WriteSuccess(ctx, workflowtypes.InstanceStateFromRuntime(state))
}

func (r *Registrar) withdrawInstance(ctx httpx.IContext) error {
	serviceCtx, err := r.serviceContext(ctx)
	if err != nil {
		return err
	}
	id, err := requiredParamValue(ctx, "id", "workflow instance id is empty")
	if err != nil {
		return err
	}
	var input workflowtypes.TaskActionInput
	if _, err := bindOptionalJSON(ctx, &input); err != nil {
		return err
	}
	state, err := r.service.WithdrawInstance(serviceCtx, runtime.ID(id), input)
	if err != nil {
		return err
	}
	return httpx.WriteSuccess(ctx, workflowtypes.InstanceStateFromRuntime(state))
}

func (r *Registrar) listTodos(ctx httpx.IContext) error {
	serviceCtx, err := r.serviceContext(ctx)
	if err != nil {
		return err
	}
	query, err := workItemQuery(ctx)
	if err != nil {
		return err
	}
	result, err := r.service.ListTodoItemsByQuery(serviceCtx, query)
	if err != nil {
		return err
	}
	return httpx.WriteSuccess(ctx, result)
}

func (r *Registrar) listMyTodos(ctx httpx.IContext) error {
	serviceCtx, err := r.serviceContext(ctx)
	if err != nil {
		return err
	}
	query, err := workItemQuery(ctx)
	if err != nil {
		return err
	}
	result, err := r.service.ListMyTodoItems(serviceCtx, query)
	if err != nil {
		return err
	}
	return httpx.WriteSuccess(ctx, result)
}

func (r *Registrar) listDone(ctx httpx.IContext) error {
	serviceCtx, err := r.serviceContext(ctx)
	if err != nil {
		return err
	}
	query, err := workItemQuery(ctx)
	if err != nil {
		return err
	}
	result, err := r.service.ListDoneItemsByQuery(serviceCtx, query)
	if err != nil {
		return err
	}
	return httpx.WriteSuccess(ctx, result)
}

func (r *Registrar) listMyDone(ctx httpx.IContext) error {
	serviceCtx, err := r.serviceContext(ctx)
	if err != nil {
		return err
	}
	query, err := workItemQuery(ctx)
	if err != nil {
		return err
	}
	result, err := r.service.ListMyDoneItems(serviceCtx, query)
	if err != nil {
		return err
	}
	return httpx.WriteSuccess(ctx, result)
}

func (r *Registrar) listStarted(ctx httpx.IContext) error {
	serviceCtx, err := r.serviceContext(ctx)
	if err != nil {
		return err
	}
	query, err := instanceQuery(ctx)
	if err != nil {
		return err
	}
	result, err := r.service.ListStartedInstances(serviceCtx, query)
	if err != nil {
		return err
	}
	return httpx.WriteSuccess(ctx, result)
}

func (r *Registrar) listMyStarted(ctx httpx.IContext) error {
	serviceCtx, err := r.serviceContext(ctx)
	if err != nil {
		return err
	}
	query, err := instanceQuery(ctx)
	if err != nil {
		return err
	}
	query.InitiatorID = ""
	result, err := r.service.ListMyStartedInstances(serviceCtx, query)
	if err != nil {
		return err
	}
	return httpx.WriteSuccess(ctx, result)
}

func selectedNextNodeID(nextNodeID *string, nextNodeIDs *[]string) (string, bool, error) {
	if nextNodeID != nil && nextNodeIDs != nil {
		return "", false, errors.NewCode(errors.InvalidInput, "workflow selected next node is ambiguous")
	}
	if nextNodeID != nil {
		selected := strings.TrimSpace(*nextNodeID)
		if selected == "" {
			return "", false, errors.NewCode(errors.InvalidInput, "workflow selected next node is empty")
		}
		return selected, true, nil
	}
	if nextNodeIDs == nil {
		return "", false, nil
	}
	selectedNextNodeIDs := *nextNodeIDs
	if len(selectedNextNodeIDs) == 0 {
		return "", false, errors.NewCode(errors.InvalidInput, "workflow selected next nodes cannot be empty")
	}
	if len(selectedNextNodeIDs) > 1 {
		return "", false, errors.NewCode(errors.InvalidInput, "workflow selected next nodes must contain at most one node").
			WithContext("selected_count", len(selectedNextNodeIDs))
	}
	selected := strings.TrimSpace(selectedNextNodeIDs[0])
	if selected == "" {
		return "", false, errors.NewCode(errors.InvalidInput, "workflow selected next node is empty")
	}
	return selected, true, nil
}

func bindOptionalJSON(ctx httpx.IContext, target any) (bool, error) {
	body, err := ctx.Body()
	if err != nil {
		return false, err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return false, nil
	}
	return true, ctx.BindJSON(target)
}

func workItemPageQuery(ctx httpx.IContext) (int, int, error) {
	if _, ok := ctx.QueryParams()["page_size"]; ok {
		return 0, 0, errors.NewCode(errors.InvalidInput, "workflow work item page_size is not supported; use size")
	}
	page, err := optionalPositiveIntQuery(ctx, "page")
	if err != nil {
		return 0, 0, err
	}
	size, err := optionalPositiveIntQuery(ctx, "size")
	if err != nil {
		return 0, 0, err
	}
	return page, size, nil
}

func workItemQuery(ctx httpx.IContext) (workflowtypes.WorkItemQuery, error) {
	page, size, err := workItemPageQuery(ctx)
	if err != nil {
		return workflowtypes.WorkItemQuery{}, err
	}
	return workflowtypes.WorkItemQuery{
		Page:         page,
		Size:         size,
		DefinitionID: strings.TrimSpace(ctx.Query("definition_id")),
		Keyword:      strings.TrimSpace(ctx.Query("keyword")),
	}, nil
}

func instanceQuery(ctx httpx.IContext) (workflowtypes.InstanceQuery, error) {
	page, size, err := workItemPageQuery(ctx)
	if err != nil {
		return workflowtypes.InstanceQuery{}, err
	}
	return workflowtypes.InstanceQuery{
		Page:         page,
		Size:         size,
		Status:       strings.TrimSpace(ctx.Query("status")),
		DefinitionID: strings.TrimSpace(ctx.Query("definition_id")),
		ActiveNodeID: strings.TrimSpace(ctx.Query("active_node_id")),
		InitiatorID:  strings.TrimSpace(ctx.Query("initiator_id")),
		AssigneeID:   strings.TrimSpace(ctx.Query("assignee_id")),
		Keyword:      strings.TrimSpace(ctx.Query("keyword")),
	}, nil
}

func optionalPositiveIntQuery(ctx httpx.IContext, key string) (int, error) {
	raw := strings.TrimSpace(ctx.Query(key))
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, errors.NewCode(errors.InvalidInput, "workflow work item query is invalid").WithContext(key, raw)
	}
	return value, nil
}

func (r *Registrar) serviceContext(ctx httpx.IContext) (context.Context, error) {
	reqCtx := ctx.RequestContext()
	scopeID := iamauth.ActiveScopeIDFromContext(reqCtx)
	scopeKind := iamauth.ActiveScopeKindFromContext(reqCtx)
	if scopeID <= 0 || strings.TrimSpace(scopeKind) == "" {
		return nil, errors.NewCode(errors.Unauthorized, "workflow active scope is required")
	}
	scopeKind = strings.ToLower(strings.TrimSpace(scopeKind))
	if scopeKind != workflowservice.ScopeKindPlatform && scopeKind != workflowservice.ScopeKindTenant {
		return nil, errors.NewCode(errors.Forbidden, "workflow active scope is unsupported").
			WithContext("scope_kind", scopeKind)
	}
	return workflowservice.WithScope(reqCtx, workflowservice.Scope{Kind: scopeKind, ID: scopeID}), nil
}

func requiredParam(ctx httpx.IContext, key string) string {
	return strings.TrimSpace(ctx.Param(key))
}

func requiredParamValue(ctx httpx.IContext, key string, message string) (string, error) {
	value := requiredParam(ctx, key)
	if value == "" {
		return "", errors.NewCode(errors.InvalidInput, message).WithContext("param", key)
	}
	return value, nil
}

func paginateInstanceSummaries(items []workflowtypes.InstanceSummary, page int, size int) *workflowtypes.InstancePage {
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
