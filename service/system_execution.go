package workflowservice

import (
	"context"
	"strings"

	hostauth "gochen-runtime/host/authz"
	"gochen-workflow/permissions"
	workflowtypes "gochen-workflow/types"
	coreauth "gochen/auth"
	"gochen/contextx"
	"gochen/domain/meta"
	"gochen/domain/spec"
	"gochen/errors"
	runtime "gochen/process/workflow"
)

// SystemExecution is the narrow tenant-scoped integration facade. Every method
// matches the compiled dataset condition before invoking the actual service.
type SystemExecution struct {
	service       *Service
	authorization *coreauth.Authorizer
}
type systemDataset struct {
	ScopeKind string
	ScopeID   int64
}
type systemDatasetPolicy struct{}

func (systemDatasetPolicy) Vocabulary() coreauth.Vocabulary {
	return coreauth.Vocabulary{Refs: []meta.Ref{meta.ByField("ScopeKind"), meta.ByField("ScopeID")}}
}
func (systemDatasetPolicy) Evaluate(ctx context.Context, _ coreauth.Request) (coreauth.Decision, error) {
	principal, ok := coreauth.PrincipalFromContext(ctx)
	if !ok || !principal.System {
		return coreauth.Decision{}, errors.NewCode(errors.Forbidden, "workflow integration requires system principal")
	}
	if strings.TrimSpace(contextx.TenantID(ctx)) == "" || !strictScopeRequired(ctx) {
		return coreauth.Decision{}, errors.NewCode(errors.Forbidden, "workflow integration requires tenant and strict scope")
	}
	scope := ScopeFromContext(ctx)
	if scope.Kind != ScopeKindTenant || scope.ID <= 0 {
		return coreauth.Decision{}, errors.NewCode(errors.Forbidden, "workflow integration requires trusted tenant scope")
	}
	return coreauth.Decision{Allowed: true, Spec: spec.And(spec.Eq(meta.ByField("ScopeKind"), scope.Kind), spec.Eq(meta.ByField("ScopeID"), scope.ID))}, nil
}
func NewSystemExecution(service *Service) (*SystemExecution, error) {
	if service == nil {
		return nil, errors.NewCode(errors.InvalidInput, "workflow service is required")
	}
	metadata, err := meta.Describe[*systemDataset]()
	if err != nil {
		return nil, err
	}
	actions := []coreauth.Action{}
	for _, name := range []string{"get_definition", "save_definition", "publish_definition", "create_instance", "start_instance", "advance_node", "get_instance"} {
		permission := permissions.WorkflowRunCode
		kind := coreauth.KindUpdate
		switch name {
		case "get_definition", "get_instance":
			permission = permissions.WorkflowReadCode
			kind = coreauth.KindRead
		case "save_definition", "publish_definition":
			permission = permissions.WorkflowDesignCode
		}
		actions = append(actions, coreauth.Action{Name: name, Kind: kind, Permission: permission})
	}
	authorization, err := coreauth.Compile(coreauth.Declaration{Descriptor: coreauth.Descriptor{Resource: "workflow.scope_dataset", Actions: actions}, ResourceAccess: coreauth.ResourceAccessData, Rules: []coreauth.Rule{{ID: "workflow.system_tenant_dataset", System: coreauth.SystemCheckConstraints, Constraints: []coreauth.IPolicy{systemDatasetPolicy{}}}}}, coreauth.CompileOptions{Checker: hostauth.NewActionChecker(), Metadata: metadata})
	if err != nil {
		return nil, err
	}
	return &SystemExecution{service: service, authorization: authorization}, nil
}
func (s *SystemExecution) authorize(ctx context.Context, action string) error {
	if s == nil || s.service == nil || s.authorization == nil {
		return errors.NewCode(errors.FailedPrecondition, "workflow system execution is unavailable")
	}
	decision, err := s.authorization.Authorize(ctx, coreauth.Request{Resource: "workflow.scope_dataset", Action: action})
	if err != nil {
		return err
	}
	if !decision.Allowed {
		return errors.NewCode(errors.Forbidden, "workflow system execution denied")
	}
	scope := ScopeFromContext(ctx)
	matched, err := spec.NewInterpreter().Match(decision.Spec, &systemDataset{ScopeKind: scope.Kind, ScopeID: scope.ID})
	if err != nil {
		return err
	}
	if !matched {
		return errors.NewCode(errors.Forbidden, "workflow dataset scope denied")
	}
	return nil
}
func (s *SystemExecution) AuthorizationBinding(action string) (coreauth.EntryBinding, error) {
	if s == nil || s.authorization == nil {
		return coreauth.EntryBinding{}, errors.NewCode(errors.FailedPrecondition, "workflow system execution is unavailable")
	}
	plan, ok := s.authorization.Plan()
	if !ok {
		return coreauth.EntryBinding{}, errors.NewCode(errors.FailedPrecondition, "workflow compiled plan required")
	}
	if _, ok = plan.Descriptor.Action(action); !ok {
		return coreauth.EntryBinding{}, errors.NewCode(errors.InvalidInput, "unknown workflow system action")
	}
	return coreauth.EntryBinding{Action: action, Plan: &plan, ResourceAccess: coreauth.ResourceAccessData, Source: "workflow.SystemExecution", ConditionPath: "SystemExecution.authorize -> Match ScopeKind/ScopeID -> scoped Service/Store"}, nil
}
func (s *SystemExecution) GetDefinition(ctx context.Context, id string) (*workflowtypes.DesignerDefinition, error) {
	if err := s.authorize(ctx, "get_definition"); err != nil {
		return nil, err
	}
	return s.service.GetDefinition(ctx, id)
}
func (s *SystemExecution) SaveDefinition(ctx context.Context, input *workflowtypes.DesignerDefinition) (*workflowtypes.DesignerDefinition, error) {
	if err := s.authorize(ctx, "save_definition"); err != nil {
		return nil, err
	}
	return s.service.SaveDefinition(ctx, input)
}
func (s *SystemExecution) PublishDefinition(ctx context.Context, id string) (*workflowtypes.DesignerDefinition, error) {
	if err := s.authorize(ctx, "publish_definition"); err != nil {
		return nil, err
	}
	return s.service.PublishDefinition(ctx, id)
}
func (s *SystemExecution) CreateInstance(ctx context.Context, id runtime.ID, definition string, data map[string]any) (*runtime.State, error) {
	if err := s.authorize(ctx, "create_instance"); err != nil {
		return nil, err
	}
	return s.service.CreateInstance(ctx, id, definition, data)
}
func (s *SystemExecution) StartInstance(ctx context.Context, id runtime.ID) (*runtime.State, error) {
	if err := s.authorize(ctx, "start_instance"); err != nil {
		return nil, err
	}
	return s.service.StartInstance(ctx, id)
}
func (s *SystemExecution) AdvanceNodeTo(ctx context.Context, id runtime.ID, node, next string) (*runtime.State, error) {
	if err := s.authorize(ctx, "advance_node"); err != nil {
		return nil, err
	}
	return s.service.AdvanceNodeTo(ctx, id, node, next)
}
func (s *SystemExecution) GetInstance(ctx context.Context, id runtime.ID) (*runtime.State, error) {
	if err := s.authorize(ctx, "get_instance"); err != nil {
		return nil, err
	}
	return s.service.GetInstance(ctx, id)
}
