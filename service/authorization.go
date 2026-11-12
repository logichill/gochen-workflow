package workflowservice

import (
	"context"
	"sync"

	hostauthz "gochen-runtime/host/authz"
	"gochen-workflow/permissions"
	"gochen/auth"
	"gochen/domain/meta"
	"gochen/domain/spec"
	"gochen/errors"
)

// 普通服务共享固定的只读计划；权限资料和 Scope 均从每次调用的可信身份取得。
var serviceAuthorization = sync.OnceValues(func() (*auth.Authorizer, error) {
	metadata, err := meta.Describe[*systemDataset]()
	if err != nil {
		return nil, err
	}
	return auth.Compile(auth.Declaration{
		Descriptor: auth.Descriptor{Resource: "workflow.scope_dataset", Actions: []auth.Action{
			{Name: "read", Kind: auth.KindRead, Permission: permissions.WorkflowReadCode},
			{Name: "design", Kind: auth.KindUpdate, Permission: permissions.WorkflowDesignCode},
			{Name: "run", Kind: auth.KindUpdate, Permission: permissions.WorkflowRunCode},
		}}, ResourceAccess: auth.ResourceAccessData,
		Rules: []auth.Rule{{ID: "workflow.service.scope", System: auth.SystemCheckConstraints, Constraints: []auth.IPolicy{serviceScopePolicy{}}}},
	}, auth.CompileOptions{Metadata: metadata, Checker: auth.ActionCheckerFunc(func(ctx context.Context, code string) error {
		for _, permission := range permissions.Definitions() {
			if permission.Code == code {
				return permissions.RequirePermission(ctx, permission)
			}
		}
		return errors.NewCode(errors.Forbidden, "unknown workflow permission")
	})})
})

type serviceScopePolicy struct{}

func (serviceScopePolicy) Vocabulary() auth.Vocabulary {
	return auth.Vocabulary{Refs: []meta.Ref{meta.ByField("ScopeKind"), meta.ByField("ScopeID")}}
}

func (serviceScopePolicy) Evaluate(ctx context.Context, _ auth.Request) (auth.Decision, error) {
	principal, err := hostauthz.RequirePrincipal(ctx)
	if err != nil {
		return auth.Decision{}, err
	}
	if !hasExplicitScope(ctx) {
		return auth.Decision{}, errors.NewCode(errors.InvalidInput, "workflow scope is required")
	}
	scope := ScopeFromContext(ctx)
	if !principal.IsSystem {
		if scope.ID <= 0 || scope.ID != principal.ActiveScopeID || scope.Kind != hostauthz.ActiveScopeKindFromContext(ctx) {
			return auth.Decision{}, errors.NewCode(errors.Forbidden, "workflow scope differs from trusted active scope")
		}
	}
	return auth.Decision{Allowed: true, Spec: spec.And(
		spec.Eq(meta.ByField("ScopeKind"), scope.Kind), spec.Eq(meta.ByField("ScopeID"), scope.ID),
	)}, nil
}

func (s *Service) validateExecution(ctx context.Context, action string) error {
	if err := s.validateReady(ctx); err != nil {
		return err
	}
	authorizer, err := serviceAuthorization()
	if err != nil {
		return err
	}
	decision, err := authorizer.Authorize(ctx, auth.Request{Resource: "workflow.scope_dataset", Action: action})
	if err != nil {
		return err
	}
	if !decision.Allowed {
		return errors.NewCode(errors.Forbidden, "workflow service authorization denied")
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

// AuthorizationBinding 暴露普通服务实际执行的身份、权限和数据边界计划。
func (s *Service) AuthorizationBinding(action string) (auth.EntryBinding, error) {
	authorizer, err := serviceAuthorization()
	if err != nil {
		return auth.EntryBinding{}, err
	}
	plan, _ := authorizer.Plan()
	return auth.BindExecution(authorizer, plan.Descriptor, auth.ResourceAccessData,
		"workflow.Service", "validateExecution -> trusted Scope match -> scoped Store; object state and actor checks", action)
}
