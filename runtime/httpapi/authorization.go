package httpapi

import (
	"context"
	"strings"

	hostauthz "gochen-runtime/host/authz"
	workflowpermissions "gochen-workflow/permissions"
	workflowservice "gochen-workflow/service"
	"gochen/auth"
	"gochen/domain/meta"
	"gochen/domain/spec"
	"gochen/errors"
)

// scopePartition is the actual Store boundary shared by designs, published
// versions, instances and their work items. Every Store operation consumes the
// same strict Scope from context; object IDs and actor rules remain in Service.
type scopePartition struct {
	ScopeKind string
	ScopeID   int64
}

const workflowResource = "workflow.scope_dataset"

type scopeExecution struct {
	authorizer *auth.Authorizer
	descriptor auth.Descriptor
}

type scopePolicy struct{}

func (scopePolicy) Vocabulary() auth.Vocabulary {
	return auth.Vocabulary{Refs: []meta.Ref{meta.ByField("ScopeKind"), meta.ByField("ScopeID")}}
}

func (scopePolicy) Evaluate(ctx context.Context, _ auth.Request) (auth.Decision, error) {
	partition, err := trustedPartition(ctx)
	if err != nil {
		return auth.Decision{}, err
	}
	return auth.Decision{Allowed: true, Spec: spec.And(
		spec.Eq(meta.ByField("ScopeKind"), partition.ScopeKind),
		spec.Eq(meta.ByField("ScopeID"), partition.ScopeID),
	)}, nil
}

func trustedPartition(ctx context.Context) (*scopePartition, error) {
	if _, err := hostauthz.RequirePrincipal(ctx); err != nil {
		return nil, err
	}
	partition := &scopePartition{ScopeKind: strings.ToLower(strings.TrimSpace(hostauthz.ActiveScopeKindFromContext(ctx))), ScopeID: hostauthz.ActiveScopeIDFromContext(ctx)}
	if partition.ScopeID <= 0 || partition.ScopeKind == "" {
		return nil, errors.NewCode(errors.Unauthorized, "workflow active scope is required")
	}
	if partition.ScopeKind != workflowservice.ScopeKindTenant && partition.ScopeKind != workflowservice.ScopeKindPlatform {
		return nil, errors.NewCode(errors.Forbidden, "workflow active scope is unsupported").WithContext("scope_kind", partition.ScopeKind)
	}
	return partition, nil
}

func newScopeExecution() (*scopeExecution, error) {
	descriptor := auth.Descriptor{Resource: workflowResource}
	for _, group := range workflowRoutes {
		for _, route := range group.routes {
			descriptor.Actions = append(descriptor.Actions, auth.Action{Name: route.action, Kind: route.kind, Permission: group.permission.Definition().Code})
		}
	}
	metadata, err := meta.Describe[*scopePartition]()
	if err != nil {
		return nil, err
	}
	checker := auth.ActionCheckerFunc(func(ctx context.Context, code string) error {
		for _, permission := range []hostauthz.PermissionSpec{workflowpermissions.WorkflowRead, workflowpermissions.WorkflowDesign, workflowpermissions.WorkflowRun} {
			if permission.Definition().Code == code {
				return workflowpermissions.RequirePermission(ctx, permission.Definition())
			}
		}
		return errors.NewCode(errors.Forbidden, "unknown workflow permission")
	})
	authorizer, err := auth.Compile(auth.Declaration{
		Descriptor: descriptor, ResourceAccess: auth.ResourceAccessData,
		Rules: []auth.Rule{{ID: "workflow.strict_scope_partition", System: auth.SystemCheckConstraints, Constraints: []auth.IPolicy{scopePolicy{}}}},
	}, auth.CompileOptions{Checker: checker, Metadata: metadata})
	if err != nil {
		return nil, err
	}
	return &scopeExecution{authorizer: authorizer, descriptor: descriptor}, nil
}

func (e *scopeExecution) AuthorizationBinding(action string) (auth.EntryBinding, error) {
	return auth.BindExecution(e.authorizer, e.descriptor, auth.ResourceAccessData,
		"workflow scoped dataset executor", "Authorize(scope kind/id) -> Match trusted partition -> WithStrictScope -> Store scoped keys/SQL; Service object and actor checks", action)
}

func (e *scopeExecution) context(ctx context.Context, action string) (context.Context, error) {
	decision, err := e.authorizer.Authorize(ctx, auth.Request{Resource: workflowResource, Action: action})
	if err != nil {
		return nil, err
	}
	if !decision.Allowed {
		return nil, errors.NewCode(errors.Forbidden, "workflow authorization denied")
	}
	partition, err := trustedPartition(ctx)
	if err != nil {
		return nil, err
	}
	allowed, err := spec.NewInterpreter().Match(decision.Spec, partition)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, errors.NewCode(errors.Forbidden, "workflow scope partition is outside authorization")
	}
	return workflowservice.WithStrictScope(workflowservice.WithScope(ctx, workflowservice.Scope{Kind: partition.ScopeKind, ID: partition.ScopeID})), nil
}
