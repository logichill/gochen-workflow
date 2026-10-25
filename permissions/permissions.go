package permissions

import (
	"context"
	"slices"

	auth "gochen-runtime/host/authz"
	"gochen/auth/action"
	"gochen/errors"
	"gochen/httpx"
)

const (
	WorkflowReadCode         = "workflow:api:read"
	WorkflowDesignCode       = "workflow:api:design"
	WorkflowRunCode          = "workflow:api:run"
	WorkflowMenuListCode     = "workflow.list:menu:view"
	WorkflowMenuDesignerCode = "workflow.designer:menu:view"
)

var (
	WorkflowRead = auth.PermissionCode(WorkflowReadCode).
			Label("流程定义读取").
			Desc("可查看流程定义与运行实例。").
			Scope(auth.PermissionScopePlatform, auth.PermissionScopeTenant)
	WorkflowDesign = auth.PermissionCode(WorkflowDesignCode).
			Label("流程定义设计").
			Desc("可创建、更新、发布、归档与删除流程定义。").
			Scope(auth.PermissionScopePlatform, auth.PermissionScopeTenant).
			Risk(auth.PermissionRiskHigh)
	WorkflowRun = auth.PermissionCode(WorkflowRunCode).
			Label("流程实例推进").
			Desc("可创建、启动、推进、认领、取消认领、转办、委托、催办、抄送与撤回流程实例。").
			Scope(auth.PermissionScopePlatform, auth.PermissionScopeTenant).
			Risk(auth.PermissionRiskHigh)
	WorkflowMenuList = auth.PermissionCode(WorkflowMenuListCode).
				Label("流程管理菜单").
				Desc("可在菜单中查看流程管理入口。").
				Scope(auth.PermissionScopePlatform, auth.PermissionScopeTenant)
	WorkflowMenuDesigner = auth.PermissionCode(WorkflowMenuDesignerCode).
				Label("流程设计器菜单").
				Desc("可在菜单中查看流程设计器入口。").
				Scope(auth.PermissionScopePlatform, auth.PermissionScopeTenant)
)

func Definitions() []auth.PermissionDefinition {
	return auth.PermissionDefinitions(
		WorkflowRead,
		WorkflowDesign,
		WorkflowRun,
		WorkflowMenuList,
		WorkflowMenuDesigner,
	)
}

// PermissionMiddleware 校验当前请求主体是否持有指定 workflow 权限及匹配的授权域。
func PermissionMiddleware(required auth.PermissionSpec) httpx.Middleware {
	requiredPermission := required.Definition()
	if !action.IsValidCode(requiredPermission.Code) {
		return func(httpx.IContext, func() error) error {
			return errors.NewCode(errors.Internal, "invalid permission definition")
		}
	}
	return func(ctx httpx.IContext, next func() error) error {
		if ctx == nil || ctx.RequestContext() == nil {
			return errors.NewCode(errors.Unauthorized, "用户未认证")
		}
		if err := requirePermission(ctx.RequestContext(), requiredPermission); err != nil {
			return err
		}
		return next()
	}
}

func requirePermission(ctx context.Context, required auth.PermissionDefinition) error {
	principal, err := auth.RequirePrincipal(ctx)
	if err != nil {
		return errors.NewCode(errors.Unauthorized, "用户未认证")
	}
	if len(required.Scopes) > 0 && !principal.IsSystem && !matchesPermissionScopes(ctx, principal, required.Scopes) {
		return errors.NewCode(errors.Forbidden, "当前授权域不允许访问此权限")
	}
	if err := auth.NewActionChecker().RequireAction(ctx, required.Code); err != nil {
		return err
	}
	return nil
}

func matchesPermissionScopes(ctx context.Context, principal auth.Principal, requiredScopes []string) bool {
	if len(principal.PermissionScopes) > 0 {
		for _, scope := range requiredScopes {
			if slices.Contains(principal.PermissionScopes, scope) {
				return true
			}
		}
		return false
	}
	if activeKind := auth.ActiveScopeKindFromContext(ctx); activeKind != "" {
		return slices.Contains(requiredScopes, activeKind)
	}
	// Principal 与上下文都没有授权域信息时无法证明匹配，按 fail-closed 拒绝；
	// 不能默认放行——该包是公共 API，无法假设宿主装配一定注入了授权域。
	return false
}

