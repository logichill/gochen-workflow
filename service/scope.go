package workflowservice

import (
	"context"
	"strconv"
	"strings"

	"gochen/contextx"
	"gochen/errors"
)

const (
	ScopeKindPlatform = "platform"
	ScopeKindTenant   = "tenant"
)

type scopeContextKey struct{}
type strictScopeContextKey struct{}

// Scope identifies the data boundary used by workflow definitions and instances.
type Scope struct {
	Kind string
	ID   int64
}

func WithScope(ctx context.Context, scope Scope) context.Context {
	if ctx == nil {
		return nil
	}
	return context.WithValue(ctx, scopeContextKey{}, normalizeScope(scope))
}

func WithStrictScope(ctx context.Context) context.Context {
	if ctx == nil {
		return nil
	}
	return context.WithValue(ctx, strictScopeContextKey{}, true)
}

func hasExplicitScope(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	if _, ok := ctx.Value(scopeContextKey{}).(Scope); ok {
		return true
	}
	// 只要上下文携带租户指示（即便值不可解析为正整数）就算显式租户意图：
	// 与 ScopeFromContext 的租户分支同口径，统一由 validateScope 拒绝 ID 无效的租户作用域，
	// 避免同一输入在两个入口产生 "scope is required" 与 "tenant scope id is required" 两种语义。
	_, hasTenant := parseContextTenantID(ctx)
	return hasTenant
}

func strictScopeRequired(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	required, _ := ctx.Value(strictScopeContextKey{}).(bool)
	return required
}

func ScopeFromContext(ctx context.Context) Scope {
	if ctx == nil {
		return Scope{}
	}
	if strictScopeRequired(ctx) && !hasExplicitScope(ctx) {
		return Scope{}
	}
	if scope, ok := ctx.Value(scopeContextKey{}).(Scope); ok {
		return normalizeScope(scope)
	}
	if tenantID, hasTenant := parseContextTenantID(ctx); hasTenant {
		return Scope{Kind: ScopeKindTenant, ID: tenantID}
	}
	return Scope{Kind: ScopeKindPlatform}
}

func parseContextTenantID(ctx context.Context) (int64, bool) {
	raw := strings.TrimSpace(contextx.TenantID(ctx))
	if raw == "" {
		return 0, false
	}
	if tenantID, err := strconv.ParseInt(raw, 10, 64); err == nil && tenantID > 0 {
		return tenantID, true
	}
	return 0, true
}

func validateScope(scope Scope) error {
	scope = normalizeScope(scope)
	switch scope.Kind {
	case ScopeKindPlatform:
		return nil
	case ScopeKindTenant:
		if scope.ID <= 0 {
			return errors.NewCode(errors.InvalidInput, "workflow tenant scope id is required")
		}
		return nil
	case "":
		return errors.NewCode(errors.InvalidInput, "workflow scope is required")
	default:
		return errors.NewCode(errors.InvalidInput, "workflow scope kind is unsupported").
			WithContext("scope_kind", scope.Kind)
	}
}

func normalizeScope(scope Scope) Scope {
	kind := strings.ToLower(strings.TrimSpace(scope.Kind))
	if scope.ID < 0 {
		scope.ID = 0
	}
	return Scope{Kind: kind, ID: scope.ID}
}
