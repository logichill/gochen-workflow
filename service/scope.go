package workflowservice

import (
	"context"
	"strings"

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
	_, ok := ctx.Value(scopeContextKey{}).(Scope)
	return ok
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
		return Scope{Kind: ScopeKindPlatform}
	}
	if scope, ok := ctx.Value(scopeContextKey{}).(Scope); ok {
		return normalizeScope(scope)
	}
	return Scope{Kind: ScopeKindPlatform}
}

func validateScope(scope Scope) error {
	scope = normalizeScope(scope)
	switch scope.Kind {
	case ScopeKindPlatform, ScopeKindTenant:
		if scope.Kind == ScopeKindTenant && scope.ID <= 0 {
			return errors.NewCode(errors.InvalidInput, "workflow tenant scope id is required")
		}
		return nil
	default:
		return errors.NewCode(errors.InvalidInput, "workflow scope kind is unsupported").
			WithContext("scope_kind", scope.Kind)
	}
}

func normalizeScope(scope Scope) Scope {
	kind := strings.ToLower(strings.TrimSpace(scope.Kind))
	switch kind {
	case "", ScopeKindPlatform:
		kind = ScopeKindPlatform
	case ScopeKindTenant:
		kind = ScopeKindTenant
	}
	if scope.ID < 0 {
		scope.ID = 0
	}
	return Scope{Kind: kind, ID: scope.ID}
}
