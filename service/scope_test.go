package workflowservice

import (
	"context"
	"path/filepath"
	"testing"

	"gochen/contextx"
	"gochen/errors"
)

func TestCurrentManagedScopeRejectsNilContext(t *testing.T) {
	if _, err := currentManagedScope(nil); err == nil || !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected nil context to return InvalidInput, got %v", err)
	}
}

func TestStoresRejectNilContextForScopeAwareOperations(t *testing.T) {
	ctx := context.Background()
	stores := []struct {
		name  string
		store Store
	}{
		{name: "memory", store: NewMemoryStore()},
		{
			name: "sqlite",
			store: func() Store {
				store, err := OpenSQLiteStore(ctx, filepath.Join(t.TempDir(), "workflow.db"))
				if err != nil {
					t.Fatalf("open sqlite store: %v", err)
				}
				return store
			}(),
		},
	}

	for _, tt := range stores {
		t.Run(tt.name, func(t *testing.T) {
			if closer, ok := tt.store.(interface{ Close() error }); ok {
				t.Cleanup(func() { _ = closer.Close() })
			}
			if _, err := tt.store.ListDesigns(nil); err == nil || !errors.Is(err, errors.InvalidInput) {
				t.Fatalf("expected nil context list designs to return InvalidInput, got %v", err)
			}
			if _, err := tt.store.ListInstances(nil); err == nil || !errors.Is(err, errors.InvalidInput) {
				t.Fatalf("expected nil context list instances to return InvalidInput, got %v", err)
			}
			if _, err := tt.store.ListStartedInstances(nil); err == nil || !errors.Is(err, errors.InvalidInput) {
				t.Fatalf("expected nil context list started instances to return InvalidInput, got %v", err)
			}
		})
	}
}

func TestOpenSQLiteStoreRejectsNilContext(t *testing.T) {
	if _, err := OpenSQLiteStore(nil, filepath.Join(t.TempDir(), "workflow.db")); err == nil || !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected nil context open to return InvalidInput, got %v", err)
	}
}

func TestStrictScopeRequiresExplicitScope(t *testing.T) {
	service := newTestService(NewMemoryStore())
	if _, err := service.ListDefinitions(WithStrictScope(context.Background())); err == nil || !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected strict scope without explicit scope to return InvalidInput, got %v", err)
	}
	ctx := WithStrictScope(WithScope(workflowTestContext(), Scope{Kind: ScopeKindPlatform}))
	if _, err := service.ListDefinitions(ctx); err != nil {
		t.Fatalf("expected explicit platform scope to satisfy strict scope, got %v", err)
	}
}

func TestTenantScopeFromContext(t *testing.T) {
	// 1. explicit tenant scope
	ctx := WithScope(context.Background(), Scope{Kind: ScopeKindTenant, ID: 88})
	scope := ScopeFromContext(ctx)
	if scope.Kind != ScopeKindTenant || scope.ID != 88 {
		t.Fatalf("WithScope expected tenant:88, got %#v", scope)
	}

	// 2. contextx.WithTenantID
	ctxTenant, err := contextx.WithTenantID(context.Background(), "99")
	if err != nil {
		t.Fatalf("WithTenantID: %v", err)
	}
	scope = ScopeFromContext(ctxTenant)
	if scope.Kind != ScopeKindTenant || scope.ID != 99 {
		t.Fatalf("contextx.WithTenantID expected tenant:99, got %#v", scope)
	}

	// 3. Strict scope satisfied by contextx.WithTenantID
	service := newTestService(NewMemoryStore())
	if _, err := service.ListDefinitions(scopedContext(t, 99)); err != nil {
		t.Fatalf("expected strict scope with contextx.TenantID to succeed, got %v", err)
	}
}

// 回归 P1-1：contextx.TenantID 非空但不是合法正整数时（如 "erp-demo"），
// ScopeFromContext 必须返回 tenant:0，下游校验 validateScope 触发 fail-closed 拦截，
// 严禁静默升级回退到 platform 作用域。
func TestTenantScopeFromContext_InvalidTenantFailsClosed(t *testing.T) {
	invalidCases := []string{"erp-demo", "-1", "0", "invalid-uuid"}
	for _, tc := range invalidCases {
		ctxTenant, err := contextx.WithTenantID(context.Background(), tc)
		if err != nil {
			t.Fatalf("WithTenantID(%q): %v", tc, err)
		}
		scope := ScopeFromContext(ctxTenant)
		if scope.Kind != ScopeKindTenant || scope.ID != 0 {
			t.Fatalf("tc=%q: expected Scope{Kind: tenant, ID: 0}, got %#v", tc, scope)
		}
		if err := validateScope(scope); err == nil || !errors.Is(err, errors.InvalidInput) {
			t.Fatalf("tc=%q: expected validateScope to fail with InvalidInput, got %v", tc, err)
		}
		service := newTestService(NewMemoryStore())
		if _, err := service.ListDefinitions(ctxTenant); err == nil || !errors.Is(err, errors.InvalidInput) {
			t.Fatalf("tc=%q: expected service.ListDefinitions to reject invalid tenant scope, got %v", tc, err)
		}
	}
}

// 验证 F40: 空作用域或未显式指定 Scope 时拒绝静默退化为 Platform:0，执行 Fail-Closed
func TestEmptyScopeFailsClosed(t *testing.T) {
	// 空 scope.Kind 不再自动转为 Platform
	emptyScope := Scope{Kind: ""}
	if err := validateScope(emptyScope); err == nil || !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected validateScope(emptyScope) to fail with InvalidInput, got %v", err)
	}

	// 显式 WithScope 设置空 Kind 也会被 fail-closed 拦截
	ctxEmptyScope := WithScope(context.Background(), Scope{})
	if _, err := currentManagedScope(ctxEmptyScope); err == nil || !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected currentManagedScope with empty Scope to fail, got %v", err)
	}

	// Store 在 strictScope 模式下缺少显式 scope 必须返回 InvalidInput
	store := NewMemoryStore()
	ctxStrictWithoutScope := WithStrictScope(context.Background())
	if _, err := store.ListDesigns(ctxStrictWithoutScope); err == nil || !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected store.ListDesigns under strictScope without explicit scope to fail, got %v", err)
	}
}

func TestUnsupportedScopeKindPreservesErrorContext(t *testing.T) {
	err := validateScope(Scope{Kind: "custom_kind"})
	if err == nil || !errors.Is(err, errors.InvalidInput) {
		t.Fatalf("expected InvalidInput for unsupported scope kind, got %v", err)
	}
	if err.Error() != "[INVALID_INPUT] workflow scope kind is unsupported" {
		t.Fatalf("expected 'workflow scope kind is unsupported', got %q", err.Error())
	}
	appErr, ok := err.(errors.IAppError)
	if !ok || appErr.Details() == nil || appErr.Details()["scope_kind"] != "custom_kind" {
		t.Fatalf("expected scope_kind context 'custom_kind', got %#v", err)
	}
}
