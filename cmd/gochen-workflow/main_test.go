package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	workflowpermissions "gochen-workflow/permissions"

	iamauth "gochen-iam/auth"
	iamconfig "gochen-iam/config"
	iammw "gochen-iam/middleware"
	auth "gochen-runtime/host/authz"
	"gochen-runtime/http/nethttp"
	"gochen/httpx"
	"gochen/observe/logging"
)

func TestListenAddressParsesSupportedForms(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		wantHost string
		wantPort int
	}{
		{name: "default", raw: "", wantHost: "0.0.0.0", wantPort: 8088},
		{name: "port only", raw: "9090", wantHost: "0.0.0.0", wantPort: 9090},
		{name: "colon port", raw: ":9091", wantHost: "0.0.0.0", wantPort: 9091},
		{name: "host port", raw: "127.0.0.1:9092", wantHost: "127.0.0.1", wantPort: 9092},
		{name: "ipv6 host port", raw: "[::1]:9093", wantHost: "::1", wantPort: 9093},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotHost, gotPort, err := listenAddress(tt.raw)
			if err != nil {
				t.Fatalf("listenAddress(%q) error = %v", tt.raw, err)
			}
			if gotHost != tt.wantHost || gotPort != tt.wantPort {
				t.Fatalf("listenAddress(%q) = (%q, %d), want (%q, %d)", tt.raw, gotHost, gotPort, tt.wantHost, tt.wantPort)
			}
		})
	}
}

func TestListenAddressRejectsInvalidPorts(t *testing.T) {
	for _, raw := range []string{"0", ":0", "127.0.0.1:0", "65536", ":70000", "127.0.0.1:99999"} {
		t.Run(raw, func(t *testing.T) {
			if host, port, err := listenAddress(raw); err == nil {
				t.Fatalf("listenAddress(%q) = (%q, %d, nil), want error", raw, host, port)
			}
		})
	}
}

func TestSQLiteStoreOptionsFromEnvRequiresPositiveLegacyScopeID(t *testing.T) {
	t.Setenv("GOCHEN_WORKFLOW_LEGACY_SCOPE_ID", "invalid")
	if _, err := sqliteStoreOptionsFromEnv(); err == nil {
		t.Fatal("expected invalid legacy scope id to fail")
	}

	t.Setenv("GOCHEN_WORKFLOW_LEGACY_SCOPE_ID", "42")
	t.Setenv("GOCHEN_WORKFLOW_SCOPE_KIND", "tenant")
	options, err := sqliteStoreOptionsFromEnv()
	if err != nil {
		t.Fatalf("sqliteStoreOptionsFromEnv returned error: %v", err)
	}
	if len(options) != 1 {
		t.Fatalf("store options = %d, want 1", len(options))
	}
}

func TestSQLiteStoreOptionsFromEnvRejectsInvalidScopeKind(t *testing.T) {
	t.Setenv("GOCHEN_WORKFLOW_SCOPE_KIND", "tennat")
	if _, err := sqliteStoreOptionsFromEnv(); err == nil {
		t.Fatal("expected invalid workflow scope kind to fail")
	}
}

func TestStandaloneAuthContextResolverAllowsScopedToken(t *testing.T) {
	t.Setenv("APP_ENV", "test")
	iammw.RegisterRequiredPermissions(workflowpermissions.WorkflowReadCode)
	token, err := iammw.GenerateToken(7, 42, "binding-v1", []string{workflowpermissions.WorkflowReadCode}, "test-secret")
	if err != nil {
		t.Fatalf("GenerateToken returned error: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "http://example.com/api/v1/workflow/definitions", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	ctx, err := nethttp.NewBaseContext(httptest.NewRecorder(), req)
	if err != nil {
		t.Fatalf("NewBaseContext returned error: %v", err)
	}

	mw := iammw.AuthMiddleware(&iammw.AuthConfig{
		Environment:     "test",
		ContextResolver: standaloneAuthContextResolver{},
		SecretKey:       "test-secret",
		TokenHeader:     "Authorization",
		TokenPrefix:     "Bearer ",
	})
	called := false
	if err := mw(ctx, func() error { called = true; return nil }); err != nil {
		t.Fatalf("AuthMiddleware returned error: %v", err)
	}
	if !called {
		t.Fatal("expected next handler to be called")
	}
	principal, ok := auth.PrincipalFromContext(ctx.RequestContext())
	if !ok {
		t.Fatal("expected principal in request context")
	}
	if principal.SubjectID != 7 || principal.ActiveScopeID != 42 {
		t.Fatalf("unexpected principal: %+v", principal)
	}
	if got := iamauth.ActiveScopeKind(ctx.RequestContext()); got != string(iammw.ScopeTenant) {
		t.Fatalf("active scope kind = %q, want %q", got, string(iammw.ScopeTenant))
	}
}

func TestWorkflowStandaloneAuthConfigRequiresSecret(t *testing.T) {
	t.Setenv("AUTH_SECRET", "")
	config := iamconfig.NewAuthConfig(iamconfig.ApplyEnvOverrides(iamconfig.DefaultRuntimeConfig()), "test")
	if err := iammw.ValidateAuthConfig(config); err == nil {
		t.Fatal("expected missing AUTH_SECRET to fail")
	}
}

func TestStandaloneRouteMiddlewaresAuthenticateBeforePermissions(t *testing.T) {
	t.Setenv("APP_ENV", "test")
	t.Setenv("AUTH_SECRET", "workflow-test-secret")
	config := iamconfig.NewAuthConfig(iamconfig.ApplyEnvOverrides(iamconfig.DefaultRuntimeConfig()), "test")
	config.ContextResolver = standaloneAuthContextResolver{}
	middlewares := standaloneRouteMiddlewares(logging.NewNoopLogger(), config)
	if len(middlewares) == 0 {
		t.Fatal("expected standalone route middlewares")
	}

	chain := append([]httpx.Middleware{}, middlewares...)
	chain = append(chain, iammw.PermissionMiddleware(workflowpermissions.WorkflowRead))
	handler := func(ctx httpx.IContext) error {
		return runMiddlewareChain(ctx, chain, func() error {
			return ctx.JSON(http.StatusOK, httpx.JSONValue(map[string]any{"ok": true}))
		})
	}

	token, err := iammw.GenerateToken(7, 42, "binding-v1", []string{workflowpermissions.WorkflowReadCode}, "workflow-test-secret")
	if err != nil {
		t.Fatalf("GenerateToken returned error: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "http://example.com/api/v1/workflow/definitions", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	ctx, err := nethttp.NewBaseContext(rec, req)
	if err != nil {
		t.Fatalf("NewBaseContext returned error: %v", err)
	}

	if err := handler(ctx); err != nil {
		_ = nethttp.WriteErrorResponse(ctx, err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
}

func runMiddlewareChain(ctx httpx.IContext, middlewares []httpx.Middleware, final func() error) error {
	next := final
	for i := len(middlewares) - 1; i >= 0; i-- {
		mw := middlewares[i]
		current := next
		next = func() error {
			return mw(ctx, current)
		}
	}
	return next()
}
