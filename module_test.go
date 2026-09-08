package workflow

import (
	"context"
	"reflect"
	"testing"

	iammw "gochen-iam/middleware"
	iamservice "gochen-iam/service"
	workflowpermissions "gochen-workflow/permissions"
	"gochen-workflow/runtime/httpapi"
	workflowservice "gochen-workflow/service"

	auth "gochen-runtime/host/authz"
	"gochen-runtime/host/capability"
	"gochen/httpx"
)

func TestNewModuleDeclaresWorkflowPermissions(t *testing.T) {
	mod, err := NewModule()
	if err != nil {
		t.Fatalf("NewModule() error = %v", err)
	}
	provider, ok := mod.(interface {
		ModuleExtensions() []any
	})
	if !ok {
		t.Fatalf("module does not expose extensions: %T", mod)
	}
	var definitions []auth.PermissionDefinition
	for _, ext := range provider.ModuleExtensions() {
		if cat, ok := ext.(auth.Catalog); ok {
			definitions = append(definitions, cat.PermissionDefinitions...)
		}
	}

	got := map[string]bool{}
	for _, def := range definitions {
		got[def.Code] = true
	}
	for _, code := range []string{
		workflowpermissions.WorkflowReadCode,
		workflowpermissions.WorkflowDesignCode,
		workflowpermissions.WorkflowRunCode,
		workflowpermissions.WorkflowMenuListCode,
		workflowpermissions.WorkflowMenuDesignerCode,
	} {
		if !got[code] {
			t.Fatalf("missing permission %s in %#v", code, definitions)
		}
	}
}

func TestNewModuleDoesNotInstallAuthMiddleware(t *testing.T) {
	mod, err := NewModule()
	if err != nil {
		t.Fatalf("NewModule() error = %v", err)
	}
	value := reflect.ValueOf(mod)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		t.Fatalf("unexpected module value: %T", mod)
	}
	desc := value.Elem().FieldByName("desc")
	if !desc.IsValid() {
		t.Fatalf("module descriptor not found on %T", mod)
	}
	middlewares := desc.FieldByName("Middlewares")
	if !middlewares.IsValid() {
		t.Fatalf("module middlewares not found on %T", mod)
	}
	if middlewares.Len() != 0 {
		t.Fatalf("workflow module should leave authentication middleware to host, got %d module middlewares", middlewares.Len())
	}
}

func TestNewModuleRegistersWorkflowPermissionMetadata(t *testing.T) {
	definitions := workflowpermissions.Definitions()
	run := findPermissionDefinition(definitions, workflowpermissions.WorkflowRunCode)
	if run == nil {
		t.Fatalf("missing workflow run permission in definitions: %#v", definitions)
	}
	if run.Name != "流程实例推进" || run.RiskLevel != string(auth.PermissionRiskHigh) {
		t.Fatalf("unexpected workflow run metadata: %#v", *run)
	}
	if run.Description != "可创建、启动、推进、认领、取消认领、转办、委托、催办、抄送与撤回流程实例。" {
		t.Fatalf("unexpected workflow run description: %#v", run.Description)
	}
	if !reflect.DeepEqual(run.Scopes, []string{string(auth.PermissionScopePlatform), string(auth.PermissionScopeTenant)}) {
		t.Fatalf("unexpected workflow run scopes: %#v", run.Scopes)
	}

	menu := findPermissionDefinition(definitions, workflowpermissions.WorkflowMenuDesignerCode)
	if menu == nil {
		t.Fatalf("missing workflow menu permission in definitions: %#v", definitions)
	}
	if menu.Name != "流程设计器菜单" {
		t.Fatalf("unexpected workflow menu metadata: %#v", *menu)
	}
}

func TestWorkflowRoutesRegisterPermissionsInIAMStrictRegistry(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	registrar := httpapi.NewRegistrar(service)
	if err := registrar.RegisterRoutes(&stubRouteGroup{}); err != nil {
		t.Fatalf("RegisterRoutes failed: %v", err)
	}

	definitions := iammw.RequiredPermissionDefinitions()
	run := findPermissionDefinition(definitions, workflowpermissions.WorkflowRunCode)
	if run == nil {
		t.Fatalf("missing workflow run permission in strict registry after route registration: %#v", definitions)
	}
	if run.Name != "流程实例推进" || run.RiskLevel != string(auth.PermissionRiskHigh) {
		t.Fatalf("unexpected workflow run metadata in strict registry: %#v", *run)
	}
}

// TestWorkflowCatalogSyncsMenuPermissionsIntoIAMStrictRegistry 覆盖完整装配链路：
// 模块 Extension(Catalog) → CatalogRegistrar → authz Registry → IAM 同步钩子 → strict registry。
//
// 菜单可见性权限不挂任何 PermissionMiddleware，只能经这条链路进入 strict registry；
// 缺了它，角色授予 workflow 菜单权限会被判为“未知权限”。
func TestWorkflowCatalogSyncsMenuPermissionsIntoIAMStrictRegistry(t *testing.T) {
	registry := auth.NewRegistry()
	if err := iamservice.InstallIAMPermissionCatalog(registry); err != nil {
		t.Fatalf("InstallIAMPermissionCatalog: %v", err)
	}

	mod, err := NewModule()
	if err != nil {
		t.Fatalf("NewModule() error = %v", err)
	}
	registrar := auth.NewCatalogRegistrar(registry)
	if err := registrar.RegisterModuleCatalog(context.Background(), []capability.ModuleCatalogEntry{
		{ID: "workflow", Module: mod},
	}); err != nil {
		t.Fatalf("RegisterModuleCatalog: %v", err)
	}

	for _, code := range []string{
		workflowpermissions.WorkflowMenuListCode,
		workflowpermissions.WorkflowMenuDesignerCode,
		workflowpermissions.WorkflowRunCode,
	} {
		if !iammw.HasRequiredPermission(code) {
			t.Fatalf("expected workflow permission %q mirrored into IAM strict registry", code)
		}
	}

	definitions := iammw.RequiredPermissionDefinitions()
	menu := findPermissionDefinition(definitions, workflowpermissions.WorkflowMenuDesignerCode)
	if menu == nil {
		t.Fatalf("missing workflow menu definition in strict registry: %#v", definitions)
	}
	if !reflect.DeepEqual(menu.Scopes, []string{string(auth.PermissionScopePlatform), string(auth.PermissionScopeTenant)}) {
		t.Fatalf("unexpected workflow menu scopes in strict registry: %#v", menu.Scopes)
	}
}

type stubRouteGroup struct{}

func (s *stubRouteGroup) Group(string) httpx.IRouteGroup                  { return s }
func (s *stubRouteGroup) Use(...httpx.Middleware) httpx.IRouteGroup       { return s }
func (s *stubRouteGroup) GET(string, httpx.Handler) httpx.IRouteGroup     { return s }
func (s *stubRouteGroup) POST(string, httpx.Handler) httpx.IRouteGroup    { return s }
func (s *stubRouteGroup) PUT(string, httpx.Handler) httpx.IRouteGroup     { return s }
func (s *stubRouteGroup) DELETE(string, httpx.Handler) httpx.IRouteGroup  { return s }
func (s *stubRouteGroup) PATCH(string, httpx.Handler) httpx.IRouteGroup   { return s }
func (s *stubRouteGroup) OPTIONS(string, httpx.Handler) httpx.IRouteGroup { return s }
func (s *stubRouteGroup) HEAD(string, httpx.Handler) httpx.IRouteGroup    { return s }
func (s *stubRouteGroup) Static(string, string)                           {}
func (s *stubRouteGroup) StaticFile(string, string)                       {}

func findPermissionDefinition(definitions []auth.PermissionDefinition, code string) *auth.PermissionDefinition {
	for i := range definitions {
		if definitions[i].Code == code {
			return &definitions[i]
		}
	}
	return nil
}
