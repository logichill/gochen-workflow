package workflow

import (
	"reflect"
	"testing"

	iammw "gochen-iam/middleware"
	workflowpermissions "gochen-workflow/permissions"

	auth "gochen/auth"
)

func TestNewModuleDeclaresWorkflowPermissions(t *testing.T) {
	mod, err := NewModule()
	if err != nil {
		t.Fatalf("NewModule() error = %v", err)
	}
	provider, ok := mod.(interface {
		AuthzRegistration() auth.ModuleRegistration
	})
	if !ok {
		t.Fatalf("module does not expose authz registration: %T", mod)
	}
	definitions := provider.AuthzRegistration().PermissionDefinitions
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
	if _, err := NewModule(); err != nil {
		t.Fatalf("NewModule() error = %v", err)
	}

	definitions := iammw.RequiredPermissionDefinitions()
	run := findPermissionDefinition(definitions, workflowpermissions.WorkflowRunCode)
	if run == nil {
		t.Fatalf("missing workflow run permission in strict registry: %#v", definitions)
	}
	if run.Name != "流程实例推进" || run.RiskLevel != string(iammw.RiskLevelHigh) {
		t.Fatalf("unexpected workflow run metadata: %#v", *run)
	}
	if run.Description != "可创建、启动、推进、认领、取消认领、转办、委托、催办、抄送与撤回流程实例。" {
		t.Fatalf("unexpected workflow run description: %#v", run.Description)
	}
	if !reflect.DeepEqual(run.Scopes, []string{string(iammw.ScopePlatform), string(iammw.ScopeTenant)}) {
		t.Fatalf("unexpected workflow run scopes: %#v", run.Scopes)
	}

	menu := findPermissionDefinition(definitions, workflowpermissions.WorkflowMenuDesignerCode)
	if menu == nil {
		t.Fatalf("missing workflow menu permission in strict registry: %#v", definitions)
	}
	if menu.Name != "流程设计器菜单" {
		t.Fatalf("unexpected workflow menu metadata: %#v", *menu)
	}
}

func findPermissionDefinition(definitions []auth.PermissionDefinition, code string) *auth.PermissionDefinition {
	for i := range definitions {
		if definitions[i].Code == code {
			return &definitions[i]
		}
	}
	return nil
}
