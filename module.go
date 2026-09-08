package workflow

import (
	workflowpermissions "gochen-workflow/permissions"
	"gochen-workflow/runtime/httpapi"
	workflowservice "gochen-workflow/service"

	"gochen-runtime/host"
	auth "gochen-runtime/host/authz"
	"gochen-runtime/host/module"
)

// NewModule 创建 workflow 业务模块，并通过 GoChen Host 注册依赖与路由。
func NewModule() (module.IModule, error) {
	definitions := workflowpermissions.Definitions()

	return host.Module("workflow").
		Name("Workflow").
		Extension(auth.Catalog{
			PermissionDefinitions: definitions,
		}).
		Provide(workflowservice.NewService).
		RouteRegistrar(httpapi.NewRegistrar).
		Build()
}
