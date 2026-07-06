package workflow

import (
	iammw "gochen-iam/middleware"
	workflowpermissions "gochen-workflow/permissions"
	"gochen-workflow/runtime/httpapi"
	workflowservice "gochen-workflow/service"

	"gochen/host"
)

// NewModule 创建 workflow 业务模块，并通过 GoChen Host 注册依赖与路由。
func NewModule() (host.IModule, error) {
	definitions := workflowpermissions.Definitions()
	iammw.RegisterRequiredPermissionDefinitions(definitions...)

	return host.Module("workflow").
		Name("Workflow").
		PermissionDefinitions(definitions...).
		Provide(workflowservice.NewService).
		RouteRegistrar(httpapi.NewRegistrar).
		Build()
}
