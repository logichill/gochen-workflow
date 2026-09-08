package permissions

import (
	iammw "gochen-iam/middleware"
	auth "gochen-runtime/host/authz"
)

const (
	WorkflowReadCode         = "workflow:api:read"
	WorkflowDesignCode       = "workflow:api:design"
	WorkflowRunCode          = "workflow:api:run"
	WorkflowMenuListCode     = "workflow.list:menu:view"
	WorkflowMenuDesignerCode = "workflow.designer:menu:view"
)

var (
	WorkflowRead = iammw.PermissionCode(WorkflowReadCode).
			Label("流程定义读取").
			Desc("可查看流程定义与运行实例。").
			Scope(iammw.ScopePlatform, iammw.ScopeTenant)
	WorkflowDesign = iammw.PermissionCode(WorkflowDesignCode).
			Label("流程定义设计").
			Desc("可创建、更新、发布、归档与删除流程定义。").
			Scope(iammw.ScopePlatform, iammw.ScopeTenant).
			Risk(iammw.RiskLevelHigh)
	WorkflowRun = iammw.PermissionCode(WorkflowRunCode).
			Label("流程实例推进").
			Desc("可创建、启动、推进、认领、取消认领、转办、委托、催办、抄送与撤回流程实例。").
			Scope(iammw.ScopePlatform, iammw.ScopeTenant).
			Risk(iammw.RiskLevelHigh)
	WorkflowMenuList = iammw.PermissionCode(WorkflowMenuListCode).
				Label("流程管理菜单").
				Desc("可在菜单中查看流程管理入口。").
				Scope(iammw.ScopePlatform, iammw.ScopeTenant)
	WorkflowMenuDesigner = iammw.PermissionCode(WorkflowMenuDesignerCode).
				Label("流程设计器菜单").
				Desc("可在菜单中查看流程设计器入口。").
				Scope(iammw.ScopePlatform, iammw.ScopeTenant)
)

func Definitions() []auth.PermissionDefinition {
	return iammw.AuthzPermissionDefinitions(
		WorkflowRead.Definition(),
		WorkflowDesign.Definition(),
		WorkflowRun.Definition(),
		WorkflowMenuList.Definition(),
		WorkflowMenuDesigner.Definition(),
	)
}
