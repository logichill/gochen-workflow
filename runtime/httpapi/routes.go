package httpapi

import (
	hostauthz "gochen-runtime/host/authz"
	workflowpermissions "gochen-workflow/permissions"
	"gochen/auth"
	"gochen/httpx"
)

type workflowRoute struct {
	method, path, action string
	kind                 auth.Kind
	handler              func(*Registrar, httpx.IContext) error
}

type workflowRouteGroup struct {
	prefix     string
	permission hostauthz.PermissionSpec
	routes     []workflowRoute
}

// The same table defines execution actions and their actual HTTP registrations.
var workflowRoutes = []workflowRouteGroup{
	{prefix: "/definitions", permission: workflowpermissions.WorkflowRead, routes: []workflowRoute{
		{"GET", "", "definition_list", auth.KindList, (*Registrar).listDefinitions},
		{"GET", "/:id", "definition_read", auth.KindRead, (*Registrar).getDefinition},
	}},
	{prefix: "/definitions", permission: workflowpermissions.WorkflowDesign, routes: []workflowRoute{
		{"POST", "", "definition_create", auth.KindCreate, (*Registrar).createDefinition},
		{"PUT", "/:id", "definition_update", auth.KindUpdate, (*Registrar).updateDefinition},
		{"DELETE", "/:id", "definition_delete", auth.KindDelete, (*Registrar).deleteDefinition},
		{"POST", "/:id/publish", "definition_publish", auth.KindUpdate, (*Registrar).publishDefinition},
		{"POST", "/:id/archive", "definition_archive", auth.KindUpdate, (*Registrar).archiveDefinition},
	}},
	{prefix: "/instances", permission: workflowpermissions.WorkflowRead, routes: []workflowRoute{
		{"GET", "", "instance_list", auth.KindList, (*Registrar).listInstances},
		{"GET", "/:id", "instance_read", auth.KindRead, (*Registrar).getInstance},
	}},
	{permission: workflowpermissions.WorkflowRead, routes: []workflowRoute{
		{"GET", "/todos", "todos", auth.KindList, (*Registrar).listTodos},
		{"GET", "/done", "done", auth.KindList, (*Registrar).listDone},
		{"GET", "/started", "started", auth.KindList, (*Registrar).listStarted},
		{"GET", "/my/todos", "my_todos", auth.KindList, (*Registrar).listMyTodos},
		{"GET", "/my/done", "my_done", auth.KindList, (*Registrar).listMyDone},
		{"GET", "/my/started", "my_started", auth.KindList, (*Registrar).listMyStarted},
	}},
	{prefix: "/instances", permission: workflowpermissions.WorkflowRun, routes: []workflowRoute{
		{"POST", "", "instance_create", auth.KindCreate, (*Registrar).createInstance},
		{"POST", "/:id/start", "instance_start", auth.KindUpdate, (*Registrar).startInstance},
		{"POST", "/:id/nodes/:node_id/advance", "instance_advance", auth.KindUpdate, (*Registrar).advanceNode},
		{"POST", "/:id/nodes/:node_id/complete", "instance_complete", auth.KindUpdate, (*Registrar).completeNode},
		{"POST", "/:id/nodes/:node_id/reject", "instance_reject", auth.KindUpdate, (*Registrar).rejectNode},
		{"POST", "/:id/nodes/:node_id/claim", "instance_claim", auth.KindUpdate, (*Registrar).claimTask},
		{"POST", "/:id/nodes/:node_id/unclaim", "instance_unclaim", auth.KindUpdate, (*Registrar).unclaimTask},
		{"POST", "/:id/nodes/:node_id/transfer", "instance_transfer", auth.KindUpdate, (*Registrar).transferTask},
		{"POST", "/:id/nodes/:node_id/delegate", "instance_delegate", auth.KindUpdate, (*Registrar).delegateTask},
		{"POST", "/:id/nodes/:node_id/remind", "instance_remind", auth.KindUpdate, (*Registrar).remindTask},
		{"POST", "/:id/cc", "instance_cc", auth.KindUpdate, (*Registrar).addCCUsers},
		{"POST", "/:id/withdraw", "instance_withdraw", auth.KindUpdate, (*Registrar).withdrawInstance},
	}},
}
