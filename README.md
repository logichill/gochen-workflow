# gochen-workflow

`gochen-workflow` 是 Galaxy 工作流设计器的 GoChen 后端模块。

它保存前端设计态 JSON，并在落库前投影为 `gochen/process/workflow.Definition`，让流程定义始终经过 GoChen runtime 校验。

## 模型约定

- 核心运行时只使用 `Definition -> Node -> Next` 这一套图模型。
- 线性流程是单出边节点串联，并行分支是多出边节点，汇聚是多入边节点等待所有前驱到达。
- `branch` 支持显式选择单一后继；未指定后继时按并行 fan-out 推进。
- 设计器可以保存 UI、位置、边标签等设计态信息；运行时投影只保留可执行的节点图。
- 设计态节点保留 `type`（`task` / `branch` / `join`）用于 UI、校验与运行时节点语义投影。
- 定义状态为 `draft` / `published` / `archived`；只有 `published` 定义允许创建运行实例。
- `published` / `archived` 定义不可原地编辑；修改流程需基于草稿重新保存，删除只允许未被实例引用的草稿。
- 流程定义与实例按 active scope 隔离，存储身份为 `(scope_kind, scope_id, id)`；不同 tenant/platform scope 可以复用相同流程 ID，但不能互相读取或推进实例。
- 审批中心应用层元数据保存在实例 `data._workflow`，HTTP DTO 会把它投影为 `business_key`、`title`、`initiator_*`、`assignee_*`、`actor_*` 等字段，并从公开 `data` 中隐藏内部 key。

## 模块边界

- 根包：暴露 `NewModule()`，供宿主项目接入。
- `types/`：流程设计定义模型。
- `service/`：流程编排、投影与存储。
- `runtime/httpapi/`：HTTP 路由注册。
- `cmd/gochen-workflow/`：独立服务启动入口。

## 运行

```sh
GOCHEN_WORKFLOW_ADDR=:8088 \
GOCHEN_WORKFLOW_SQLITE_DSN='file:gochen-workflow.db' \
AUTH_SECRET='local-dev-secret' \
go run ./cmd/gochen-workflow
```

`GOCHEN_WORKFLOW_SQLITE_DSN` 为空时，默认写入 `file:gochen-workflow.db`。
独立服务内置 HS256 JWT 鉴权中间件；除 health/ping 等跳过路径外，API 请求需要携带 `Authorization: Bearer <token>`。
本地联调时应保证 token 由相同 `AUTH_SECRET` 签发，并包含 `active_scope_id` 与对应 API 权限；独立服务会将 `active_scope_id` 作为当前租户级 scope。宿主模块接入时，HTTP 层把 `gochen-runtime/host/authz` 的 active scope 映射为 workflow service scope。普通 `Service` 通过编译计划统一校验可信身份、read/design/run 权限和明确的 Scope，再访问 Store；普通主体的 Scope 类型和 ID 必须与可信 active scope 一致。直接服务调用也需由可信组合根建立这些上下文。

设计和运行操作中的内部读取复用该次操作权限。公开读取入口要求 read 权限，操作人及对象状态规则继续独立校验。`Service.AuthorizationBinding` 提供 read/design/run 执行绑定；ERP 内部协作沿用 `SystemExecution`，保持 System、租户与严格 tenant Scope 约束。历史 `platform/0` 数据可由显式绑定该 Scope 的可信 System 进行迁移和验证。

直接调用 `CreateInstanceWithOptions` 时，普通主体的显式发起人须等于当前认证主体；可信 System 可以代表已知发起人执行。

旧版本未启用 scope 的 SQLite 数据会迁移到 `platform/0`，不会被任何 HTTP tenant 自动认领。确认数据归属后，使用 `GOCHEN_WORKFLOW_LEGACY_SCOPE_ID=<positive-id>` 启动一次服务，将这些记录原子迁移到当前 `GOCHEN_WORKFLOW_SCOPE_KIND`（未设置时为 `tenant`）和指定 ID；目标 scope 已存在同 ID 记录时迁移会失败并回滚。

权限码：

- `workflow:api:read`：读取流程定义与实例。
- `workflow:api:design`：创建、更新、发布、归档与删除流程定义。
- `workflow:api:run`：创建、启动、推进、认领、转办、委托、催办、抄送与撤回流程实例。
- `workflow.list:menu:view`：查看流程管理菜单入口。
- `workflow.designer:menu:view`：查看流程设计器菜单入口。

## 接入

宿主项目可直接引入根包模块：

```go
import workflow "gochen-workflow"
import "gochen-runtime/quick"

app := quick.New(hostOptions...)
app.Group("").Use(authMiddleware).Modules(workflow.NewModule)
err := app.Run(ctx)
```

Host 未配置模块前缀时，会根据模块 ID 自动挂载到 `/workflow`。

模块依赖 `service.Store`、`ident.IGenerator[string]` 与 `clock.IClock`，由宿主通过 GoChen DI 注入。当前仓库的 [`go.mod`](go.mod) 通过 `replace` 指向相邻工作区 `../gochen` 与 `../gochen-runtime`；若脱离该 mono-workspace 单独使用，需要改成可解析的模块来源。

## API

- `GET /api/v1/workflow/definitions`
- `POST /api/v1/workflow/definitions`
- `GET /api/v1/workflow/definitions/{id}`
- `PUT /api/v1/workflow/definitions/{id}`
- `DELETE /api/v1/workflow/definitions/{id}`
- `POST /api/v1/workflow/definitions/{id}/publish`
- `POST /api/v1/workflow/definitions/{id}/archive`
- `POST /api/v1/workflow/instances`
- `GET /api/v1/workflow/instances`
- `GET /api/v1/workflow/instances/{id}`
- `POST /api/v1/workflow/instances/{id}/start`
- `POST /api/v1/workflow/instances/{id}/nodes/{node_id}/advance`
- `POST /api/v1/workflow/instances/{id}/nodes/{node_id}/complete`
- `POST /api/v1/workflow/instances/{id}/nodes/{node_id}/reject`
- `POST /api/v1/workflow/instances/{id}/nodes/{node_id}/claim`
- `POST /api/v1/workflow/instances/{id}/nodes/{node_id}/unclaim`
- `POST /api/v1/workflow/instances/{id}/nodes/{node_id}/transfer`
- `POST /api/v1/workflow/instances/{id}/nodes/{node_id}/delegate`
- `POST /api/v1/workflow/instances/{id}/nodes/{node_id}/remind`
- `POST /api/v1/workflow/instances/{id}/cc`
- `POST /api/v1/workflow/instances/{id}/withdraw`
- `GET /api/v1/workflow/todos`
- `GET /api/v1/workflow/done`
- `GET /api/v1/workflow/started`
- `GET /api/v1/workflow/my/todos`
- `GET /api/v1/workflow/my/done`
- `GET /api/v1/workflow/my/started`

响应统一使用 GoChen `httpx.ResponseMessage`，业务数据位于 `data`；实例响应使用 snake_case DTO，不直接暴露核心 runtime 结构体字段。
列表分页统一使用 `page/size`，不支持 `page_size`。`/todos`、`/done`、`/started` 和 `/instances` 是当前 scope 监控视角；`/my/*` 按当前 `Principal.SubjectID` 过滤个人发起、待办和已办。
创建实例可传 `business_key`、`title`、`assignee_id`、`assignee_name`、`candidate_user_ids`、`candidate_role_ids`、`cc_user_ids`、`due_at` 与 `data`。HTTP 请求中的 `initiator_id`/`initiator_name` 会被忽略，发起人始终取认证 `Principal.SubjectID`；可信内部 service 调用可通过 `CreateInstanceWithOptions` 显式指定发起人。
推进分支节点时，请求体可传 `{"next_node_id":"approved"}` 显式选择单一后继；不传请求体或传空对象时按 runtime 默认 fan-out 推进。完成、认领、取消认领、转办、委托、催办、抄送和撤回请求体可传 `comment`；完成、转办和委托还可传下一处理人字段和 `due_at`。转办/委托未传 `due_at` 时继承原任务到期时间。

## 验证

```sh
# 最小必要验证
go test ./runtime/httpapi ./service ./types

# 仓库全量验证
go test ./...
```
