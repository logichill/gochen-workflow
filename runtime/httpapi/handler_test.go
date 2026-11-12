package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	workflowpermissions "gochen-workflow/permissions"
	workflowservice "gochen-workflow/service"
	workflowtypes "gochen-workflow/types"

	auth "gochen-runtime/host/authz"
	"gochen-runtime/http/nethttp"
	"gochen/httpx"
)

func TestRegistrarDefinitionAndInstanceAPI(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	handlers := registerTestRoutes(t, NewRegistrar(service))
	payload := map[string]any{
		"id":            "approval",
		"name":          "Approval",
		"start_node_id": "start",
		"nodes": []map[string]any{
			{"id": "start", "name": "Start", "next": []string{"end"}, "position": map[string]any{"x": 0, "y": 0}},
			{"id": "end", "name": "End", "next": []string{}, "position": map[string]any{"x": 120, "y": 0}},
		},
		"edges": []map[string]any{{"id": "e1", "source": "start", "target": "end"}},
	}

	createResp := doJSON(t, handlers["POST /definitions"], nil, payload)
	if createResp.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", createResp.Code, createResp.Body.String())
	}
	assertMessageCode(t, createResp, "ok")

	publishResp := doJSON(t, handlers["POST /definitions/:id/publish"], map[string]string{"id": "approval"}, nil)
	if publishResp.Code != http.StatusOK {
		t.Fatalf("publish status = %d body=%s", publishResp.Code, publishResp.Body.String())
	}

	instanceResp := doJSON(t, handlers["POST /instances"], nil, map[string]any{"id": "inst-1", "definition_id": "approval"})
	if instanceResp.Code != http.StatusCreated {
		t.Fatalf("instance status = %d body=%s", instanceResp.Code, instanceResp.Body.String())
	}

	startResp := doJSON(t, handlers["POST /instances/:id/start"], map[string]string{"id": "inst-1"}, nil)
	if startResp.Code != http.StatusOK {
		t.Fatalf("start status = %d body=%s", startResp.Code, startResp.Body.String())
	}
	var message httpx.ResponseMessage
	if err := json.Unmarshal(startResp.Body.Bytes(), &message); err != nil {
		t.Fatalf("decode response message: %v", err)
	}
	raw, err := json.Marshal(message.Data)
	if err != nil {
		t.Fatalf("encode state data: %v", err)
	}
	var state struct {
		ActiveNodeIDs []string `json:"active_node_ids"`
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatalf("decode state data: %v", err)
	}
	if len(state.ActiveNodeIDs) != 1 || state.ActiveNodeIDs[0] != "start" {
		t.Fatalf("active nodes = %#v", state.ActiveNodeIDs)
	}
}

func TestRegistrarCreateInstanceUsesAuthenticatedPrincipal(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	handlers := registerTestRoutes(t, NewRegistrar(service))
	payload := map[string]any{
		"id":            "principal-definition",
		"name":          "Principal Definition",
		"start_node_id": "start",
		"nodes": []map[string]any{
			{"id": "start", "name": "Start", "next": []string{"end"}, "position": map[string]any{"x": 0, "y": 0}},
			{"id": "end", "name": "End", "position": map[string]any{"x": 120, "y": 0}},
		},
		"edges": []map[string]any{{"id": "principal-edge", "source": "start", "target": "end"}},
	}
	if resp := doJSON(t, handlers["POST /definitions"], nil, payload); resp.Code != http.StatusCreated {
		t.Fatalf("create definition status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /definitions/:id/publish"], map[string]string{"id": "principal-definition"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("publish definition status = %d body=%s", resp.Code, resp.Body.String())
	}
	resp := doJSONAsUser(t, handlers["POST /instances"], nil, map[string]any{
		"id":             "principal-instance",
		"definition_id":  "principal-definition",
		"initiator_id":   "9999",
		"initiator_name": "forged name",
	}, 1001)
	if resp.Code != http.StatusCreated {
		t.Fatalf("create instance status = %d body=%s", resp.Code, resp.Body.String())
	}
	var state workflowtypes.InstanceState
	decodeResponseData(t, resp, &state)
	if state.InitiatorID != "1001" || state.InitiatorName != "" {
		t.Fatalf("initiator = %q/%q, want authenticated id and empty untrusted name", state.InitiatorID, state.InitiatorName)
	}

	unauthenticatedReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"definition_id":"principal-definition"}`))
	unauthenticatedResp := httptest.NewRecorder()
	httpCtx, err := nethttp.NewBaseContext(unauthenticatedResp, unauthenticatedReq)
	if err != nil {
		t.Fatalf("new unauthenticated context: %v", err)
	}
	httpCtx.SetContext(httpCtx.RequestContext().WithContext(
		auth.BindActiveScopeContext(httpCtx.RequestContext(), 1, workflowservice.ScopeKindTenant)))
	if err := handlers["POST /instances"](httpCtx); err != nil {
		_ = nethttp.WriteErrorResponse(httpCtx, err)
	}
	if unauthenticatedResp.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated create status = %d body=%s", unauthenticatedResp.Code, unauthenticatedResp.Body.String())
	}
}

func TestRegistrarListDefinitionsIncludesStatusStats(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	handlers := registerTestRoutes(t, NewRegistrar(service))
	createPayload := func(id string) map[string]any {
		return map[string]any{
			"id":            id,
			"name":          id,
			"start_node_id": "start",
			"nodes": []map[string]any{
				{"id": "start", "name": "Start", "next": []string{"end"}, "position": map[string]any{"x": 0, "y": 0}},
				{"id": "end", "name": "End", "next": []string{}, "position": map[string]any{"x": 120, "y": 0}},
			},
			"edges": []map[string]any{{"id": "e1", "source": "start", "target": "end"}},
		}
	}

	for _, id := range []string{"draft-flow", "published-flow", "archived-flow"} {
		if resp := doJSON(t, handlers["POST /definitions"], nil, createPayload(id)); resp.Code != http.StatusCreated {
			t.Fatalf("create %s status = %d body=%s", id, resp.Code, resp.Body.String())
		}
	}
	if resp := doJSON(t, handlers["POST /definitions/:id/publish"], map[string]string{"id": "published-flow"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("publish status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /definitions/:id/publish"], map[string]string{"id": "archived-flow"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("publish archived candidate status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /definitions/:id/archive"], map[string]string{"id": "archived-flow"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("archive status = %d body=%s", resp.Code, resp.Body.String())
	}

	resp := doGet(t, handlers["GET /definitions"], "/")
	if resp.Code != http.StatusOK {
		t.Fatalf("list status = %d body=%s", resp.Code, resp.Body.String())
	}
	var message httpx.ResponseMessage
	if err := json.Unmarshal(resp.Body.Bytes(), &message); err != nil {
		t.Fatalf("decode response message: %v", err)
	}
	raw, err := json.Marshal(message.Data)
	if err != nil {
		t.Fatalf("encode list data: %v", err)
	}
	var list workflowtypes.DefinitionListResponse
	if err := json.Unmarshal(raw, &list); err != nil {
		t.Fatalf("decode list data: %v", err)
	}
	if list.Total != 3 || len(list.Items) != 3 {
		t.Fatalf("list total/items = %d/%d, want 3/3", list.Total, len(list.Items))
	}
	if list.Stats.Draft != 1 || list.Stats.Published != 1 || list.Stats.Archived != 1 {
		t.Fatalf("stats = %#v, want one per status", list.Stats)
	}
	if list.StatusCounts != list.Stats {
		t.Fatalf("status_counts = %#v, want %#v", list.StatusCounts, list.Stats)
	}

	filteredResp := doGet(t, handlers["GET /definitions"], "/?page=1&size=1&keyword=flow&status=draft")
	if filteredResp.Code != http.StatusOK {
		t.Fatalf("filtered list status = %d body=%s", filteredResp.Code, filteredResp.Body.String())
	}
	var filtered workflowtypes.DefinitionListResponse
	decodeResponseData(t, filteredResp, &filtered)
	if filtered.Total != 1 || len(filtered.Items) != 1 || filtered.Items[0].ID != "draft-flow" || filtered.Page != 1 || filtered.Size != 1 {
		t.Fatalf("unexpected filtered definition page: %#v", filtered)
	}
	if filtered.Stats.Total != 3 {
		t.Fatalf("keyword-scoped stats total = %d, want 3", filtered.Stats.Total)
	}

	unsupportedResp := doGet(t, handlers["GET /definitions"], "/?page_size=1")
	if unsupportedResp.Code != http.StatusBadRequest {
		t.Fatalf("page_size status = %d body=%s", unsupportedResp.Code, unsupportedResp.Body.String())
	}
}

func TestRegistrarWorkItemAPI(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	handlers := registerTestRoutes(t, NewRegistrar(service))
	payload := map[string]any{
		"id":            "approval-work-items",
		"name":          "Approval Work Items",
		"start_node_id": "start",
		"nodes": []map[string]any{
			{"id": "start", "name": "Start", "next": []string{"end"}, "position": map[string]any{"x": 0, "y": 0}},
			{"id": "end", "name": "End", "next": []string{}, "position": map[string]any{"x": 120, "y": 0}},
		},
		"edges": []map[string]any{{"id": "e1", "source": "start", "target": "end"}},
	}

	if resp := doJSON(t, handlers["POST /definitions"], nil, payload); resp.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /definitions/:id/publish"], map[string]string{"id": "approval-work-items"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("publish status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances"], nil, map[string]any{"id": "inst-work-items", "definition_id": "approval-work-items"}); resp.Code != http.StatusCreated {
		t.Fatalf("instance status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances/:id/start"], map[string]string{"id": "inst-work-items"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("start status = %d body=%s", resp.Code, resp.Body.String())
	}

	todoResp := doGet(t, handlers["GET /todos"], "/?page=1&size=10")
	if todoResp.Code != http.StatusOK {
		t.Fatalf("todos status = %d body=%s", todoResp.Code, todoResp.Body.String())
	}
	var todoPage struct {
		Items []struct {
			Type           string `json:"type"`
			InstanceID     string `json:"instance_id"`
			DefinitionName string `json:"definition_name"`
			NodeID         string `json:"node_id"`
			NodeName       string `json:"node_name"`
		} `json:"items"`
		Total int `json:"total"`
		Page  int `json:"page"`
		Size  int `json:"size"`
	}
	decodeResponseData(t, todoResp, &todoPage)
	if todoPage.Total != 1 || todoPage.Page != 1 || todoPage.Size != 10 || len(todoPage.Items) != 1 {
		t.Fatalf("todo page = %#v", todoPage)
	}
	if item := todoPage.Items[0]; item.Type != "todo" || item.InstanceID != "inst-work-items" || item.DefinitionName != "Approval Work Items" || item.NodeID != "start" || item.NodeName != "Start" {
		t.Fatalf("todo item = %#v", item)
	}

	if resp := doJSON(t, handlers["POST /instances/:id/nodes/:node_id/advance"], map[string]string{"id": "inst-work-items", "node_id": "start"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("advance status = %d body=%s", resp.Code, resp.Body.String())
	}
	doneResp := doGet(t, handlers["GET /done"], "/?page=1&size=10")
	if doneResp.Code != http.StatusOK {
		t.Fatalf("done status = %d body=%s", doneResp.Code, doneResp.Body.String())
	}
	var donePage struct {
		Items []struct {
			Type        string  `json:"type"`
			InstanceID  string  `json:"instance_id"`
			NodeID      string  `json:"node_id"`
			CompletedAt *string `json:"completed_at"`
		} `json:"items"`
		Total int `json:"total"`
	}
	decodeResponseData(t, doneResp, &donePage)
	if donePage.Total != 1 || len(donePage.Items) != 1 {
		t.Fatalf("done page = %#v", donePage)
	}
	if item := donePage.Items[0]; item.Type != "done" || item.InstanceID != "inst-work-items" || item.NodeID != "start" || item.CompletedAt == nil {
		t.Fatalf("done item = %#v", item)
	}
}

func TestRegistrarWorkItemAPIRejectsLegacyPageSize(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	handlers := registerTestRoutes(t, NewRegistrar(service))

	resp := doGet(t, handlers["GET /todos"], "/?page_size=10")
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 body=%s", resp.Code, resp.Body.String())
	}
}

// TestRegistrarTodosHonorsKeywordFilter 验证管理端 /todos 应用 keyword 过滤，
// 而不是像旧实现那样仅传 page/size 丢弃过滤条件。
func TestRegistrarTodosHonorsKeywordFilter(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	handlers := registerTestRoutes(t, NewRegistrar(service))
	payload := map[string]any{
		"id":            "approval-keyword",
		"name":          "Approval Keyword Flow",
		"start_node_id": "start",
		"nodes": []map[string]any{
			{"id": "start", "name": "Start", "next": []string{"end"}, "position": map[string]any{"x": 0, "y": 0}},
			{"id": "end", "name": "End", "next": []string{}, "position": map[string]any{"x": 120, "y": 0}},
		},
		"edges": []map[string]any{{"id": "e1", "source": "start", "target": "end"}},
	}
	if resp := doJSON(t, handlers["POST /definitions"], nil, payload); resp.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /definitions/:id/publish"], map[string]string{"id": "approval-keyword"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("publish status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances"], nil, map[string]any{"id": "inst-keyword", "definition_id": "approval-keyword"}); resp.Code != http.StatusCreated {
		t.Fatalf("instance status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances/:id/start"], map[string]string{"id": "inst-keyword"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("start status = %d body=%s", resp.Code, resp.Body.String())
	}

	var page struct {
		Total int `json:"total"`
	}
	matched := doGet(t, handlers["GET /todos"], "/?page=1&size=10&keyword=Keyword")
	decodeResponseData(t, matched, &page)
	if page.Total != 1 {
		t.Fatalf("matching keyword total = %d, want 1", page.Total)
	}
	page.Total = 0
	missed := doGet(t, handlers["GET /todos"], "/?page=1&size=10&keyword=NoSuchThing")
	decodeResponseData(t, missed, &page)
	if page.Total != 0 {
		t.Fatalf("non-matching keyword total = %d, want 0", page.Total)
	}
}

func TestRegistrarWorkflowCenterAPI(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	handlers := registerTestRoutes(t, NewRegistrar(service))
	payload := map[string]any{
		"id":            "approval-center",
		"name":          "Approval Center",
		"start_node_id": "start",
		"nodes": []map[string]any{
			{"id": "start", "name": "Start", "next": []string{"end"}, "position": map[string]any{"x": 0, "y": 0}},
			{"id": "end", "name": "End", "next": []string{}, "position": map[string]any{"x": 120, "y": 0}},
		},
		"edges": []map[string]any{{"id": "e1", "source": "start", "target": "end"}},
	}

	if resp := doJSON(t, handlers["POST /definitions"], nil, payload); resp.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /definitions/:id/publish"], map[string]string{"id": "approval-center"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("publish status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances"], nil, map[string]any{
		"id":             "inst-center",
		"definition_id":  "approval-center",
		"business_key":   "REQ-HTTP",
		"title":          "HTTP 审批",
		"initiator_id":   "1001",
		"initiator_name": "发起人",
		"assignee_id":    "2002",
		"assignee_name":  "审批人",
		"data":           map[string]any{"form_id": "F-HTTP"},
	}); resp.Code != http.StatusCreated {
		t.Fatalf("instance status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances/:id/start"], map[string]string{"id": "inst-center"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("start status = %d body=%s", resp.Code, resp.Body.String())
	}

	startedResp := doGetAsUser(t, handlers["GET /my/started"], "/?page=1&size=20", 1001)
	if startedResp.Code != http.StatusOK {
		t.Fatalf("my started status = %d body=%s", startedResp.Code, startedResp.Body.String())
	}
	var startedPage struct {
		Items []struct {
			ID          string `json:"id"`
			Title       string `json:"title"`
			BusinessKey string `json:"business_key"`
		} `json:"items"`
		Total int `json:"total"`
	}
	decodeResponseData(t, startedResp, &startedPage)
	if startedPage.Total != 1 || startedPage.Items[0].ID != "inst-center" || startedPage.Items[0].BusinessKey != "REQ-HTTP" {
		t.Fatalf("started page = %#v", startedPage)
	}

	todoResp := doGetAsUser(t, handlers["GET /my/todos"], "/?page=1&size=20", 2002)
	if todoResp.Code != http.StatusOK {
		t.Fatalf("my todo status = %d body=%s", todoResp.Code, todoResp.Body.String())
	}
	var todoPage struct {
		Items []struct {
			InstanceID string `json:"instance_id"`
			AssigneeID string `json:"assignee_id"`
			Title      string `json:"title"`
		} `json:"items"`
		Total int `json:"total"`
	}
	decodeResponseData(t, todoResp, &todoPage)
	if todoPage.Total != 1 || todoPage.Items[0].InstanceID != "inst-center" || todoPage.Items[0].AssigneeID != "2002" {
		t.Fatalf("todo page = %#v", todoPage)
	}

	transferResp := doJSONAsUser(t, handlers["POST /instances/:id/nodes/:node_id/transfer"], map[string]string{"id": "inst-center", "node_id": "start"}, map[string]any{
		"assignee_id":   "3003",
		"assignee_name": "转办人",
	}, 2002)
	if transferResp.Code != http.StatusOK {
		t.Fatalf("transfer status = %d body=%s", transferResp.Code, transferResp.Body.String())
	}
	transferredResp := doGetAsUser(t, handlers["GET /my/todos"], "/?page=1&size=20", 3003)
	decodeResponseData(t, transferredResp, &todoPage)
	if transferredResp.Code != http.StatusOK || todoPage.Total != 1 || todoPage.Items[0].AssigneeID != "3003" {
		t.Fatalf("transferred todo status=%d page=%#v body=%s", transferredResp.Code, todoPage, transferredResp.Body.String())
	}

	completeResp := doJSONAsUser(t, handlers["POST /instances/:id/nodes/:node_id/complete"], map[string]string{"id": "inst-center", "node_id": "start"}, map[string]any{"comment": "同意"}, 3003)
	if completeResp.Code != http.StatusOK {
		t.Fatalf("complete status = %d body=%s", completeResp.Code, completeResp.Body.String())
	}
	doneResp := doGetAsUser(t, handlers["GET /my/done"], "/?page=1&size=20", 3003)
	if doneResp.Code != http.StatusOK {
		t.Fatalf("my done status = %d body=%s", doneResp.Code, doneResp.Body.String())
	}
	var donePage struct {
		Items []struct {
			InstanceID string `json:"instance_id"`
			ActorID    string `json:"actor_id"`
			Comment    string `json:"comment"`
		} `json:"items"`
		Total int `json:"total"`
	}
	decodeResponseData(t, doneResp, &donePage)
	if donePage.Total != 1 || donePage.Items[0].ActorID != "3003" || donePage.Items[0].Comment != "同意" {
		t.Fatalf("done page = %#v", donePage)
	}
}

func TestRegistrarMyStartedIgnoresInitiatorOverride(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	handlers := registerTestRoutes(t, NewRegistrar(service))
	payload := map[string]any{
		"id":            "approval-my-started",
		"name":          "Approval My Started",
		"start_node_id": "start",
		"nodes": []map[string]any{
			{"id": "start", "name": "Start", "next": []string{"end"}, "position": map[string]any{"x": 0, "y": 0}},
			{"id": "end", "name": "End", "next": []string{}, "position": map[string]any{"x": 120, "y": 0}},
		},
		"edges": []map[string]any{{"id": "e1", "source": "start", "target": "end"}},
	}

	if resp := doJSON(t, handlers["POST /definitions"], nil, payload); resp.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /definitions/:id/publish"], map[string]string{"id": "approval-my-started"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("publish status = %d body=%s", resp.Code, resp.Body.String())
	}

	createAndStart := func(instanceID string, initiatorID int64) {
		t.Helper()
		if resp := doJSONAsUser(t, handlers["POST /instances"], nil, map[string]any{
			"id":            instanceID,
			"definition_id": "approval-my-started",
			"initiator_id":  "9999",
		}, initiatorID); resp.Code != http.StatusCreated {
			t.Fatalf("instance status = %d body=%s", resp.Code, resp.Body.String())
		}
		if resp := doJSON(t, handlers["POST /instances/:id/start"], map[string]string{"id": instanceID}, nil); resp.Code != http.StatusOK {
			t.Fatalf("start status = %d body=%s", resp.Code, resp.Body.String())
		}
	}

	createAndStart("inst-my-started-self", 1001)
	createAndStart("inst-my-started-other", 3003)

	resp := doGetAsUser(t, handlers["GET /my/started"], "/?page=1&size=20&initiator_id=3003", 1001)
	if resp.Code != http.StatusOK {
		t.Fatalf("my started status = %d body=%s", resp.Code, resp.Body.String())
	}
	var startedPage struct {
		Items []struct {
			ID          string `json:"id"`
			InitiatorID string `json:"initiator_id"`
		} `json:"items"`
		Total int `json:"total"`
	}
	decodeResponseData(t, resp, &startedPage)
	if startedPage.Total != 1 || len(startedPage.Items) != 1 {
		t.Fatalf("started page = %#v", startedPage)
	}
	if item := startedPage.Items[0]; item.ID != "inst-my-started-self" || item.InitiatorID != "1001" {
		t.Fatalf("started item = %#v", item)
	}
}

func TestRegistrarStartedViewsExcludePendingInstances(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	handlers := registerTestRoutes(t, NewRegistrar(service))
	payload := map[string]any{
		"id":            "approval-started-filter",
		"name":          "Approval Started Filter",
		"start_node_id": "start",
		"nodes": []map[string]any{
			{"id": "start", "name": "Start", "next": []string{"end"}, "position": map[string]any{"x": 0, "y": 0}},
			{"id": "end", "name": "End", "next": []string{}, "position": map[string]any{"x": 120, "y": 0}},
		},
		"edges": []map[string]any{{"id": "e1", "source": "start", "target": "end"}},
	}

	if resp := doJSON(t, handlers["POST /definitions"], nil, payload); resp.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /definitions/:id/publish"], map[string]string{"id": "approval-started-filter"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("publish status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances"], nil, map[string]any{
		"id":            "inst-pending",
		"definition_id": "approval-started-filter",
		"initiator_id":  "1001",
	}); resp.Code != http.StatusCreated {
		t.Fatalf("pending instance status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances"], nil, map[string]any{
		"id":            "inst-started",
		"definition_id": "approval-started-filter",
		"initiator_id":  "1001",
	}); resp.Code != http.StatusCreated {
		t.Fatalf("started instance status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances/:id/start"], map[string]string{"id": "inst-started"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("start status = %d body=%s", resp.Code, resp.Body.String())
	}

	startedResp := doGet(t, handlers["GET /started"], "/?page=1&size=20")
	if startedResp.Code != http.StatusOK {
		t.Fatalf("started status = %d body=%s", startedResp.Code, startedResp.Body.String())
	}
	var startedPage struct {
		Items []struct {
			ID        string  `json:"id"`
			StartedAt *string `json:"started_at"`
		} `json:"items"`
		Total int `json:"total"`
	}
	decodeResponseData(t, startedResp, &startedPage)
	if startedPage.Total != 1 || len(startedPage.Items) != 1 {
		t.Fatalf("started page = %#v", startedPage)
	}
	if item := startedPage.Items[0]; item.ID != "inst-started" || item.StartedAt == nil {
		t.Fatalf("started item = %#v", item)
	}

	myStartedResp := doGetAsUser(t, handlers["GET /my/started"], "/?page=1&size=20", 1001)
	if myStartedResp.Code != http.StatusOK {
		t.Fatalf("my started status = %d body=%s", myStartedResp.Code, myStartedResp.Body.String())
	}
	var myStartedPage struct {
		Items []struct {
			ID        string  `json:"id"`
			StartedAt *string `json:"started_at"`
		} `json:"items"`
		Total int `json:"total"`
	}
	decodeResponseData(t, myStartedResp, &myStartedPage)
	if myStartedPage.Total != 1 || len(myStartedPage.Items) != 1 {
		t.Fatalf("my started page = %#v", myStartedPage)
	}
	if item := myStartedPage.Items[0]; item.ID != "inst-started" || item.StartedAt == nil {
		t.Fatalf("my started item = %#v", item)
	}
}

func TestRegistrarWorkflowCenterP2API(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	handlers := registerTestRoutes(t, NewRegistrar(service))
	payload := map[string]any{
		"id":            "approval-center-p2",
		"name":          "Approval Center P2",
		"start_node_id": "start",
		"nodes": []map[string]any{
			{"id": "start", "name": "Start", "next": []string{"end"}, "position": map[string]any{"x": 0, "y": 0}},
			{"id": "end", "name": "End", "next": []string{}, "position": map[string]any{"x": 120, "y": 0}},
		},
		"edges": []map[string]any{{"id": "e1", "source": "start", "target": "end"}},
	}
	dueAt := time.Now().Add(-time.Hour).UTC()

	if resp := doJSON(t, handlers["POST /definitions"], nil, payload); resp.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /definitions/:id/publish"], map[string]string{"id": "approval-center-p2"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("publish status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances"], nil, map[string]any{
		"id":            "inst-center-p2",
		"definition_id": "approval-center-p2",
		"title":         "HTTP P2 审批",
		"initiator_id":  "1001",
		"assignee_id":   "2002",
		"cc_user_ids":   []string{"4004"},
		"due_at":        dueAt,
	}); resp.Code != http.StatusCreated {
		t.Fatalf("instance status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances/:id/start"], map[string]string{"id": "inst-center-p2"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("start status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSONAsUser(t, handlers["POST /instances/:id/nodes/:node_id/remind"], map[string]string{"id": "inst-center-p2", "node_id": "start"}, map[string]any{"comment": "请处理"}, 1001); resp.Code != http.StatusOK {
		t.Fatalf("remind status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSONAsUser(t, handlers["POST /instances/:id/cc"], map[string]string{"id": "inst-center-p2"}, map[string]any{"cc_user_ids": []string{"4004", "5005"}}, 1001); resp.Code != http.StatusOK {
		t.Fatalf("cc status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSONAsUser(t, handlers["POST /instances/:id/nodes/:node_id/delegate"], map[string]string{"id": "inst-center-p2", "node_id": "start"}, map[string]any{"assignee_id": "3003", "assignee_name": "代理人"}, 2002); resp.Code != http.StatusOK {
		t.Fatalf("delegate status = %d body=%s", resp.Code, resp.Body.String())
	}

	todoResp := doGetAsUser(t, handlers["GET /my/todos"], "/?page=1&size=20", 3003)
	if todoResp.Code != http.StatusOK {
		t.Fatalf("delegated todo status = %d body=%s", todoResp.Code, todoResp.Body.String())
	}
	var todoPage struct {
		Items []struct {
			AssigneeID  string   `json:"assignee_id"`
			DelegatorID string   `json:"delegator_id"`
			CCUserIDs   []string `json:"cc_user_ids"`
			DueAt       *string  `json:"due_at"`
			Overdue     bool     `json:"overdue"`
		} `json:"items"`
		Total int `json:"total"`
	}
	decodeResponseData(t, todoResp, &todoPage)
	if todoPage.Total != 1 || todoPage.Items[0].AssigneeID != "3003" || todoPage.Items[0].DelegatorID != "2002" || len(todoPage.Items[0].CCUserIDs) != 2 || todoPage.Items[0].DueAt == nil || !todoPage.Items[0].Overdue {
		t.Fatalf("delegated todo page = %#v", todoPage)
	}

	instancesResp := doGetAsUser(t, handlers["GET /instances"], "/?page=1&size=20", 1001)
	if instancesResp.Code != http.StatusOK {
		t.Fatalf("instances status = %d body=%s", instancesResp.Code, instancesResp.Body.String())
	}
	var instancePage struct {
		Items []struct {
			EarliestDueAt *string `json:"earliest_due_at"`
			OverdueCount  int     `json:"overdue_count"`
		} `json:"items"`
		Total int `json:"total"`
	}
	decodeResponseData(t, instancesResp, &instancePage)
	if instancePage.Total != 1 || instancePage.Items[0].EarliestDueAt == nil || instancePage.Items[0].OverdueCount != 1 {
		t.Fatalf("instance page = %#v", instancePage)
	}
}

func TestRegistrarCompleteBindsNextTaskSLAAndCC(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	handlers := registerTestRoutes(t, NewRegistrar(service))
	payload := map[string]any{
		"id":            "approval-complete-payload",
		"name":          "Approval Complete Payload",
		"start_node_id": "start",
		"nodes": []map[string]any{
			{"id": "start", "name": "Start", "next": []string{"end"}, "position": map[string]any{"x": 0, "y": 0}},
			{"id": "end", "name": "End", "next": []string{}, "position": map[string]any{"x": 120, "y": 0}},
		},
		"edges": []map[string]any{{"id": "e1", "source": "start", "target": "end"}},
	}
	dueAt := time.Now().Add(-time.Hour).UTC()

	if resp := doJSON(t, handlers["POST /definitions"], nil, payload); resp.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /definitions/:id/publish"], map[string]string{"id": "approval-complete-payload"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("publish status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances"], nil, map[string]any{
		"id":            "inst-complete-payload",
		"definition_id": "approval-complete-payload",
		"initiator_id":  "1001",
		"assignee_id":   "2002",
	}); resp.Code != http.StatusCreated {
		t.Fatalf("instance status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances/:id/start"], map[string]string{"id": "inst-complete-payload"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("start status = %d body=%s", resp.Code, resp.Body.String())
	}
	completeResp := doJSONAsUser(t, handlers["POST /instances/:id/nodes/:node_id/complete"], map[string]string{"id": "inst-complete-payload", "node_id": "start"}, map[string]any{
		"assignee_id": "3003",
		"cc_user_ids": []string{"4004", "5005"},
		"due_at":      dueAt,
	}, 2002)
	if completeResp.Code != http.StatusOK {
		t.Fatalf("complete status = %d body=%s", completeResp.Code, completeResp.Body.String())
	}

	todoResp := doGetAsUser(t, handlers["GET /my/todos"], "/?page=1&size=20", 3003)
	if todoResp.Code != http.StatusOK {
		t.Fatalf("todo status = %d body=%s", todoResp.Code, todoResp.Body.String())
	}
	var todoPage struct {
		Items []struct {
			NodeID     string   `json:"node_id"`
			AssigneeID string   `json:"assignee_id"`
			CCUserIDs  []string `json:"cc_user_ids"`
			DueAt      *string  `json:"due_at"`
			Overdue    bool     `json:"overdue"`
		} `json:"items"`
		Total int `json:"total"`
	}
	decodeResponseData(t, todoResp, &todoPage)
	if todoPage.Total != 1 || todoPage.Items[0].NodeID != "end" || todoPage.Items[0].AssigneeID != "3003" || len(todoPage.Items[0].CCUserIDs) != 2 || todoPage.Items[0].DueAt == nil || !todoPage.Items[0].Overdue {
		t.Fatalf("todo page = %#v", todoPage)
	}
}

func TestRejectNodeAPIReactivatesTarget(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	handlers := registerTestRoutes(t, NewRegistrar(service))
	payload := map[string]any{
		"id":            "approval-reject-api",
		"name":          "Approval Reject API",
		"start_node_id": "submit",
		"nodes": []map[string]any{
			{"id": "submit", "name": "Submit", "type": "task", "position": map[string]any{"x": 0, "y": 0}},
			{"id": "review", "name": "Review", "type": "task", "position": map[string]any{"x": 120, "y": 0}},
			{"id": "approve", "name": "Approve", "type": "task", "reject_to": []string{"review"}, "position": map[string]any{"x": 240, "y": 0}},
		},
		"edges": []map[string]any{
			{"id": "submit-review", "source": "submit", "target": "review"},
			{"id": "review-approve", "source": "review", "target": "approve"},
		},
	}

	if resp := doJSON(t, handlers["POST /definitions"], nil, payload); resp.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /definitions/:id/publish"], map[string]string{"id": "approval-reject-api"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("publish status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSONAsUser(t, handlers["POST /instances"], nil, map[string]any{
		"id":            "inst-reject-api",
		"definition_id": "approval-reject-api",
		"assignee_id":   "2002",
	}, 2002); resp.Code != http.StatusCreated {
		t.Fatalf("instance status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSONAsUser(t, handlers["POST /instances/:id/start"], map[string]string{"id": "inst-reject-api"}, nil, 2002); resp.Code != http.StatusOK {
		t.Fatalf("start status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSONAsUser(t, handlers["POST /instances/:id/nodes/:node_id/complete"], map[string]string{"id": "inst-reject-api", "node_id": "submit"}, map[string]any{"assignee_id": "2002"}, 2002); resp.Code != http.StatusOK {
		t.Fatalf("complete submit status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSONAsUser(t, handlers["POST /instances/:id/nodes/:node_id/complete"], map[string]string{"id": "inst-reject-api", "node_id": "review"}, map[string]any{"assignee_id": "2002"}, 2002); resp.Code != http.StatusOK {
		t.Fatalf("complete review status = %d body=%s", resp.Code, resp.Body.String())
	}

	rejectResp := doJSONAsUser(t, handlers["POST /instances/:id/nodes/:node_id/reject"], map[string]string{"id": "inst-reject-api", "node_id": "approve"}, map[string]any{
		"target_node_id": "review",
		"reason":         "请补充材料",
	}, 2002)
	if rejectResp.Code != http.StatusOK {
		t.Fatalf("reject status = %d body=%s", rejectResp.Code, rejectResp.Body.String())
	}
	var state workflowtypes.InstanceState
	decodeResponseData(t, rejectResp, &state)
	if len(state.ActiveNodeIDs) != 1 || state.ActiveNodeIDs[0] != "review" {
		t.Fatalf("active nodes after reject = %#v, want review", state.ActiveNodeIDs)
	}
	lastHistory := state.History[len(state.History)-1]
	if lastHistory.Action != "reject" || lastHistory.NodeID != "review" || lastHistory.FromNodeID != "approve" {
		t.Fatalf("last history = %#v, want reject approve -> review", lastHistory)
	}
}

func TestUpdateDefinitionRejectsEmptyPathID(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	handlers := registerTestRoutes(t, NewRegistrar(service))
	payload := map[string]any{
		"name":          "Approval",
		"start_node_id": "start",
		"nodes": []map[string]any{
			{"id": "start", "name": "Start", "next": []string{"end"}, "position": map[string]any{"x": 0, "y": 0}},
			{"id": "end", "name": "End", "next": []string{}, "position": map[string]any{"x": 120, "y": 0}},
		},
		"edges": []map[string]any{{"id": "e1", "source": "start", "target": "end"}},
	}

	updateResp := doJSON(t, handlers["PUT /definitions/:id"], map[string]string{"id": " "}, payload)
	if updateResp.Code != http.StatusBadRequest {
		t.Fatalf("update status = %d, want 400 body=%s", updateResp.Code, updateResp.Body.String())
	}
	items, err := service.ListDefinitions(workflowTestScopeContext(t))
	if err != nil {
		t.Fatalf("list definitions: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("definitions after rejected update = %d, want 0", len(items))
	}
}

func TestRegistrarRejectsEmptyPathParamsAtBoundary(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	handlers := registerTestRoutes(t, NewRegistrar(service))

	tests := []struct {
		name    string
		route   string
		params  map[string]string
		payload any
	}{
		{name: "get definition", route: "GET /definitions/:id", params: map[string]string{"id": " "}},
		{name: "delete definition", route: "DELETE /definitions/:id", params: map[string]string{"id": " "}},
		{name: "publish definition", route: "POST /definitions/:id/publish", params: map[string]string{"id": " "}},
		{name: "archive definition", route: "POST /definitions/:id/archive", params: map[string]string{"id": " "}},
		{name: "get instance", route: "GET /instances/:id", params: map[string]string{"id": " "}},
		{name: "start instance", route: "POST /instances/:id/start", params: map[string]string{"id": " "}},
		{name: "advance instance id", route: "POST /instances/:id/nodes/:node_id/advance", params: map[string]string{"id": " ", "node_id": "start"}},
		{name: "advance node id", route: "POST /instances/:id/nodes/:node_id/advance", params: map[string]string{"id": "inst-1", "node_id": " "}},
		{name: "advance selected node id", route: "POST /instances/:id/nodes/:node_id/advance", params: map[string]string{"id": "inst-1", "node_id": " "}, payload: map[string]any{"next_node_id": "end"}},
		{name: "reject node id", route: "POST /instances/:id/nodes/:node_id/reject", params: map[string]string{"id": "inst-1", "node_id": " "}, payload: map[string]any{"target_node_id": "start"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := doJSON(t, handlers[tt.route], tt.params, tt.payload)
			if resp.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 body=%s", resp.Code, resp.Body.String())
			}
		})
	}
}

func TestRegistrarRejectsMissingActiveScope(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	handlers := registerTestRoutes(t, NewRegistrar(service))

	resp := doRawWithoutScope(t, handlers["GET /definitions"], nil, "")
	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 body=%s", resp.Code, resp.Body.String())
	}
}

func TestRegistrarRejectsUnsupportedActiveScopeKind(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	handlers := registerTestRoutes(t, NewRegistrar(service))

	resp := doRawWithScope(t, handlers["GET /definitions"], nil, "", 1, "department")
	if resp.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 body=%s", resp.Code, resp.Body.String())
	}
}

func TestAdvanceNodeRejectsMalformedJSONWithoutAdvancing(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	handlers := registerTestRoutes(t, NewRegistrar(service))
	payload := map[string]any{
		"id":            "approval",
		"name":          "Approval",
		"start_node_id": "start",
		"nodes": []map[string]any{
			{"id": "start", "name": "Start", "next": []string{"end"}, "position": map[string]any{"x": 0, "y": 0}},
			{"id": "end", "name": "End", "next": []string{}, "position": map[string]any{"x": 120, "y": 0}},
		},
		"edges": []map[string]any{{"id": "e1", "source": "start", "target": "end"}},
	}

	if resp := doJSON(t, handlers["POST /definitions"], nil, payload); resp.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /definitions/:id/publish"], map[string]string{"id": "approval"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("publish status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances"], nil, map[string]any{"id": "inst-1", "definition_id": "approval"}); resp.Code != http.StatusCreated {
		t.Fatalf("instance status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances/:id/start"], map[string]string{"id": "inst-1"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("start status = %d body=%s", resp.Code, resp.Body.String())
	}

	advanceResp := doRaw(t, handlers["POST /instances/:id/nodes/:node_id/advance"], map[string]string{
		"id":      "inst-1",
		"node_id": "start",
	}, `{"next_node_ids":`)
	if advanceResp.Code != http.StatusBadRequest {
		t.Fatalf("advance status = %d, want 400 body=%s", advanceResp.Code, advanceResp.Body.String())
	}

	state, err := service.GetInstance(workflowTestScopeContext(t), "inst-1")
	if err != nil {
		t.Fatalf("get instance: %v", err)
	}
	if len(state.ActiveNodeIDs) != 1 || state.ActiveNodeIDs[0] != "start" {
		t.Fatalf("active nodes after malformed advance = %#v", state.ActiveNodeIDs)
	}
}

func TestAdvanceNodeTreatsEmptyObjectBodyAsDefaultAdvance(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	handlers := registerTestRoutes(t, NewRegistrar(service))
	payload := map[string]any{
		"id":            "approval-empty-body",
		"name":          "Approval Empty Body",
		"start_node_id": "start",
		"nodes": []map[string]any{
			{"id": "start", "name": "Start", "next": []string{"end"}, "position": map[string]any{"x": 0, "y": 0}},
			{"id": "end", "name": "End", "next": []string{}, "position": map[string]any{"x": 120, "y": 0}},
		},
		"edges": []map[string]any{{"id": "e1", "source": "start", "target": "end"}},
	}

	if resp := doJSON(t, handlers["POST /definitions"], nil, payload); resp.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /definitions/:id/publish"], map[string]string{"id": "approval-empty-body"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("publish status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances"], nil, map[string]any{"id": "inst-empty-body", "definition_id": "approval-empty-body"}); resp.Code != http.StatusCreated {
		t.Fatalf("instance status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances/:id/start"], map[string]string{"id": "inst-empty-body"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("start status = %d body=%s", resp.Code, resp.Body.String())
	}

	advanceResp := doJSON(t, handlers["POST /instances/:id/nodes/:node_id/advance"], map[string]string{
		"id":      "inst-empty-body",
		"node_id": "start",
	}, map[string]any{})
	if advanceResp.Code != http.StatusOK {
		t.Fatalf("advance status = %d, want 200 body=%s", advanceResp.Code, advanceResp.Body.String())
	}

	state, err := service.GetInstance(workflowTestScopeContext(t), "inst-empty-body")
	if err != nil {
		t.Fatalf("get instance: %v", err)
	}
	if len(state.ActiveNodeIDs) != 1 || state.ActiveNodeIDs[0] != "end" {
		t.Fatalf("active nodes after empty object advance = %#v", state.ActiveNodeIDs)
	}
}

func TestAdvanceNodeAcceptsSingleSelectedNextNodeID(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	handlers := registerTestRoutes(t, NewRegistrar(service))
	payload := map[string]any{
		"id":            "approval-single-choice",
		"name":          "Approval Single Choice",
		"start_node_id": "review",
		"nodes": []map[string]any{
			{"id": "review", "name": "Review", "type": "branch", "next": []string{"approved", "rejected"}, "position": map[string]any{"x": 0, "y": 0}},
			{"id": "approved", "name": "Approved", "next": []string{}, "position": map[string]any{"x": 120, "y": -40}},
			{"id": "rejected", "name": "Rejected", "next": []string{}, "position": map[string]any{"x": 120, "y": 40}},
		},
		"edges": []map[string]any{
			{"id": "approved-edge", "source": "review", "target": "approved"},
			{"id": "rejected-edge", "source": "review", "target": "rejected"},
		},
	}

	if resp := doJSON(t, handlers["POST /definitions"], nil, payload); resp.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /definitions/:id/publish"], map[string]string{"id": "approval-single-choice"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("publish status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances"], nil, map[string]any{"id": "choice-single-1", "definition_id": "approval-single-choice"}); resp.Code != http.StatusCreated {
		t.Fatalf("instance status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances/:id/start"], map[string]string{"id": "choice-single-1"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("start status = %d body=%s", resp.Code, resp.Body.String())
	}

	advanceResp := doJSON(t, handlers["POST /instances/:id/nodes/:node_id/advance"], map[string]string{
		"id":      "choice-single-1",
		"node_id": "review",
	}, map[string]any{"next_node_id": "approved"})
	if advanceResp.Code != http.StatusOK {
		t.Fatalf("advance status = %d, want 200 body=%s", advanceResp.Code, advanceResp.Body.String())
	}

	state, err := service.GetInstance(workflowTestScopeContext(t), "choice-single-1")
	if err != nil {
		t.Fatalf("get instance: %v", err)
	}
	if len(state.ActiveNodeIDs) != 1 || state.ActiveNodeIDs[0] != "approved" {
		t.Fatalf("active nodes after selected advance = %#v", state.ActiveNodeIDs)
	}
	if got := state.CompletedNodeIDs; len(got) != 1 || got[0] != "review" {
		t.Fatalf("completed nodes after selected advance = %#v", got)
	}
}

func TestAdvanceNodeRejectsAmbiguousSelectedNextNodePayload(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	handlers := registerTestRoutes(t, NewRegistrar(service))

	advanceResp := doJSON(t, handlers["POST /instances/:id/nodes/:node_id/advance"], map[string]string{
		"id":      "choice-ambiguous",
		"node_id": "review",
	}, map[string]any{"next_node_id": "approved", "next_node_ids": []string{"rejected"}})
	if advanceResp.Code != http.StatusBadRequest {
		t.Fatalf("advance status = %d, want 400 body=%s", advanceResp.Code, advanceResp.Body.String())
	}
}

func TestAdvanceNodeAllowsMatchingDualWriteSelectedNextNodePayload(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	handlers := registerTestRoutes(t, NewRegistrar(service))
	payload := map[string]any{
		"id":            "approval-choice-dual",
		"name":          "Approval Choice Dual",
		"start_node_id": "review",
		"nodes": []map[string]any{
			{"id": "review", "name": "Review", "type": "branch", "next": []string{"approved", "rejected"}, "position": map[string]any{"x": 0, "y": 0}},
			{"id": "approved", "name": "Approved", "next": []string{}, "position": map[string]any{"x": 120, "y": -40}},
			{"id": "rejected", "name": "Rejected", "next": []string{}, "position": map[string]any{"x": 120, "y": 40}},
		},
		"edges": []map[string]any{
			{"id": "approved-edge", "source": "review", "target": "approved"},
			{"id": "rejected-edge", "source": "review", "target": "rejected"},
		},
	}

	if resp := doJSON(t, handlers["POST /definitions"], nil, payload); resp.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /definitions/:id/publish"], map[string]string{"id": "approval-choice-dual"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("publish status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances"], nil, map[string]any{"id": "choice-dual-1", "definition_id": "approval-choice-dual"}); resp.Code != http.StatusCreated {
		t.Fatalf("instance status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances/:id/start"], map[string]string{"id": "choice-dual-1"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("start status = %d body=%s", resp.Code, resp.Body.String())
	}

	advanceResp := doJSON(t, handlers["POST /instances/:id/nodes/:node_id/advance"], map[string]string{
		"id":      "choice-dual-1",
		"node_id": "review",
	}, map[string]any{"next_node_id": "approved", "next_node_ids": []string{"approved"}})
	if advanceResp.Code != http.StatusOK {
		t.Fatalf("advance status = %d, want 200 body=%s", advanceResp.Code, advanceResp.Body.String())
	}
}

func TestAdvanceNodeRejectsMultipleSelectedNextNodes(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	handlers := registerTestRoutes(t, NewRegistrar(service))
	payload := map[string]any{
		"id":            "approval-choice",
		"name":          "Approval Choice",
		"start_node_id": "review",
		"nodes": []map[string]any{
			{"id": "review", "name": "Review", "type": "branch", "next": []string{"approved", "rejected"}, "position": map[string]any{"x": 0, "y": 0}},
			{"id": "approved", "name": "Approved", "next": []string{}, "position": map[string]any{"x": 120, "y": -40}},
			{"id": "rejected", "name": "Rejected", "next": []string{}, "position": map[string]any{"x": 120, "y": 40}},
		},
		"edges": []map[string]any{
			{"id": "approved-edge", "source": "review", "target": "approved"},
			{"id": "rejected-edge", "source": "review", "target": "rejected"},
		},
	}

	if resp := doJSON(t, handlers["POST /definitions"], nil, payload); resp.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /definitions/:id/publish"], map[string]string{"id": "approval-choice"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("publish status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances"], nil, map[string]any{"id": "choice-1", "definition_id": "approval-choice"}); resp.Code != http.StatusCreated {
		t.Fatalf("instance status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances/:id/start"], map[string]string{"id": "choice-1"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("start status = %d body=%s", resp.Code, resp.Body.String())
	}

	advanceResp := doJSON(t, handlers["POST /instances/:id/nodes/:node_id/advance"], map[string]string{
		"id":      "choice-1",
		"node_id": "review",
	}, map[string]any{"next_node_ids": []string{"approved", "rejected"}})
	if advanceResp.Code != http.StatusBadRequest {
		t.Fatalf("advance status = %d, want 400 body=%s", advanceResp.Code, advanceResp.Body.String())
	}

	state, err := service.GetInstance(workflowTestScopeContext(t), "choice-1")
	if err != nil {
		t.Fatalf("get instance: %v", err)
	}
	if len(state.ActiveNodeIDs) != 1 || state.ActiveNodeIDs[0] != "review" {
		t.Fatalf("active nodes after rejected advance = %#v", state.ActiveNodeIDs)
	}
	if len(state.CompletedNodeIDs) != 0 {
		t.Fatalf("completed nodes after rejected advance = %#v", state.CompletedNodeIDs)
	}
}

func TestAdvanceNodeRejectsExplicitEmptySelectedNextNodes(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	handlers := registerTestRoutes(t, NewRegistrar(service))
	payload := map[string]any{
		"id":            "approval-empty-choice",
		"name":          "Approval Empty Choice",
		"start_node_id": "review",
		"nodes": []map[string]any{
			{"id": "review", "name": "Review", "type": "branch", "next": []string{"approved", "rejected"}, "position": map[string]any{"x": 0, "y": 0}},
			{"id": "approved", "name": "Approved", "next": []string{}, "position": map[string]any{"x": 120, "y": -40}},
			{"id": "rejected", "name": "Rejected", "next": []string{}, "position": map[string]any{"x": 120, "y": 40}},
		},
		"edges": []map[string]any{
			{"id": "approved-edge", "source": "review", "target": "approved"},
			{"id": "rejected-edge", "source": "review", "target": "rejected"},
		},
	}

	if resp := doJSON(t, handlers["POST /definitions"], nil, payload); resp.Code != http.StatusCreated {
		t.Fatalf("create status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /definitions/:id/publish"], map[string]string{"id": "approval-empty-choice"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("publish status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances"], nil, map[string]any{"id": "choice-empty-1", "definition_id": "approval-empty-choice"}); resp.Code != http.StatusCreated {
		t.Fatalf("instance status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /instances/:id/start"], map[string]string{"id": "choice-empty-1"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("start status = %d body=%s", resp.Code, resp.Body.String())
	}

	advanceResp := doJSON(t, handlers["POST /instances/:id/nodes/:node_id/advance"], map[string]string{
		"id":      "choice-empty-1",
		"node_id": "review",
	}, map[string]any{"next_node_ids": []string{}})
	if advanceResp.Code != http.StatusBadRequest {
		t.Fatalf("advance status = %d, want 400 body=%s", advanceResp.Code, advanceResp.Body.String())
	}

	state, err := service.GetInstance(workflowTestScopeContext(t), "choice-empty-1")
	if err != nil {
		t.Fatalf("get instance: %v", err)
	}
	if len(state.ActiveNodeIDs) != 1 || state.ActiveNodeIDs[0] != "review" {
		t.Fatalf("active nodes after rejected empty selection = %#v", state.ActiveNodeIDs)
	}
	if len(state.CompletedNodeIDs) != 0 {
		t.Fatalf("completed nodes after rejected empty selection = %#v", state.CompletedNodeIDs)
	}
}

func TestRegistrarAppliesPermissionMiddlewareGroups(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	group := &recordingGroup{handlers: map[string]httpx.Handler{}, useByPrefix: map[string]int{}}
	if err := NewRegistrar(service).RegisterRoutes(group); err != nil {
		t.Fatalf("RegisterRoutes failed: %v", err)
	}
	if group.useByPrefix["/definitions"] != 2 {
		t.Fatalf("definition middleware groups = %d, want 2", group.useByPrefix["/definitions"])
	}
	if group.useByPrefix["/instances"] != 2 {
		t.Fatalf("instance middleware groups = %d, want 2", group.useByPrefix["/instances"])
	}
}

func TestRegistrarPermissionMiddlewareBehavior(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	group := &middlewareGroup{
		handlers:    map[string]httpx.Handler{},
		middlewares: []httpx.Middleware{workflowTestAuthMiddleware()},
	}
	if err := NewRegistrar(service).RegisterRoutes(group); err != nil {
		t.Fatalf("RegisterRoutes failed: %v", err)
	}

	readHandler := group.handlers["GET /definitions"]
	if readHandler == nil {
		t.Fatal("expected GET /definitions handler")
	}

	t.Run("rejects unauthenticated request", func(t *testing.T) {
		rec := doRawWithoutScope(t, readHandler, nil, "")
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401 body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("rejects authenticated request without permission", func(t *testing.T) {
		token := generateWorkflowToken(t, nil)
		rec := doRawWithToken(t, readHandler, nil, "", token)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want 403 body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("allows authenticated request with permission", func(t *testing.T) {
		token := generateWorkflowToken(t, []string{workflowpermissions.WorkflowReadCode})
		rec := doRawWithToken(t, readHandler, nil, "", token)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
		}
		assertMessageCode(t, rec, "ok")
	})
}

// Regression（R3e-F01）：handler.go 曾声明无任何调用者的 paginateInstanceSummaries
// （实例分页实际由 service.ListInstances 承担），该死代码同时是 gochen/app/query 导入的唯一使用者。
// 本用例固定存活分页路径 GET /instances 的真实语义（跨页不重复、total 稳定），
// 防止把已删除的手写分页辅助重新接回 handler 时改变行为。
func TestRegistrarListInstancesPaginationServesPagesAfterDeadHelperRemoval(t *testing.T) {
	service := workflowservice.NewService(workflowservice.NewMemoryStore(), nil, nil)
	handlers := registerTestRoutes(t, NewRegistrar(service))
	payload := map[string]any{
		"id":            "pagination-live",
		"name":          "Pagination Live",
		"start_node_id": "start",
		"nodes": []map[string]any{
			{"id": "start", "name": "Start", "next": []string{"end"}, "position": map[string]any{"x": 0, "y": 0}},
			{"id": "end", "name": "End", "next": []string{}, "position": map[string]any{"x": 120, "y": 0}},
		},
		"edges": []map[string]any{{"id": "pagination-edge", "source": "start", "target": "end"}},
	}
	if resp := doJSON(t, handlers["POST /definitions"], nil, payload); resp.Code != http.StatusCreated {
		t.Fatalf("create definition status = %d body=%s", resp.Code, resp.Body.String())
	}
	if resp := doJSON(t, handlers["POST /definitions/:id/publish"], map[string]string{"id": "pagination-live"}, nil); resp.Code != http.StatusOK {
		t.Fatalf("publish definition status = %d body=%s", resp.Code, resp.Body.String())
	}
	for _, id := range []string{"pagination-live-1", "pagination-live-2", "pagination-live-3"} {
		if resp := doJSON(t, handlers["POST /instances"], nil, map[string]any{"id": id, "definition_id": "pagination-live"}); resp.Code != http.StatusCreated {
			t.Fatalf("create instance %s status = %d body=%s", id, resp.Code, resp.Body.String())
		}
	}

	first := doGetAsUser(t, handlers["GET /instances"], "/?page=1&size=2", 1001)
	if first.Code != http.StatusOK {
		t.Fatalf("page 1 status = %d body=%s", first.Code, first.Body.String())
	}
	var firstPage workflowtypes.InstancePage
	decodeResponseData(t, first, &firstPage)
	if firstPage.Total != 3 || firstPage.Page != 1 || firstPage.Size != 2 || len(firstPage.Items) != 2 {
		t.Fatalf("page 1 = %#v", firstPage)
	}

	second := doGetAsUser(t, handlers["GET /instances"], "/?page=2&size=2", 1001)
	if second.Code != http.StatusOK {
		t.Fatalf("page 2 status = %d body=%s", second.Code, second.Body.String())
	}
	var secondPage workflowtypes.InstancePage
	decodeResponseData(t, second, &secondPage)
	if secondPage.Total != 3 || secondPage.Page != 2 || secondPage.Size != 2 || len(secondPage.Items) != 1 {
		t.Fatalf("page 2 = %#v", secondPage)
	}

	seen := make(map[string]bool, firstPage.Total)
	for _, item := range append(append([]workflowtypes.InstanceSummary(nil), firstPage.Items...), secondPage.Items...) {
		if seen[item.ID] {
			t.Fatalf("instance %q returned on more than one page", item.ID)
		}
		seen[item.ID] = true
	}
	if len(seen) != 3 {
		t.Fatalf("distinct instances across pages = %d, want 3", len(seen))
	}
}

func registerTestRoutes(t *testing.T, registrar *Registrar) map[string]httpx.Handler {
	t.Helper()
	group := &recordingGroup{handlers: map[string]httpx.Handler{}, useByPrefix: map[string]int{}}
	if err := registrar.RegisterRoutes(group); err != nil {
		t.Fatalf("RegisterRoutes failed: %v", err)
	}
	return group.handlers
}

func doJSON(t *testing.T, handler httpx.Handler, params map[string]string, payload any) *httptest.ResponseRecorder {
	t.Helper()
	var body bytes.Buffer
	if payload != nil {
		if err := json.NewEncoder(&body).Encode(payload); err != nil {
			t.Fatalf("encode payload: %v", err)
		}
	}
	return doRaw(t, handler, params, body.String())
}

func doJSONAsUser(t *testing.T, handler httpx.Handler, params map[string]string, payload any, userID int64) *httptest.ResponseRecorder {
	t.Helper()
	var body bytes.Buffer
	if payload != nil {
		if err := json.NewEncoder(&body).Encode(payload); err != nil {
			t.Fatalf("encode payload: %v", err)
		}
	}
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body.String()))
	return doRequestAsUser(t, handler, params, req, userID)
}

func doRaw(t *testing.T, handler httpx.Handler, params map[string]string, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	return doRequest(t, handler, params, req, true)
}

func doGet(t *testing.T, handler httpx.Handler, target string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	return doRequest(t, handler, nil, req, true)
}

func doGetAsUser(t *testing.T, handler httpx.Handler, target string, userID int64) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	return doRequestAsUser(t, handler, nil, req, userID)
}

func doRawWithoutScope(t *testing.T, handler httpx.Handler, params map[string]string, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	return doRequest(t, handler, params, req, false)
}

func doRawWithScope(t *testing.T, handler httpx.Handler, params map[string]string, body string, scopeID int64, scopeKind string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	return doRequestWithScope(t, handler, params, req, scopeID, scopeKind)
}

func doRawWithToken(t *testing.T, handler httpx.Handler, params map[string]string, body, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	return doRequest(t, handler, params, req, false)
}

func doRequest(t *testing.T, handler httpx.Handler, params map[string]string, req *http.Request, bindScope bool) *httptest.ResponseRecorder {
	if !bindScope {
		return doRequestWithScope(t, handler, params, req, 0, "")
	}
	return doRequestWithScope(t, handler, params, req, 1, "tenant")
}

func doRequestWithScope(t *testing.T, handler httpx.Handler, params map[string]string, req *http.Request, scopeID int64, scopeKind string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	ctx, err := nethttp.NewBaseContext(rec, req)
	if err != nil {
		t.Fatalf("new context: %v", err)
	}
	if scopeID > 0 || strings.TrimSpace(scopeKind) != "" {
		reqCtx, principalErr := auth.WithPrincipal(ctx.RequestContext(), auth.Principal{SubjectID: "1001", ActiveScopeID: scopeID, Permissions: []string{workflowpermissions.WorkflowReadCode, workflowpermissions.WorkflowDesignCode, workflowpermissions.WorkflowRunCode}})
		if principalErr != nil {
			t.Fatalf("with principal: %v", principalErr)
		}
		reqCtx = auth.BindActiveScopeContext(reqCtx, scopeID, scopeKind)
		ctx.SetContext(ctx.RequestContext().WithContext(reqCtx))
	}
	for key, value := range params {
		ctx.SetParam(key, value)
	}
	if err := handler(ctx); err != nil {
		_ = nethttp.WriteErrorResponse(ctx, err)
	}
	return rec
}

func doRequestAsUser(t *testing.T, handler httpx.Handler, params map[string]string, req *http.Request, userID int64) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	ctx, err := nethttp.NewBaseContext(rec, req)
	if err != nil {
		t.Fatalf("new context: %v", err)
	}
	reqCtx, err := auth.WithPrincipal(ctx.RequestContext(), auth.Principal{SubjectID: strconv.FormatInt(userID, 10), ActiveScopeID: 1, Permissions: []string{workflowpermissions.WorkflowReadCode, workflowpermissions.WorkflowDesignCode, workflowpermissions.WorkflowRunCode}})
	if err != nil {
		t.Fatalf("with principal: %v", err)
	}
	reqCtx = auth.BindActiveScopeContext(reqCtx, 1, "tenant")
	ctx.SetContext(ctx.RequestContext().WithContext(reqCtx))
	for key, value := range params {
		ctx.SetParam(key, value)
	}
	if err := handler(ctx); err != nil {
		_ = nethttp.WriteErrorResponse(ctx, err)
	}
	return rec
}

func decodeResponseData(t *testing.T, rec *httptest.ResponseRecorder, target any) {
	t.Helper()
	var message httpx.ResponseMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &message); err != nil {
		t.Fatalf("decode response message: %v", err)
	}
	raw, err := json.Marshal(message.Data)
	if err != nil {
		t.Fatalf("encode response data: %v", err)
	}
	if err := json.Unmarshal(raw, target); err != nil {
		t.Fatalf("decode response data: %v", err)
	}
}

func generateWorkflowToken(t *testing.T, permissions []string) string {
	t.Helper()
	raw, err := json.Marshal(permissions)
	if err != nil {
		t.Fatalf("encode permissions: %v", err)
	}
	return string(raw)
}

func workflowTestScopeContext(t *testing.T) context.Context {
	t.Helper()
	ctx, err := auth.WithPrincipal(t.Context(), auth.Principal{IsSystem: true})
	if err != nil {
		t.Fatal(err)
	}
	return workflowservice.WithScope(ctx, workflowservice.Scope{Kind: workflowservice.ScopeKindTenant, ID: 1})
}

func workflowTestAuthMiddleware() httpx.Middleware {
	return func(ctx httpx.IContext, next func() error) error {
		header := strings.TrimSpace(ctx.Header("Authorization"))
		if !strings.HasPrefix(header, "Bearer ") {
			return next()
		}
		rawPerms := strings.TrimPrefix(header, "Bearer ")
		var perms []string
		if err := json.Unmarshal([]byte(rawPerms), &perms); err != nil {
			return err
		}
		baseCtx, err := auth.WithPrincipal(ctx.RequestContext(), auth.Principal{
			SubjectID:        "1",
			ActiveScopeID:    1,
			Permissions:      perms,
			PermissionScopes: []string{string(auth.PermissionScopePlatform)},
		})
		if err != nil {
			return err
		}
		baseCtx = auth.BindActiveScopeContext(baseCtx, 1, string(auth.PermissionScopePlatform))
		ctx.SetContext(ctx.RequestContext().WithContext(baseCtx))
		return next()
	}
}

func assertMessageCode(t *testing.T, rec *httptest.ResponseRecorder, code string) {
	t.Helper()
	var message httpx.ResponseMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &message); err != nil {
		t.Fatalf("decode response message: %v", err)
	}
	if message.Code != code {
		t.Fatalf("response code = %q, want %q body=%s", message.Code, code, rec.Body.String())
	}
}

type recordingGroup struct {
	prefix      string
	handlers    map[string]httpx.Handler
	useByPrefix map[string]int
}

func (g *recordingGroup) GET(path string, handler httpx.Handler) httpx.IRouteGroup {
	g.handlers["GET "+g.prefix+path] = handler
	return g
}

func (g *recordingGroup) POST(path string, handler httpx.Handler) httpx.IRouteGroup {
	g.handlers["POST "+g.prefix+path] = handler
	return g
}

func (g *recordingGroup) PUT(path string, handler httpx.Handler) httpx.IRouteGroup {
	g.handlers["PUT "+g.prefix+path] = handler
	return g
}

func (g *recordingGroup) DELETE(path string, handler httpx.Handler) httpx.IRouteGroup {
	g.handlers["DELETE "+g.prefix+path] = handler
	return g
}

func (g *recordingGroup) PATCH(path string, handler httpx.Handler) httpx.IRouteGroup {
	g.handlers["PATCH "+g.prefix+path] = handler
	return g
}

func (g *recordingGroup) HEAD(path string, handler httpx.Handler) httpx.IRouteGroup {
	g.handlers["HEAD "+g.prefix+path] = handler
	return g
}

func (g *recordingGroup) OPTIONS(path string, handler httpx.Handler) httpx.IRouteGroup {
	g.handlers["OPTIONS "+g.prefix+path] = handler
	return g
}

func (g *recordingGroup) Group(prefix string) httpx.IRouteGroup {
	return &recordingGroup{prefix: g.prefix + prefix, handlers: g.handlers, useByPrefix: g.useByPrefix}
}

func (g *recordingGroup) Use(middlewares ...httpx.Middleware) httpx.IRouteGroup {
	if g.useByPrefix != nil && len(middlewares) > 0 {
		g.useByPrefix[g.prefix]++
	}
	return g
}

type middlewareGroup struct {
	prefix      string
	handlers    map[string]httpx.Handler
	middlewares []httpx.Middleware
}

func (g *middlewareGroup) GET(path string, handler httpx.Handler) httpx.IRouteGroup {
	g.handlers["GET "+g.prefix+path] = wrapTestHandler(g.middlewares, handler)
	return g
}

func (g *middlewareGroup) POST(path string, handler httpx.Handler) httpx.IRouteGroup {
	g.handlers["POST "+g.prefix+path] = wrapTestHandler(g.middlewares, handler)
	return g
}

func (g *middlewareGroup) PUT(path string, handler httpx.Handler) httpx.IRouteGroup {
	g.handlers["PUT "+g.prefix+path] = wrapTestHandler(g.middlewares, handler)
	return g
}

func (g *middlewareGroup) DELETE(path string, handler httpx.Handler) httpx.IRouteGroup {
	g.handlers["DELETE "+g.prefix+path] = wrapTestHandler(g.middlewares, handler)
	return g
}

func (g *middlewareGroup) PATCH(path string, handler httpx.Handler) httpx.IRouteGroup {
	g.handlers["PATCH "+g.prefix+path] = wrapTestHandler(g.middlewares, handler)
	return g
}

func (g *middlewareGroup) HEAD(path string, handler httpx.Handler) httpx.IRouteGroup {
	g.handlers["HEAD "+g.prefix+path] = wrapTestHandler(g.middlewares, handler)
	return g
}

func (g *middlewareGroup) OPTIONS(path string, handler httpx.Handler) httpx.IRouteGroup {
	g.handlers["OPTIONS "+g.prefix+path] = wrapTestHandler(g.middlewares, handler)
	return g
}

func (g *middlewareGroup) Group(prefix string) httpx.IRouteGroup {
	return &middlewareGroup{
		prefix:      g.prefix + prefix,
		handlers:    g.handlers,
		middlewares: append([]httpx.Middleware(nil), g.middlewares...),
	}
}

func (g *middlewareGroup) Use(middlewares ...httpx.Middleware) httpx.IRouteGroup {
	g.middlewares = append(g.middlewares, middlewares...)
	return g
}

func wrapTestHandler(middlewares []httpx.Middleware, handler httpx.Handler) httpx.Handler {
	return func(ctx httpx.IContext) error {
		var run func(int) error
		run = func(index int) error {
			if index >= len(middlewares) {
				return handler(ctx)
			}
			return middlewares[index](ctx, func() error {
				return run(index + 1)
			})
		}
		return run(0)
	}
}
