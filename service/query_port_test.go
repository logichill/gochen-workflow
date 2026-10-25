package workflowservice_test

import (
	"context"
	"testing"

	workflowservice "gochen-workflow/service"
	workflowtypes "gochen-workflow/types"
)

type externalQueryStore struct {
	workflowservice.Store
	query workflowtypes.DefinitionQuery
}

func (s *externalQueryStore) QueryDesigns(_ context.Context, query workflowtypes.DefinitionQuery) (*workflowtypes.DefinitionListResponse, error) {
	s.query = query
	return &workflowtypes.DefinitionListResponse{Total: 37, Page: query.Page, Size: query.Size}, nil
}

func TestExternalStoreCanProvideDefinitionQuery(t *testing.T) {
	store := &externalQueryStore{}
	service := workflowservice.NewService(store, nil, nil)
	ctx := workflowservice.WithScope(context.Background(), workflowservice.Scope{Kind: workflowservice.ScopeKindTenant, ID: 1})
	page, err := service.BuildDefinitionListResponseWithQuery(ctx, workflowtypes.DefinitionQuery{Keyword: " FLOW ", Page: 2, Size: 5})
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 37 || store.query.Keyword != "flow" || store.query.Page != 2 || store.query.Size != 5 {
		t.Fatalf("external query adapter not used: page=%+v query=%+v", page, store.query)
	}
}
