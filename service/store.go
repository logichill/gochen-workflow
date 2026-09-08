package workflowservice

import (
	"context"
	"database/sql"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"

	workflowtypes "gochen-workflow/types"

	"gochen/errors"
	runtime "gochen/process/workflow"
)

// Store 定义 workflow 服务需要的设计态与运行态持久化能力。
// SaveDefinition 的显式版本必须高于当前最大版本，0 表示自动分配下一版本；
// SaveDesign 则按设计版本逐次递增。已保存的版本不能覆盖。
type Store interface {
	runtime.IStore
	runtime.IOptimisticStore
	ListDesigns(ctx context.Context) ([]*workflowtypes.DesignerDefinition, error)
	ListInstances(ctx context.Context) ([]*runtime.State, error)
	ListStartedInstances(ctx context.Context) ([]*runtime.State, error)
	GetDesign(ctx context.Context, id string) (*workflowtypes.DesignerDefinition, error)
	SaveDesign(ctx context.Context, design *workflowtypes.DesignerDefinition, runtimeDef *runtime.Definition) error
	DeleteDesign(ctx context.Context, id string) error
	CountInstancesByDefinition(ctx context.Context, id string) (int, error)
}

type definitionListQuery struct {
	page    int
	size    int
	offset  int
	keyword string
	status  workflowtypes.DefinitionStatus
}

type definitionQueryStore interface {
	queryDesigns(ctx context.Context, query definitionListQuery) (*workflowtypes.DefinitionListResponse, error)
}

// MemoryStore 是用于测试和本地开发的内存 workflow 存储。
type MemoryStore struct {
	mu              sync.RWMutex
	designs         map[scopedDefinitionKey]*workflowtypes.DesignerDefinition
	designVersions  map[scopedDefinitionKey]map[uint32]*workflowtypes.DesignerDefinition
	runtime         map[scopedDefinitionKey]*runtime.Definition
	runtimeVersions map[scopedDefinitionKey]map[uint32]*runtime.Definition
	latestVersions  map[scopedDefinitionKey]uint32
	states          map[scopedInstanceKey]*runtime.State
}

// NewMemoryStore 创建内存 workflow 存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		designs:         map[scopedDefinitionKey]*workflowtypes.DesignerDefinition{},
		designVersions:  map[scopedDefinitionKey]map[uint32]*workflowtypes.DesignerDefinition{},
		runtime:         map[scopedDefinitionKey]*runtime.Definition{},
		runtimeVersions: map[scopedDefinitionKey]map[uint32]*runtime.Definition{},
		latestVersions:  map[scopedDefinitionKey]uint32{},
		states:          map[scopedInstanceKey]*runtime.State{},
	}
}

type scopedDefinitionKey struct {
	scopeKind string
	scopeID   int64
	id        string
}

type scopedInstanceKey struct {
	scopeKind string
	scopeID   int64
	id        runtime.ID
}

func currentManagedScope(ctx context.Context) (Scope, error) {
	if ctx == nil {
		return Scope{}, errors.NewCode(errors.InvalidInput, "workflow context is nil")
	}
	scope := ScopeFromContext(ctx)
	if err := validateScope(scope); err != nil {
		return Scope{}, err
	}
	return scope, nil
}

func scopedDefinition(ctx context.Context, id string) (scopedDefinitionKey, error) {
	scope, err := currentManagedScope(ctx)
	if err != nil {
		return scopedDefinitionKey{}, err
	}
	return scopedDefinitionKey{scopeKind: scope.Kind, scopeID: scope.ID, id: id}, nil
}

func scopedInstance(ctx context.Context, id runtime.ID) (scopedInstanceKey, error) {
	scope, err := currentManagedScope(ctx)
	if err != nil {
		return scopedInstanceKey{}, err
	}
	return scopedInstanceKey{scopeKind: scope.Kind, scopeID: scope.ID, id: id}, nil
}

func (s *MemoryStore) ListDesigns(ctx context.Context) ([]*workflowtypes.DesignerDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	scope, err := currentManagedScope(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]*workflowtypes.DesignerDefinition, 0, len(s.designs))
	for key, design := range s.designs {
		if key.scopeKind != scope.Kind || key.scopeID != scope.ID {
			continue
		}
		items = append(items, cloneDesign(design))
	}
	return items, nil
}

func (s *MemoryStore) GetDesign(ctx context.Context, id string) (*workflowtypes.DesignerDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key, err := scopedDefinition(ctx, id)
	if err != nil {
		return nil, err
	}
	if design, ok := s.designs[key]; ok {
		return cloneDesign(design), nil
	}
	return nil, nil
}

func (s *MemoryStore) SaveDesign(ctx context.Context, design *workflowtypes.DesignerDefinition, runtimeDef *runtime.Definition) error {
	if design == nil || runtimeDef == nil {
		return errors.NewCode(errors.InvalidInput, "workflow design and runtime definition are required")
	}
	if design.ID != runtimeDef.ID {
		return errors.NewCode(errors.InvalidInput, "workflow design and runtime definition id mismatch").
			WithContext("definition_id", design.ID).
			WithContext("runtime_definition_id", runtimeDef.ID)
	}
	if runtimeDef.Version != 0 && runtimeDef.Version != uint32(design.Version) {
		return errors.NewCode(errors.InvalidInput, "workflow design and runtime version mismatch")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key, err := scopedDefinition(ctx, design.ID)
	if err != nil {
		return err
	}
	current, exists := s.designs[key]
	if design.Version <= 0 {
		return errors.NewCode(errors.InvalidInput, "workflow definition version is invalid").
			WithContext("definition_id", design.ID).
			WithContext("version", design.Version)
	}
	switch {
	case !exists && design.Version != 1:
		return errors.NewCode(errors.Conflict, "workflow definition does not exist").
			WithContext("definition_id", design.ID)
	case exists && design.Version != current.Version+1:
		return errors.NewCode(errors.Conflict, "workflow definition version mismatch").
			WithContext("definition_id", design.ID).
			WithContext("expected_version", current.Version+1).
			WithContext("actual_version", design.Version)
	}
	s.designs[key] = cloneDesign(design)
	runtimeKey, err := scopedDefinition(ctx, runtimeDef.ID)
	if err != nil {
		return err
	}
	ver := uint32(design.Version)
	if runtimeDef.Version > 0 {
		ver = runtimeDef.Version
	} else {
		runtimeDef.Version = ver
	}
	s.runtime[runtimeKey] = cloneRuntimeDefinition(runtimeDef)
	if s.runtimeVersions[runtimeKey] == nil {
		s.runtimeVersions[runtimeKey] = map[uint32]*runtime.Definition{}
	}
	s.runtimeVersions[runtimeKey][ver] = cloneRuntimeDefinition(runtimeDef)
	if s.designVersions[runtimeKey] == nil {
		s.designVersions[runtimeKey] = map[uint32]*workflowtypes.DesignerDefinition{}
	}
	s.designVersions[runtimeKey][ver] = cloneDesign(design)
	if ver > s.latestVersions[runtimeKey] {
		s.latestVersions[runtimeKey] = ver
	}
	return nil
}

func (s *MemoryStore) DeleteDesign(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key, err := scopedDefinition(ctx, id)
	if err != nil {
		return err
	}
	delete(s.designs, key)
	delete(s.designVersions, key)
	delete(s.runtime, key)
	delete(s.runtimeVersions, key)
	delete(s.latestVersions, key)
	return nil
}

func (s *MemoryStore) CountInstancesByDefinition(ctx context.Context, id string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	scope, err := currentManagedScope(ctx)
	if err != nil {
		return 0, err
	}
	count := 0
	for key, st := range s.states {
		if key.scopeKind != scope.Kind || key.scopeID != scope.ID {
			continue
		}
		if st.DefinitionID == id {
			count++
		}
	}
	return count, nil
}

func (s *MemoryStore) ListInstances(ctx context.Context) ([]*runtime.State, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	scope, err := currentManagedScope(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]*runtime.State, 0, len(s.states))
	for key, st := range s.states {
		if key.scopeKind != scope.Kind || key.scopeID != scope.ID {
			continue
		}
		items = append(items, cloneState(st))
	}
	return items, nil
}

func (s *MemoryStore) ListStartedInstances(ctx context.Context) ([]*runtime.State, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	scope, err := currentManagedScope(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]*runtime.State, 0, len(s.states))
	for key, st := range s.states {
		if key.scopeKind != scope.Kind || key.scopeID != scope.ID || st.StartedAt.IsZero() {
			continue
		}
		items = append(items, cloneState(st))
	}
	return items, nil
}

func (s *MemoryStore) GetDefinition(ctx context.Context, id string, version uint32) (*runtime.Definition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key, err := scopedDefinition(ctx, id)
	if err != nil {
		return nil, err
	}
	if version > 0 {
		if verMap, ok := s.runtimeVersions[key]; ok {
			if def, ok := verMap[version]; ok {
				return cloneRuntimeDefinition(def), nil
			}
		}
		if def, ok := s.runtime[key]; ok && def.Version == version {
			return cloneRuntimeDefinition(def), nil
		}
		return nil, nil
	}
	if verMap, ok := s.runtimeVersions[key]; ok {
		latest := s.latestVersions[key]
		if latest > 0 {
			if def, ok := verMap[latest]; ok {
				return cloneRuntimeDefinition(def), nil
			}
		}
	}
	if def, ok := s.runtime[key]; ok {
		return cloneRuntimeDefinition(def), nil
	}
	return nil, nil
}

func (s *MemoryStore) SaveDefinition(ctx context.Context, def *runtime.Definition) error {
	if def == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key, err := scopedDefinition(ctx, def.ID)
	if err != nil {
		return err
	}
	now := def.UpdatedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	design := designFromRuntimeDefinition(def, now)
	ver := def.Version
	if ver == 0 {
		ver = s.latestVersions[key] + 1
	}
	if ver == 0 || ver <= s.latestVersions[key] {
		return errors.NewCode(errors.Conflict, "workflow definition version must exceed latest version")
	}
	if existing := s.designs[key]; existing != nil {
		design.Description = existing.Description
		design.Version = existing.Version + 1
		design.Status = existing.Status
		design.Viewport = existing.Viewport
		design.CreatedAt = existing.CreatedAt
		if design.CreatedAt.IsZero() {
			design.CreatedAt = now
		}
		design.PublishedAt = nil
		if existing.PublishedAt != nil {
			publishedAt := *existing.PublishedAt
			design.PublishedAt = &publishedAt
		}
	}
	design.Version = int(ver)
	def.Version = ver
	s.designs[key] = cloneDesign(design)
	if s.designVersions[key] == nil {
		s.designVersions[key] = map[uint32]*workflowtypes.DesignerDefinition{}
	}
	s.designVersions[key][ver] = cloneDesign(design)
	s.runtime[key] = cloneRuntimeDefinition(def)
	if s.runtimeVersions[key] == nil {
		s.runtimeVersions[key] = map[uint32]*runtime.Definition{}
	}
	s.runtimeVersions[key][ver] = cloneRuntimeDefinition(def)
	if ver > s.latestVersions[key] {
		s.latestVersions[key] = ver
	}
	return nil
}

func (s *MemoryStore) DeleteDefinition(ctx context.Context, id string, version uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key, err := scopedDefinition(ctx, id)
	if err != nil {
		return err
	}
	if version == 0 {
		delete(s.runtime, key)
		delete(s.runtimeVersions, key)
		delete(s.latestVersions, key)
		delete(s.designs, key)
		delete(s.designVersions, key)
		return nil
	}
	if verMap, ok := s.runtimeVersions[key]; ok {
		delete(verMap, version)
		delete(s.designVersions[key], version)
		if len(verMap) == 0 {
			delete(s.runtimeVersions, key)
			delete(s.latestVersions, key)
			delete(s.runtime, key)
			delete(s.designs, key)
			delete(s.designVersions, key)
		} else if s.latestVersions[key] == version {
			var newMax uint32
			for v := range verMap {
				if v > newMax {
					newMax = v
				}
			}
			s.latestVersions[key] = newMax
			if def, ok := verMap[newMax]; ok {
				s.runtime[key] = cloneRuntimeDefinition(def)
			}
			// 设计态同步回退，否则 GetDesign 仍会返回已删除版本（SQLite 侧回写主表）。
			if design, ok := s.designVersions[key][newMax]; ok {
				s.designs[key] = cloneDesign(design)
			}
		}
	}
	return nil
}

func (s *MemoryStore) Get(ctx context.Context, id runtime.ID) (*runtime.State, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key, err := scopedInstance(ctx, id)
	if err != nil {
		return nil, err
	}
	if st, ok := s.states[key]; ok {
		return cloneState(st), nil
	}
	return nil, nil
}

func (s *MemoryStore) Save(ctx context.Context, st *runtime.State) error {
	if st == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key, err := scopedInstance(ctx, st.ID)
	if err != nil {
		return err
	}
	cp := cloneState(st)
	if cp.Version == 0 {
		cp.Version = 1
	}
	s.states[key] = cp
	return nil
}

func (s *MemoryStore) SaveIfVersion(ctx context.Context, st *runtime.State, expectedVersion uint64) error {
	if st == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	key, err := scopedInstance(ctx, st.ID)
	if err != nil {
		return err
	}
	current, exists := s.states[key]
	switch {
	case expectedVersion == 0:
		if exists {
			return errors.NewCode(errors.Conflict, "workflow instance already exists").
				WithContext("instance_id", string(st.ID))
		}
	case !exists:
		return errors.NewCode(errors.Conflict, "workflow instance does not exist").
			WithContext("instance_id", string(st.ID))
	case current.Version != expectedVersion:
		return errors.NewCode(errors.Conflict, "workflow instance version mismatch").
			WithContext("instance_id", string(st.ID)).
			WithContext("expected_version", expectedVersion).
			WithContext("current_version", current.Version)
	}

	cp := cloneState(st)
	cp.Version = expectedVersion + 1
	s.states[key] = cp
	return nil
}

func (s *MemoryStore) Delete(ctx context.Context, id runtime.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key, err := scopedInstance(ctx, id)
	if err != nil {
		return err
	}
	delete(s.states, key)
	return nil
}

// SQLiteStore 是基于 SQLite 的 workflow 持久化存储。
type SQLiteStore struct {
	db              *sql.DB
	managed         bool
	legacyDataScope *Scope
}

// SQLiteStoreOption 配置 SQLite workflow 存储初始化。
type SQLiteStoreOption func(*SQLiteStore) error

// WithLegacyDataScope 将引入 managed scope 前创建的记录显式归属到目标 scope。
//
// 未配置时，platform/0 历史记录保持隔离，不会被 HTTP scope 隐式接管。
func WithLegacyDataScope(scope Scope) SQLiteStoreOption {
	return func(store *SQLiteStore) error {
		scope = normalizeScope(scope)
		if err := validateScope(scope); err != nil {
			return err
		}
		if scope.ID <= 0 {
			return errors.NewCode(errors.InvalidInput, "workflow legacy data scope id is required")
		}
		store.legacyDataScope = &scope
		return nil
	}
}

// NewSQLiteStore 基于外部提供的 *sql.DB 创建 workflow 存储并执行内置迁移。
// 调用方负责数据库连接的生命周期管理（关闭）。
func NewSQLiteStore(ctx context.Context, db *sql.DB, options ...SQLiteStoreOption) (*SQLiteStore, error) {
	if ctx == nil {
		return nil, errors.NewCode(errors.InvalidInput, "workflow context is nil")
	}
	if db == nil {
		return nil, errors.NewCode(errors.InvalidInput, "workflow db is nil")
	}
	store := &SQLiteStore{db: db, managed: false}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(store); err != nil {
			return nil, err
		}
	}
	if err := store.Migrate(ctx); err != nil {
		return nil, err
	}
	return store, nil
}

// OpenSQLiteStore 打开 SQLite workflow 存储并执行内置迁移。
// Deprecated: 推荐使用 NewSQLiteStore 接收外部数据库连接。
func OpenSQLiteStore(ctx context.Context, dsn string, options ...SQLiteStoreOption) (*SQLiteStore, error) {
	if ctx == nil {
		return nil, errors.NewCode(errors.InvalidInput, "workflow context is nil")
	}
	if strings.TrimSpace(dsn) == "" {
		dsn = "file:gochen-workflow.db"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, errors.Wrap(err, errors.Database, "open workflow sqlite store")
	}
	db.SetMaxOpenConns(1)
	store := &SQLiteStore{db: db, managed: true}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(store); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	if err := store.Migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *SQLiteStore) Close() error {
	if s.managed {
		return s.db.Close()
	}
	return nil
}

func (s *SQLiteStore) Migrate(ctx context.Context) error {
	if ctx == nil {
		return errors.NewCode(errors.InvalidInput, "workflow context is nil")
	}
	if _, err := s.db.ExecContext(ctx, `PRAGMA foreign_keys = ON`); err != nil {
		return errors.Wrap(err, errors.Database, "enable workflow foreign keys")
	}
	statements := []string{
		createWorkflowDefinitionsTable("workflow_definitions"),
		createWorkflowDefinitionVersionsTable("workflow_definition_versions"),
		createWorkflowInstancesTable("workflow_instances"),
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return errors.Wrap(err, errors.Database, "migrate workflow store")
		}
	}
	if err := s.ensureInstanceVersionColumn(ctx); err != nil {
		return err
	}
	if err := s.ensureManagedScopeColumns(ctx, "workflow_definitions"); err != nil {
		return err
	}
	if err := s.ensureManagedScopeColumns(ctx, "workflow_instances"); err != nil {
		return err
	}
	if err := s.ensureScopedPrimaryKeys(ctx); err != nil {
		return err
	}
	if err := s.upgradeLegacyDefinitionJSONs(ctx); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO workflow_definition_versions
		(managed_scope_kind, managed_scope_id, id, version, runtime_definition_json, graph_json, created_at)
		SELECT managed_scope_kind, managed_scope_id, id, version, runtime_definition_json, graph_json, updated_at
		FROM workflow_definitions WHERE version > 0`); err != nil {
		return errors.Wrap(err, errors.Database, "migrate workflow definition versions")
	}
	// 存量实例没有 definition_version（读出为 0 即"跟随最新版"）；把它们钉到主表当时的版本，
	// 否则之后一发布新版本，运行中的老实例会静默切到新图，当前节点若被删掉就再也推进不了。
	if _, err := s.db.ExecContext(ctx, `UPDATE workflow_instances
		SET state_json = json_set(state_json, '$.definition_version',
			(SELECT d.version FROM workflow_definitions d
			 WHERE d.managed_scope_kind = workflow_instances.managed_scope_kind
			   AND d.managed_scope_id = workflow_instances.managed_scope_id
			   AND d.id = workflow_instances.definition_id))
		WHERE COALESCE(json_extract(state_json, '$.definition_version'), 0) = 0
		  AND EXISTS (SELECT 1 FROM workflow_definitions d
			 WHERE d.managed_scope_kind = workflow_instances.managed_scope_kind
			   AND d.managed_scope_id = workflow_instances.managed_scope_id
			   AND d.id = workflow_instances.definition_id)`); err != nil {
		return errors.Wrap(err, errors.Database, "pin legacy workflow instances to definition version")
	}
	if s.legacyDataScope != nil {
		if err := s.migrateLegacyDataScope(ctx, *s.legacyDataScope); err != nil {
			return err
		}
	}
	return s.ensureScopeIndexes(ctx)
}

// upgradeLegacyDefinitionJSONs 把存量定义里旧版 next 出边就地升级为 edges 结构。
// 必须在版本表回填之前执行，保证回填快照与后续加载拿到的都是升级后的定义。
func (s *SQLiteStore) upgradeLegacyDefinitionJSONs(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT managed_scope_kind, managed_scope_id, id, runtime_definition_json FROM workflow_definitions`)
	if err != nil {
		return errors.Wrap(err, errors.Database, "scan workflow definitions for legacy upgrade")
	}
	type pendingUpgrade struct {
		kind    string
		scopeID int64
		id      string
		json    string
	}
	var pending []pendingUpgrade
	for rows.Next() {
		var item pendingUpgrade
		if err := rows.Scan(&item.kind, &item.scopeID, &item.id, &item.json); err != nil {
			_ = rows.Close()
			return errors.Wrap(err, errors.Database, "read workflow definition for legacy upgrade")
		}
		upgraded, changed, err := upgradeLegacyRuntimeDefinitionJSON(item.json)
		if err != nil {
			_ = rows.Close()
			return errors.Wrap(err, errors.Database, "upgrade legacy workflow definition").
				WithContext("definition_id", item.id)
		}
		if changed {
			item.json = upgraded
			pending = append(pending, item)
		}
	}
	if err := rows.Close(); err != nil {
		return errors.Wrap(err, errors.Database, "close workflow definition scan")
	}
	for _, item := range pending {
		if _, err := s.db.ExecContext(ctx, `UPDATE workflow_definitions
			SET runtime_definition_json = ?
			WHERE managed_scope_kind = ? AND managed_scope_id = ? AND id = ?`,
			item.json, item.kind, item.scopeID, item.id); err != nil {
			return errors.Wrap(err, errors.Database, "persist upgraded workflow definition").
				WithContext("definition_id", item.id)
		}
	}
	return nil
}

// upgradeLegacyRuntimeDefinitionJSON 将旧版节点上的 "next": ["target", ...] 出边
// 转换为现行 "edges": [{"target": ...}] 结构，并移除 next 字段。
// 节点已有非空 edges 时保留 edges、只清理 next。
func upgradeLegacyRuntimeDefinitionJSON(raw string) (string, bool, error) {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		return raw, false, errors.Wrap(err, errors.InvalidInput, "decode legacy workflow definition")
	}
	nodesRaw, ok := doc["nodes"]
	if !ok {
		return raw, false, nil
	}
	var nodes []map[string]json.RawMessage
	if err := json.Unmarshal(nodesRaw, &nodes); err != nil {
		return raw, false, errors.Wrap(err, errors.InvalidInput, "decode legacy workflow definition nodes")
	}
	changed := false
	for _, node := range nodes {
		nextRaw, hasNext := node["next"]
		if !hasNext {
			continue
		}
		var targets []string
		if err := json.Unmarshal(nextRaw, &targets); err != nil {
			return raw, false, errors.Wrap(err, errors.InvalidInput, "decode legacy workflow node next")
		}
		delete(node, "next")
		changed = true
		if len(targets) == 0 {
			continue
		}
		if edgesRaw, hasEdges := node["edges"]; hasEdges {
			var existing []json.RawMessage
			if err := json.Unmarshal(edgesRaw, &existing); err == nil && len(existing) > 0 {
				continue
			}
		}
		edges := make([]runtime.Edge, 0, len(targets))
		for _, target := range targets {
			edges = append(edges, runtime.Edge{Target: target})
		}
		encoded, err := json.Marshal(edges)
		if err != nil {
			return raw, false, errors.Wrap(err, errors.Internal, "encode upgraded workflow node edges")
		}
		node["edges"] = encoded
	}
	if !changed {
		return raw, false, nil
	}
	encodedNodes, err := json.Marshal(nodes)
	if err != nil {
		return raw, false, errors.Wrap(err, errors.Internal, "encode upgraded workflow definition nodes")
	}
	doc["nodes"] = encodedNodes
	upgraded, err := json.Marshal(doc)
	if err != nil {
		return raw, false, errors.Wrap(err, errors.Internal, "encode upgraded workflow definition")
	}
	return string(upgraded), true, nil
}

func (s *SQLiteStore) migrateLegacyDataScope(ctx context.Context, target Scope) (err error) {
	target = normalizeScope(target)
	if err := validateScope(target); err != nil {
		return err
	}
	if target.ID <= 0 {
		return errors.NewCode(errors.InvalidInput, "workflow legacy data scope id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, errors.Database, "begin workflow legacy scope migration")
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err = tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
		return errors.Wrap(err, errors.Database, "defer workflow foreign keys for legacy scope migration")
	}

	tables := []string{"workflow_definitions", "workflow_definition_versions", "workflow_instances"}
	for _, table := range tables {
		var conflict int
		versionMatch := ""
		if table == "workflow_definition_versions" {
			versionMatch = " AND target.version = legacy.version"
		}
		query := `SELECT EXISTS(
			SELECT 1 FROM ` + table + ` legacy
			JOIN ` + table + ` target ON target.id = legacy.id` + versionMatch + `
			WHERE legacy.managed_scope_kind = 'platform' AND legacy.managed_scope_id = 0
				AND target.managed_scope_kind = ? AND target.managed_scope_id = ?
		)`
		if scanErr := tx.QueryRowContext(ctx, query, target.Kind, target.ID).Scan(&conflict); scanErr != nil {
			return errors.Wrap(scanErr, errors.Database, "check workflow legacy scope conflicts").
				WithContext("table", table)
		}
		if conflict != 0 {
			return errors.NewCode(errors.Conflict, "workflow legacy data conflicts with target scope").
				WithContext("table", table).
				WithContext("scope_kind", target.Kind).
				WithContext("scope_id", target.ID)
		}
	}

	for _, table := range tables {
		if _, err = tx.ExecContext(ctx, `UPDATE `+table+`
			SET managed_scope_kind = ?, managed_scope_id = ?
			WHERE managed_scope_kind = 'platform' AND managed_scope_id = 0`, target.Kind, target.ID); err != nil {
			return errors.Wrap(err, errors.Database, "assign workflow legacy data scope").
				WithContext("table", table)
		}
	}
	if err = tx.Commit(); err != nil {
		return errors.Wrap(err, errors.Database, "commit workflow legacy scope migration")
	}
	return nil
}

func (s *SQLiteStore) ensureInstanceVersionColumn(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `ALTER TABLE workflow_instances ADD COLUMN version INTEGER NOT NULL DEFAULT 1`)
	if err == nil {
		return nil
	}
	if strings.Contains(strings.ToLower(err.Error()), "duplicate column name") {
		return nil
	}
	return errors.Wrap(err, errors.Database, "ensure workflow instance version column")
}

func (s *SQLiteStore) ensureManagedScopeColumns(ctx context.Context, table string) error {
	if table != "workflow_definitions" && table != "workflow_instances" {
		return errors.NewCode(errors.InvalidInput, "invalid workflow scope table").WithContext("table", table)
	}
	if err := s.ensureColumn(ctx, table, "managed_scope_kind", "TEXT NOT NULL DEFAULT 'platform'"); err != nil {
		return err
	}
	return s.ensureColumn(ctx, table, "managed_scope_id", "INTEGER NOT NULL DEFAULT 0")
}

func (s *SQLiteStore) ensureColumn(ctx context.Context, table string, column string, definition string) error {
	_, err := s.db.ExecContext(ctx, `ALTER TABLE `+table+` ADD COLUMN `+column+` `+definition)
	if err == nil {
		return nil
	}
	if strings.Contains(strings.ToLower(err.Error()), "duplicate column name") {
		return nil
	}
	return errors.Wrap(err, errors.Database, "ensure workflow column").
		WithContext("table", table).
		WithContext("column", column)
}

func createWorkflowDefinitionsTable(table string) string {
	return `CREATE TABLE IF NOT EXISTS ` + table + ` (
		managed_scope_kind TEXT NOT NULL DEFAULT 'platform',
		managed_scope_id INTEGER NOT NULL DEFAULT 0,
		id TEXT NOT NULL,
		name TEXT NOT NULL,
		version INTEGER NOT NULL,
		status TEXT NOT NULL,
		graph_json TEXT NOT NULL,
		runtime_definition_json TEXT NOT NULL,
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		published_at TEXT,
		PRIMARY KEY (managed_scope_kind, managed_scope_id, id)
	)`
}

func createWorkflowDefinitionVersionsTable(table string) string {
	return `CREATE TABLE IF NOT EXISTS ` + table + ` (
		managed_scope_kind TEXT NOT NULL DEFAULT 'platform',
		managed_scope_id INTEGER NOT NULL DEFAULT 0,
		id TEXT NOT NULL,
		version INTEGER NOT NULL,
		runtime_definition_json TEXT NOT NULL,
		graph_json TEXT NOT NULL,
		created_at TEXT NOT NULL,
		PRIMARY KEY (managed_scope_kind, managed_scope_id, id, version)
	)`
}

func createWorkflowInstancesTable(table string) string {
	return `CREATE TABLE IF NOT EXISTS ` + table + ` (
		managed_scope_kind TEXT NOT NULL DEFAULT 'platform',
		managed_scope_id INTEGER NOT NULL DEFAULT 0,
		id TEXT NOT NULL,
		definition_id TEXT NOT NULL,
		status TEXT NOT NULL,
		version INTEGER NOT NULL DEFAULT 1,
		state_json TEXT NOT NULL,
		data_json TEXT NOT NULL,
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		started_at TEXT,
		completed_at TEXT,
		PRIMARY KEY (managed_scope_kind, managed_scope_id, id),
		FOREIGN KEY(managed_scope_kind, managed_scope_id, definition_id)
			REFERENCES workflow_definitions(managed_scope_kind, managed_scope_id, id) ON DELETE RESTRICT
	)`
}

func (s *SQLiteStore) ensureScopedPrimaryKeys(ctx context.Context) error {
	definitionsOK, err := s.primaryKeyMatches(ctx, "workflow_definitions", []string{"managed_scope_kind", "managed_scope_id", "id"})
	if err != nil {
		return err
	}
	instancesOK, err := s.primaryKeyMatches(ctx, "workflow_instances", []string{"managed_scope_kind", "managed_scope_id", "id"})
	if err != nil {
		return err
	}
	if definitionsOK && instancesOK {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `PRAGMA foreign_keys = OFF`); err != nil {
		return errors.Wrap(err, errors.Database, "disable workflow foreign keys")
	}
	defer func() {
		_, _ = s.db.ExecContext(ctx, `PRAGMA foreign_keys = ON`)
	}()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, errors.Database, "begin workflow scope migration")
	}
	if err := rebuildScopedTables(ctx, tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, errors.Database, "commit workflow scope migration")
	}
	return nil
}

func rebuildScopedTables(ctx context.Context, tx *sql.Tx) error {
	statements := []string{
		`DROP TABLE IF EXISTS workflow_definitions_scoped_new`,
		`DROP TABLE IF EXISTS workflow_instances_scoped_new`,
		createWorkflowDefinitionsTable("workflow_definitions_scoped_new"),
		createWorkflowInstancesTable("workflow_instances_scoped_new"),
		`INSERT INTO workflow_definitions_scoped_new
			(managed_scope_kind, managed_scope_id, id, name, version, status, graph_json, runtime_definition_json, created_at, updated_at, published_at)
			SELECT managed_scope_kind, managed_scope_id, id, name, version, status, graph_json, runtime_definition_json, created_at, updated_at, published_at
			FROM workflow_definitions`,
		`INSERT INTO workflow_instances_scoped_new
			(managed_scope_kind, managed_scope_id, id, definition_id, status, version, state_json, data_json, created_at, updated_at, started_at, completed_at)
			SELECT managed_scope_kind, managed_scope_id, id, definition_id, status, version, state_json, data_json, created_at, updated_at, started_at, completed_at
			FROM workflow_instances`,
		`DROP TABLE workflow_instances`,
		`DROP TABLE workflow_definitions`,
		`ALTER TABLE workflow_definitions_scoped_new RENAME TO workflow_definitions`,
		`ALTER TABLE workflow_instances_scoped_new RENAME TO workflow_instances`,
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return errors.Wrap(err, errors.Database, "rebuild workflow scoped tables")
		}
	}
	return nil
}

func (s *SQLiteStore) primaryKeyMatches(ctx context.Context, table string, want []string) (bool, error) {
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return false, errors.Wrap(err, errors.Database, "inspect workflow table").WithContext("table", table)
	}
	defer rows.Close()
	// pkByIndex 收集完整主键（pkIndex 从 1 开始），不假设主键列数，
	// 避免实际主键列数多于 want 时被静默截断导致误判。
	pkByIndex := make(map[int]string)
	maxIndex := 0
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue any
		var pkIndex int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pkIndex); err != nil {
			return false, errors.Wrap(err, errors.Database, "scan workflow table info").WithContext("table", table)
		}
		if pkIndex > 0 {
			pkByIndex[pkIndex] = name
			if pkIndex > maxIndex {
				maxIndex = pkIndex
			}
		}
	}
	if err := rows.Err(); err != nil {
		return false, errors.Wrap(err, errors.Database, "iterate workflow table info").WithContext("table", table)
	}
	if maxIndex != len(want) {
		return false, nil
	}
	for i := range want {
		if pkByIndex[i+1] != want[i] {
			return false, nil
		}
	}
	return true, nil
}

func (s *SQLiteStore) ensureScopeIndexes(ctx context.Context) error {
	scopeIndexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_workflow_definitions_scope_updated ON workflow_definitions(managed_scope_kind, managed_scope_id, updated_at DESC, id ASC)`,
		`CREATE INDEX IF NOT EXISTS idx_workflow_instances_definition_scope ON workflow_instances(managed_scope_kind, managed_scope_id, definition_id)`,
		`CREATE INDEX IF NOT EXISTS idx_workflow_instances_scope_updated ON workflow_instances(managed_scope_kind, managed_scope_id, updated_at DESC, id ASC)`,
	}
	for _, statement := range scopeIndexes {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return errors.Wrap(err, errors.Database, "migrate workflow scope indexes")
		}
	}
	return nil
}

func (s *SQLiteStore) ListDesigns(ctx context.Context) ([]*workflowtypes.DesignerDefinition, error) {
	scope, err := currentManagedScope(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT graph_json, runtime_definition_json FROM workflow_definitions WHERE managed_scope_kind = ? AND managed_scope_id = ? ORDER BY updated_at DESC, id ASC`, scope.Kind, scope.ID)
	if err != nil {
		return nil, errors.Wrap(err, errors.Database, "list workflow definitions")
	}
	defer rows.Close()
	var items []*workflowtypes.DesignerDefinition
	for rows.Next() {
		design, err := scanDesignRows(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, design)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, errors.Database, "iterate workflow definitions")
	}
	return items, nil
}

func (s *SQLiteStore) queryDesigns(ctx context.Context, query definitionListQuery) (*workflowtypes.DefinitionListResponse, error) {
	scope, err := currentManagedScope(ctx)
	if err != nil {
		return nil, err
	}
	where := "managed_scope_kind = ? AND managed_scope_id = ?"
	baseArgs := []any{scope.Kind, scope.ID}
	if query.keyword != "" {
		where += " AND instr(lower(id || ' ' || name || ' ' || COALESCE(json_extract(graph_json, '$.description'), '')), ?) > 0"
		baseArgs = append(baseArgs, query.keyword)
	}

	stats := workflowtypes.DefinitionStats{}
	rows, err := s.db.QueryContext(ctx, "SELECT status, COUNT(1) FROM workflow_definitions WHERE "+where+" GROUP BY status", baseArgs...)
	if err != nil {
		return nil, errors.Wrap(err, errors.Database, "count workflow definition statuses")
	}
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			_ = rows.Close()
			return nil, errors.Wrap(err, errors.Database, "scan workflow definition status count")
		}
		stats.Total += count
		switch workflowtypes.DefinitionStatus(status) {
		case workflowtypes.StatusDraft:
			stats.Draft += count
		case workflowtypes.StatusPublished:
			stats.Published += count
		case workflowtypes.StatusArchived:
			stats.Archived += count
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, errors.Wrap(err, errors.Database, "iterate workflow definition status counts")
	}
	if err := rows.Close(); err != nil {
		return nil, errors.Wrap(err, errors.Database, "close workflow definition status counts")
	}

	filteredWhere := where
	filteredArgs := append([]any(nil), baseArgs...)
	total := stats.Total
	if query.status != "" {
		filteredWhere += " AND status = ?"
		filteredArgs = append(filteredArgs, string(query.status))
		if err := s.db.QueryRowContext(ctx, "SELECT COUNT(1) FROM workflow_definitions WHERE "+filteredWhere, filteredArgs...).Scan(&total); err != nil {
			return nil, errors.Wrap(err, errors.Database, "count filtered workflow definitions")
		}
	}

	pageArgs := append(append([]any(nil), filteredArgs...), query.size, query.offset)
	pageRows, err := s.db.QueryContext(ctx,
		"SELECT graph_json, runtime_definition_json FROM workflow_definitions WHERE "+filteredWhere+` ORDER BY
			substr(updated_at, 1, 19) DESC,
			CASE
				WHEN instr(updated_at, '.') > 0 THEN substr(
					substr(updated_at, instr(updated_at, '.') + 1, instr(updated_at, 'Z') - instr(updated_at, '.') - 1) || '000000000',
					1,
					9
				)
				ELSE '000000000'
			END DESC,
			id ASC LIMIT ? OFFSET ?`,
		pageArgs...,
	)
	if err != nil {
		return nil, errors.Wrap(err, errors.Database, "query workflow definition page")
	}
	defer pageRows.Close()
	items := make([]*workflowtypes.DesignerDefinition, 0, query.size)
	for pageRows.Next() {
		design, err := scanDesignRows(pageRows)
		if err != nil {
			return nil, err
		}
		items = append(items, design)
	}
	if err := pageRows.Err(); err != nil {
		return nil, errors.Wrap(err, errors.Database, "iterate workflow definition page")
	}
	return &workflowtypes.DefinitionListResponse{
		Items:        items,
		Total:        total,
		Page:         query.page,
		Size:         query.size,
		Stats:        stats,
		StatusCounts: stats,
	}, nil
}

func (s *SQLiteStore) GetDesign(ctx context.Context, id string) (*workflowtypes.DesignerDefinition, error) {
	scope, err := currentManagedScope(ctx)
	if err != nil {
		return nil, err
	}
	row := s.db.QueryRowContext(ctx, `SELECT graph_json, runtime_definition_json FROM workflow_definitions WHERE managed_scope_kind = ? AND managed_scope_id = ? AND id = ?`, scope.Kind, scope.ID, id)
	design, err := scanDesignRows(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return design, err
}

func (s *SQLiteStore) SaveDesign(ctx context.Context, design *workflowtypes.DesignerDefinition, runtimeDef *runtime.Definition) error {
	if design == nil {
		return errors.NewCode(errors.InvalidInput, "workflow design is required")
	}
	return s.saveDesign(ctx, design, runtimeDef, design.Version-1)
}

// saveDesign 使用读取到的最新版本做 CAS，运行态允许显式跳号，设计态仍逐版保存。
func (s *SQLiteStore) saveDesign(ctx context.Context, design *workflowtypes.DesignerDefinition, runtimeDef *runtime.Definition, expectedVersion int) error {
	if design == nil || runtimeDef == nil {
		return errors.NewCode(errors.InvalidInput, "workflow design and runtime definition are required")
	}
	if design.ID != runtimeDef.ID {
		return errors.NewCode(errors.InvalidInput, "workflow design and runtime definition id mismatch").
			WithContext("definition_id", design.ID).
			WithContext("runtime_definition_id", runtimeDef.ID)
	}
	if design.Version <= 0 {
		return errors.NewCode(errors.InvalidInput, "workflow definition version is invalid").
			WithContext("definition_id", design.ID).
			WithContext("version", design.Version)
	}
	if runtimeDef.Version == 0 {
		runtimeDef.Version = uint32(design.Version)
	} else if runtimeDef.Version != uint32(design.Version) {
		return errors.NewCode(errors.InvalidInput, "workflow design and runtime version mismatch")
	}
	graphJSON, err := json.Marshal(design)
	if err != nil {
		return errors.Wrap(err, errors.InvalidInput, "encode workflow design")
	}
	runtimeJSON, err := json.Marshal(runtimeDef)
	if err != nil {
		return errors.Wrap(err, errors.InvalidInput, "encode runtime definition")
	}
	scope, err := currentManagedScope(ctx)
	if err != nil {
		return err
	}
	publishedAt := nullableTime(design.PublishedAt)
	if expectedVersion == 0 {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return errors.Wrap(err, errors.Database, "begin save workflow definition transaction")
		}
		defer func() { _ = tx.Rollback() }()

		result, err := tx.ExecContext(ctx, `INSERT INTO workflow_definitions
			(managed_scope_kind, managed_scope_id, id, name, version, status, graph_json, runtime_definition_json, created_at, updated_at, published_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(managed_scope_kind, managed_scope_id, id) DO NOTHING`,
			scope.Kind, scope.ID, design.ID, design.Name, design.Version, string(design.Status), string(graphJSON), string(runtimeJSON), formatTime(design.CreatedAt), formatTime(design.UpdatedAt), publishedAt)
		if err != nil {
			return errors.Wrap(err, errors.Database, "save workflow definition")
		}
		if affected, _ := result.RowsAffected(); affected > 0 {
			if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO workflow_definition_versions
				(managed_scope_kind, managed_scope_id, id, version, runtime_definition_json, graph_json, created_at)
				VALUES (?, ?, ?, ?, ?, ?, ?)`,
				scope.Kind, scope.ID, design.ID, design.Version, string(runtimeJSON), string(graphJSON), formatTime(design.UpdatedAt)); err != nil {
				return errors.Wrap(err, errors.Database, "save workflow definition version")
			}
			return tx.Commit()
		}

		var currentVersion uint32
		err = tx.QueryRowContext(ctx, `SELECT version FROM workflow_definitions WHERE managed_scope_kind = ? AND managed_scope_id = ? AND id = ?`, scope.Kind, scope.ID, design.ID).Scan(&currentVersion)
		if err != nil {
			if err == sql.ErrNoRows {
				return errors.NewCode(errors.Conflict, "workflow definition does not exist").
					WithContext("definition_id", design.ID)
			}
			return errors.Wrap(err, errors.Database, "query current workflow definition version")
		}
		return errors.NewCode(errors.Conflict, "workflow definition already exists").
			WithContext("definition_id", design.ID).
			WithContext("current_version", currentVersion)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, errors.Database, "begin save workflow definition transaction")
	}
	defer func() { _ = tx.Rollback() }()

	result, err := tx.ExecContext(ctx, `UPDATE workflow_definitions SET
		name = ?,
		version = ?,
		status = ?,
		graph_json = ?,
		runtime_definition_json = ?,
		updated_at = ?,
		published_at = ?
		WHERE managed_scope_kind = ? AND managed_scope_id = ? AND id = ? AND version = ?`,
		design.Name, design.Version, string(design.Status), string(graphJSON), string(runtimeJSON), formatTime(design.UpdatedAt), publishedAt, scope.Kind, scope.ID, design.ID, expectedVersion)
	if err != nil {
		return errors.Wrap(err, errors.Database, "save workflow definition")
	}
	if affected, _ := result.RowsAffected(); affected > 0 {
		if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO workflow_definition_versions
			(managed_scope_kind, managed_scope_id, id, version, runtime_definition_json, graph_json, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			scope.Kind, scope.ID, design.ID, design.Version, string(runtimeJSON), string(graphJSON), formatTime(design.UpdatedAt)); err != nil {
			return errors.Wrap(err, errors.Database, "save workflow definition version")
		}
		return tx.Commit()
	}

	var currentVersion uint32
	err = tx.QueryRowContext(ctx, `SELECT version FROM workflow_definitions WHERE managed_scope_kind = ? AND managed_scope_id = ? AND id = ?`, scope.Kind, scope.ID, design.ID).Scan(&currentVersion)
	if err != nil {
		if err == sql.ErrNoRows {
			return errors.NewCode(errors.Conflict, "workflow definition does not exist").
				WithContext("definition_id", design.ID)
		}
		return errors.Wrap(err, errors.Database, "query current workflow definition version")
	}
	return errors.NewCode(errors.Conflict, "workflow definition version mismatch").
		WithContext("definition_id", design.ID).
		WithContext("expected_version", expectedVersion).
		WithContext("current_version", currentVersion)
}

func (s *SQLiteStore) DeleteDesign(ctx context.Context, id string) error {
	scope, err := currentManagedScope(ctx)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, errors.Database, "begin delete workflow design transaction")
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `DELETE FROM workflow_definitions WHERE managed_scope_kind = ? AND managed_scope_id = ? AND id = ?`, scope.Kind, scope.ID, id); err != nil {
		return errors.Wrap(err, errors.Database, "delete workflow definition")
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM workflow_definition_versions WHERE managed_scope_kind = ? AND managed_scope_id = ? AND id = ?`, scope.Kind, scope.ID, id); err != nil {
		return errors.Wrap(err, errors.Database, "delete workflow definition versions")
	}
	return tx.Commit()
}

func (s *SQLiteStore) CountInstancesByDefinition(ctx context.Context, id string) (int, error) {
	scope, err := currentManagedScope(ctx)
	if err != nil {
		return 0, err
	}
	row := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM workflow_instances WHERE managed_scope_kind = ? AND managed_scope_id = ? AND definition_id = ?`, scope.Kind, scope.ID, id)
	var count int
	if err := row.Scan(&count); err != nil {
		return 0, errors.Wrap(err, errors.Database, "count workflow definition instances")
	}
	return count, nil
}

func (s *SQLiteStore) ListInstances(ctx context.Context) ([]*runtime.State, error) {
	return s.listInstancesWhere(ctx, "")
}

func (s *SQLiteStore) ListStartedInstances(ctx context.Context) ([]*runtime.State, error) {
	return s.listInstancesWhere(ctx, " AND started_at IS NOT NULL")
}

func (s *SQLiteStore) listInstancesWhere(ctx context.Context, extraWhere string) ([]*runtime.State, error) {
	scope, err := currentManagedScope(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT state_json, version FROM workflow_instances WHERE managed_scope_kind = ? AND managed_scope_id = ?`+extraWhere+` ORDER BY updated_at DESC, id ASC`, scope.Kind, scope.ID)
	if err != nil {
		return nil, errors.Wrap(err, errors.Database, "list workflow instances")
	}
	defer rows.Close()
	var items []*runtime.State
	for rows.Next() {
		state, err := scanStateRows(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, state)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, errors.Database, "iterate workflow instances")
	}
	return items, nil
}

func (s *SQLiteStore) GetDefinition(ctx context.Context, id string, version uint32) (*runtime.Definition, error) {
	scope, err := currentManagedScope(ctx)
	if err != nil {
		return nil, err
	}
	var raw string
	if version > 0 {
		row := s.db.QueryRowContext(ctx, `SELECT runtime_definition_json FROM workflow_definition_versions WHERE managed_scope_kind = ? AND managed_scope_id = ? AND id = ? AND version = ?`, scope.Kind, scope.ID, id, version)
		if err := row.Scan(&raw); err == nil {
			var def runtime.Definition
			if err := json.Unmarshal([]byte(raw), &def); err != nil {
				return nil, errors.Wrap(err, errors.Database, "decode runtime definition")
			}
			return &def, nil
		} else if err != sql.ErrNoRows {
			return nil, errors.Wrap(err, errors.Database, "get runtime definition version")
		}

		// 版本表没有命中时回落主表：主表当前版本恰好等于所求版本的记录也算命中。
		row = s.db.QueryRowContext(ctx, `SELECT runtime_definition_json FROM workflow_definitions WHERE managed_scope_kind = ? AND managed_scope_id = ? AND id = ? AND version = ?`, scope.Kind, scope.ID, id, version)
		if err := row.Scan(&raw); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}
			return nil, errors.Wrap(err, errors.Database, "get runtime definition")
		}
	} else {
		row := s.db.QueryRowContext(ctx, `SELECT runtime_definition_json FROM workflow_definitions WHERE managed_scope_kind = ? AND managed_scope_id = ? AND id = ?`, scope.Kind, scope.ID, id)
		if err := row.Scan(&raw); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}
			return nil, errors.Wrap(err, errors.Database, "get runtime definition")
		}
	}
	var def runtime.Definition
	if err := json.Unmarshal([]byte(raw), &def); err != nil {
		return nil, errors.Wrap(err, errors.Database, "decode runtime definition")
	}
	return &def, nil
}

func (s *SQLiteStore) SaveDefinition(ctx context.Context, def *runtime.Definition) error {
	if def == nil {
		return nil
	}
	now := def.UpdatedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	design := designFromRuntimeDefinition(def, now)
	existing, err := s.GetDesign(ctx, def.ID)
	if err != nil {
		return err
	}
	expectedVersion := 0
	if existing != nil {
		expectedVersion = existing.Version
		design.Description = existing.Description
		design.Version = existing.Version + 1
		if design.Version <= 0 {
			design.Version = 1
		}
		design.Status = existing.Status
		design.Viewport = existing.Viewport
		design.CreatedAt = existing.CreatedAt
		if design.CreatedAt.IsZero() {
			design.CreatedAt = now
		}
		design.PublishedAt = nil
		if existing.PublishedAt != nil {
			publishedAt := *existing.PublishedAt
			design.PublishedAt = &publishedAt
		}
	}
	if def.Version != 0 {
		design.Version = int(def.Version)
	}
	if design.Version <= expectedVersion {
		return errors.NewCode(errors.Conflict, "workflow definition version must exceed latest version")
	}
	return s.saveDesign(ctx, design, def, expectedVersion)
}

func (s *SQLiteStore) DeleteDefinition(ctx context.Context, id string, version uint32) error {
	if version == 0 {
		return s.DeleteDesign(ctx, id)
	}
	scope, err := currentManagedScope(ctx)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, errors.Database, "begin delete definition version transaction")
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(ctx, `DELETE FROM workflow_definition_versions WHERE managed_scope_kind = ? AND managed_scope_id = ? AND id = ? AND version = ?`, scope.Kind, scope.ID, id, version)
	if err != nil {
		return errors.Wrap(err, errors.Database, "delete workflow definition version")
	}

	var currentMainVersion uint32
	err = tx.QueryRowContext(ctx, `SELECT version FROM workflow_definitions WHERE managed_scope_kind = ? AND managed_scope_id = ? AND id = ?`, scope.Kind, scope.ID, id).Scan(&currentMainVersion)
	if err != nil {
		if err == sql.ErrNoRows {
			return tx.Commit()
		}
		return errors.Wrap(err, errors.Database, "query workflow definition main version")
	}

	// 如果删除的是当前主表指向的最新版本，需重算最新版本或清理主表
	if currentMainVersion == version {
		var newMaxVersion uint32
		var newRuntimeJSON, newGraphJSON string
		err = tx.QueryRowContext(ctx, `SELECT version, runtime_definition_json, graph_json FROM workflow_definition_versions
			WHERE managed_scope_kind = ? AND managed_scope_id = ? AND id = ?
			ORDER BY version DESC LIMIT 1`, scope.Kind, scope.ID, id).Scan(&newMaxVersion, &newRuntimeJSON, &newGraphJSON)
		if err != nil {
			if err == sql.ErrNoRows {
				// 已无任何版本，删除主表
				if _, err := tx.ExecContext(ctx, `DELETE FROM workflow_definitions WHERE managed_scope_kind = ? AND managed_scope_id = ? AND id = ?`, scope.Kind, scope.ID, id); err != nil {
					return errors.Wrap(err, errors.Database, "delete empty workflow definition")
				}
				return tx.Commit()
			}
			return errors.Wrap(err, errors.Database, "query next latest workflow definition version")
		}

		// 回写新的最新版本到主表：name/status/published_at 只存在于 graph_json 中，
		// 不一并回写会让主表留下被删版本的元数据。
		var newDesign workflowtypes.DesignerDefinition
		if err := json.Unmarshal([]byte(newGraphJSON), &newDesign); err != nil {
			return errors.Wrap(err, errors.Database, "decode workflow design of next version")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE workflow_definitions SET
			name = ?,
			version = ?,
			status = ?,
			runtime_definition_json = ?,
			graph_json = ?,
			updated_at = ?,
			published_at = ?
			WHERE managed_scope_kind = ? AND managed_scope_id = ? AND id = ?`,
			newDesign.Name, newMaxVersion, string(newDesign.Status), newRuntimeJSON, newGraphJSON,
			formatTime(newDesign.UpdatedAt), nullableTime(newDesign.PublishedAt),
			scope.Kind, scope.ID, id); err != nil {
			return errors.Wrap(err, errors.Database, "update workflow definition to next version")
		}
	}

	return tx.Commit()
}

func (s *SQLiteStore) Get(ctx context.Context, id runtime.ID) (*runtime.State, error) {
	scope, err := currentManagedScope(ctx)
	if err != nil {
		return nil, err
	}
	row := s.db.QueryRowContext(ctx, `SELECT state_json, version FROM workflow_instances WHERE managed_scope_kind = ? AND managed_scope_id = ? AND id = ?`, scope.Kind, scope.ID, string(id))
	var raw string
	var version uint64
	if err := row.Scan(&raw, &version); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, errors.Wrap(err, errors.Database, "get workflow instance")
	}
	var state runtime.State
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		return nil, errors.Wrap(err, errors.Database, "decode workflow instance")
	}
	state.Version = version
	return &state, nil
}

func (s *SQLiteStore) Save(ctx context.Context, st *runtime.State) error {
	if st == nil {
		return nil
	}
	cp := cloneState(st)
	if cp.Version == 0 {
		cp.Version = 1
	}
	stateJSON, err := json.Marshal(cp)
	if err != nil {
		return errors.Wrap(err, errors.InvalidInput, "encode workflow instance")
	}
	dataJSON, err := json.Marshal(cp.Data)
	if err != nil {
		return errors.Wrap(err, errors.InvalidInput, "encode workflow instance data")
	}
	scope, err := currentManagedScope(ctx)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO workflow_instances
		(managed_scope_kind, managed_scope_id, id, definition_id, status, version, state_json, data_json, created_at, updated_at, started_at, completed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(managed_scope_kind, managed_scope_id, id) DO UPDATE SET
		definition_id = excluded.definition_id,
		status = excluded.status,
		version = excluded.version,
		state_json = excluded.state_json,
		data_json = excluded.data_json,
		updated_at = excluded.updated_at,
		started_at = excluded.started_at,
		completed_at = excluded.completed_at`,
		scope.Kind, scope.ID, string(cp.ID), cp.DefinitionID, string(cp.Status), cp.Version, string(stateJSON), string(dataJSON), formatTime(cp.CreatedAt), formatTime(cp.UpdatedAt), nullableValueTime(cp.StartedAt), nullableValueTime(cp.CompletedAt))
	if err != nil {
		return errors.Wrap(err, errors.Database, "save workflow instance")
	}
	return nil
}

func (s *SQLiteStore) SaveIfVersion(ctx context.Context, st *runtime.State, expectedVersion uint64) (err error) {
	if st == nil {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, errors.Database, "begin workflow instance transaction")
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	cp := cloneState(st)
	cp.Version = expectedVersion + 1
	stateJSON, err := json.Marshal(cp)
	if err != nil {
		return errors.Wrap(err, errors.InvalidInput, "encode workflow instance")
	}
	dataJSON, err := json.Marshal(cp.Data)
	if err != nil {
		return errors.Wrap(err, errors.InvalidInput, "encode workflow instance data")
	}
	scope, err := currentManagedScope(ctx)
	if err != nil {
		return err
	}
	if expectedVersion == 0 {
		var result sql.Result
		result, err = tx.ExecContext(ctx, `INSERT INTO workflow_instances
			(managed_scope_kind, managed_scope_id, id, definition_id, status, version, state_json, data_json, created_at, updated_at, started_at, completed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(managed_scope_kind, managed_scope_id, id) DO NOTHING`,
			scope.Kind, scope.ID, string(cp.ID), cp.DefinitionID, string(cp.Status), cp.Version, string(stateJSON), string(dataJSON), formatTime(cp.CreatedAt), formatTime(cp.UpdatedAt), nullableValueTime(cp.StartedAt), nullableValueTime(cp.CompletedAt))
		if err != nil {
			return errors.Wrap(err, errors.Database, "save workflow instance")
		}
		affected, _ := result.RowsAffected()
		if affected == 0 {
			return errors.NewCode(errors.Conflict, "workflow instance already exists").
				WithContext("instance_id", string(cp.ID))
		}
	} else {
		var result sql.Result
		result, err = tx.ExecContext(ctx, `UPDATE workflow_instances SET
			definition_id = ?,
			status = ?,
			version = ?,
			state_json = ?,
			data_json = ?,
			updated_at = ?,
			started_at = ?,
			completed_at = ?
			WHERE managed_scope_kind = ? AND managed_scope_id = ? AND id = ? AND version = ?`,
			cp.DefinitionID, string(cp.Status), cp.Version, string(stateJSON), string(dataJSON), formatTime(cp.UpdatedAt), nullableValueTime(cp.StartedAt), nullableValueTime(cp.CompletedAt), scope.Kind, scope.ID, string(cp.ID), expectedVersion)
		if err != nil {
			return errors.Wrap(err, errors.Database, "save workflow instance")
		}
		affected, _ := result.RowsAffected()
		if affected == 0 {
			currentVersion, exists, err := s.workflowInstanceVersion(ctx, tx, cp.ID, scope)
			if err != nil {
				return err
			}
			if !exists {
				return errors.NewCode(errors.Conflict, "workflow instance does not exist").
					WithContext("instance_id", string(cp.ID))
			}
			return errors.NewCode(errors.Conflict, "workflow instance version mismatch").
				WithContext("instance_id", string(cp.ID)).
				WithContext("expected_version", expectedVersion).
				WithContext("current_version", currentVersion)
		}
	}
	if err = tx.Commit(); err != nil {
		return errors.Wrap(err, errors.Database, "commit workflow instance transaction")
	}
	return nil
}

func (s *SQLiteStore) workflowInstanceVersion(ctx context.Context, tx *sql.Tx, id runtime.ID, scope Scope) (uint64, bool, error) {
	var version uint64
	err := tx.QueryRowContext(ctx, `SELECT version FROM workflow_instances WHERE managed_scope_kind = ? AND managed_scope_id = ? AND id = ?`, scope.Kind, scope.ID, string(id)).Scan(&version)
	if err == nil {
		return version, true, nil
	}
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	return 0, false, errors.Wrap(err, errors.Database, "get workflow instance version")
}

func (s *SQLiteStore) Delete(ctx context.Context, id runtime.ID) error {
	scope, err := currentManagedScope(ctx)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `DELETE FROM workflow_instances WHERE managed_scope_kind = ? AND managed_scope_id = ? AND id = ?`, scope.Kind, scope.ID, string(id))
	if err != nil {
		return errors.Wrap(err, errors.Database, "delete workflow instance")
	}
	return nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanDesignRows(row rowScanner) (*workflowtypes.DesignerDefinition, error) {
	var graphRaw, runtimeRaw string
	if err := row.Scan(&graphRaw, &runtimeRaw); err != nil {
		if err == sql.ErrNoRows {
			return nil, sql.ErrNoRows
		}
		return nil, errors.Wrap(err, errors.Database, "scan workflow definition")
	}
	var design workflowtypes.DesignerDefinition
	if err := json.Unmarshal([]byte(graphRaw), &design); err != nil {
		return nil, errors.Wrap(err, errors.Database, "decode workflow design")
	}
	var runtimeDef runtime.Definition
	if err := json.Unmarshal([]byte(runtimeRaw), &runtimeDef); err != nil {
		return nil, errors.Wrap(err, errors.Database, "decode runtime definition")
	}
	design.RuntimeDefinition = workflowtypes.RuntimeDefinitionFromRuntime(&runtimeDef, &design)
	return &design, nil
}

func scanStateRows(row rowScanner) (*runtime.State, error) {
	var raw string
	var version uint64
	if err := row.Scan(&raw, &version); err != nil {
		if err == sql.ErrNoRows {
			return nil, sql.ErrNoRows
		}
		return nil, errors.Wrap(err, errors.Database, "scan workflow instance")
	}
	var state runtime.State
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		return nil, errors.Wrap(err, errors.Database, "decode workflow instance")
	}
	state.Version = version
	return &state, nil
}

func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func nullableTime(t *time.Time) any {
	if t == nil || t.IsZero() {
		return nil
	}
	return formatTime(*t)
}

func nullableValueTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return formatTime(t)
}

func designFromRuntimeDefinition(def *runtime.Definition, now time.Time) *workflowtypes.DesignerDefinition {
	publishedAt := now
	design := &workflowtypes.DesignerDefinition{
		ID:          def.ID,
		Name:        def.Name,
		Version:     1,
		Status:      workflowtypes.StatusPublished,
		StartNodeID: def.StartNodeID,
		Viewport:    workflowtypes.Viewport{Zoom: 1},
		CreatedAt:   def.CreatedAt,
		UpdatedAt:   now,
		PublishedAt: &publishedAt,
	}
	if design.CreatedAt.IsZero() {
		design.CreatedAt = now
	}
	for i, node := range def.Nodes {
		nextNodes := node.NextNodes()
		design.Nodes = append(design.Nodes, workflowtypes.DesignerNode{
			ID:       node.ID,
			Name:     node.Name,
			Type:     workflowtypes.NodeKind(node.Kind),
			Next:     cloneStrings(nextNodes),
			RejectTo: cloneStrings(node.RejectTo),
			Position: workflowtypes.Position{X: float64(i * 180), Y: 0},
		})
		for _, next := range nextNodes {
			design.Edges = append(design.Edges, workflowtypes.DesignerEdge{
				ID:     "edge-" + strconv.Itoa(len(design.Edges)+1),
				Source: node.ID,
				Target: next,
			})
		}
	}
	design.Normalize(now)
	design.RuntimeDefinition = workflowtypes.RuntimeDefinitionFromRuntime(def, design)
	return design
}
