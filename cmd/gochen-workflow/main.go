package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	stderrors "errors"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	workflow "gochen-workflow"
	workflowservice "gochen-workflow/service"

	"gochen-runtime/db/sql/stdsql"
	"gochen-runtime/di"
	dibasic "gochen-runtime/di/basic"
	auth "gochen-runtime/host/authz"
	hostconfig "gochen-runtime/host/config"
	"gochen-runtime/http/middleware"
	"gochen-runtime/quick"
	"gochen/auth/scoped"
	"gochen/clock"
	"gochen/contextx"
	"gochen/db"
	"gochen/errors"
	"gochen/gen"
	"gochen/httpx"
	"gochen/observe/logging"
)

const (
	maxTCPPort            = 65535
	defaultAccessTokenTTL = 24 * time.Hour
	revokedTokenTable     = "iam_revoked_tokens"
	revokedTokenClockSkew = 30 * time.Second
)

func main() {
	logger := logging.NewLogger(logging.Config{Prefix: "gochen-workflow", Level: logging.InfoLevel, Format: logging.TextFormat})
	if err := run(context.Background(), logger); err != nil {
		logger.Error(context.Background(), "service exited", logging.Error(err))
		os.Exit(1)
	}
}

func run(ctx context.Context, logger logging.ILogger) error {
	startupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	dsn := os.Getenv("GOCHEN_WORKFLOW_SQLITE_DSN")
	if strings.TrimSpace(dsn) == "" {
		dsn = "file:gochen-workflow.db"
	}
	database, err := stdsql.NewWithContext(startupCtx, db.DBConfig{
		Driver:       "sqlite",
		Database:     dsn,
		MaxOpenConns: 1,
	})
	if err != nil {
		return errors.Wrap(err, errors.Database, "open workflow database")
	}
	defer func() {
		if err := database.Close(); err != nil && logger != nil {
			logger.Warn(ctx, "close workflow database failed", logging.Error(err))
		}
	}()
	revokedTokenStore, err := newDatabaseRevokedTokenStore(database)
	if err != nil {
		return errors.Wrap(err, errors.Internal, "create workflow token revocation store")
	}
	authConfig, err := newStandaloneAuthConfig(revokedTokenStore)
	if err != nil {
		return errors.Wrap(err, errors.InvalidInput, "workflow auth config validation failed")
	}
	if err := ensureDatabaseRevokedTokenSchema(startupCtx, database); err != nil {
		return errors.Wrap(err, errors.Database, "initialize workflow token revocation schema")
	}

	sqlDB, ok := stdsql.DBOf(database)
	if !ok {
		return errors.NewCode(errors.Internal, "workflow database does not expose *sql.DB")
	}

	storeOptions, err := sqliteStoreOptionsFromEnv()
	if err != nil {
		return err
	}
	store, err := workflowservice.NewSQLiteStore(startupCtx, sqlDB, storeOptions...)
	if err != nil {
		return errors.Wrap(err, errors.Database, "open workflow store")
	}

	container := dibasic.New()
	authRegistry := auth.NewRegistry()
	if err := di.RegisterInstance[workflowservice.Store](container, store); err != nil {
		return errors.Wrap(err, errors.Internal, "register workflow store")
	}
	if err := di.RegisterInstance[gen.IGenerator[string]](container, gen.NewUUIDGenerator()); err != nil {
		return errors.Wrap(err, errors.Internal, "register workflow id generator")
	}
	if err := di.RegisterInstance[clock.IClock](container, clock.NewRealClock()); err != nil {
		return errors.Wrap(err, errors.Internal, "register workflow clock")
	}

	hostName, port, err := listenAddress(os.Getenv("GOCHEN_WORKFLOW_ADDR"))
	if err != nil {
		return err
	}

	server := quick.New(
		hostconfig.WithName("gochen-workflow"),
		hostconfig.WithLogger(logger),
		hostconfig.WithContainer(container),
		hostconfig.WithCatalogRegistrar(auth.NewCatalogRegistrar(authRegistry)),
		hostconfig.WithEntryValidator(auth.NewEntryRegistry()),
		hostconfig.WithAddress(hostName),
		hostconfig.WithPort(port),
		hostconfig.WithBasePath("/api/v1"),
		hostconfig.WithFailFastOnRouteConflicts(true),
	)
	server.Use(standaloneRouteMiddlewares(logger, authConfig)...)
	server.Modules(workflow.NewModule)
	return server.Run(ctx)
}

// sqliteStoreOptionsFromEnv 将历史无 scope 数据显式绑定到 standalone 服务配置的 scope。
func sqliteStoreOptionsFromEnv() ([]workflowservice.SQLiteStoreOption, error) {
	scopeKind, err := standaloneScopeKind()
	if err != nil {
		return nil, err
	}
	rawID := strings.TrimSpace(os.Getenv("GOCHEN_WORKFLOW_LEGACY_SCOPE_ID"))
	if rawID == "" {
		return nil, nil
	}
	scopeID, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil || scopeID <= 0 {
		return nil, errors.NewCode(errors.InvalidInput, "invalid workflow legacy scope id").
			WithContext("scope_id", rawID)
	}
	return []workflowservice.SQLiteStoreOption{
		workflowservice.WithLegacyDataScope(workflowservice.Scope{Kind: scopeKind, ID: scopeID}),
	}, nil
}

func listenAddress(raw string) (string, int, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "0.0.0.0", 8088, nil
	}
	if strings.HasPrefix(value, ":") {
		port, err := parseWorkflowPort(strings.TrimPrefix(value, ":"), raw)
		if err != nil {
			return "", 0, err
		}
		return "0.0.0.0", port, nil
	}
	hostName, portText, err := net.SplitHostPort(value)
	if err != nil {
		port, parseErr := parseWorkflowPort(value, raw)
		if parseErr == nil {
			return "0.0.0.0", port, nil
		}
		return "", 0, errors.Wrap(err, errors.InvalidInput, "invalid workflow listen address").WithContext("addr", raw)
	}
	port, err := parseWorkflowPort(portText, raw)
	if err != nil {
		return "", 0, err
	}
	if strings.TrimSpace(hostName) == "" {
		hostName = "0.0.0.0"
	}
	return hostName, port, nil
}

func parseWorkflowPort(portText string, rawAddr string) (int, error) {
	port, err := strconv.Atoi(portText)
	if err != nil || port <= 0 || port > maxTCPPort {
		return 0, errors.NewCode(errors.InvalidInput, "invalid workflow port").WithContext("addr", rawAddr)
	}
	return port, nil
}

type revokedTokenStore interface {
	IsRevoked(ctx context.Context, jti string) (bool, error)
}

type databaseRevokedTokenStore struct {
	database db.IDatabase
}

func newDatabaseRevokedTokenStore(database db.IDatabase) (*databaseRevokedTokenStore, error) {
	if database == nil {
		return nil, errors.NewCode(errors.InvalidInput, "database is nil")
	}
	return &databaseRevokedTokenStore{database: database}, nil
}

func ensureDatabaseRevokedTokenSchema(ctx context.Context, database db.IDatabase) error {
	if database == nil {
		return errors.NewCode(errors.InvalidInput, "database is nil")
	}
	if ctx == nil {
		return errors.NewCode(errors.InvalidInput, "ctx is nil")
	}
	const ddl = `CREATE TABLE IF NOT EXISTS iam_revoked_tokens (
  jti VARCHAR(191) PRIMARY KEY,
  expires_at BIGINT NOT NULL
)`
	if _, err := database.Exec(ctx, ddl); err != nil {
		return errors.Wrap(err, errors.Internal, "initialize token revocation table failed")
	}
	const indexDDL = `CREATE INDEX IF NOT EXISTS idx_iam_revoked_tokens_expires_at
ON iam_revoked_tokens (expires_at)`
	if _, err := database.Exec(ctx, indexDDL); err != nil {
		return errors.Wrap(err, errors.Internal, "initialize token revocation index failed")
	}
	return nil
}

func (s *databaseRevokedTokenStore) IsRevoked(ctx context.Context, jti string) (bool, error) {
	if ctx == nil {
		return false, errors.NewCode(errors.InvalidInput, "ctx is nil")
	}
	jti = strings.TrimSpace(jti)
	if jti == "" {
		return false, nil
	}
	if s == nil || s.database == nil {
		return false, errors.NewCode(errors.ServiceUnavailable, "token revocation store unavailable")
	}
	var expiresAtUnix int64
	err := s.database.QueryRow(ctx,
		"SELECT expires_at FROM "+revokedTokenTable+" WHERE jti = ?",
		jti,
	).Scan(&expiresAtUnix)
	if stderrors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, errors.Wrap(err, errors.ServiceUnavailable, "query token revocation status failed")
	}
	cleanupBeforeUnix := time.Now().Add(-revokedTokenClockSkew).Unix()
	if expiresAtUnix <= cleanupBeforeUnix {
		_, _ = s.database.Exec(ctx,
			"DELETE FROM "+revokedTokenTable+" WHERE jti = ? AND expires_at <= ?",
			jti,
			cleanupBeforeUnix,
		)
		return false, nil
	}
	return true, nil
}

type jwtClaims struct {
	TokenType      string   `json:"token_type"`
	UserID         int64    `json:"user_id"`
	ActiveScopeID  int64    `json:"active_scope_id"`
	BindingVersion string   `json:"binding_version,omitempty"`
	Permissions    []string `json:"permissions,omitempty"`
	ID             string   `json:"jti,omitempty"`
	ExpiresAt      int64    `json:"exp,omitempty"`
	IssuedAt       int64    `json:"iat,omitempty"`
}

type resolvedAuthContext struct {
	ActiveScopeID   int64
	ActiveScopeKind string
	VisibleScopeIDs []int64
}

type authContextResolver interface {
	ResolveAuthContext(ctx context.Context, claims *jwtClaims) (*resolvedAuthContext, error)
}

type standaloneAuthConfig struct {
	Environment       string
	SecretKey         string
	TokenHeader       string
	TokenPrefix       string
	AllowQueryToken   bool
	SkipPaths         []string
	ContextResolver   authContextResolver
	RevokedTokenStore revokedTokenStore
}

func isDevEnv(environment string) bool {
	switch strings.ToLower(strings.TrimSpace(environment)) {
	case "development", "dev", "test", "testing":
		return true
	default:
		return false
	}
}

func defaultStandaloneAuthConfig(environment string) *standaloneAuthConfig {
	allowQuery := false
	if raw := strings.TrimSpace(os.Getenv("AUTH_ALLOW_QUERY_TOKEN")); raw != "" {
		if parsed, err := strconv.ParseBool(raw); err == nil {
			allowQuery = parsed
		}
	}
	return &standaloneAuthConfig{
		Environment:     strings.TrimSpace(environment),
		SecretKey:       strings.TrimSpace(os.Getenv("AUTH_SECRET")),
		TokenHeader:     "Authorization",
		TokenPrefix:     "Bearer ",
		AllowQueryToken: allowQuery,
		SkipPaths:       []string{"/health", "/ping", "/api/v1/health", "/api/v1/ping", "/api/v1/healthz", "/api/v1/readyz"},
	}
}

func validateStandaloneAuthConfig(config *standaloneAuthConfig) error {
	if config == nil {
		return errors.NewCode(errors.InvalidInput, "auth config is required")
	}
	if strings.TrimSpace(config.SecretKey) == "" {
		return errors.NewCode(errors.Internal, "auth secret is required")
	}
	if !isDevEnv(config.Environment) && config.AllowQueryToken {
		return errors.NewCode(errors.Internal, "生产环境禁止启用 AUTH_ALLOW_QUERY_TOKEN")
	}
	if !isDevEnv(config.Environment) && config.RevokedTokenStore == nil {
		return errors.NewCode(errors.Internal, "非开发环境必须配置持久化 token 吊销存储")
	}
	return nil
}

func newStandaloneAuthConfig(store revokedTokenStore) (*standaloneAuthConfig, error) {
	environment := strings.TrimSpace(os.Getenv("APP_ENV"))
	authConfig := defaultStandaloneAuthConfig(environment)
	authConfig.ContextResolver = standaloneAuthContextResolver{}
	authConfig.RevokedTokenStore = store
	if err := validateStandaloneAuthConfig(authConfig); err != nil {
		return nil, err
	}
	return authConfig, nil
}

func standaloneRouteMiddlewares(logger logging.ILogger, authConfig *standaloneAuthConfig) []httpx.Middleware {
	return []httpx.Middleware{
		middleware.AccessLog(middleware.AccessLogConfig{Logger: logger}),
		standaloneAuthMiddleware(authConfig),
	}
}

func standaloneAuthMiddleware(config *standaloneAuthConfig) httpx.Middleware {
	if config == nil {
		return func(httpx.IContext, func() error) error {
			return errors.NewCode(errors.InvalidInput, "auth config is required")
		}
	}
	var validateOnce sync.Once
	var validateErr error

	return func(ctx httpx.IContext, next func() error) error {
		path := ctx.Path()
		for _, skipPath := range config.SkipPaths {
			if matchSkipPath(path, skipPath) {
				return next()
			}
		}

		token := extractBearerToken(ctx, config)
		if token == "" {
			return errors.NewCode(errors.Unauthorized, "用户未认证")
		}

		validateOnce.Do(func() {
			validateErr = validateStandaloneAuthConfig(config)
		})
		if validateErr != nil {
			return validateErr
		}

		reqCtx := ctx.RequestContext()
		opCtx := context.Background()
		if reqCtx != nil {
			opCtx = reqCtx
		}
		claims, err := validateAccessToken(opCtx, token, config)
		if err != nil {
			return err
		}
		boundCtx, err := injectClaimsRequestContext(reqCtx, claims, config.ContextResolver)
		if err != nil {
			return err
		}
		ctx.SetContext(boundCtx)
		return next()
	}
}

func matchSkipPath(path, skipPath string) bool {
	if skipPath == "" {
		return false
	}
	if strings.HasSuffix(skipPath, "/") {
		return strings.HasPrefix(path, skipPath)
	}
	return path == skipPath || path == skipPath+"/"
}

func extractBearerToken(ctx httpx.IContext, config *standaloneAuthConfig) string {
	if ctx == nil || config == nil {
		return ""
	}
	headerKey := config.TokenHeader
	if headerKey == "" {
		headerKey = "Authorization"
	}
	prefix := config.TokenPrefix
	if prefix == "" {
		prefix = "Bearer "
	}
	raw := ctx.Header(headerKey)
	if strings.HasPrefix(raw, prefix) {
		return strings.TrimSpace(strings.TrimPrefix(raw, prefix))
	}
	if config.AllowQueryToken && isDevEnv(config.Environment) {
		return strings.TrimSpace(ctx.Query("token"))
	}
	return ""
}

func validateAccessToken(ctx context.Context, token string, config *standaloneAuthConfig) (*jwtClaims, error) {
	if ctx == nil {
		return nil, errors.NewCode(errors.InvalidInput, "ctx is nil")
	}
	if config == nil {
		return nil, errors.NewCode(errors.InvalidInput, "auth config is required")
	}
	claims, err := parseToken(token, config.SecretKey)
	if err != nil {
		return nil, err
	}
	if config.RevokedTokenStore != nil {
		revoked, err := config.RevokedTokenStore.IsRevoked(ctx, claims.ID)
		if err != nil {
			return nil, errors.Wrap(err, errors.ServiceUnavailable, "token 吊销状态不可用")
		}
		if revoked {
			return nil, errors.NewCode(errors.Unauthorized, "token 已失效")
		}
	}
	return claims, nil
}

func parseToken(tokenStr, secretKey string) (*jwtClaims, error) {
	if secretKey == "" {
		return nil, errors.NewCode(errors.Unauthorized, "认证配置错误")
	}
	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		return nil, errors.NewCode(errors.Unauthorized, "token 解析失败")
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, errors.NewCode(errors.Unauthorized, "token 解析失败")
	}
	var header struct {
		Alg string `json:"alg"`
	}
	if err := json.Unmarshal(headerBytes, &header); err != nil || header.Alg != "HS256" {
		return nil, errors.NewCode(errors.Unauthorized, "不支持的签名方法")
	}
	mac := hmac.New(sha256.New, []byte(secretKey))
	_, _ = mac.Write([]byte(parts[0] + "." + parts[1]))
	expectedSig := mac.Sum(nil)
	actualSig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || !hmac.Equal(actualSig, expectedSig) {
		return nil, errors.NewCode(errors.Unauthorized, "token 解析失败")
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, errors.NewCode(errors.Unauthorized, "token 解析失败")
	}
	var claims jwtClaims
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return nil, errors.NewCode(errors.Unauthorized, "token 解析失败")
	}
	legacyAccess := claims.TokenType == "" && claims.ActiveScopeID > 0 && strings.TrimSpace(claims.ID) != ""
	if (claims.TokenType != "access" && !legacyAccess) ||
		claims.UserID <= 0 || claims.ActiveScopeID <= 0 || claims.ExpiresAt <= 0 ||
		strings.TrimSpace(claims.ID) == "" || time.Now().Unix() >= claims.ExpiresAt {
		return nil, errors.NewCode(errors.Unauthorized, "无效的token")
	}
	return &claims, nil
}

func injectClaimsRequestContext(
	reqCtx httpx.IRequestContext,
	claims *jwtClaims,
	resolver authContextResolver,
) (httpx.IRequestContext, error) {
	if reqCtx == nil {
		return nil, errors.NewCode(errors.InvalidInput, "request context is nil")
	}
	if claims == nil {
		return nil, errors.NewCode(errors.InvalidInput, "claims are required")
	}
	baseCtx := context.Context(reqCtx)
	var err error
	if claims.UserID > 0 {
		baseCtx, err = contextx.WithUserID(baseCtx, strconv.FormatInt(claims.UserID, 10))
		if err != nil {
			return nil, err
		}
	}
	baseCtx = auth.BindActiveScopeContext(baseCtx, claims.ActiveScopeID, "")
	var resolved *resolvedAuthContext
	if claims.ActiveScopeID > 0 {
		if resolver == nil {
			return nil, errors.NewCode(errors.InvalidInput, "auth context resolver is required")
		}
		resolved, err = resolver.ResolveAuthContext(baseCtx, claims)
		if err != nil {
			return nil, err
		}
		if resolved == nil {
			return nil, errors.NewCode(errors.InvalidInput, "resolved auth context is required")
		}
	}
	principal := auth.Principal{
		SubjectID:     strconv.FormatInt(claims.UserID, 10),
		Permissions:   claims.Permissions,
		ActiveScopeID: claims.ActiveScopeID,
	}
	if resolved != nil {
		principal.ActiveScopeID = resolved.ActiveScopeID
		principal.PermissionScopes = []string{resolved.ActiveScopeKind}
	}
	baseCtx, err = auth.WithPrincipal(baseCtx, principal)
	if err != nil {
		return nil, err
	}
	// 范围资料必须绑定到最终身份，建立 Principal 前的绑定会随身份切换失效。
	if resolved != nil {
		baseCtx = auth.BindActiveScopeContext(baseCtx, resolved.ActiveScopeID, resolved.ActiveScopeKind)
		baseCtx, err = scoped.WithDataScope(baseCtx, scoped.Filtered(resolved.VisibleScopeIDs...))
		if err != nil {
			return nil, err
		}
	}
	return reqCtx.WithContext(baseCtx), nil
}

type standaloneAuthContextResolver struct{}

func (standaloneAuthContextResolver) ResolveAuthContext(_ context.Context, claims *jwtClaims) (*resolvedAuthContext, error) {
	if claims == nil || claims.ActiveScopeID <= 0 {
		return nil, errors.NewCode(errors.Unauthorized, "active scope is required")
	}
	scopeKind, err := standaloneScopeKind()
	if err != nil {
		return nil, err
	}
	return &resolvedAuthContext{
		ActiveScopeID:   claims.ActiveScopeID,
		ActiveScopeKind: scopeKind,
		VisibleScopeIDs: []int64{claims.ActiveScopeID},
	}, nil
}

// standaloneScopeKind 决定 standalone 模式的作用域类型。
// JWTClaims 不携带 scope kind，故由 GOCHEN_WORKFLOW_SCOPE_KIND 显式配置，默认 tenant。
func standaloneScopeKind() (string, error) {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("GOCHEN_WORKFLOW_SCOPE_KIND"))) {
	case string(auth.PermissionScopePlatform):
		return string(auth.PermissionScopePlatform), nil
	case "", string(auth.PermissionScopeTenant):
		return string(auth.PermissionScopeTenant), nil
	default:
		return "", errors.NewCode(errors.InvalidInput, "invalid workflow scope kind").
			WithContext("scope_kind", os.Getenv("GOCHEN_WORKFLOW_SCOPE_KIND"))
	}
}
