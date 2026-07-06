package main

import (
	"context"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	workflow "gochen-workflow"
	workflowservice "gochen-workflow/service"

	iammw "gochen-iam/middleware"
	"gochen/clock"
	"gochen/db"
	"gochen/db/sql/stdsql"
	"gochen/di"
	dibasic "gochen/di/basic"
	"gochen/errors"
	"gochen/host"
	"gochen/httpx"
	"gochen/ident"
	"gochen/logging"
)

const maxTCPPort = 65535

func main() {
	logger := logging.NewLogger(logging.Config{Prefix: "gochen-workflow", Level: logging.InfoLevel, Format: logging.TextFormat})
	if err := run(context.Background(), logger); err != nil {
		logger.Error(context.Background(), "service exited", logging.Error(err))
		os.Exit(1)
	}
}

func run(ctx context.Context, logger logging.ILogger) error {
	installStandaloneAuthContextResolver()
	if err := iammw.ValidateAuthConfig(nil); err != nil {
		return errors.Wrap(err, errors.Internal, "workflow auth config validation failed")
	}

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

	sqlDB, ok := stdsql.DBOf(database)
	if !ok {
		return errors.NewCode(errors.Internal, "workflow database does not expose *sql.DB")
	}

	store, err := workflowservice.NewSQLiteStore(startupCtx, sqlDB)
	if err != nil {
		return errors.Wrap(err, errors.Database, "open workflow store")
	}

	container := dibasic.New()
	if err := di.RegisterInstance[workflowservice.Store](container, store); err != nil {
		return errors.Wrap(err, errors.Internal, "register workflow store")
	}
	if err := di.RegisterInstance[ident.IGenerator[string]](container, ident.DefaultStringGenerator()); err != nil {
		return errors.Wrap(err, errors.Internal, "register workflow id generator")
	}
	if err := di.RegisterInstance[clock.IClock](container, clock.NewRealClock()); err != nil {
		return errors.Wrap(err, errors.Internal, "register workflow clock")
	}

	hostName, port, err := listenAddress(os.Getenv("GOCHEN_WORKFLOW_ADDR"))
	if err != nil {
		return err
	}

	return host.Run(ctx,
		host.WithName("gochen-workflow"),
		host.WithLogger(logger),
		host.WithContainer(container),
		host.WithAddress(hostName),
		host.WithPort(port),
		host.WithBasePath("/api/v1"),
		host.WithRouteMiddlewares(standaloneRouteMiddlewares()...),
		host.WithModuleHTTP("workflow", host.ModuleHTTPConfig{Prefix: "/workflows"}),
		host.WithModules(workflow.NewModule),
		host.WithFailFastOnRouteConflicts(true),
	)
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

func standaloneRouteMiddlewares() []httpx.Middleware {
	return []httpx.Middleware{iammw.AuthMiddleware(nil)}
}

type standaloneAuthContextResolver struct{}

func installStandaloneAuthContextResolver() {
	iammw.InstallAuthContextResolver(standaloneAuthContextResolver{})
}

func (standaloneAuthContextResolver) ResolveAuthContext(_ context.Context, claims *iammw.JWTClaims) (*iammw.ResolvedAuthContext, error) {
	if claims == nil || claims.ActiveScopeID <= 0 {
		return nil, errors.NewCode(errors.Unauthorized, "active scope is required")
	}
	return &iammw.ResolvedAuthContext{
		ActiveScopeID:   claims.ActiveScopeID,
		ActiveScopeKind: standaloneScopeKind(),
		VisibleScopeIDs: []int64{claims.ActiveScopeID},
	}, nil
}

// standaloneScopeKind 决定 standalone 模式的作用域类型。
// JWTClaims 不携带 scope kind，故由 GOCHEN_WORKFLOW_SCOPE_KIND 显式配置，默认 tenant。
func standaloneScopeKind() string {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("GOCHEN_WORKFLOW_SCOPE_KIND"))) {
	case string(iammw.ScopePlatform):
		return string(iammw.ScopePlatform)
	default:
		return string(iammw.ScopeTenant)
	}
}
