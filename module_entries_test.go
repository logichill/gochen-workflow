package workflow

import (
	"context"
	"testing"
	"time"

	"gochen-runtime/di"
	"gochen-runtime/di/basic"
	hostauthz "gochen-runtime/host/authz"
	"gochen-runtime/host/config"
	"gochen-runtime/host/module"
	moduleruntime "gochen-runtime/host/module/runtime"
	"gochen-runtime/http/nethttp"
	workflowservice "gochen-workflow/service"
	"gochen/auth"
	"gochen/clock"
	"gochen/gen"
)

func TestWorkflowModuleActualHostPassesDeclaredEntryGate(t *testing.T) {
	container := basic.New()
	if err := di.RegisterInstance[workflowservice.Store](container, workflowservice.NewMemoryStore()); err != nil {
		t.Fatal(err)
	}
	if err := di.RegisterInstance[gen.IGenerator[string]](container, gen.NewUUIDGenerator()); err != nil {
		t.Fatal(err)
	}
	if err := di.RegisterInstance[clock.IClock](container, clock.NewRealClock()); err != nil {
		t.Fatal(err)
	}
	registry := hostauthz.NewEntryRegistry()
	server := nethttp.NewServer(nil)
	host := moduleruntime.NewHost([]module.ModuleCtor{NewModule}, config.WithContainer(container), config.WithHTTPServer(server), config.WithDisableHealthRoute(true), config.WithEntryValidator(registry))
	if err := host.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := host.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	if err := host.StartBackground(t.Context()); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range registry.Report() {
		if entry.Class == auth.EntryDeclared {
			count++
			if entry.Plan.Descriptor.Resource != "workflow.scope_dataset" {
				t.Fatalf("unexpected workflow resource: %+v", entry)
			}
		} else if entry.Class != auth.EntryAllowed {
			t.Fatalf("invalid workflow entry: %+v", entry)
		}
	}
	if count < 27 {
		t.Fatalf("workflow host only declared %d routes", count)
	}
}
