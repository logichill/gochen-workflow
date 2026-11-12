package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	workflowpermissions "gochen-workflow/permissions"
	"gochen/observe/logging"
)

func TestStandaloneSQLiteStartupAndDeclaredHTTPExecution(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOCHEN_WORKFLOW_ADDR", address)
	t.Setenv("GOCHEN_WORKFLOW_SQLITE_DSN", "file:"+filepath.Join(t.TempDir(), "workflow.db"))
	t.Setenv("GOCHEN_WORKFLOW_LEGACY_SCOPE_ID", "")
	t.Setenv("GOCHEN_WORKFLOW_SCOPE_KIND", "tenant")
	t.Setenv("APP_ENV", "test")
	t.Setenv("AUTH_SECRET", "workflow-startup-test-secret")
	t.Setenv("AUTH_ALLOW_QUERY_TOKEN", "false")
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	runReturned := false
	go func() { done <- run(ctx, logging.NewNoopLogger()) }()
	t.Cleanup(func() {
		cancel()
		if runReturned {
			return
		}
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("standalone shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("standalone server did not shut down")
		}
	})

	client := &http.Client{Timeout: time.Second}
	base := "http://" + address + "/api/v1"
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		response, err := client.Get(base + "/healthz")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("health status = %d", response.StatusCode)
			}
			break
		}
		select {
		case err := <-done:
			runReturned = true
			t.Fatalf("standalone startup failed: %v", err)
		case <-deadline.C:
			t.Fatalf("standalone server did not become ready: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}

	request := func(token string, status int) {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, base+"/workflow/definitions", nil)
		if err != nil {
			t.Fatal(err)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != status {
			t.Fatalf("definitions status = %d, want %d", response.StatusCode, status)
		}
	}
	request("", http.StatusUnauthorized)
	token, err := generateToken(7, 42, "binding-v1", []string{workflowpermissions.WorkflowReadCode}, "workflow-startup-test-secret")
	if err != nil {
		t.Fatal(err)
	}
	request(token, http.StatusOK)
	token, err = generateToken(7, 42, "binding-v1", nil, "workflow-startup-test-secret")
	if err != nil {
		t.Fatal(err)
	}
	request(token, http.StatusForbidden)
}
