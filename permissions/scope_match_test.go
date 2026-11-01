package permissions

import (
	"context"
	"testing"

	auth "gochen-runtime/host/authz"
)

func TestMatchesPermissionScopesFailsClosedWithoutScopeInfo(t *testing.T) {
	principal := auth.Principal{SubjectID: "7"}
	if matchesPermissionScopes(context.Background(), principal, []string{string(auth.PermissionScopePlatform)}) {
		t.Fatal("expected fail-closed denial when neither principal nor context carries scope info")
	}
}

func TestMatchesPermissionScopesUsesPrincipalScopes(t *testing.T) {
	principal := auth.Principal{SubjectID: "7", PermissionScopes: []string{string(auth.PermissionScopeTenant)}}
	if !matchesPermissionScopes(context.Background(), principal, []string{string(auth.PermissionScopePlatform), string(auth.PermissionScopeTenant)}) {
		t.Fatal("expected tenant principal to match tenant-required permission")
	}
	if matchesPermissionScopes(context.Background(), principal, []string{string(auth.PermissionScopePlatform)}) {
		t.Fatal("expected tenant principal to be denied platform-only permission")
	}
}
