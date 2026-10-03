/*
Copyright 2026 Grafana Labs
*/

package config

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	xpmeta "github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	xpresource "github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	ujconfig "github.com/crossplane/upjet/v2/pkg/config"
	"github.com/crossplane/upjet/v2/pkg/controller"
	"github.com/crossplane/upjet/v2/pkg/metrics"
	"github.com/crossplane/upjet/v2/pkg/terraform"
	"github.com/grafana/grafana-com-public-clients/go/gcom"
	grafanaProvider "github.com/grafana/terraform-provider-grafana/v4/pkg/provider"
	tfsdk "github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/grafana/crossplane-provider-grafana/v2/apis/cluster/cloud/v1alpha1"
	ossv1alpha1 "github.com/grafana/crossplane-provider-grafana/v2/apis/cluster/oss/v1alpha1"
)

// accessPolicyRegion is the region the access policy rotating token is created
// in. Its external name is "<region>:<token ID>".
const accessPolicyRegion = "eu"

// fakeCloud is an in-memory Grafana Cloud API and Grafana instance API. It
// serves the endpoints the three rotating token resources call:
//   - grafana_cloud_stack_service_account_rotating_token: the stack instance
//     lookup, the stack readiness endpoints and the stack service account
//     token endpoints of the Cloud API;
//   - grafana_cloud_access_policy_rotating_token: the Cloud API token
//     endpoints (/api/v1/tokens), which take a region query parameter;
//   - grafana_service_account_rotating_token: the service account token
//     endpoints of the Grafana instance HTTP API (/api/serviceaccounts).
//
// Each test drives exactly one kind against its own server.
type fakeCloud struct {
	mu      sync.Mutex
	srv     *httptest.Server
	nextID  int64
	tokens  map[string]*fakeToken
	creates int
	updates int
	deletes int
}

type fakeToken struct {
	id             string
	key            string
	name           string
	displayName    string
	accessPolicyID string
	expiration     time.Time
}

func newFakeCloud(t *testing.T) *fakeCloud {
	t.Helper()
	f := &fakeCloud{tokens: map[string]*fakeToken{}}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeCloud) counts() (creates, deletes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.creates, f.deletes
}

func (f *fakeCloud) updateCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.updates
}

func (f *fakeCloud) token(id string) (fakeToken, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	tok, ok := f.tokens[id]
	if !ok {
		return fakeToken{}, false
	}
	return *tok, true
}

// moveIntoRotationWindow simulates the clock advancing: the token now expires
// in one minute, which is inside the early rotation window the tests configure.
func (f *fakeCloud) moveIntoRotationWindow(t *testing.T, id string) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	tok, ok := f.tokens[id]
	if !ok {
		t.Fatalf("token %s does not exist in the fake", id)
	}
	tok.expiration = time.Now().Add(time.Minute).UTC().Truncate(time.Second)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (f *fakeCloud) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch strings.Trim(r.URL.Path, "/") {
	case "login":
		w.WriteHeader(http.StatusOK)
		return
	case "api/health":
		writeJSON(w, map[string]string{"database": "ok"})
		return
	}

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	switch {
	case len(parts) >= 3 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "tokens":
		f.handleAccessPolicyTokens(w, r, parts[1:])
	case len(parts) >= 3 && parts[0] == "api" && parts[1] == "serviceaccounts":
		f.handleServiceAccountTokens(w, r, parts)
	default:
		f.handleStackServiceAccountTokens(w, r, parts)
	}
}

// handleStackServiceAccountTokens serves
// /api/instances/{slug}[/api/serviceaccounts/{sa}/tokens[/{id}]].
func (f *fakeCloud) handleStackServiceAccountTokens(w http.ResponseWriter, r *http.Request, parts []string) {
	switch {
	case len(parts) == 3 && parts[0] == "api" && parts[1] == "instances" && r.Method == http.MethodGet:
		// The stack URL is the fake itself, so readiness checks hit /login and /api/health.
		writeJSON(w, gcom.FormattedApiInstance{Slug: parts[2], Url: f.srv.URL})
	case len(parts) == 7 && parts[6] == "tokens" && r.Method == http.MethodGet:
		out := make([]gcom.GrafanaTokenDTO, 0, len(f.tokens))
		for _, tok := range f.tokens {
			id, err := strconv.ParseInt(tok.id, 10, 64)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			name, exp := tok.name, tok.expiration
			expired := exp.Before(time.Now())
			out = append(out, gcom.GrafanaTokenDTO{Id: &id, Name: &name, Expiration: &exp, HasExpired: &expired})
		}
		writeJSON(w, out)
	case len(parts) == 7 && parts[6] == "tokens" && r.Method == http.MethodPost:
		var req gcom.PostInstanceServiceAccountTokensRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ttl := int32(0)
		if req.SecondsToLive != nil {
			ttl = *req.SecondsToLive
		}
		tok := f.mint(strconv.FormatInt(f.nextID+1, 10), "glsa_fake_key_", req.Name, time.Duration(ttl)*time.Second)
		id, err := strconv.ParseInt(tok.id, 10, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		key, name := tok.key, tok.name
		writeJSON(w, gcom.GrafanaNewApiKeyResult{Id: &id, Key: &key, Name: &name})
	case len(parts) == 8 && parts[6] == "tokens" && r.Method == http.MethodDelete:
		f.deleteToken(w, r, parts[7])
	default:
		http.NotFound(w, r)
	}
}

// handleAccessPolicyTokens serves the Cloud API /api/v1/tokens endpoints, with
// the leading "api" segment already trimmed. Every call must carry the region query parameter.
func (f *fakeCloud) handleAccessPolicyTokens(w http.ResponseWriter, r *http.Request, parts []string) {
	if got := r.URL.Query().Get("region"); got != accessPolicyRegion {
		http.Error(w, "region query parameter = "+got, http.StatusBadRequest)
		return
	}
	authToken := func(tok *fakeToken) gcom.AuthToken {
		id, exp, created := tok.id, tok.expiration, time.Now().UTC()
		display := tok.displayName
		return gcom.AuthToken{Id: &id, AccessPolicyId: tok.accessPolicyID, Name: tok.name, DisplayName: &display, ExpiresAt: &exp, CreatedAt: &created}
	}
	switch {
	case len(parts) == 2 && r.Method == http.MethodPost:
		var req gcom.PostTokensRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ttl := time.Hour
		if req.ExpiresAt != nil {
			ttl = time.Until(*req.ExpiresAt)
		}
		tok := f.mint("policy-token-"+strconv.FormatInt(f.nextID+1, 10), "glc_fake_token_", req.Name, ttl)
		tok.accessPolicyID = req.AccessPolicyId
		tok.displayName = tok.name
		if req.DisplayName != nil && *req.DisplayName != "" {
			tok.displayName = *req.DisplayName
		}
		at := authToken(tok)
		secret := tok.key
		writeJSON(w, gcom.AuthTokenWithSecret{
			Id: at.Id, AccessPolicyId: at.AccessPolicyId, Name: at.Name, DisplayName: at.DisplayName,
			ExpiresAt: at.ExpiresAt, CreatedAt: at.CreatedAt, Token: &secret,
		})
	case len(parts) == 3 && r.Method == http.MethodGet:
		tok, ok := f.tokens[parts[2]]
		if !ok {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, authToken(tok))
	case len(parts) == 3 && r.Method == http.MethodPost:
		// The non-rotating update: only the display name is mutable.
		tok, ok := f.tokens[parts[2]]
		if !ok {
			http.NotFound(w, r)
			return
		}
		var req gcom.PostTokenRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.DisplayName != nil {
			tok.displayName = *req.DisplayName
		}
		f.updates++
		writeJSON(w, authToken(tok))
	case len(parts) == 3 && r.Method == http.MethodDelete:
		f.deleteToken(w, r, parts[2])
	default:
		http.NotFound(w, r)
	}
}

// handleServiceAccountTokens serves the Grafana instance HTTP API
// /api/serviceaccounts/{sa}/tokens[/{id}].
func (f *fakeCloud) handleServiceAccountTokens(w http.ResponseWriter, r *http.Request, parts []string) {
	switch {
	case len(parts) == 4 && parts[3] == "tokens" && r.Method == http.MethodGet:
		out := make([]map[string]any, 0, len(f.tokens))
		for _, tok := range f.tokens {
			id, err := strconv.ParseInt(tok.id, 10, 64)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			out = append(out, map[string]any{
				"id":         id,
				"name":       tok.name,
				"expiration": tok.expiration.Format(time.RFC3339),
				"hasExpired": tok.expiration.Before(time.Now()),
			})
		}
		writeJSON(w, out)
	case len(parts) == 4 && parts[3] == "tokens" && r.Method == http.MethodPost:
		var req struct {
			Name          string `json:"name"`
			SecondsToLive int64  `json:"secondsToLive"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		tok := f.mint(strconv.FormatInt(f.nextID+1, 10), "glsa_fake_key_", req.Name, time.Duration(req.SecondsToLive)*time.Second)
		id, err := strconv.ParseInt(tok.id, 10, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]any{"id": id, "name": tok.name, "key": tok.key})
	case len(parts) == 5 && parts[3] == "tokens" && r.Method == http.MethodDelete:
		f.deleteToken(w, r, parts[4])
	default:
		http.NotFound(w, r)
	}
}

// mint records a new token. The caller holds f.mu.
func (f *fakeCloud) mint(id, keyPrefix, name string, ttl time.Duration) *fakeToken {
	f.nextID++
	f.creates++
	tok := &fakeToken{
		id:         id,
		key:        keyPrefix + strconv.FormatInt(f.nextID, 10),
		name:       name,
		expiration: time.Now().Add(ttl).UTC().Truncate(time.Second),
	}
	f.tokens[tok.id] = tok
	return tok
}

// deleteToken removes a token. The caller holds f.mu.
func (f *fakeCloud) deleteToken(w http.ResponseWriter, r *http.Request, id string) {
	if _, ok := f.tokens[id]; !ok {
		http.NotFound(w, r)
		return
	}
	f.deletes++
	delete(f.tokens, id)
	w.WriteHeader(http.StatusOK)
}

// rotatingKind describes one rotating token resource under test.
type rotatingKind struct {
	name     string
	resource string
	gvk      schema.GroupVersionKind
	// connKey is the connection detail Upjet publishes the token secret under
	// (the sensitive Terraform attribute name, prefixed).
	connKey string
	// providerConfig is the Grafana provider configuration pointing at the fake.
	providerConfig func(baseURL string) map[string]any
	newMR          func(deleteOnDestroy bool) xpresource.Managed
	// tokenID extracts the token ID from the MR's external name, failing the
	// test if the external name does not have the shape this kind uses.
	tokenID func(t *testing.T, mr xpresource.Managed) string
}

func cloudProviderConfig(baseURL string) map[string]any {
	return map[string]any{
		"cloud_api_url":             baseURL,
		"cloud_access_policy_token": "dummy",
	}
}

func initTestMR(mr xpresource.Managed, gvk schema.GroupVersionKind, uid string) {
	mr.SetName("rotating")
	mr.SetUID(types.UID(uid))
	mr.GetObjectKind().SetGroupVersionKind(gvk)
	mr.SetManagementPolicies(xpv2.ManagementPolicies{xpv2.ManagementActionAll})
}

// numericTokenID reads external names that are the bare numeric token ID.
func numericTokenID(t *testing.T, mr xpresource.Managed) string {
	t.Helper()
	name := xpmeta.GetExternalName(mr)
	if _, err := strconv.ParseInt(name, 10, 64); err != nil {
		t.Fatalf("external-name %q is not a token ID: %v", name, err)
	}
	return name
}

func rotatingKinds() []rotatingKind {
	str := func(s string) *string { return &s }
	num := func(n float64) *float64 { return &n }

	return []rotatingKind{
		{
			name:           "StackServiceAccount",
			resource:       "grafana_cloud_stack_service_account_rotating_token",
			gvk:            v1alpha1.StackServiceAccountRotatingToken_GroupVersionKind,
			connKey:        "attribute.key",
			providerConfig: cloudProviderConfig,
			tokenID:        numericTokenID,
			newMR: func(deleteOnDestroy bool) xpresource.Managed {
				mr := &v1alpha1.StackServiceAccountRotatingToken{}
				initTestMR(mr, v1alpha1.StackServiceAccountRotatingToken_GroupVersionKind, "6f2b1c5e-0000-4000-8000-000000000001")
				mr.Spec.ForProvider = v1alpha1.StackServiceAccountRotatingTokenParameters{
					StackSlug:                  str("teststack"),
					ServiceAccountID:           str("1"),
					NamePrefix:                 str("crossplane"),
					SecondsToLive:              num(3600),
					EarlyRotationWindowSeconds: num(600),
					DeleteOnDestroy:            &deleteOnDestroy,
				}
				return mr
			},
		},
		{
			name:           "AccessPolicy",
			resource:       "grafana_cloud_access_policy_rotating_token",
			gvk:            v1alpha1.AccessPolicyRotatingToken_GroupVersionKind,
			connKey:        "attribute.token",
			providerConfig: cloudProviderConfig,
			// The external name is "<region>:<token ID>", the token ID being a string.
			tokenID: func(t *testing.T, mr xpresource.Managed) string {
				t.Helper()
				name := xpmeta.GetExternalName(mr)
				id, ok := strings.CutPrefix(name, accessPolicyRegion+":")
				if !ok || !strings.HasPrefix(id, "policy-token-") {
					t.Fatalf("external-name %q, want %q<token ID>", name, accessPolicyRegion+":")
				}
				return id
			},
			newMR: func(deleteOnDestroy bool) xpresource.Managed {
				mr := &v1alpha1.AccessPolicyRotatingToken{}
				initTestMR(mr, v1alpha1.AccessPolicyRotatingToken_GroupVersionKind, "6f2b1c5e-0000-4000-8000-000000000002")
				mr.Spec.ForProvider = v1alpha1.AccessPolicyRotatingTokenParameters{
					AccessPolicyID:      str("policy-1"),
					Region:              str(accessPolicyRegion),
					NamePrefix:          str("crossplane"),
					ExpireAfter:         str("1h"),
					EarlyRotationWindow: str("10m"),
					DeleteOnDestroy:     &deleteOnDestroy,
				}
				return mr
			},
		},
		{
			name:     "ServiceAccount",
			resource: "grafana_service_account_rotating_token",
			gvk:      ossv1alpha1.ServiceAccountRotatingToken_GroupVersionKind,
			connKey:  "attribute.key",
			providerConfig: func(baseURL string) map[string]any {
				return map[string]any{"url": baseURL, "auth": "glsa_dummy"}
			},
			tokenID: numericTokenID,
			newMR: func(deleteOnDestroy bool) xpresource.Managed {
				mr := &ossv1alpha1.ServiceAccountRotatingToken{}
				initTestMR(mr, ossv1alpha1.ServiceAccountRotatingToken_GroupVersionKind, "6f2b1c5e-0000-4000-8000-000000000003")
				mr.Spec.ForProvider = ossv1alpha1.ServiceAccountRotatingTokenParameters{
					ServiceAccountID:           str("1"),
					NamePrefix:                 str("crossplane"),
					SecondsToLive:              num(3600),
					EarlyRotationWindowSeconds: num(600),
					DeleteOnDestroy:            &deleteOnDestroy,
				}
				return mr
			},
		},
	}
}

// rotationHarness wires the real generated-provider resource configuration to
// the real Upjet Terraform Plugin SDK connector and the real Grafana Terraform
// provider, with only the Grafana HTTP APIs faked.
type rotationHarness struct {
	t    *testing.T
	kind rotatingKind
	api  *fakeCloud
	cfg  *ujconfig.Resource
	meta any
}

func newRotationHarness(t *testing.T, kind rotatingKind) *rotationHarness {
	t.Helper()
	api := newFakeCloud(t)

	p, err := GetProvider(false)
	if err != nil {
		t.Fatalf("GetProvider: %v", err)
	}
	cfg, ok := p.Resources[kind.resource]
	if !ok {
		t.Fatalf("resource %s is not configured", kind.resource)
	}

	// The same construction as internal/clients: configure the Grafana
	// Terraform provider and hand its meta to the Upjet connector.
	tfProvider := grafanaProvider.Provider("crossplane")
	diags := tfProvider.Configure(t.Context(), tfsdk.NewResourceConfigRaw(kind.providerConfig(api.srv.URL)))
	if diags.HasError() {
		t.Fatalf("configure the Grafana provider: %v", diags)
	}
	return &rotationHarness{t: t, kind: kind, api: api, cfg: cfg, meta: tfProvider.Meta()}
}

// newConnector returns a connector with its own OperationTrackerStore, which
// is what a controller process restart discards.
func (h *rotationHarness) newConnector() *controller.TerraformPluginSDKConnector {
	setup := func(context.Context, client.Client, xpresource.Managed) (terraform.Setup, error) {
		return terraform.Setup{Meta: h.meta}, nil
	}
	return controller.NewTerraformPluginSDKConnector(
		fake.NewClientBuilder().Build(),
		setup,
		h.cfg,
		controller.NewOperationStore(logging.NewNopLogger()),
		controller.WithTerraformPluginSDKLogger(logging.NewNopLogger()),
		controller.WithTerraformPluginSDKMetricRecorder(metrics.NewMetricRecorder(h.kind.gvk, nil, time.Minute)),
	)
}

func (h *rotationHarness) connect(conn *controller.TerraformPluginSDKConnector, mr xpresource.Managed) managed.ExternalClient {
	h.t.Helper()
	ec, err := conn.Connect(h.t.Context(), mr)
	if err != nil {
		h.t.Fatalf("Connect: %v", err)
	}
	return ec
}

// observe runs the start of a managed reconcile: Connect then Observe. The MR
// is mutated in place, standing in for the reconciler persisting the
// annotations and status Upjet sets.
func (h *rotationHarness) observe(conn *controller.TerraformPluginSDKConnector, mr xpresource.Managed) (managed.ExternalClient, managed.ExternalObservation) {
	h.t.Helper()
	ec := h.connect(conn, mr)
	obs, err := ec.Observe(h.t.Context(), mr)
	if err != nil {
		h.t.Fatalf("Observe: %v", err)
	}
	return ec, obs
}

// createToken drives the first two reconciles: Observe finds nothing, Create
// mints the first token, and the next Observe sees it up to date.
func (h *rotationHarness) createToken(conn *controller.TerraformPluginSDKConnector, mr xpresource.Managed) (id, key string) {
	h.t.Helper()
	ec, obs := h.observe(conn, mr)
	if obs.ResourceExists {
		h.t.Fatalf("first Observe: ResourceExists = true, want false")
	}
	created, err := ec.Create(h.t.Context(), mr)
	if err != nil {
		h.t.Fatalf("Create: %v", err)
	}
	key = string(created.ConnectionDetails[h.kind.connKey])
	if key == "" {
		h.t.Fatalf("Create returned no %s in connection details: %v", h.kind.connKey, created.ConnectionDetails)
	}

	_, obs = h.observe(conn, mr)
	if !obs.ResourceExists || !obs.ResourceUpToDate {
		h.t.Fatalf("Observe after Create: exists=%t upToDate=%t, want both true", obs.ResourceExists, obs.ResourceUpToDate)
	}
	id = h.kind.tokenID(h.t, mr)
	if got := string(obs.ConnectionDetails[h.kind.connKey]); got != key {
		h.t.Fatalf("Observe connection details %s = %q, want %q", h.kind.connKey, got, key)
	}
	return id, key
}

func TestRotatingToken(t *testing.T) {
	for _, kind := range rotatingKinds() {
		t.Run(kind.name, func(t *testing.T) { testRotatingToken(t, kind) })
	}
}

func testRotatingToken(t *testing.T, kind rotatingKind) {
	// A token inside its early rotation window must be rotated by the Update
	// the managed reconciler issues when Observe reports it is not up to date.
	t.Run("RotatesInPlace", func(t *testing.T) {
		h := newRotationHarness(t, kind)
		conn := h.newConnector()
		mr := kind.newMR(false)

		oldID, oldKey := h.createToken(conn, mr)

		h.api.moveIntoRotationWindow(t, oldID)
		ec, obs := h.observe(conn, mr)
		if !obs.ResourceExists {
			t.Fatalf("Observe in window: ResourceExists = false, want true")
		}
		if obs.ResourceUpToDate {
			t.Fatalf("Observe in window: ResourceUpToDate = true, want false so the reconciler updates")
		}

		if _, err := ec.Update(t.Context(), mr); err != nil {
			t.Fatalf("Update: %v", err)
		}

		_, obs = h.observe(conn, mr)
		if !obs.ResourceExists || !obs.ResourceUpToDate {
			t.Fatalf("Observe after rotation: exists=%t upToDate=%t, want both true", obs.ResourceExists, obs.ResourceUpToDate)
		}
		newID := kind.tokenID(t, mr)
		if newID == oldID {
			t.Fatalf("external-name is still the old token ID %s after rotation", oldID)
		}
		newTok, ok := h.api.token(newID)
		if !ok {
			t.Fatalf("external-name %s is not a token in the fake", newID)
		}
		if got := string(obs.ConnectionDetails[kind.connKey]); got != newTok.key || got == oldKey {
			t.Fatalf("connection details %s = %q, want the new token's key %q (old key %q)", kind.connKey, got, newTok.key, oldKey)
		}

		creates, deletes := h.api.counts()
		if creates != 2 || deletes != 0 {
			t.Fatalf("API calls: creates=%d deletes=%d, want creates=2 deletes=0", creates, deletes)
		}
		if _, ok := h.api.token(oldID); !ok {
			t.Fatalf("predecessor token %s was revoked, want it left to expire", oldID)
		}
	})

	t.Run("SurvivesColdRestartAfterRotation", func(t *testing.T) {
		h := newRotationHarness(t, kind)
		conn := h.newConnector()
		mr := kind.newMR(false)

		oldID, _ := h.createToken(conn, mr)
		oldName := xpmeta.GetExternalName(mr)
		h.api.moveIntoRotationWindow(t, oldID)
		ec, obs := h.observe(conn, mr)
		if obs.ResourceUpToDate {
			t.Fatalf("Observe in window: ResourceUpToDate = true, want false")
		}
		if _, err := ec.Update(t.Context(), mr); err != nil {
			t.Fatalf("Update: %v", err)
		}
		if _, obs = h.observe(conn, mr); !obs.ResourceUpToDate {
			t.Fatalf("Observe after rotation: ResourceUpToDate = false, want true")
		}
		rotatedName := xpmeta.GetExternalName(mr)
		if rotatedName == oldName {
			t.Fatalf("external-name did not move to the new token")
		}
		createsBefore, deletesBefore := h.api.counts()

		// A restarted controller has an empty OperationTrackerStore and rebuilds
		// its Terraform state from what the API server persisted on the MR.
		restarted, ok := mr.DeepCopyObject().(xpresource.Managed)
		if !ok {
			t.Fatalf("DeepCopyObject of %T is not a managed resource", mr)
		}
		_, obs = h.observe(h.newConnector(), restarted)
		if !obs.ResourceExists || !obs.ResourceUpToDate {
			t.Fatalf("Observe after restart: exists=%t upToDate=%t, want both true", obs.ResourceExists, obs.ResourceUpToDate)
		}
		if got := xpmeta.GetExternalName(restarted); got != rotatedName {
			t.Fatalf("external-name after restart = %q, want %q", got, rotatedName)
		}
		if creates, deletes := h.api.counts(); creates != createsBefore || deletes != deletesBefore {
			t.Fatalf("restart changed the API: creates %d->%d deletes %d->%d, want no change", createsBefore, creates, deletesBefore, deletes)
		}
	})

	// Upjet's Delete reuses the diff Observe computed. With a pending rotation
	// the Terraform SDK would replace the token inside Delete, minting a new
	// token as the resource is being removed.
	for name, deleteOnDestroy := range map[string]bool{
		"DeleteInsideWindowCreatesNothing/deleteOnDestroy": true,
		"DeleteInsideWindowCreatesNothing/keepOnDestroy":   false,
	} {
		t.Run(name, func(t *testing.T) {
			h := newRotationHarness(t, kind)
			conn := h.newConnector()
			mr := kind.newMR(deleteOnDestroy)

			id, _ := h.createToken(conn, mr)
			h.api.moveIntoRotationWindow(t, id)
			ec, obs := h.observe(conn, mr)
			if obs.ResourceUpToDate {
				t.Fatalf("Observe in window: ResourceUpToDate = true, want the pending rotation diff")
			}

			now := metav1.Now()
			mr.SetDeletionTimestamp(&now)
			if _, err := ec.Delete(t.Context(), mr); err != nil {
				t.Fatalf("Delete must remove the resource without replacing the token: %v", err)
			}

			creates, deletes := h.api.counts()
			wantDeletes := 0
			if deleteOnDestroy {
				wantDeletes = 1
			}
			if creates != 1 || deletes != wantDeletes {
				t.Fatalf("API calls: creates=%d deletes=%d, want creates=1 (the original) deletes=%d", creates, deletes, wantDeletes)
			}

			_, obs = h.observe(conn, mr)
			if obs.ResourceExists {
				t.Fatalf("Observe after Delete: ResourceExists = true, want false so the finalizer is released")
			}
		})
	}
}

// The access policy rotating token is the only kind whose original Update
// does something: a display name change is sent to the API. The rotation
// wrapper must leave that path alone and not mint a token when nothing is
// rotating.
func TestAccessPolicyRotatingTokenPlainUpdateDoesNotRotate(t *testing.T) {
	var kind rotatingKind
	for _, k := range rotatingKinds() {
		if k.name == "AccessPolicy" {
			kind = k
		}
	}
	h := newRotationHarness(t, kind)
	conn := h.newConnector()
	mr := kind.newMR(false)

	id, key := h.createToken(conn, mr)
	name := xpmeta.GetExternalName(mr)

	ap, ok := mr.(*v1alpha1.AccessPolicyRotatingToken)
	if !ok {
		t.Fatalf("unexpected MR type %T", mr)
	}
	displayName := "renamed"
	ap.Spec.ForProvider.DisplayName = &displayName

	ec, obs := h.observe(conn, mr)
	if !obs.ResourceExists || obs.ResourceUpToDate {
		t.Fatalf("Observe after display name change: exists=%t upToDate=%t, want exists and a pending update", obs.ResourceExists, obs.ResourceUpToDate)
	}
	if _, err := ec.Update(t.Context(), mr); err != nil {
		t.Fatalf("Update: %v", err)
	}

	_, obs = h.observe(conn, mr)
	if !obs.ResourceExists || !obs.ResourceUpToDate {
		t.Fatalf("Observe after Update: exists=%t upToDate=%t, want both true", obs.ResourceExists, obs.ResourceUpToDate)
	}
	if got := xpmeta.GetExternalName(mr); got != name {
		t.Fatalf("external-name = %q after a plain update, want it unchanged at %q", got, name)
	}
	if got := string(obs.ConnectionDetails[kind.connKey]); got != key {
		t.Fatalf("connection details %s = %q, want the original %q", kind.connKey, got, key)
	}
	if creates, deletes := h.api.counts(); creates != 1 || deletes != 0 {
		t.Fatalf("API calls: creates=%d deletes=%d, want creates=1 deletes=0", creates, deletes)
	}
	if h.api.updateCount() != 1 {
		t.Fatalf("display name updates sent to the API = %d, want 1", h.api.updateCount())
	}
	if tok, _ := h.api.token(id); tok.displayName != displayName {
		t.Fatalf("display name at the API = %q, want %q", tok.displayName, displayName)
	}
}
