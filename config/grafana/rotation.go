/*
Copyright 2026 Grafana Labs
*/

package grafana

import (
	"context"

	ujconfig "github.com/crossplane/upjet/v2/pkg/config"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

// rotatingTokenResources rotate by setting the computed ForceNew attribute
// ready_for_rotation, so Terraform replaces them once the early rotation window
// opens. Upjet never replaces a managed resource: it refuses the update with
// CannotUpdateExternalResource, and the token expires without being replaced.
var rotatingTokenResources = []string{
	"grafana_cloud_access_policy_rotating_token",
	"grafana_cloud_stack_service_account_rotating_token",
	"grafana_service_account_rotating_token",
}

func configureRotatingTokens(p *ujconfig.Provider) {
	for _, name := range rotatingTokenResources {
		p.AddResourceConfigurator(name, rotateInPlace)
	}
}

// rotateInPlace turns rotation into an in-place update: the successor token is
// minted inside Update, the managed resource keeps its identity, and Observe
// moves the external name to the new token ID. The predecessor is not deleted
// and stays valid until its own expiration, so consumers holding it get the
// early rotation window to pick up the new one, as they do under Terraform
// with delete_on_destroy unset.
//
// The predecessor is kept even when delete_on_destroy is set, unlike a
// Terraform replacement. Upjet's Update returns no connection details, so the
// connection secret keeps the old key until the next poll's Observe publishes
// the new one; revoking the old token here would leave consumers with a dead
// key for up to a poll interval. delete_on_destroy still applies when the
// managed resource is deleted.
func rotateInPlace(r *ujconfig.Resource) {
	res := r.TerraformResource
	// The generation provider is built from the JSON schema and has no CRUD
	// functions. ForceNew does not affect generated types, so skip it there.
	if res == nil || res.CreateContext == nil || res.UpdateContext == nil {
		return
	}
	res.Schema["ready_for_rotation"].ForceNew = false

	create, update := res.CreateContext, res.UpdateContext
	res.UpdateContext = func(ctx context.Context, d *schema.ResourceData, meta any) diag.Diagnostics {
		if !d.Get("ready_for_rotation").(bool) {
			return update(ctx, d, meta)
		}
		if diags := create(ctx, d, meta); diags.HasError() {
			return diags
		}
		return diag.FromErr(d.Set("ready_for_rotation", false))
	}
}
