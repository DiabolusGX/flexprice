package models

import (
	ierr "github.com/flexprice/flexprice/internal/errors"
)

// PriceSyncWorkflowInput represents input for the price sync workflow.
//
// ShardCount only affects PriceSyncV2Workflow: when >1, the workflow fans out
// that many parallel SyncPlanPricesV2Shard activities that split the stale-sub
// set by hashtext(id) % ShardCount. Left at 0 the workflow uses
// DefaultPriceSyncShardCount.
type PriceSyncWorkflowInput struct {
	PlanID        string `json:"plan_id"`
	TenantID      string `json:"tenant_id"`
	EnvironmentID string `json:"environment_id"`
	UserID        string `json:"user_id"`
	ShardCount    int    `json:"shard_count,omitempty"`
}

// DefaultPriceSyncShardCount is used when PriceSyncWorkflowInput.ShardCount
// is unset. 8 shards is a conservative default that keeps parallel writers
// comfortably below Postgres connection-pool + index-contention limits for
// the SLI/subscriptions tables on RDS-class hardware. Raise per plan via
// PriceSyncWorkflowInput.ShardCount when a specific run is much larger.
const DefaultPriceSyncShardCount = 8

func (p *PriceSyncWorkflowInput) Validate() error {
	if p.PlanID == "" {
		return ierr.NewError("plan ID is required").
			WithHint("Plan ID is required").
			Mark(ierr.ErrValidation)
	}

	if p.TenantID == "" || p.EnvironmentID == "" || p.UserID == "" {
		return ierr.NewError("tenant ID, environment ID and user ID are required").
			WithHint("Tenant ID and environment ID are required").
			Mark(ierr.ErrValidation)
	}

	if p.ShardCount < 0 {
		return ierr.NewError("shard_count must be non-negative").
			WithHint("Leave shard_count unset for the default, or provide a positive integer").
			Mark(ierr.ErrValidation)
	}

	return nil
}

// QuickBooksPriceSyncWorkflowInput represents input for the QuickBooks price sync workflow
type QuickBooksPriceSyncWorkflowInput struct {
	PriceID       string `json:"price_id"`
	PlanID        string `json:"plan_id"`
	TenantID      string `json:"tenant_id"`
	EnvironmentID string `json:"environment_id"`
	UserID        string `json:"user_id"`
}

func (q *QuickBooksPriceSyncWorkflowInput) Validate() error {
	if q.PriceID == "" || q.PlanID == "" {
		return ierr.NewError("price ID and plan ID are required").
			WithHint("Price ID and Plan ID are required").
			Mark(ierr.ErrValidation)
	}

	if q.TenantID == "" || q.EnvironmentID == "" || q.UserID == "" {
		return ierr.NewError("tenant ID, environment ID and user ID are required").
			WithHint("Tenant ID, environment ID and user ID are required").
			Mark(ierr.ErrValidation)
	}

	return nil
}
