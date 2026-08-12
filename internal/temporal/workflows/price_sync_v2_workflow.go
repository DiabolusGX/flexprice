// internal/temporal/workflows/price_sync_v2_workflow.go
package workflows

import (
	"time"

	"github.com/flexprice/flexprice/internal/api/dto"
	planActivities "github.com/flexprice/flexprice/internal/temporal/activities/plan"
	"github.com/flexprice/flexprice/internal/temporal/models"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	// V2 workflow + activity names. Coexists with V1 (PriceSyncWorkflow /
	// SyncPlanPrices); the API handler picks one based on
	// PlanPriceSyncConfig.UseV2ForPlan.
	//
	// SyncPlanPricesV2 stays registered for backward compat with in-flight
	// workflows started before the shard-fanout deploy — new workflow runs
	// invoke SyncPlanPricesV2Shard instead.
	WorkflowPriceSyncV2           = "PriceSyncV2Workflow"
	ActivitySyncPlanPricesV2      = "SyncPlanPricesV2"
	ActivitySyncPlanPricesV2Shard = "SyncPlanPricesV2Shard"
	ActivityReleasePriceSyncLock  = "ReleasePriceSyncLock"

	// priceSyncV2FanoutVersion is the workflow.GetVersion tag guarding the
	// fanout path. Workflows started before the deploy replay under
	// DefaultVersion (single activity); new workflows run the fanout code
	// path. Never rename this constant — Temporal keys history events off
	// the literal string.
	priceSyncV2FanoutVersion = "price-sync-v2-fanout"
)

// PriceSyncV2Workflow runs the sequence-driven plan-price sync.
//
// Fanout: when input.ShardCount > 1 (or the default kicks in), the workflow
// dispatches that many SyncPlanPricesV2Shard activities in parallel. Each
// shard runs the same discover → create → terminate → stamp loop but scoped
// to a disjoint slice of stale subs (partitioned by hashtext(id) % ShardCount).
// One shard failing does NOT cancel the others — the workflow still returns
// an aggregated result and propagates the first shard error at the end.
//
// Backward compatibility: workflows scheduled before this deploy replay under
// workflow.DefaultVersion, which keeps the pre-fanout single-activity path so
// their recorded history stays deterministic.
func PriceSyncV2Workflow(ctx workflow.Context, in models.PriceSyncWorkflowInput) (*dto.SyncPlanPricesResponse, error) {
	if err := in.Validate(); err != nil {
		return nil, err
	}

	// Retry policy is shared by the pre-fanout activity and each shard.
	// Shards are per-partition, so a retry re-processes only that partition's
	// subs — stamping makes the retry pick up from where it left off.
	retry := &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute * 5,
		MaximumAttempts:    3,
	}

	version := workflow.GetVersion(ctx, priceSyncV2FanoutVersion, workflow.DefaultVersion, 1)

	if version == workflow.DefaultVersion {
		// Legacy path — kept only to make already-scheduled workflows
		// deterministic under replay after this deploy.
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: time.Hour * 3,
			RetryPolicy:         retry,
		}
		ctx = workflow.WithActivityOptions(ctx, ao)
		var out dto.SyncPlanPricesResponse
		if err := workflow.ExecuteActivity(ctx, ActivitySyncPlanPricesV2, planActivities.SyncPlanPricesInput{
			PlanID:        in.PlanID,
			TenantID:      in.TenantID,
			EnvironmentID: in.EnvironmentID,
			UserID:        in.UserID,
		}).Get(ctx, &out); err != nil {
			return nil, err
		}
		return &out, nil
	}

	shardCount := in.ShardCount
	if shardCount <= 0 {
		shardCount = models.DefaultPriceSyncShardCount
	}

	// Activity options for the shard fanout.
	// - StartToCloseTimeout is generous because even a fully parallel run
	//   over multiple millions of subs can take hours; the tight bound is
	//   HeartbeatTimeout, which detects a hung DB call within minutes.
	// - HeartbeatTimeout matches the per-page cadence set in
	//   SyncPlanPricesV2Shard (one heartbeat per page after stamping).
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour * 3,
		HeartbeatTimeout:    time.Minute * 2,
		RetryPolicy:         retry,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	futures := make([]workflow.Future, shardCount)
	for i := 0; i < shardCount; i++ {
		futures[i] = workflow.ExecuteActivity(ctx, ActivitySyncPlanPricesV2Shard, planActivities.SyncPlanPricesShardInput{
			SyncPlanPricesInput: planActivities.SyncPlanPricesInput{
				PlanID:        in.PlanID,
				TenantID:      in.TenantID,
				EnvironmentID: in.EnvironmentID,
				UserID:        in.UserID,
			},
			ShardCount: shardCount,
			ShardIdx:   i,
		})
	}

	// Collect every shard's result before returning — even on error, so
	// partial progress is visible in the aggregated summary. Report the
	// first shard error at the end; a retried workflow re-runs all shards
	// but each shard skips already-stamped subs via its discovery filter.
	agg := dto.SyncPlanPricesResponse{
		PlanID:  in.PlanID,
		Message: "Plan prices synchronized to subscription line items successfully (v2, fanout)",
	}
	var firstErr error
	for i, f := range futures {
		var out dto.SyncPlanPricesResponse
		if err := f.Get(ctx, &out); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			workflow.GetLogger(ctx).Error("price sync v2 shard failed",
				"plan_id", in.PlanID,
				"shard_idx", i,
				"shard_count", shardCount,
				"error", err)
			continue
		}
		agg.Summary.LineItemsFoundForCreation += out.Summary.LineItemsFoundForCreation
		agg.Summary.LineItemsCreated += out.Summary.LineItemsCreated
		agg.Summary.LineItemsTerminated += out.Summary.LineItemsTerminated
	}

	// Release the plan-wide sync lock exactly once, after all shards have
	// completed (or errored out). A disconnected context ensures the release
	// still runs if the workflow itself was cancelled mid-fanout. Uses a
	// short timeout + no retry: the Redis lock TTL is the backstop, so a
	// missed release just delays the next allowed sync by at most the TTL.
	releaseCtx, _ := workflow.NewDisconnectedContext(ctx)
	releaseCtx = workflow.WithActivityOptions(releaseCtx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumAttempts:    2,
		},
	})
	if err := workflow.ExecuteActivity(releaseCtx, ActivityReleasePriceSyncLock, planActivities.ReleasePriceSyncLockInput{
		PlanID: in.PlanID,
	}).Get(releaseCtx, nil); err != nil {
		// Non-fatal — the TTL will release eventually. Log and continue.
		workflow.GetLogger(ctx).Error("price sync v2 lock release failed",
			"plan_id", in.PlanID,
			"error", err)
	}

	if firstErr != nil {
		return &agg, firstErr
	}
	return &agg, nil
}
