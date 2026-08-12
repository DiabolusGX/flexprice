package activities

import (
	"context"
	"time"

	"github.com/flexprice/flexprice/internal/api/dto"
	"github.com/flexprice/flexprice/internal/cache"
	"github.com/flexprice/flexprice/internal/ee/service"
	ierr "github.com/flexprice/flexprice/internal/errors"
	"github.com/flexprice/flexprice/internal/interfaces"
	"github.com/flexprice/flexprice/internal/logger"
	"github.com/flexprice/flexprice/internal/types"
	"go.temporal.io/sdk/activity"
)

const ActivityPrefix = "PlanActivities"

// PlanActivities contains all plan-related activities
// When registered with Temporal, methods will be called as "PlanActivities.SyncPlanPrices"
type PlanActivities struct {
	planService service.PlanService
}

// NewPlanActivities creates a new PlanActivities instance
func NewPlanActivities(planService service.PlanService) *PlanActivities {
	return &PlanActivities{
		planService: planService,
	}
}

// SyncPlanPricesInput represents the input for the SyncPlanPrices activity
type SyncPlanPricesInput struct {
	PlanID        string `json:"plan_id"`
	TenantID      string `json:"tenant_id"`
	UserID        string `json:"user_id"`
	EnvironmentID string `json:"environment_id"`
}

// SyncPlanPrices syncs plan prices
// This method will be registered as "SyncPlanPrices" in Temporal
func (a *PlanActivities) SyncPlanPrices(ctx context.Context, input SyncPlanPricesInput) (*dto.SyncPlanPricesResponse, error) {

	// Validate input parameters
	if input.PlanID == "" {
		return nil, ierr.NewError("plan ID is required").
			WithHint("Plan ID is required").
			Mark(ierr.ErrValidation)
	}

	if input.TenantID == "" || input.EnvironmentID == "" {
		return nil, ierr.NewError("tenant ID and environment ID are required").
			WithHint("Tenant ID and environment ID are required").
			Mark(ierr.ErrValidation)
	}

	ctx = types.SetTenantID(ctx, input.TenantID)
	ctx = types.SetEnvironmentID(ctx, input.EnvironmentID)
	ctx = types.SetUserID(ctx, input.UserID)

	lockKey := cache.PrefixPriceSyncLock + input.PlanID
	log := logger.NewNoopLogger()
	defer func() {
		redisCache := cache.GetRedisCache()
		if redisCache == nil {
			log.Info(context.Background(), "price_sync_lock_release_skipped", "plan_id", input.PlanID, "lock_key", lockKey, "reason", "redis_cache_nil")
			return
		}
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		redisCache.Delete(releaseCtx, lockKey)
		log.Info(ctx, "price_sync_lock_released", "plan_id", input.PlanID, "lock_key", lockKey)
	}()

	result, err := a.planService.SyncPlanPrices(ctx, input.PlanID)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// SyncPlanPricesV2 is the V2 (sequence-driven) plan-price sync activity. It
// reuses the same input shape, lock-release semantics, and response shape as
// SyncPlanPrices so the workflow code stays minimal — the algorithmic
// difference lives in planService.SyncPlanPricesV2.
//
// Kept for backward compatibility with in-flight workflows scheduled before the
// fanout deploy — new workflow runs should call SyncPlanPricesV2Shard instead.
func (a *PlanActivities) SyncPlanPricesV2(ctx context.Context, input SyncPlanPricesInput) (*dto.SyncPlanPricesResponse, error) {
	return a.SyncPlanPricesV2Shard(ctx, SyncPlanPricesShardInput{
		SyncPlanPricesInput: input,
		ShardCount:          0,
		ShardIdx:            0,
	})
}

// SyncPlanPricesShardInput carries the same core fields as SyncPlanPricesInput
// plus the shard partition assignment. ShardCount<=1 means "no sharding — this
// activity processes every stale sub".
type SyncPlanPricesShardInput struct {
	SyncPlanPricesInput
	ShardCount int `json:"shard_count"`
	ShardIdx   int `json:"shard_idx"`
}

// SyncPlanPricesV2Shard is the shard-aware V2 activity. The workflow fans out
// N of these in parallel to divide the stale-sub set. It heartbeats after each
// page so a stuck DB call is detected within HeartbeatTimeout, and so a worker
// crash mid-run doesn't wait out the full StartToCloseTimeout before restart.
func (a *PlanActivities) SyncPlanPricesV2Shard(ctx context.Context, input SyncPlanPricesShardInput) (*dto.SyncPlanPricesResponse, error) {
	if input.PlanID == "" {
		return nil, ierr.NewError("plan ID is required").
			WithHint("Plan ID is required").
			Mark(ierr.ErrValidation)
	}
	if input.TenantID == "" || input.EnvironmentID == "" {
		return nil, ierr.NewError("tenant ID and environment ID are required").
			WithHint("Tenant ID and environment ID are required").
			Mark(ierr.ErrValidation)
	}
	if input.ShardCount < 0 || input.ShardIdx < 0 || (input.ShardCount > 0 && input.ShardIdx >= input.ShardCount) {
		return nil, ierr.NewError("invalid shard params").
			WithReportableDetails(map[string]interface{}{
				"shard_count": input.ShardCount,
				"shard_idx":   input.ShardIdx,
			}).
			Mark(ierr.ErrValidation)
	}

	ctx = types.SetTenantID(ctx, input.TenantID)
	ctx = types.SetEnvironmentID(ctx, input.EnvironmentID)
	ctx = types.SetUserID(ctx, input.UserID)

	// The plan-wide sync lock is NOT released here. With fanout, N shard
	// activities run against the same plan lock; if each shard released it,
	// the first finisher would let a second sync start while later shards
	// are still writing. The workflow releases the lock exactly once via
	// ReleasePriceSyncLock after all shards complete.

	return a.planService.SyncPlanPricesV2Shard(ctx, input.PlanID, interfaces.SyncPlanPricesV2Options{
		ShardCount: input.ShardCount,
		ShardIdx:   input.ShardIdx,
		OnPageComplete: func(page int) {
			activity.RecordHeartbeat(ctx, page)
		},
	})
}

// ReleasePriceSyncLockInput is the input to ReleasePriceSyncLock.
type ReleasePriceSyncLockInput struct {
	PlanID string `json:"plan_id"`
}

// ReleasePriceSyncLock deletes the Redis lock the API handler acquired before
// starting the V2 sync workflow. Called by PriceSyncV2Workflow after every
// shard has completed so the lock lifetime matches "the whole plan is caught
// up", not "the first shard finished". Safe to no-op when Redis is
// unavailable — the lock TTL (see cache.ExpiryPriceSyncLock) is the backstop.
func (a *PlanActivities) ReleasePriceSyncLock(ctx context.Context, input ReleasePriceSyncLockInput) error {
	if input.PlanID == "" {
		return ierr.NewError("plan ID is required").
			WithHint("Plan ID is required").
			Mark(ierr.ErrValidation)
	}
	lockKey := cache.PrefixPriceSyncLock + input.PlanID
	log := logger.NewNoopLogger()
	redisCache := cache.GetRedisCache()
	if redisCache == nil {
		log.Info(ctx, "price_sync_lock_release_skipped", "plan_id", input.PlanID, "lock_key", lockKey, "reason", "redis_cache_nil")
		return nil
	}
	releaseCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	redisCache.Delete(releaseCtx, lockKey)
	log.Info(ctx, "price_sync_lock_released", "plan_id", input.PlanID, "lock_key", lockKey, "version", "v2")
	return nil
}

