package syncer

import (
	"context"
	"fmt"
	"sync"

	"github.com/GBA-BI/tes-k8s-agent/pkg/log"
	"github.com/panjf2000/ants/v2"

	"github.com/GBA-BI/tes-k8s-agent/pkg/accelerate"
	"github.com/GBA-BI/tes-k8s-agent/pkg/consts"
	"github.com/GBA-BI/tes-k8s-agent/pkg/crontab"
	"github.com/GBA-BI/tes-k8s-agent/pkg/localstore"
	"github.com/GBA-BI/tes-k8s-agent/pkg/offload"
	"github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient"
	"github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient/models"
)

type syncer struct {
	vetesClient      vetesclient.Client
	localStoreHelper localstore.Helper
	offloadHelper    offload.Helper
	accelerator      accelerate.Accelerator
	clusterID        string
	concurrency      int
	offloadThreshold int // for test
}

// RegisterCrontab ...
func RegisterCrontab(cron *crontab.Crontab, vetesClient vetesclient.Client, localStoreHelper localstore.Helper,
	offloadHelper offload.Helper, accelerator accelerate.Accelerator, clusterID string, opts *Options) error {
	s := &syncer{
		vetesClient:      vetesClient,
		localStoreHelper: localStoreHelper,
		offloadHelper:    offloadHelper,
		accelerator:      accelerator,
		clusterID:        clusterID,
		concurrency:      opts.Concurrency,
		offloadThreshold: consts.OffloadThreshold,
	}

	return cron.RegisterCron(opts.Period, func() {
		ctx := context.Background()
		if err := s.syncTasks(ctx); err != nil {
			log.Errorw("sync tasks failed", "err", err)
		}
	})
}

func (s *syncer) syncTasks(ctx context.Context) error {
	tasks, err := s.listTasks(ctx)
	if err != nil {
		return err
	}

	taskPool, err := ants.NewPool(s.concurrency)
	if err != nil { // never
		return fmt.Errorf("failed to initialize submission goroutine pool: %w", err)
	}
	defer taskPool.Release()

	var wg sync.WaitGroup
	for _, task := range tasks {
		localTask := taskMinimal{id: task.ID, state: task.State}
		wg.Add(1)
		if err := taskPool.Submit(func() {
			defer wg.Done()
			if err := s.syncTask(ctx, localTask); err != nil {
				log.Errorw("failed to sync task", "task", task.ID, "err", err)
			}
		}); err != nil {
			log.Errorw("failed to submit to task pool", "err", err)
			wg.Done()
		}
	}
	wg.Wait()

	return nil
}

func (s *syncer) listTasks(ctx context.Context) ([]*models.Task, error) {
	res := make([]*models.Task, 0)
	var pageToken string
	for {
		resp, err := s.vetesClient.ListTasks(ctx, &models.ListTasksRequest{
			State:     []string{consts.TaskQueued, consts.TaskCanceling},
			ClusterID: s.clusterID,
			View:      consts.MinimalView,
			PageSize:  consts.MaximumPageSize,
			PageToken: pageToken,
		})
		if err != nil {
			return nil, err
		}
		res = append(res, resp.Tasks...)
		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}
	return res, nil
}

type taskMinimal struct {
	id    string
	state string
}

func (s *syncer) syncTask(ctx context.Context, task taskMinimal) error {
	if task.state == consts.TaskQueued {
		return s.syncQueuedTask(ctx, task)
	}
	if task.state == consts.TaskCanceling {
		return s.syncCancelingTask(ctx, task)
	}
	return nil
}
