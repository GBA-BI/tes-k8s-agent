package syncer

import (
	"context"
	"errors"
	"fmt"

	"github.com/GBA-BI/tes-k8s-agent/pkg/consts"
	"github.com/GBA-BI/tes-k8s-agent/pkg/localstore"
	"github.com/GBA-BI/tes-k8s-agent/pkg/utils"
	"github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient/models"
)

func (s *syncer) syncCancelingTask(ctx context.Context, task taskMinimal) error {
	taskInfo, err := s.localStoreHelper.GetTask(ctx, task.id)
	if err == nil {
		if taskInfo.Stop != nil {
			return nil
		}
		return s.localStoreHelper.StopTask(ctx, task.id, consts.TaskCanceled)
	}
	if !errors.Is(err, localstore.ErrNotFound) {
		return err
	}
	if _, err := s.vetesClient.UpdateTask(ctx, &models.UpdateTaskRequest{ID: task.id, State: utils.Point(consts.TaskCanceled)}); err != nil {
		return fmt.Errorf("failed to directly cancel task: %w", err)
	}
	return nil
}
