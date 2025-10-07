package accelerate

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/GBA-BI/tes-k8s-agent/pkg/localstore"
	"github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient/models"
)

type null struct {
}

var _ Accelerator = (*null)(nil)

// CronCleanFunc ...
func (n *null) CronCleanFunc() (func(), *time.Duration) {
	return nil, nil
}

// ModifySyncTask ...
func (n *null) ModifySyncTask(_ context.Context, _ *models.Task) (accelerateNames []string, err error) {
	return nil, nil
}

// ModifyInputsFiler ...
func (n *null) ModifyInputsFiler(_ *corev1.PodTemplateSpec, _ *localstore.Task) {
	return
}

// ModifyExecutor ...
func (n *null) ModifyExecutor(_ *corev1.PodTemplateSpec, _ *localstore.Task) {
	return
}

// ModifyOutputsFiler ...
func (n *null) ModifyOutputsFiler(_ *corev1.PodTemplateSpec, _ *localstore.Task) {
	return
}

// OnProcessTask ...
func (n *null) OnProcessTask(_ context.Context, _ *localstore.Task) (ctrl.Result, error) {
	return ctrl.Result{}, nil
}

// OnFinishTask ...
func (n *null) OnFinishTask(_ context.Context, _ *localstore.Task) error {
	return nil
}
