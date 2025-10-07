package accelerate

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/GBA-BI/tes-k8s-agent/pkg/accelerate/mounttos"
	"github.com/GBA-BI/tes-k8s-agent/pkg/consts"
	"github.com/GBA-BI/tes-k8s-agent/pkg/crontab"
	"github.com/GBA-BI/tes-k8s-agent/pkg/localstore"
	"github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient"
	"github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient/models"
)

// Accelerator ...
type Accelerator interface {
	// CronCleanFunc returns a cron function and cron duration. This is for clean resources
	CronCleanFunc() (func(), *time.Duration)

	// ModifySyncTask is executed in syncer, modify task and store accelerateNames to local store
	ModifySyncTask(ctx context.Context, taskFull *models.Task) (accelerateNames []string, err error)

	// ModifyInputsFiler is executed before create inputs-filer
	ModifyInputsFiler(podTemplate *corev1.PodTemplateSpec, localTask *localstore.Task)
	// ModifyExecutor is executed before create executor
	ModifyExecutor(podTemplate *corev1.PodTemplateSpec, localTask *localstore.Task)
	// ModifyOutputsFiler is executed before create outputs-filer
	ModifyOutputsFiler(podTemplate *corev1.PodTemplateSpec, localTask *localstore.Task)

	// OnProcessTask is executed when a task begins to process
	OnProcessTask(ctx context.Context, localTask *localstore.Task) (ctrl.Result, error)
	// OnFinishTask is executed when a task is finished
	OnFinishTask(ctx context.Context, localTask *localstore.Task) error
}

// NewAccelerator ...
func NewAccelerator(vetesClient vetesclient.Client, kubeClient ctrlclient.Client, namespace string, opts *Options) (Accelerator, error) {
	switch opts.Type {
	case consts.NullAccelerateType:
		return &null{}, nil
	case consts.MountTOSAccelerateType:
		return mounttos.New(vetesClient, kubeClient, namespace, opts.MountTOS), nil
	default:
		return nil, fmt.Errorf("unsopportted accelerate type: %s", opts.Type)
	}
}

// RegisterCrontab ...
func RegisterCrontab(cron *crontab.Crontab, accelerator Accelerator) error {
	fn, period := accelerator.CronCleanFunc()
	if fn == nil || period == nil {
		return nil
	}
	return cron.RegisterCron(*period, fn)
}
