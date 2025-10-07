package runner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/GBA-BI/tes-k8s-agent/pkg/log"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/GBA-BI/tes-k8s-agent/pkg/accelerate"
	"github.com/GBA-BI/tes-k8s-agent/pkg/crontab"
	"github.com/GBA-BI/tes-k8s-agent/pkg/filelog"
	"github.com/GBA-BI/tes-k8s-agent/pkg/localstore"
	"github.com/GBA-BI/tes-k8s-agent/pkg/offload"
	"github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient"
)

// Runner ...
type Runner struct {
	opts *Options

	vetesClient      vetesclient.Client
	localStoreHelper localstore.Helper
	offloadHelper    offload.Helper
	accelerator      accelerate.Accelerator
	kubeClientNative kubernetes.Interface
	kubeClient       ctrlclient.Client
	clusterID        string
	namespace        string

	filerResources corev1.ResourceRequirements

	taskProcessingLock sync.Mutex
	taskProcessing     map[string]struct{}
}

// New ...
func New(vetesClient vetesclient.Client, localStoreHelper localstore.Helper, offloadHelper offload.Helper, accelerator accelerate.Accelerator,
	kubeClientNative kubernetes.Interface, kubeClient ctrlclient.Client, clusterID, namespace string, opts *Options) (*Runner, error) {
	res := &Runner{
		opts:             opts,
		vetesClient:      vetesClient,
		localStoreHelper: localStoreHelper,
		offloadHelper:    offloadHelper,
		accelerator:      accelerator,
		kubeClientNative: kubeClientNative,
		kubeClient:       kubeClient,
		clusterID:        clusterID,
		namespace:        namespace,
	}

	res.filerResources.Requests = make(corev1.ResourceList)
	for resourceName, quantity := range opts.FilerResources.Requests {
		res.filerResources.Requests[corev1.ResourceName(resourceName)] = resource.MustParse(quantity)
	}
	res.filerResources.Limits = make(corev1.ResourceList)
	for resourceName, quantity := range opts.FilerResources.Limits {
		res.filerResources.Limits[corev1.ResourceName(resourceName)] = resource.MustParse(quantity)
	}
	var err error
	if opts.S3.Enable {
		if opts.S3.SDKConfigmapName != "" {
			if _, err = kubeClientNative.CoreV1().ConfigMaps(namespace).Get(context.Background(), opts.S3.SDKConfigmapName, metav1.GetOptions{}); err != nil {
				return nil, fmt.Errorf("failed to get s3 sdk configmap %s: %w", opts.S3.SDKConfigmapName, err)
			}
		}
		if opts.S3.StaticSecretName != "" {
			if _, err = kubeClientNative.CoreV1().Secrets(namespace).Get(context.Background(), opts.S3.StaticSecretName, metav1.GetOptions{}); err != nil {
				return nil, fmt.Errorf("failed to get s3 static secret %s: %w", opts.S3.StaticSecretName, err)
			}
		}
	}

	if opts.FilerImage.ImagePullSecretName != "" {
		if _, err = kubeClientNative.CoreV1().Secrets(namespace).Get(context.Background(), opts.FilerImage.ImagePullSecretName, metav1.GetOptions{}); err != nil {
			return nil, fmt.Errorf("failed to get filer image pull secret %s: %w", opts.FilerImage.ImagePullSecretName, err)
		}
	}
	if opts.ExecutorImagePullSecret.StaticName != "" {
		if _, err = kubeClientNative.CoreV1().Secrets(namespace).Get(context.Background(), opts.ExecutorImagePullSecret.StaticName, metav1.GetOptions{}); err != nil {
			return nil, fmt.Errorf("failed to get static executor image pull secret %s: %w", opts.ExecutorImagePullSecret.StaticName, err)
		}
	}

	res.taskProcessing = make(map[string]struct{})

	return res, nil
}

func (r *Runner) taskLogger(taskID string) filelog.Logger {
	return filelog.NewLoggerWithWriteToFile(filepath.Join(r.opts.TaskLog.OutputDir, taskID, taskLogFileName))
}

func (r *Runner) removeTaskLogFile(taskID string) {
	_ = os.RemoveAll(filepath.Join(r.opts.TaskLog.OutputDir, taskID))
}

func (r *Runner) tryProcessTask(taskID string) bool {
	r.taskProcessingLock.Lock()
	defer r.taskProcessingLock.Unlock()

	if _, ok := r.taskProcessing[taskID]; ok {
		return false
	}
	r.taskProcessing[taskID] = struct{}{}
	return true
}

func (r *Runner) releaseProcessTask(taskID string) {
	r.taskProcessingLock.Lock()
	defer r.taskProcessingLock.Unlock()
	delete(r.taskProcessing, taskID)
}

const tryProcessLatency = time.Second

const taskLogFileName = "app.log"

// RegisterCrontab ...
func RegisterCrontab(cron *crontab.Crontab, runner *Runner) error {
	return cron.RegisterCron(time.Hour, runner.cleanTaskLogFiles)
}

// Sometimes, task log file or directory will remain after task finished, because multiple
// reconciles, or mounted pod terminating. We can clean these files periodically.
func (r *Runner) cleanTaskLogFiles() {
	files, err := os.ReadDir(r.opts.TaskLog.OutputDir)
	if err != nil {
		log.Warnw("failed to list task log files", "err", err)
		return
	}
	ctx := context.Background()
	for _, file := range files {
		if !file.IsDir() {
			continue
		}
		taskID := file.Name()
		_, err = r.localStoreHelper.GetTask(ctx, taskID)
		if err == nil {
			continue
		}
		if !errors.Is(err, localstore.ErrNotFound) {
			log.Warnw("failed to get task from local store", "task", taskID, "err", err)
			continue
		}
		r.removeTaskLogFile(taskID)
	}
}
