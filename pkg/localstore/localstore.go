package localstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/GBA-BI/tes-k8s-agent/pkg/log"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	k8sapierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/cluster-api/util/patch"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/GBA-BI/tes-k8s-agent/pkg/consts"
	"github.com/GBA-BI/tes-k8s-agent/pkg/utils"
)

// ErrNotFound ...
var ErrNotFound = errors.New("not found")

// Helper ...
type Helper interface {
	StoreTask(ctx context.Context, task *Task) error
	GetTask(ctx context.Context, taskID string) (*TaskInfo, error)
	StopTask(ctx context.Context, taskID, state string) error
	DeleteTask(ctx context.Context, taskID string) error
	RecordTaskStage(ctx context.Context, taskID string, stage int) error
	RecordTaskExecutorStage(ctx context.Context, taskID string, stage int) error
	StoreType() ctrlclient.Object
}

type impl struct {
	kubeClient ctrlclient.Client
	namespace  string
}

// NewHelper ...
func NewHelper(kubeClient ctrlclient.Client, namespace string) Helper {
	return &impl{
		kubeClient: kubeClient,
		namespace:  namespace,
	}
}

// StoreTask ...
func (i *impl) StoreTask(ctx context.Context, task *Task) error {
	var taskYaml bytes.Buffer
	encoder := yaml.NewEncoder(&taskYaml)
	encoder.SetIndent(2)
	if err := encoder.Encode(task); err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}
	configmap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: i.namespace,
			Name:      configmapName(task.ID),
			Labels: map[string]string{
				consts.LabelTaskID: task.ID,
			},
			Finalizers: []string{consts.ProcessTaskFinalizer},
		},
		Data: map[string]string{
			task.ID: taskYaml.String(),
		},
	}
	if err := i.kubeClient.Create(ctx, configmap); err != nil {
		return fmt.Errorf("failed to create configmap: %w", err)
	}
	return nil
}

// GetTask ...
func (i *impl) GetTask(ctx context.Context, taskID string) (taskInfo *TaskInfo, err error) {
	configmapKey := ctrlclient.ObjectKey{Namespace: i.namespace, Name: configmapName(taskID)}
	configmap := &corev1.ConfigMap{}
	if err = i.kubeClient.Get(ctx, configmapKey, configmap); err != nil {
		if k8sapierrors.IsNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to get configmap: %w", err)
	}
	taskYaml, ok := configmap.Data[taskID]
	if !ok {
		return nil, errors.New("empty configmap data")
	}
	taskInfo = new(TaskInfo)
	if err = yaml.Unmarshal([]byte(taskYaml), &taskInfo.Task); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task: %w", err)
	}
	if configmap.Annotations != nil {
		if stopState, ok := configmap.Annotations[consts.AnnoStop]; ok {
			taskInfo.Stop = utils.Point(stopState)
		}
		if stageStr, ok := configmap.Annotations[consts.AnnoStage]; ok {
			stage, err := strconv.Atoi(stageStr)
			if err != nil {
				log.Warnw("invalid stage annotation", "stage", stageStr, "err", err)
			} else {
				taskInfo.Stage = utils.Point(stage)
			}
		}
		if executorStageStr, ok := configmap.Annotations[consts.AnnoExecutorStage]; ok {
			executorStage, err := strconv.Atoi(executorStageStr)
			if err != nil {
				log.Warnw("invalid executor stage annotation", "stage", executorStageStr, "err", err)
			} else {
				taskInfo.ExecutorStage = utils.Point(executorStage)
			}
		}
	}
	return taskInfo, nil
}

// StopTask ...
func (i *impl) StopTask(ctx context.Context, taskID, state string) error {
	configmapKey := ctrlclient.ObjectKey{Namespace: i.namespace, Name: configmapName(taskID)}
	configmap := &corev1.ConfigMap{}
	if err := i.kubeClient.Get(ctx, configmapKey, configmap); err != nil {
		if k8sapierrors.IsNotFound(err) {
			return ErrNotFound
		}
		return fmt.Errorf("failed to get configmap: %w", err)
	}

	patchHelper, err := patch.NewHelper(configmap, i.kubeClient)
	if err != nil {
		return fmt.Errorf("failed to create configmap patchHelper: %w", err)
	}
	if configmap.Annotations == nil {
		configmap.Annotations = make(map[string]string)
	}
	configmap.Annotations[consts.AnnoStop] = state
	if err = patchHelper.Patch(ctx, configmap); err != nil {
		return fmt.Errorf("failed to mark task cancel on configmap: %w", err)
	}
	return nil
}

// DeleteTask ...
func (i *impl) DeleteTask(ctx context.Context, taskID string) error {
	configmapKey := ctrlclient.ObjectKey{Namespace: i.namespace, Name: configmapName(taskID)}
	configmap := &corev1.ConfigMap{}
	if err := i.kubeClient.Get(ctx, configmapKey, configmap); err != nil {
		if k8sapierrors.IsNotFound(err) {
			return ErrNotFound
		}
		return fmt.Errorf("failed to get configmap: %w", err)
	}

	if err := utils.RemoveObjectFinalizer(ctx, i.kubeClient, configmap, consts.ProcessTaskFinalizer); err != nil {
		return err
	}

	if err := i.kubeClient.Delete(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Namespace: i.namespace,
		Name:      configmapName(taskID),
	}}, ctrlclient.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
		if k8sapierrors.IsNotFound(err) {
			return ErrNotFound
		}
		return fmt.Errorf("failed to delete configmap: %w", err)
	}
	return nil
}

// RecordTaskStage ...
func (i *impl) RecordTaskStage(ctx context.Context, taskID string, stage int) error {
	configmapKey := ctrlclient.ObjectKey{Namespace: i.namespace, Name: configmapName(taskID)}
	configmap := &corev1.ConfigMap{}
	if err := i.kubeClient.Get(ctx, configmapKey, configmap); err != nil {
		if k8sapierrors.IsNotFound(err) {
			return ErrNotFound
		}
		return fmt.Errorf("failed to get configmap: %w", err)
	}

	patchHelper, err := patch.NewHelper(configmap, i.kubeClient)
	if err != nil {
		return fmt.Errorf("failed to create configmap patchHelper: %w", err)
	}
	if configmap.Annotations == nil {
		configmap.Annotations = make(map[string]string)
	}
	configmap.Annotations[consts.AnnoStage] = strconv.Itoa(stage)
	if err = patchHelper.Patch(ctx, configmap); err != nil {
		return fmt.Errorf("failed to record task stage on configmap: %w", err)
	}
	return nil
}

// RecordTaskExecutorStage ...
func (i *impl) RecordTaskExecutorStage(ctx context.Context, taskID string, stage int) error {
	configmapKey := ctrlclient.ObjectKey{Namespace: i.namespace, Name: configmapName(taskID)}
	configmap := &corev1.ConfigMap{}
	if err := i.kubeClient.Get(ctx, configmapKey, configmap); err != nil {
		if k8sapierrors.IsNotFound(err) {
			return ErrNotFound
		}
		return fmt.Errorf("failed to get configmap: %w", err)
	}

	patchHelper, err := patch.NewHelper(configmap, i.kubeClient)
	if err != nil {
		return fmt.Errorf("failed to create configmap patchHelper: %w", err)
	}
	if configmap.Annotations == nil {
		configmap.Annotations = make(map[string]string)
	}
	configmap.Annotations[consts.AnnoExecutorStage] = strconv.Itoa(stage)
	if err = patchHelper.Patch(ctx, configmap); err != nil {
		return fmt.Errorf("failed to record executor stage on configmap: %w", err)
	}
	return nil
}

// StoreType ...
func (i *impl) StoreType() ctrlclient.Object {
	return &corev1.ConfigMap{}
}

func configmapName(taskID string) string {
	return taskID
}
