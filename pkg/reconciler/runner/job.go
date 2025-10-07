package runner

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8sapierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/cluster-api/util/patch"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/GBA-BI/tes-k8s-agent/pkg/consts"
	"github.com/GBA-BI/tes-k8s-agent/pkg/filelog"
	"github.com/GBA-BI/tes-k8s-agent/pkg/utils"
)

func inputsFilerJobName(taskID string) string {
	return fmt.Sprintf("%s-inputs-filer", taskID)
}

func outputsFilerJobName(taskID string) string {
	return fmt.Sprintf("%s-outputs-filer", taskID)
}

func executorJobName(taskID string, index int) string {
	return fmt.Sprintf("%s-ex-%02d", taskID, index)
}

type jobStatus int

const (
	jobRunning jobStatus = iota
	jobFailed
	jobComplete
)

func getJobStatus(job *batchv1.Job) jobStatus {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return jobFailed
		}
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			return jobComplete
		}
	}
	return jobRunning
}

func jobFinished(job *batchv1.Job) bool {
	return getJobStatus(job) != jobRunning
}

func (r *Runner) createJob(ctx context.Context, logger filelog.Logger, job *batchv1.Job) error {
	controllerutil.AddFinalizer(job, consts.ProcessTaskFinalizer)
	if err := r.kubeClient.Create(ctx, job); err != nil {
		if k8sapierrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("failed to create job: %w", err)
	}
	logger.Infof("created job %s", job.Name)
	return nil
}

func (r *Runner) deleteJob(ctx context.Context, logger filelog.Logger, jobName string) error {
	job := &batchv1.Job{}
	if err := r.kubeClient.Get(ctx, ctrlclient.ObjectKey{Namespace: r.namespace, Name: jobName}, job); err != nil {
		if k8sapierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get job: %w", err)
	}

	if err := utils.RemoveObjectFinalizer(ctx, r.kubeClient, job, consts.ProcessTaskFinalizer); err != nil {
		return err
	}

	if err := r.kubeClient.Delete(ctx, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Namespace: r.namespace,
		Name:      jobName,
	}}, ctrlclient.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
		if k8sapierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to delete job %s: %w", jobName, err)
	}
	logger.Infof("deleted job %s", jobName)
	return nil
}

func (r *Runner) stopJob(ctx context.Context, logger filelog.Logger, job *batchv1.Job) error {
	patchHelper, err := patch.NewHelper(job, r.kubeClient)
	if err != nil {
		return fmt.Errorf("failed to create job patchHelper: %w", err)
	}
	job.Spec.ActiveDeadlineSeconds = utils.Point[int64](0)
	if err := patchHelper.Patch(ctx, job); err != nil {
		if k8sapierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to stop job %s: %w", job.Name, err)
	}
	logger.Infof("stopped job %s", job.Name)
	return nil
}

func (r *Runner) deleteJobPods(ctx context.Context, logger filelog.Logger, jobName string) (bool, error) {
	// If job finished, pods should not be deleted immediately, because some timing bugs may occur in k8s,
	// and job-controller will create new pod although the job has finished.
	// As recommended by vke, wait 100ms here.
	// Bug refer to https://github.com/kubernetes/kubernetes/issues/109902
	time.Sleep(100 * time.Millisecond)

	if err := r.kubeClient.DeleteAllOf(ctx, &corev1.Pod{}, ctrlclient.InNamespace(r.namespace), ctrlclient.MatchingLabels{
		consts.LabelJobName: jobName,
	}); err != nil {
		return false, fmt.Errorf("failed to delete pods of job %s: %w", jobName, err)
	}
	pods := &corev1.PodList{}
	if err := r.kubeClient.List(ctx, pods, ctrlclient.InNamespace(r.namespace), ctrlclient.MatchingLabels{
		consts.LabelJobName: jobName,
	}); err != nil {
		return false, fmt.Errorf("failed to list pods of job %s: %w", jobName, err)
	}
	if len(pods.Items) == 0 {
		logger.Infof("deleted pods of job %s", jobName)
		return true, nil
	}
	return false, nil
}

func (r *Runner) recordJobFailedMessage(ctx context.Context, logger filelog.Logger, jobName string) error {
	pods := &corev1.PodList{}
	if err := r.kubeClient.List(ctx, pods, ctrlclient.InNamespace(r.namespace), ctrlclient.MatchingLabels{
		consts.LabelJobName: jobName,
	}); err != nil {
		return fmt.Errorf("failed to list pods of job %s: %w", jobName, err)
	}
	for _, pod := range pods.Items {
		if pod.Status.Phase != corev1.PodFailed {
			continue
		}
		for _, containerStatus := range pod.Status.ContainerStatuses {
			if containerStatus.State.Terminated == nil {
				continue
			}
			if containerStatus.State.Terminated.ExitCode != 0 {
				logger.Errorf("pod %s of job %s failed with exitCode[%d], reason[%s] and message: %s", pod.Name, jobName,
					containerStatus.State.Terminated.ExitCode, containerStatus.State.Terminated.Reason, containerStatus.State.Terminated.Message)
			}
		}
	}
	return nil
}
