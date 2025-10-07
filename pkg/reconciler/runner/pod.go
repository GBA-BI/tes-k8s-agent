package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/GBA-BI/tes-k8s-agent/pkg/log"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8sapierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/GBA-BI/tes-k8s-agent/pkg/consts"
	"github.com/GBA-BI/tes-k8s-agent/pkg/filelog"
	"github.com/GBA-BI/tes-k8s-agent/pkg/utils"
	"github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient"
	"github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient/models"
)

// ProcessPod only check pod status
func (r *Runner) ProcessPod(ctx context.Context, podName string) (ctrl.Result, error) {
	pod := &corev1.Pod{}
	if err := r.kubeClient.Get(ctx, ctrlclient.ObjectKey{Namespace: r.namespace, Name: podName}, pod); err != nil {
		if k8sapierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	taskID, ok := pod.Labels[consts.LabelTaskID]
	if !ok {
		return ctrl.Result{}, nil
	}
	if !r.tryProcessTask(taskID) {
		return ctrl.Result{RequeueAfter: tryProcessLatency}, nil
	}
	defer r.releaseProcessTask(taskID)
	newLogger := r.taskLogger(taskID)

	r.recordExecutorLog(ctx, newLogger, pod)

	result1, err := r.processExecutorTime(ctx, newLogger, taskID, pod)
	if err != nil {
		return ctrl.Result{}, err
	}

	result2, err := r.processImagePullBackoff(ctx, newLogger, pod)
	if err != nil {
		return ctrl.Result{}, err
	}
	return utils.MergeCtrlResults(result1, result2), nil
}

// Normally executor don't have any logs. In case pod failed unexpectedly, we record executor pod logs when it finished.
func (r *Runner) recordExecutorLog(ctx context.Context, newLogger filelog.Logger, pod *corev1.Pod) {
	if pod.Labels[consts.LabelType] != consts.ExecutorType {
		return
	}
	if pod.Status.Phase != corev1.PodFailed && pod.Status.Phase != corev1.PodSucceeded {
		return
	}
	req := r.kubeClientNative.CoreV1().Pods(r.namespace).GetLogs(pod.Name, &corev1.PodLogOptions{})
	podLogs, err := req.Stream(ctx)
	if err != nil {
		errMsg := strings.ToLower(err.Error())
		if strings.Contains(errMsg, "not found") || strings.Contains(errMsg, "notfound") {
			return
		}
		newLogger.Errorf("failed to get executor pod %s logs: %s", pod.Name, err.Error())
		return
	}
	defer podLogs.Close()

	logs, err := io.ReadAll(podLogs)
	if err != nil {
		newLogger.Errorf("failed to read executor pod %s logs: %s", pod.Name, err.Error())
		return
	}
	if len(logs) > 0 {
		newLogger.Infof("executor pod %s logs:\n%s", pod.Name, string(logs))
	}
}

func (r *Runner) processExecutorTime(ctx context.Context, newLogger filelog.Logger, taskID string, pod *corev1.Pod) (ctrl.Result, error) {
	if pod.Labels[consts.LabelType] != consts.ExecutorType {
		return ctrl.Result{}, nil
	}
	executorNoStr := pod.Labels[consts.LabelExecutorNo]
	executorNo, err := strconv.Atoi(executorNoStr)
	if err != nil {
		newLogger.Warnf("invalid executorNo %s", executorNoStr)
		return ctrl.Result{}, nil
	}

	startTime, endTime := getExecutorTime(pod)
	if startTime == nil && endTime == nil {
		return ctrl.Result{}, nil
	}

	task, err := r.vetesClient.GetTask(ctx, &models.GetTaskRequest{ID: taskID, View: consts.BasicView})
	if err != nil {
		return ctrl.Result{}, err
	}
	if task.ClusterID != r.clusterID {
		return ctrl.Result{}, nil
	}

	if startTime != nil && endTime == nil {
		// no new finalizers can be added if the object is being deleted
		if !pod.DeletionTimestamp.IsZero() {
			// if pod started and being deleted and stuck for a long time, endTime will be set to be DeletionTimestamp,
			// so we should requeue after a while to avoid not reconcile
			return ctrl.Result{RequeueAfter: time.Minute}, nil
		}
		if err = utils.AddObjectFinalizer(ctx, r.kubeClient, pod, consts.ProcessExecutorTimeFinalizer); err != nil {
			return ctrl.Result{}, err
		}
	}

	taskLogs := r.genUpdateTaskLogsExecutor(task.Logs, executorNo, pod.Name, startTime, endTime)
	updateTaskReq := &models.UpdateTaskRequest{ID: taskID, Logs: taskLogs}
	if _, err = r.vetesClient.UpdateTask(ctx, updateTaskReq); err != nil {
		if errors.Is(err, vetesclient.ErrBadRequest) {
			log.Warnw("bad request for update task, maybe because time conflict", "err", err)
			return ctrl.Result{RequeueAfter: waitUpdateTaskTimeConflict}, nil
		}
		return ctrl.Result{}, err
	}

	if endTime != nil {
		if err = utils.RemoveObjectFinalizer(ctx, r.kubeClient, pod, consts.ProcessExecutorTimeFinalizer); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *Runner) genUpdateTaskLogsExecutor(taskLogs []*models.TaskLog, executorNo int, executorID string, startTime, endTime *string) []*models.TaskLog {
	res := []*models.TaskLog{{
		ClusterID: r.clusterID,
		Logs:      make([][]*models.ExecutorLog, executorNo+1),
	}}
	res[0].Logs[executorNo] = []*models.ExecutorLog{{
		ExecutorID: executorID,
	}}

	matchedTaskLog := r.getMatchedTaskLog(taskLogs)

	if matchedTaskLog == nil || matchedTaskLog.StartTime == nil {
		res[0].StartTime = startTime
	}

	var matchedExecutorLog *models.ExecutorLog
	if matchedTaskLog != nil && len(matchedTaskLog.Logs) >= executorNo+1 {
		matchedExecutorLog = getMatchedExecutorLog(matchedTaskLog.Logs[executorNo], executorID)
	}

	if matchedExecutorLog == nil || matchedExecutorLog.StartTime == nil {
		res[0].Logs[executorNo][0].StartTime = startTime
		res[0].Logs[executorNo][0].EndTime = endTime
		// avoid endTime before startTime
		if startTime != nil && endTime != nil && invalidEndTime(*startTime, *endTime) {
			res[0].Logs[executorNo][0].EndTime = startTime
		}
	} else if matchedExecutorLog.EndTime == nil {
		res[0].Logs[executorNo][0].EndTime = endTime
		// avoid endTime before startTime
		if endTime != nil && invalidEndTime(*matchedExecutorLog.StartTime, *endTime) {
			res[0].Logs[executorNo][0].EndTime = matchedExecutorLog.StartTime
		}
	}

	return res
}

func getExecutorTime(pod *corev1.Pod) (startTime, endTime *string) {
	defer func() {
		// pod never be running, just set the time
		if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			if startTime == nil {
				if !pod.Status.StartTime.IsZero() && pod.Status.StartTime.Unix() > 0 {
					startTime = utils.Point(pod.Status.StartTime.Format(time.RFC3339))
				} else {
					startTime = utils.Point(time.Now().Format(time.RFC3339))
				}
			}
			if endTime == nil {
				endTime = utils.Point(time.Now().Format(time.RFC3339))
			}
		} else if pod.Status.Phase == corev1.PodRunning {
			// if pod started and being deleted and stuck for a long time, set endTime to DeletionTimestamp
			if startTime != nil && endTime == nil &&
				!pod.DeletionTimestamp.IsZero() && pod.DeletionTimestamp.Add(time.Minute).Before(time.Now()) {
				endTime = utils.Point(pod.DeletionTimestamp.Format(time.RFC3339))
			}
		}
	}()

	if len(pod.Status.ContainerStatuses) == 0 {
		return nil, nil
	}

	containerState := pod.Status.ContainerStatuses[0].State
	if containerState.Running != nil {
		if !containerState.Running.StartedAt.IsZero() && containerState.Running.StartedAt.Unix() > 0 {
			startTime = utils.Point(containerState.Running.StartedAt.Format(time.RFC3339))
		}
		return
	}
	if containerState.Terminated != nil {
		if !containerState.Terminated.StartedAt.IsZero() && containerState.Terminated.StartedAt.Unix() > 0 {
			startTime = utils.Point(containerState.Terminated.StartedAt.Format(time.RFC3339))
		}
		if !containerState.Terminated.FinishedAt.IsZero() && containerState.Terminated.FinishedAt.Unix() > 0 {
			endTime = utils.Point(containerState.Terminated.FinishedAt.Format(time.RFC3339))
		} else {
			endTime = utils.Point(time.Now().Format(time.RFC3339)) // anyway, terminated must be with endTime
		}
		if startTime == nil && endTime != nil {
			startTime = endTime
		}
	}
	return
}

func invalidEndTime(startTimeStr, endTimeStr string) bool {
	startTime, _ := time.Parse(time.RFC3339, startTimeStr)
	endTime, _ := time.Parse(time.RFC3339, endTimeStr)
	return endTime.Before(startTime)
}

func getMatchedExecutorLog(executorLogs []*models.ExecutorLog, executorID string) *models.ExecutorLog {
	for _, executorLog := range executorLogs {
		if executorLog != nil && executorLog.ExecutorID == executorID {
			return executorLog
		}
	}
	return nil
}

func (r *Runner) processImagePullBackoff(ctx context.Context, newLogger filelog.Logger, pod *corev1.Pod) (ctrl.Result, error) {
	jobName, ok := pod.Labels[consts.LabelJobName]
	if !ok {
		return ctrl.Result{}, nil
	}
	job := &batchv1.Job{}
	if err := r.kubeClient.Get(ctx, ctrlclient.ObjectKey{Namespace: r.namespace, Name: jobName}, job); err != nil {
		if k8sapierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get job: %w", err)
	}

	if jobFinished(job) {
		return ctrl.Result{}, nil
	}
	if pod.Status.Phase != corev1.PodPending {
		return ctrl.Result{}, nil
	}
	if !r.podImagePullBackoffTimeout(pod) {
		return ctrl.Result{RequeueAfter: r.opts.PodPollInterval}, nil
	}
	r.printImagePullBackOffReason(ctx, newLogger, pod)
	if err := r.stopJob(ctx, newLogger, job); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *Runner) podImagePullBackoffTimeout(pod *corev1.Pod) bool {
	if pod.Status.StartTime == nil {
		return false
	}
	delta := time.Since(pod.Status.StartTime.Time)
	if delta <= r.opts.PodImagePullBackoffTimeout || !podImagePullBackoff(pod) {
		return false
	}
	return true
}

func podImagePullBackoff(pod *corev1.Pod) bool {
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.State.Waiting != nil && containerStatus.State.Waiting.Reason == "ImagePullBackOff" {
			return true
		}
	}
	return false
}

func (r *Runner) printImagePullBackOffReason(ctx context.Context, logger filelog.Logger, pod *corev1.Pod) {
	events, err := r.kubeClientNative.CoreV1().Events(r.namespace).List(ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("involvedObject.name=%s,involvedObject.namespace=%s,involvedObject.uid=%s", pod.Name, pod.Namespace, string(pod.UID)),
	})
	if err != nil {
		logger.Errorf("ImagePullBackOff: failed to list events of pod: %s", err.Error())
		return
	}
	for _, event := range events.Items {
		if event.Reason == "Failed" && strings.HasPrefix(event.Message, "Failed to pull image") {
			logger.Errorf("ImagePullBackOff: %s", event.Message)
			return
		}
	}
	logger.Errorf("ImagePullBackOff: no related events")
}
