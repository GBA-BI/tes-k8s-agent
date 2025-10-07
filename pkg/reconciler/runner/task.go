package runner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/GBA-BI/tes-k8s-agent/pkg/log"
	batchv1 "k8s.io/api/batch/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/GBA-BI/tes-k8s-agent/pkg/consts"
	"github.com/GBA-BI/tes-k8s-agent/pkg/filelog"
	"github.com/GBA-BI/tes-k8s-agent/pkg/localstore"
	"github.com/GBA-BI/tes-k8s-agent/pkg/utils"
	"github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient"
	"github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient/models"
)

const (
	waitPodDeleted             = 5 * time.Second
	waitUpdateTaskTimeConflict = time.Second
)

// ProcessTask ...
func (r *Runner) ProcessTask(ctx context.Context, taskID string) (ctrl.Result, error) {
	if !r.tryProcessTask(taskID) {
		return ctrl.Result{RequeueAfter: tryProcessLatency}, nil
	}
	defer r.releaseProcessTask(taskID)
	newLogger := r.taskLogger(taskID)

	task, err := r.vetesClient.GetTask(ctx, &models.GetTaskRequest{ID: taskID, View: consts.BasicView})
	if err != nil {
		return ctrl.Result{}, err
	}
	if task.ClusterID != r.clusterID {
		return ctrl.Result{}, nil
	}

	switch task.State {
	case consts.TaskCanceling:
		newLogger.Infof("stop task because cancel task")
		return r.stopAndCleanTask(ctx, newLogger, task.Task, consts.TaskCanceled)
	case consts.TaskSystemError, consts.TaskExecutorError, consts.TaskCanceled, consts.TaskComplete: // local store may clean failed. no need to print log
		return r.stopAndCleanTask(ctx, newLogger, task.Task, task.State)
	default:
		return r.runTask(ctx, newLogger, task.Task)
	}
}

func (r *Runner) runTask(ctx context.Context, logger filelog.Logger, task *models.Task) (ctrl.Result, error) {
	taskInfo, err := r.localStoreHelper.GetTask(ctx, task.ID)
	if err != nil {
		return ctrl.Result{}, err
	}
	if taskInfo.Stop != nil {
		return r.stopAndCleanTask(ctx, logger, task, *taskInfo.Stop)
	}

	result, err := r.accelerator.OnProcessTask(ctx, &taskInfo.Task)
	if err != nil || !result.IsZero() {
		return result, err
	}

	s3SecretName := ""
	if r.opts.S3.Enable {
		s3SecretName = r.opts.S3.StaticSecretName
	}
	executorImagePullSecret := r.opts.ExecutorImagePullSecret.StaticName

	if taskInfo.Stage == nil {
		return ctrl.Result{}, r.localStoreHelper.RecordTaskStage(ctx, task.ID, taskStageInit)
	}
	currentStage := *taskInfo.Stage

	switch {
	case currentStage < taskStageInitializing:
		return ctrl.Result{}, r.doInitializing(ctx, logger, task)
	case currentStage < taskStagePVCCreated:
		return ctrl.Result{}, r.doCreatePVC(ctx, logger, &taskInfo.Task)
	case currentStage < taskStageInputsFilerCreated:
		return ctrl.Result{}, r.doCreateInputsFiler(ctx, logger, &taskInfo.Task, s3SecretName)
	case currentStage < taskStageInputsFilerFinished:
		return r.doWatchInputsFiler(ctx, logger, task, &taskInfo.Task)
	case currentStage < taskStageRunning:
		return ctrl.Result{}, r.doRunning(ctx, logger, task)
	case currentStage < taskStageExecutorsFinished:
		return r.doExecutors(ctx, logger, task, taskInfo, executorImagePullSecret)
	case currentStage < taskStageOutputsFilerCreated:
		return ctrl.Result{}, r.doCreateOutputsFiler(ctx, logger, &taskInfo.Task, s3SecretName)
	case currentStage < taskStageOutputsFilerFinished:
		return r.doWatchOutputsFiler(ctx, logger, task, &taskInfo.Task)
	default:
		return r.doComplete(ctx, logger, task, taskInfo)
	}
}

func (r *Runner) doInitializing(ctx context.Context, logger filelog.Logger, task *models.Task) error {
	updateTaskReq := &models.UpdateTaskRequest{
		ID:    task.ID,
		State: utils.Point(consts.TaskInitializing),
		Logs:  r.genUpdateTaskLogsInitializing(task.Logs),
	}
	if _, err := r.vetesClient.UpdateTask(ctx, updateTaskReq); err != nil {
		return err
	}
	logger.Infof("start task: Initializing")
	return r.localStoreHelper.RecordTaskStage(ctx, task.ID, taskStageInitializing)
}

func (r *Runner) genUpdateTaskLogsInitializing(taskLogs []*models.TaskLog) []*models.TaskLog {
	matched := r.getMatchedTaskLog(taskLogs)
	if matched != nil && matched.StartTime != nil {
		return nil
	}
	return []*models.TaskLog{{
		ClusterID: r.clusterID,
		StartTime: utils.Point(time.Now().Format(time.RFC3339)),
	}}
}

func (r *Runner) doCreatePVC(ctx context.Context, logger filelog.Logger, localTask *localstore.Task) error {
	if shouldCreatePVC(localTask) {
		if err := r.createPVC(ctx, logger, localTask.ID, localTask.Resources.DiskGB); err != nil {
			return err
		}
	}
	return r.localStoreHelper.RecordTaskStage(ctx, localTask.ID, taskStagePVCCreated)
}

func (r *Runner) doCreateInputsFiler(ctx context.Context, logger filelog.Logger, localTask *localstore.Task, s3SecretName string) error {
	if shouldCreateInputsFiler(localTask) {
		inputsFilerJob := r.initFiler(localTask, consts.InputsMode, s3SecretName)
		if err := r.createJob(ctx, logger, inputsFilerJob); err != nil {
			return err
		}
	}
	return r.localStoreHelper.RecordTaskStage(ctx, localTask.ID, taskStageInputsFilerCreated)
}

func (r *Runner) doWatchInputsFiler(ctx context.Context, logger filelog.Logger, task *models.Task, localTask *localstore.Task) (ctrl.Result, error) {
	if shouldCreateInputsFiler(localTask) {
		inputsFilerJob := &batchv1.Job{}
		if err := r.kubeClient.Get(ctx, ctrlclient.ObjectKey{Namespace: r.namespace, Name: inputsFilerJobName(localTask.ID)}, inputsFilerJob); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to get job: %w", err)
		}
		switch getJobStatus(inputsFilerJob) {
		case jobRunning:
			return ctrl.Result{}, nil
		case jobFailed:
			logger.Errorf("stop task because job %s failed", inputsFilerJob.Name)
			if err := r.recordJobFailedMessage(ctx, logger, inputsFilerJob.Name); err != nil {
				return ctrl.Result{}, err
			}
			return r.stopAndCleanTask(ctx, logger, task, consts.TaskSystemError)
		case jobComplete:
			deleted, err := r.deleteJobPods(ctx, logger, inputsFilerJob.Name)
			if err != nil {
				return ctrl.Result{}, err
			}
			if !deleted {
				return ctrl.Result{RequeueAfter: waitPodDeleted}, nil
			}
		}
	}
	return ctrl.Result{}, r.localStoreHelper.RecordTaskStage(ctx, localTask.ID, taskStageInputsFilerFinished)
}

func (r *Runner) doRunning(ctx context.Context, logger filelog.Logger, task *models.Task) error {
	updateTaskReq := &models.UpdateTaskRequest{
		ID:    task.ID,
		State: utils.Point(consts.TaskRunning),
	}
	if _, err := r.vetesClient.UpdateTask(ctx, updateTaskReq); err != nil {
		return err
	}
	logger.Infof("start task: Running")
	return r.localStoreHelper.RecordTaskStage(ctx, task.ID, taskStageRunning)
}

func (r *Runner) doExecutors(ctx context.Context, logger filelog.Logger, task *models.Task, taskInfo *localstore.TaskInfo, executorImagePullSecret string) (ctrl.Result, error) {
	if taskInfo.ExecutorStage == nil {
		return ctrl.Result{}, r.localStoreHelper.RecordTaskExecutorStage(ctx, task.ID, newExecutorStageValue(0, executorStatusToCreate))
	}
	eStage := parseExecutorStageValue(*taskInfo.ExecutorStage)
	maxIndex := len(task.Executors) - 1

	switch {
	case eStage.status == executorStatusToCreate:
		return r.doCreateExecutor(ctx, logger, &taskInfo.Task, eStage.index, executorImagePullSecret)
	case eStage.status == executorStatusCreated:
		return r.doWatchExecutor(ctx, logger, task.ID, eStage.index)
	case eStage.status == executorStatusSuccess && eStage.index < maxIndex:
		return ctrl.Result{}, r.localStoreHelper.RecordTaskExecutorStage(ctx, task.ID, newExecutorStageValue(eStage.index+1, executorStatusToCreate))
	default:
		if eStage.status == executorStatusSuccess {
			logger.Infof("finished all executors: Success")
		} else if eStage.status == executorStatusFailed {
			logger.Infof("finished all executors: Failed")
		}
		return ctrl.Result{}, r.localStoreHelper.RecordTaskStage(ctx, task.ID, taskStageExecutorsFinished)
	}
}

func (r *Runner) doWatchExecutor(ctx context.Context, logger filelog.Logger, taskID string, executorIndex int) (ctrl.Result, error) {
	executorJob := &batchv1.Job{}
	if err := r.kubeClient.Get(ctx, ctrlclient.ObjectKey{Namespace: r.namespace, Name: executorJobName(taskID, executorIndex)}, executorJob); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get job: %w", err)
	}
	var eStatus executorStatus
	switch getJobStatus(executorJob) {
	case jobRunning:
		return ctrl.Result{}, nil
	case jobFailed:
		eStatus = executorStatusFailed
		logger.Errorf("executor job %s failed", executorJob.Name)
		if err := r.recordJobFailedMessage(ctx, logger, executorJob.Name); err != nil {
			return ctrl.Result{}, err
		}
	case jobComplete:
		eStatus = executorStatusSuccess
	}
	deleted, err := r.deleteJobPods(ctx, logger, executorJob.Name)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !deleted {
		return ctrl.Result{RequeueAfter: waitPodDeleted}, nil
	}
	return ctrl.Result{}, r.localStoreHelper.RecordTaskExecutorStage(ctx, taskID, newExecutorStageValue(executorIndex, eStatus))
}

func (r *Runner) doCreateOutputsFiler(ctx context.Context, logger filelog.Logger, localTask *localstore.Task, s3SecretName string) error {
	if shouldCreateOutputsFiler(localTask) {
		outputsFilerJob := r.initFiler(localTask, consts.OutputsMode, s3SecretName)
		if err := r.createJob(ctx, logger, outputsFilerJob); err != nil {
			return err
		}
	}
	return r.localStoreHelper.RecordTaskStage(ctx, localTask.ID, taskStageOutputsFilerCreated)
}

func (r *Runner) doWatchOutputsFiler(ctx context.Context, logger filelog.Logger, task *models.Task, localTask *localstore.Task) (ctrl.Result, error) {
	if shouldCreateOutputsFiler(localTask) {
		outputsFilerJob := &batchv1.Job{}
		if err := r.kubeClient.Get(ctx, ctrlclient.ObjectKey{Namespace: r.namespace, Name: outputsFilerJobName(localTask.ID)}, outputsFilerJob); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to get job: %w", err)
		}
		switch getJobStatus(outputsFilerJob) {
		case jobRunning:
			return ctrl.Result{}, nil
		case jobFailed:
			logger.Errorf("stop task because job %s failed", outputsFilerJob.Name)
			if err := r.recordJobFailedMessage(ctx, logger, outputsFilerJob.Name); err != nil {
				return ctrl.Result{}, err
			}
			return r.stopAndCleanTask(ctx, logger, task, consts.TaskSystemError)
		case jobComplete:
			deleted, err := r.deleteJobPods(ctx, logger, outputsFilerJob.Name)
			if err != nil {
				return ctrl.Result{}, err
			}
			if !deleted {
				return ctrl.Result{RequeueAfter: waitPodDeleted}, nil
			}
		}
	}
	return ctrl.Result{}, r.localStoreHelper.RecordTaskStage(ctx, localTask.ID, taskStageOutputsFilerFinished)
}

func (r *Runner) doComplete(ctx context.Context, logger filelog.Logger, task *models.Task, taskInfo *localstore.TaskInfo) (ctrl.Result, error) {
	var executorsSuccess bool
	if taskInfo.ExecutorStage == nil {
		logger.Errorf("task %s has no executor stage", task.ID)
		executorsSuccess = false
	} else {
		executorsSuccess = parseExecutorStageValue(*taskInfo.ExecutorStage).status == executorStatusSuccess
	}

	var finishState string
	if executorsSuccess {
		finishState = consts.TaskComplete
	} else {
		finishState = consts.TaskExecutorError
	}
	return r.stopAndCleanTask(ctx, logger, task, finishState)
}

func (r *Runner) stopAndCleanTask(ctx context.Context, logger filelog.Logger, task *models.Task, state string) (ctrl.Result, error) {
	taskInfo, err := r.localStoreHelper.GetTask(ctx, task.ID)
	if err != nil {
		if errors.Is(err, localstore.ErrNotFound) {
			return ctrl.Result{}, nil
		}
	}
	if taskInfo.Stop == nil {
		if err = r.localStoreHelper.StopTask(ctx, task.ID, state); err != nil {
			return ctrl.Result{}, err
		}
	}
	currentStage := taskStageInit
	if taskInfo.Stage != nil {
		currentStage = *taskInfo.Stage
	}

	if currentStage >= taskStageOutputsFilerToCreate {
		if shouldCreateOutputsFiler(&taskInfo.Task) {
			if err = r.deleteJob(ctx, logger, outputsFilerJobName(task.ID)); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	if currentStage >= taskStageExecutorsToCreate && taskInfo.ExecutorStage != nil {
		for index := 0; index <= parseExecutorStageValue(*taskInfo.ExecutorStage).index; index++ {
			if err = r.deleteJob(ctx, logger, executorJobName(task.ID, index)); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	if currentStage >= taskStageInputsFilerToCreate {
		if shouldCreateInputsFiler(&taskInfo.Task) {
			if err = r.deleteJob(ctx, logger, inputsFilerJobName(task.ID)); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	if currentStage >= taskStagePVCToCreate {
		if shouldCreatePVC(&taskInfo.Task) {
			if err = r.deletePVC(ctx, logger, pvcName(task.ID)); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	logger.Sync()
	message, err := logger.GetFileContent()
	if err != nil {
		if !os.IsNotExist(err) {
			return ctrl.Result{}, fmt.Errorf("failed to read log file content: %w", err)
		}
	}
	updateTaskReq := &models.UpdateTaskRequest{
		ID:   task.ID,
		Logs: r.genUpdateTaskLogsFinish(task.Logs, string(message)),
	}
	if task.State != state {
		updateTaskReq.State = &state
	}
	if _, err = r.vetesClient.UpdateTask(ctx, updateTaskReq); err != nil {
		if errors.Is(err, vetesclient.ErrBadRequest) {
			log.Warnw("bad request for update task, maybe because executor end_time not filled", "err", err)
			return ctrl.Result{RequeueAfter: waitUpdateTaskTimeConflict}, nil
		}
		return ctrl.Result{}, err
	}

	if taskInfo.InputsRef != "" || taskInfo.OutputsRef != "" {
		r.offloadHelper.DeleteOffloadFile(task.ID)
	}
	r.removeTaskLogFile(task.ID)
	if err = r.accelerator.OnFinishTask(ctx, &taskInfo.Task); err != nil {
		return ctrl.Result{}, err
	}
	if err = r.localStoreHelper.DeleteTask(ctx, task.ID); err != nil && !errors.Is(err, localstore.ErrNotFound) {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *Runner) genUpdateTaskLogsFinish(taskLogs []*models.TaskLog, message string) []*models.TaskLog {
	if message == "" {
		message = "<empty>"
	}
	res := []*models.TaskLog{{
		ClusterID:  r.clusterID,
		SystemLogs: []string{message},
	}}

	now := utils.Point(time.Now().Format(time.RFC3339))

	matched := r.getMatchedTaskLog(taskLogs)
	if matched == nil || matched.StartTime == nil {
		res[0].StartTime = now
		res[0].EndTime = now
	} else if matched.EndTime == nil {
		res[0].EndTime = now
	}
	return res
}

func shouldCreateInputsFiler(task *localstore.Task) bool {
	return len(task.InputsJSON) > 0 || len(task.InputsRef) > 0
}

func shouldCreateOutputsFiler(task *localstore.Task) bool {
	return len(task.OutputsJSON) > 0 || len(task.OutputsRef) > 0
}

func shouldCreatePVC(task *localstore.Task) bool {
	return shouldCreateInputsFiler(task) || shouldCreateOutputsFiler(task) || len(task.Volumes) > 0
}

func (r *Runner) getMatchedTaskLog(taskLogs []*models.TaskLog) *models.TaskLog {
	for _, taskLog := range taskLogs {
		if taskLog != nil && taskLog.ClusterID == r.clusterID {
			return taskLog
		}
	}
	return nil
}
