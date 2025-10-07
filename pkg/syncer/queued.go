package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	neturl "net/url"

	"github.com/GBA-BI/tes-k8s-agent/pkg/log"

	"github.com/GBA-BI/tes-k8s-agent/pkg/consts"
	"github.com/GBA-BI/tes-k8s-agent/pkg/localstore"
	"github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient/models"
)

func (s *syncer) syncQueuedTask(ctx context.Context, task taskMinimal) (reterr error) {
	_, err := s.localStoreHelper.GetTask(ctx, task.id)
	if err == nil {
		return nil
	}
	if !errors.Is(err, localstore.ErrNotFound) {
		return err
	}

	taskFull, err := s.vetesClient.GetTask(ctx, &models.GetTaskRequest{ID: task.id, View: consts.FullView})
	if err != nil {
		return fmt.Errorf("failed to get full task %s: %w", task.id, err)
	}
	accelerateNames, err := s.accelerator.ModifySyncTask(ctx, taskFull.Task)
	if err != nil {
		return err
	}
	taskStore := taskFullToStore(taskFull.Task)
	taskStore.AccelerateNames = accelerateNames

	defer func() {
		if reterr == nil {
			return
		}
		if taskStore.InputsRef != "" || taskStore.OutputsRef != "" {
			s.offloadHelper.DeleteOffloadFile(task.id)
		}
	}()

	if taskFull.BioosInfo != nil && taskFull.BioosInfo.Meta != nil && taskFull.BioosInfo.Meta.BucketsAuthInfo != nil && len(taskFull.BioosInfo.Meta.BucketsAuthInfo.External) > 0 {
		externalBucketsAuthInfoMap := externalBucketsAuthInfoToMap(taskFull.BioosInfo.Meta.BucketsAuthInfo.External)
		for index := range taskFull.Inputs {
			if taskFull.Inputs[index] == nil {
				continue
			}
			taskFull.Inputs[index].URL = modifyURLByAuthInfo(taskFull.Inputs[index].URL, externalBucketsAuthInfoMap)
		}
		for index := range taskFull.Outputs {
			if taskFull.Outputs[index] == nil {
				continue
			}
			taskFull.Outputs[index].URL = modifyURLByAuthInfo(taskFull.Outputs[index].URL, externalBucketsAuthInfoMap)
		}
	}

	if len(taskFull.Inputs) > 0 {
		// compatible with the original filer implementation
		inputsJSON, err := json.Marshal(map[string]interface{}{"inputs": taskFull.Inputs})
		if err != nil {
			return fmt.Errorf("failed to marshal inputs for task %s: %w", task.id, err)
		}
		if len(inputsJSON) <= s.offloadThreshold {
			taskStore.InputsJSON = string(inputsJSON)
		} else {
			refValue, err := s.offloadHelper.OffloadInputs(task.id, inputsJSON)
			if err != nil {
				return err
			}
			taskStore.InputsRef = refValue
		}
	}

	if len(taskFull.Outputs) > 0 {
		// compatible with the original filer implementation
		outputsJSON, err := json.Marshal(map[string]interface{}{"outputs": taskFull.Outputs})
		if err != nil {
			return fmt.Errorf("failed to marshal outputs for task %s: %w", task.id, err)
		}
		if len(outputsJSON) <= s.offloadThreshold {
			taskStore.OutputsJSON = string(outputsJSON)
		} else {
			refValue, err := s.offloadHelper.OffloadOutputs(task.id, outputsJSON)
			if err != nil {
				return err
			}
			taskStore.OutputsRef = refValue
		}
	}

	return s.localStoreHelper.StoreTask(ctx, taskStore)
}

func externalBucketsAuthInfoToMap(authInfo []*models.ExternalBucketAuthInfo) map[string]*models.ExternalBucketAuthInfo {
	res := make(map[string]*models.ExternalBucketAuthInfo, len(authInfo))
	for _, info := range authInfo {
		res[info.Bucket] = info
	}
	return res
}

func modifyURLByAuthInfo(url string, externalBucketAuthInfoMap map[string]*models.ExternalBucketAuthInfo) string {
	u, err := neturl.Parse(url)
	if err != nil { // never happen, because vetes-api has validated the url
		log.Errorw("failed to parse url", "err", err)
		return url
	}
	if u.Scheme != consts.S3Type {
		return url
	}
	bucket := u.Host
	if authInfo, ok := externalBucketAuthInfoMap[bucket]; ok {
		u.User = neturl.UserPassword(authInfo.AK, authInfo.SK)
		return u.String()
	}
	return url
}

func taskFullToStore(task *models.Task) *localstore.Task {
	res := &localstore.Task{
		ID:        task.ID,
		Name:      task.Name,
		Resources: resourcesFullToStore(task.Resources),
		BioosInfo: bioosInfoFullToStore(task.BioosInfo),
		Volumes:   task.Volumes,
	}
	if len(task.Executors) > 0 {
		res.Executors = make([]*localstore.Executor, len(task.Executors))
		for index := range task.Executors {
			res.Executors[index] = executorFullToStore(task.Executors[index])
		}
	}
	return res
}

func resourcesFullToStore(resources *models.Resources) *localstore.Resources {
	if resources == nil {
		return nil
	}
	res := &localstore.Resources{
		CPUCores: resources.CPUCores,
		RamGB:    resources.RamGB,
		DiskGB:   resources.DiskGB,
	}
	if resources.GPU != nil {
		res.GPU = &localstore.GPUResource{
			Type:  resources.GPU.Type,
			Count: resources.GPU.Count,
		}
	}
	return res
}

func bioosInfoFullToStore(bioosInfo *models.BioosInfo) *localstore.BioosInfo {
	if bioosInfo == nil {
		return nil
	}
	var meta *localstore.BioosInfoMeta
	if bioosInfo.Meta != nil {
		meta = &localstore.BioosInfoMeta{
			AAIPassport:     bioosInfo.Meta.AAIPassport,
			MountTOS:        bioosInfo.Meta.MountTOS,
			BucketsAuthInfo: bucketsAuthInfoFullToStore(bioosInfo.Meta.BucketsAuthInfo),
		}
	}
	return &localstore.BioosInfo{
		AccountID:    bioosInfo.AccountID,
		UserID:       bioosInfo.UserID,
		SubmissionID: bioosInfo.SubmissionID,
		RunID:        bioosInfo.RunID,
		Meta:         meta,
	}
}

func bucketsAuthInfoFullToStore(bucketsAuthInfo *models.BucketsAuthInfo) *localstore.BucketsAuthInfo {
	if bucketsAuthInfo == nil {
		return nil
	}
	res := &localstore.BucketsAuthInfo{
		ReadOnly:  bucketsAuthInfo.ReadOnly,
		ReadWrite: bucketsAuthInfo.ReadWrite,
	}
	if len(bucketsAuthInfo.External) > 0 {
		res.External = make([]*localstore.ExternalBucketAuthInfo, len(bucketsAuthInfo.External))
		for index := range bucketsAuthInfo.External {
			res.External[index] = &localstore.ExternalBucketAuthInfo{
				Bucket: bucketsAuthInfo.External[index].Bucket,
				AK:     bucketsAuthInfo.External[index].AK,
				SK:     bucketsAuthInfo.External[index].SK,
			}
		}
	}
	return res
}

func executorFullToStore(executor *models.Executor) *localstore.Executor {
	if executor == nil {
		return nil
	}
	return &localstore.Executor{
		Image:   executor.Image,
		Command: executor.Command,
		Workdir: executor.Workdir,
		Stdin:   executor.Stdin,
		Stdout:  executor.Stdout,
		Stderr:  executor.Stderr,
		Env:     executor.Env,
	}
}
