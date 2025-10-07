package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/GBA-BI/tes-k8s-agent/pkg/log"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/GBA-BI/tes-k8s-agent/pkg/consts"
	"github.com/GBA-BI/tes-k8s-agent/pkg/filelog"
	"github.com/GBA-BI/tes-k8s-agent/pkg/localstore"
	"github.com/GBA-BI/tes-k8s-agent/pkg/utils"
)

func (r *Runner) doCreateExecutor(ctx context.Context, logger filelog.Logger, localTask *localstore.Task, index int, executorImagePullSecretName string) (ctrl.Result, error) {
	job := r.initExecutorBase(localTask, index, executorImagePullSecretName)
	r.addECSInfo(job, localTask.Resources)

	if err := r.createJob(ctx, logger, job); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.localStoreHelper.RecordTaskExecutorStage(ctx, localTask.ID, newExecutorStageValue(index, executorStatusCreated))
}

// initExecutorBase ...
func (r *Runner) initExecutorBase(localTask *localstore.Task, index int, executorImagePullSecretName string) *batchv1.Job {
	name := executorJobName(localTask.ID, index)
	res := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.namespace,
			Name:      name,
			Labels: map[string]string{
				consts.LabelTaskID:     localTask.ID,
				consts.LabelType:       consts.ExecutorType,
				consts.LabelExecutorNo: strconv.Itoa(index),
			},
			Annotations: map[string]string{
				consts.AnnoTESTaskName: localTask.Name,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: utils.Point(int32(r.opts.ExecutorRetries)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						consts.LabelTaskID:     localTask.ID,
						consts.LabelType:       consts.ExecutorType,
						consts.LabelExecutorNo: strconv.Itoa(index),
					},
					Annotations: map[string]string{
						consts.AnnoTESTaskName: localTask.Name,
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name: name,
						SecurityContext: &corev1.SecurityContext{
							RunAsUser: utils.Point(int64(0)),
						},
						Image:           localTask.Executors[index].Image,
						Command:         getCommandsWithStreamRedirects(localTask.Executors[index]),
						WorkingDir:      localTask.Executors[index].Workdir,
						ImagePullPolicy: corev1.PullAlways,
						Env:             make([]corev1.EnvVar, 0),
						VolumeMounts:    make([]corev1.VolumeMount, 0),
					}},
					RestartPolicy:                corev1.RestartPolicyNever,
					DNSPolicy:                    corev1.DNSDefault,
					EnableServiceLinks:           utils.Point(false),
					AutomountServiceAccountToken: utils.Point(false),
					Volumes:                      make([]corev1.Volume, 0),
				},
			},
		},
	}
	if executorImagePullSecretName != "" {
		res.Spec.Template.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: executorImagePullSecretName}}
	}

	for k, v := range localTask.Executors[index].Env {
		res.Spec.Template.Spec.Containers[0].Env = append(res.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: k, Value: v})
	}
	for k, v := range r.opts.ExecutorPodEnv {
		res.Spec.Template.Spec.Containers[0].Env = append(res.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: strings.ToUpper(k), Value: v})
	}
	addBioosInfo(res, localTask.BioosInfo)
	addMeteringInfo(res, localTask)
	if shouldCreatePVC(localTask) {
		r.addTaskVolumeMount(res, localTask)
		r.accelerator.ModifyExecutor(&res.Spec.Template, localTask)
	}
	if r.opts.Transfer.Enable {
		r.addTransferMount(res, true)
	}
	return res
}

func addBioosInfo(job *batchv1.Job, bioosInfo *localstore.BioosInfo) {
	if bioosInfo == nil {
		return
	}
	if bioosInfo.AccountID != "" {
		job.Spec.Template.Labels[consts.LabelAccountID] = bioosInfo.AccountID
	}
	if bioosInfo.UserID != "" {
		job.Spec.Template.Labels[consts.LabelUserID] = bioosInfo.UserID
	}
	if bioosInfo.SubmissionID != "" {
		job.Spec.Template.Labels[consts.LabelSubmissionID] = bioosInfo.SubmissionID
	}
	if bioosInfo.RunID != "" {
		job.Spec.Template.Labels[consts.LabelRunID] = bioosInfo.RunID
	}
}

func addMeteringInfo(job *batchv1.Job, localTask *localstore.Task) {
	if localTask.BioosInfo == nil || localTask.BioosInfo.AccountID == "" {
		return
	}
	if localTask.Resources == nil {
		return
	}

	meteringUserInfo := localTask.BioosInfo.AccountID
	if localTask.BioosInfo.UserID != "" {
		meteringUserInfo += "-" + localTask.BioosInfo.UserID
	}
	job.Spec.Template.Annotations[consts.AnnoMeteringUserInfo] = meteringUserInfo

	resourceMap := map[string]string{
		"cpu":     fmt.Sprintf("%d", localTask.Resources.CPUCores),
		"memory":  utils.Float2String(localTask.Resources.RamGB) + "Gi",
		"storage": utils.Float2String(localTask.Resources.DiskGB) + "Gi",
	}
	if localTask.Resources.GPU != nil && localTask.Resources.GPU.Count > 0 && localTask.Resources.GPU.Type != "" {
		gpuMap := map[string]string{
			localTask.Resources.GPU.Type: utils.Float2String(localTask.Resources.GPU.Count),
		}
		gpu, err := json.Marshal(gpuMap)
		if err != nil {
			log.Errorw("task gpu resource json marshal error", "task", localTask.ID, "err", err)
		} else {
			resourceMap["gpu"] = string(gpu)
		}
	}

	meteringResource, err := json.Marshal(resourceMap)
	if err != nil {
		log.Errorw("task resources json marshal error", "task", localTask.ID, "err", err)
		return
	}
	job.Spec.Template.Annotations[consts.AnnoMeteringResource] = string(meteringResource)
}

func (r *Runner) addECSInfo(job *batchv1.Job, resources *localstore.Resources) {
	for k, v := range r.opts.ExecutorECSPodLabels {
		job.Spec.Template.Labels[k] = v
	}
	for k, v := range r.opts.ExecutorECSPodAnnotations {
		job.Spec.Template.Annotations[k] = v
	}

	if resources == nil {
		return
	}
	addResources(job, resources)

	if resources.GPU != nil {
		setGPUTypeAffinity(job, resources.GPU.Type)
	}
}

func addResources(job *batchv1.Job, resources *localstore.Resources) {
	if resources == nil {
		return
	}

	cpuResource := resource.MustParse(strconv.Itoa(resources.CPUCores))
	memoryResource := resource.MustParse(utils.Float2String(resources.RamGB) + "Gi")

	job.Spec.Template.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    cpuResource,
			corev1.ResourceMemory: memoryResource,
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    cpuResource,
			corev1.ResourceMemory: memoryResource,
		},
	}

	addGPUResources(job, resources.GPU)
}

func addGPUResources(job *batchv1.Job, gpu *localstore.GPUResource) {
	if gpu == nil {
		return
	}
	if job.Spec.Template.Spec.Containers[0].Resources.Limits == nil {
		job.Spec.Template.Spec.Containers[0].Resources.Limits = make(corev1.ResourceList)
	}
	if job.Spec.Template.Spec.Containers[0].Resources.Requests == nil {
		job.Spec.Template.Spec.Containers[0].Resources.Requests = make(corev1.ResourceList)
	}
	gpuResource := resource.MustParse(utils.Float2String(gpu.Count))
	job.Spec.Template.Spec.Containers[0].Resources.Requests[consts.NvidiaGPUResource] = gpuResource
	job.Spec.Template.Spec.Containers[0].Resources.Limits[consts.NvidiaGPUResource] = gpuResource
}

func setGPUTypeAffinity(job *batchv1.Job, gpuType string) {
	if gpuType == "" {
		return
	}
	job.Spec.Template.Spec.Affinity = &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      consts.GPUNameAffinityKey,
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{gpuType},
					}},
				}},
			},
		},
	}
}

func getCommandsWithStreamRedirects(executor *localstore.Executor) []string {
	if executor.Stdin == "" && executor.Stdout == "" && executor.Stderr == "" {
		return executor.Command
	}

	res := []string{"/bin/sh", "-c"}
	cmd := make([]string, 0)

	for _, command := range executor.Command {
		if len(specialChars.FindAllString(command, -1)) > 0 {
			if strings.Contains(command, "'") {
				command = strings.ReplaceAll(command, "'", "'\"'\"'")
			}
			command = "'" + command + "'"
		}
		cmd = append(cmd, command)
	}

	if executor.Stdin != "" {
		cmd = append(cmd, "<", executor.Stdin)
	}
	if executor.Stdout != "" {
		cmd = append(cmd, ">", executor.Stdout)
	}
	if executor.Stderr != "" {
		cmd = append(cmd, "2>", executor.Stderr)
	}
	res = append(res, strings.Join(cmd, " "))
	return res
}

var specialChars = regexp.MustCompile("[ !\"#$&'()*;<>?\\[\\\\`{|~\\t\\n]")
