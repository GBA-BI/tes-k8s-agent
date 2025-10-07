package runner

import (
	"fmt"
	"path/filepath"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/GBA-BI/tes-k8s-agent/pkg/consts"
	"github.com/GBA-BI/tes-k8s-agent/pkg/localstore"
	"github.com/GBA-BI/tes-k8s-agent/pkg/utils"
)

func (r *Runner) initFiler(localTask *localstore.Task, mode, s3SecretName string) *batchv1.Job {
	var name string
	switch mode {
	case consts.InputsMode:
		name = inputsFilerJobName(localTask.ID)
	case consts.OutputsMode:
		name = outputsFilerJobName(localTask.ID)
	}
	res := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.namespace,
			Name:      name,
			Labels: map[string]string{
				consts.LabelTaskID: localTask.ID,
				consts.LabelType:   fmt.Sprintf("%s%s", mode, consts.FilerTypeSuffix),
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: utils.Point(int32(r.opts.FilerRetries)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						consts.LabelTaskID: localTask.ID,
						consts.LabelType:   fmt.Sprintf("%s%s", mode, consts.FilerTypeSuffix),
					},
					Annotations: make(map[string]string),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:            name,
						Image:           r.opts.FilerImage.Image,
						Args:            []string{mode},
						Resources:       r.filerResources,
						ImagePullPolicy: corev1.PullAlways,
						Env:             make([]corev1.EnvVar, 0),
						VolumeMounts:    make([]corev1.VolumeMount, 0),
					}},
					RestartPolicy: corev1.RestartPolicyNever,
					Volumes:       make([]corev1.Volume, 0),
				},
			},
		},
	}
	if r.opts.FilerImage.ImagePullSecretName != "" {
		res.Spec.Template.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: r.opts.FilerImage.ImagePullSecretName}}
	}

	r.addTaskVolumeMount(res, localTask)
	r.addFilerInputsOutputsAnnotation(res, localTask, mode)
	r.addFilerAccelerateMount(res, localTask, mode)
	r.addFilerLogMount(res, localTask.ID)
	if r.opts.Transfer.Enable {
		r.addTransferEnv(res)
		r.addTransferMount(res, false)
	}
	if r.opts.S3.Enable {
		r.addFilerS3Mount(res, s3SecretName)
	}

	if localTask.BioosInfo != nil && localTask.BioosInfo.Meta != nil && localTask.BioosInfo.Meta.AAIPassport != nil {
		res.Spec.Template.Spec.Containers[0].Env = append(res.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{
			Name:  consts.AAIPassport,
			Value: *localTask.BioosInfo.Meta.AAIPassport,
		})
	}

	for k, v := range r.opts.FilerPodLabels {
		res.Spec.Template.Labels[k] = v
	}
	for k, v := range r.opts.FilerPodAnnotations {
		res.Spec.Template.Annotations[k] = v
	}
	for k, v := range r.opts.FilerPodEnv {
		res.Spec.Template.Spec.Containers[0].Env = append(res.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: strings.ToUpper(k), Value: v})
	}
	return res
}

func (r *Runner) addTaskVolumeMount(job *batchv1.Job, localTask *localstore.Task) {
	job.Spec.Template.Spec.Containers[0].VolumeMounts = append(job.Spec.Template.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name:      "task-volume",
		MountPath: strings.TrimSuffix(r.opts.ExecutorBasePath, "/"),
		SubPath:   "dir-base",
	})
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: "task-volume",
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: pvcName(localTask.ID),
			},
		},
	})
	for index, volume := range localTask.Volumes {
		job.Spec.Template.Spec.Containers[0].VolumeMounts = append(job.Spec.Template.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
			Name:      "task-volume",
			MountPath: volume,
			SubPath:   fmt.Sprintf("dir%d", index),
		})
	}
}

func (r *Runner) addFilerInputsOutputsAnnotation(job *batchv1.Job, localTask *localstore.Task, mode string) {
	switch mode {
	case consts.InputsMode:
		if len(localTask.InputsJSON) > 0 {
			job.Spec.Template.Annotations[consts.AnnoTaskInputs] = localTask.InputsJSON
		} else if len(localTask.InputsRef) > 0 {
			job.Spec.Template.Annotations[consts.AnnoTaskInputsRef] = localTask.InputsRef
			r.offloadHelper.ModifyInputsFiler(localTask.ID, &job.Spec.Template)
		}
	case consts.OutputsMode:
		if len(localTask.OutputsJSON) > 0 {
			job.Spec.Template.Annotations[consts.AnnoTaskOutputs] = localTask.OutputsJSON
		} else if len(localTask.OutputsRef) > 0 {
			job.Spec.Template.Annotations[consts.AnnoTaskOutputsRef] = localTask.OutputsRef
			r.offloadHelper.ModifyOutputsFiler(localTask.ID, &job.Spec.Template)
		}
	}
	job.Spec.Template.Spec.Containers[0].Env = append(job.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{
		Name:  consts.PodInfoAnnotationsFile,
		Value: "/podinfo/annotations",
	})
	job.Spec.Template.Spec.Containers[0].VolumeMounts = append(job.Spec.Template.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name:      "podinfo",
		MountPath: "/podinfo",
		ReadOnly:  true,
	})
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: "podinfo",
		VolumeSource: corev1.VolumeSource{
			DownwardAPI: &corev1.DownwardAPIVolumeSource{Items: []corev1.DownwardAPIVolumeFile{{
				Path:     "annotations",
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.annotations"},
			}}},
		},
	})
}

func (r *Runner) addFilerAccelerateMount(job *batchv1.Job, localTask *localstore.Task, mode string) {
	switch mode {
	case consts.InputsMode:
		r.accelerator.ModifyInputsFiler(&job.Spec.Template, localTask)
	case consts.OutputsMode:
		r.accelerator.ModifyOutputsFiler(&job.Spec.Template, localTask)
	}
}

func (r *Runner) addFilerLogMount(job *batchv1.Job, taskID string) {
	job.Spec.Template.Spec.Containers[0].Args = append(job.Spec.Template.Spec.Containers[0].Args,
		[]string{"--log-level", r.opts.TaskLog.FilerLogLevel, "--log-file", filepath.Join(r.opts.TaskLog.OutputDir, taskID, taskLogFileName)}...)
	job.Spec.Template.Spec.Containers[0].VolumeMounts = append(job.Spec.Template.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name:      "log-volume",
		MountPath: filepath.Join(r.opts.TaskLog.OutputDir, taskID),
		SubPath:   taskID,
	})
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: "log-volume",
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: r.opts.TaskLog.PVCName,
			},
		},
	})
}

func (r *Runner) addTransferEnv(job *batchv1.Job) {
	job.Spec.Template.Spec.Containers[0].Env = append(job.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{
		Name:  consts.HostBasePath,
		Value: r.opts.Transfer.WESBasePath,
	})
	job.Spec.Template.Spec.Containers[0].Env = append(job.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{
		Name:  consts.ContainerBasePath,
		Value: r.opts.Transfer.TESBasePath,
	})
}
func (r *Runner) addTransferMount(job *batchv1.Job, readOnly bool) {
	job.Spec.Template.Spec.Containers[0].VolumeMounts = append(job.Spec.Template.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name:      "transfer-volume",
		MountPath: r.opts.Transfer.TESBasePath,
	})
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: "transfer-volume",
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: r.opts.Transfer.PVCName,
				ReadOnly:  readOnly,
			},
		},
	})
}

func (r *Runner) addFilerS3Mount(job *batchv1.Job, s3SecretName string) {
	job.Spec.Template.Spec.Containers[0].Env = append(job.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{
		Name:  consts.AWSSharedCredentialsFile,
		Value: "/aws/credentials",
	})
	job.Spec.Template.Spec.Containers[0].Env = append(job.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{
		Name:  consts.AWSCredentialsExpiredTimeFile,
		Value: "/aws/expiredTime",
	})
	job.Spec.Template.Spec.Containers[0].VolumeMounts = append(job.Spec.Template.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name:      "s3-secret",
		MountPath: "/aws",
		ReadOnly:  true,
	})
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: "s3-secret",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: s3SecretName,
				Items: []corev1.KeyToPath{
					{Key: "credentials", Path: "credentials"},
					{Key: "expiredTime", Path: "expiredTime"},
				},
				Optional: utils.Point(true),
			},
		},
	})

	job.Spec.Template.Spec.Containers[0].Env = append(job.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{
		Name:  consts.S3SDKConfigFile,
		Value: "/s3sdk/config",
	})
	job.Spec.Template.Spec.Containers[0].VolumeMounts = append(job.Spec.Template.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name:      "s3sdk-config",
		MountPath: "/s3sdk",
		ReadOnly:  true,
	})
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: "s3sdk-config",
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: r.opts.S3.SDKConfigmapName,
				},
				Items: []corev1.KeyToPath{
					{Key: "config", Path: "config"},
				},
				Optional: utils.Point(true),
			},
		},
	})
}
