package mounttos

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8sutilerrors "k8s.io/apimachinery/pkg/util/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/GBA-BI/tes-k8s-agent/pkg/consts"
	"github.com/GBA-BI/tes-k8s-agent/pkg/localstore"
	"github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient"
	"github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient/models"
)

// Refer to https://bytedance.feishu.cn/docx/RgjYdS6uiohpPcxIA82cwiInn5g
//
// MountTOS 通过挂载 tos 桶实现加速。挂载后可以按需读文件，而不必将整个文件下载到 ebs，
// 从而实现加速。
//
// 在 TES 规范中，一个 input 包含 url 和 path 两个字段，inputs-filer 将文件从 url
// 下载到 path，executor 通过 path 处理文件。url 形如`s3://bucket/path/to/file`
// path 形如`/cromwell-executions/xxx/file`.
//
// 本方案将 tos 桶挂载到 inputs-filer 和 executor 上。`s3://bucket/path/to/file`
// 挂载路径为`/tos-data/bucket/path/to/file`。并修改 input 的 url 为
// `/tos-data/bucket/path/to/file`。这样 url 为文件路径类型，inputs-filer 会创建
// 软链接`/cromwell-executions/xxx/file`到`/tos-data/bucket/path/to/file`，
// 没有实际的数据传输。executor 通过访问`/cromwell-executions/xxx/file`软链接从而
// 访问到挂载的 tos 桶内的数据。

// Impl ...
type Impl struct {
	vetesClient vetesclient.Client
	kubeClient  ctrlclient.Client
	namespace   string

	opts *Options

	pvcWithTasksCacheLock sync.Mutex
	pvcWithTasksCache     map[string]map[string]struct{} // pvcName -> taskID set
}

// New ...
func New(vetesClient vetesclient.Client, kubeClient ctrlclient.Client, namespace string, opts *Options) *Impl {
	return &Impl{
		vetesClient:       vetesClient,
		kubeClient:        kubeClient,
		namespace:         namespace,
		opts:              opts,
		pvcWithTasksCache: make(map[string]map[string]struct{}),
	}
}

// ModifySyncTask ...
func (i *Impl) ModifySyncTask(ctx context.Context, taskFull *models.Task) (accelerateNames []string, retErr error) {
	if taskFull.BioosInfo == nil || taskFull.BioosInfo.Meta == nil ||
		taskFull.BioosInfo.Meta.MountTOS == nil || !*taskFull.BioosInfo.Meta.MountTOS {
		return nil, nil
	}

	externalBucketSet := make(map[string]struct{})
	if taskFull.BioosInfo.Meta.BucketsAuthInfo != nil && len(taskFull.BioosInfo.Meta.BucketsAuthInfo.External) > 0 {
		for _, bucketAuthInfo := range taskFull.BioosInfo.Meta.BucketsAuthInfo.External {
			externalBucketSet[bucketAuthInfo.Bucket] = struct{}{}
		}
	}

	buckets, err := i.getAccelerateBuckets(ctx, taskFull, externalBucketSet)
	if err != nil {
		return nil, err
	}
	if len(buckets) == 0 {
		return nil, nil
	}

	bucketSet := make(map[string]struct{}, len(buckets))
	pvcNames := make([]string, 0, len(buckets))
	for _, bucket := range buckets {
		if _, ok := externalBucketSet[bucket]; ok {
			pvcNames = append(pvcNames, pvcNameFromExternalBucket(taskFull.BioosInfo.SubmissionID, bucket))
		} else {
			pvcNames = append(pvcNames, pvcNameFromBucket(bucket))
		}
		bucketSet[bucket] = struct{}{}
	}
	for _, input := range taskFull.Inputs {
		bucket := extractBucket(input.URL)
		if bucket == "" {
			continue
		}
		if _, ok := bucketSet[bucket]; !ok {
			continue
		}
		input.URL = strings.Replace(input.URL, fmt.Sprintf("s3://%s", bucket), mountTOSPath(bucket), 1)
	}

	return pvcNames, nil
}

func (i *Impl) getAccelerateBuckets(ctx context.Context, taskFull *models.Task, externalBucketSet map[string]struct{}) ([]string, error) {
	buckets := extractBucketsAndSort(taskFull.Inputs)
	if len(buckets) == 0 {
		return nil, nil
	}

	defaultSecretName := i.opts.StaticTOSSecret.Name
	// if no aksk config for shared cluster or no static tos secret for private cloud, defaultSupport is false,
	// but there may be some external buckets can be mounted
	var defaultSupportMountTOS bool = defaultSecretName != ""

	// if default not support mount tos, only filter external buckets
	if !defaultSupportMountTOS {
		externalBuckets := make([]string, 0)
		for _, bucket := range buckets {
			if _, ok := externalBucketSet[bucket]; ok {
				externalBuckets = append(externalBuckets, bucket)
			}
		}
		buckets = externalBuckets
	}
	if len(buckets) > i.opts.BucketNumPerTask {
		buckets = buckets[:i.opts.BucketNumPerTask]
	}
	return buckets, nil
}

// ModifyInputsFiler ...
func (i *Impl) ModifyInputsFiler(podTemplate *corev1.PodTemplateSpec, localTask *localstore.Task) {
	i.mountPVC(podTemplate, localTask)
	i.addMountTOSEnv(podTemplate, localTask)
}

// ModifyExecutor ...
func (i *Impl) ModifyExecutor(podTemplate *corev1.PodTemplateSpec, localTask *localstore.Task) {
	i.mountPVC(podTemplate, localTask)
}

// ModifyOutputsFiler ...
func (i *Impl) ModifyOutputsFiler(podTemplate *corev1.PodTemplateSpec, localTask *localstore.Task) {
	// although we don't need to upload file by mount tos, but sometimes we have to upload file from input,
	// such as "script", so we have to mount tos on outputs-filer
	i.mountPVC(podTemplate, localTask)
	i.addMountTOSEnv(podTemplate, localTask)
}

// OnProcessTask ...
func (i *Impl) OnProcessTask(ctx context.Context, localTask *localstore.Task) (ctrl.Result, error) {
	if localTask.BioosInfo == nil {
		return ctrl.Result{}, nil
	}
	pvcNames := localTask.AccelerateNames
	if len(pvcNames) == 0 {
		return ctrl.Result{}, nil
	}

	defaultSecretName := i.opts.StaticTOSSecret.Name

	externalBucketAuthInfo := make(map[string][]string) // bucket -> [ak, sk]
	if localTask.BioosInfo.Meta != nil && localTask.BioosInfo.Meta.BucketsAuthInfo != nil && len(localTask.BioosInfo.Meta.BucketsAuthInfo.External) > 0 {
		for _, bucketAuthInfo := range localTask.BioosInfo.Meta.BucketsAuthInfo.External {
			externalBucketAuthInfo[bucketAuthInfo.Bucket] = []string{bucketAuthInfo.AK, bucketAuthInfo.SK}
		}
	}

	i.pvcWithTasksCacheLock.Lock()
	defer i.pvcWithTasksCacheLock.Unlock()

	var errs []error
	existDeleting := false
	for _, pvcName := range pvcNames {
		if _, ok := i.pvcWithTasksCache[pvcName]; ok {
			i.pvcWithTasksCache[pvcName][localTask.ID] = struct{}{}
			continue
		}
		bucket := parseBucketFromPVCName(pvcName)
		secretName := defaultSecretName
		if authInfo, ok := externalBucketAuthInfo[bucket]; ok {
			secretName = externalBucketTOSSecretName(localTask.BioosInfo.SubmissionID, bucket)
			deleting, err := i.storeKubeSecret(ctx, secretName, authInfo[0], authInfo[1])
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if deleting {
				existDeleting = true
				continue
			}
		}
		deleting, err := i.createPVAndPVC(ctx, pvcName, secretName, i.opts.FusePodResources, i.opts.AdditionalArgs)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if deleting {
			existDeleting = true
			continue
		}
		i.pvcWithTasksCache[pvcName] = make(map[string]struct{})
		i.pvcWithTasksCache[pvcName][localTask.ID] = struct{}{}
	}

	reterr := k8sutilerrors.NewAggregate(errs)
	if reterr != nil {
		return ctrl.Result{}, reterr
	}
	if existDeleting {
		return ctrl.Result{RequeueAfter: deletingWaitDuration}, nil
	}
	return ctrl.Result{}, nil
}

// OnFinishTask ...
func (i *Impl) OnFinishTask(ctx context.Context, localTask *localstore.Task) error {
	if localTask.BioosInfo == nil {
		return nil
	}
	pvcNames := localTask.AccelerateNames
	if len(pvcNames) == 0 {
		return nil
	}

	externalBucketSet := make(map[string]struct{})
	if localTask.BioosInfo.Meta != nil && localTask.BioosInfo.Meta.BucketsAuthInfo != nil && len(localTask.BioosInfo.Meta.BucketsAuthInfo.External) > 0 {
		for _, bucketAuthInfo := range localTask.BioosInfo.Meta.BucketsAuthInfo.External {
			externalBucketSet[bucketAuthInfo.Bucket] = struct{}{}
		}
	}

	i.pvcWithTasksCacheLock.Lock()
	defer i.pvcWithTasksCacheLock.Unlock()

	var errs []error
	for _, pvcName := range pvcNames {
		if _, ok := i.pvcWithTasksCache[pvcName]; ok {
			delete(i.pvcWithTasksCache[pvcName], localTask.ID)
			if len(i.pvcWithTasksCache[pvcName]) > 0 {
				continue
			}
		}
		if err := i.deletePVAndPVC(ctx, pvcName); err != nil {
			errs = append(errs, err)
			continue
		}
		bucket := parseBucketFromPVCName(pvcName)
		if _, ok := externalBucketSet[bucket]; ok {
			secretName := externalBucketTOSSecretName(localTask.BioosInfo.SubmissionID, bucket)
			if err := i.deleteKubeSecret(ctx, i.namespace, secretName); err != nil {
				errs = append(errs, err)
				continue
			}
		}
		delete(i.pvcWithTasksCache, pvcName)
	}

	return k8sutilerrors.NewAggregate(errs)
}

func (i *Impl) mountPVC(podTemplate *corev1.PodTemplateSpec, localTask *localstore.Task) {
	if len(localTask.AccelerateNames) == 0 {
		return
	}

	for _, pvcName := range localTask.AccelerateNames {
		podTemplate.Spec.Volumes = append(podTemplate.Spec.Volumes, corev1.Volume{
			Name: pvcName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvcName,
					ReadOnly:  true,
				},
			},
		})
		for i := range podTemplate.Spec.Containers {
			podTemplate.Spec.Containers[i].VolumeMounts = append(podTemplate.Spec.Containers[i].VolumeMounts, corev1.VolumeMount{
				Name:      pvcName,
				ReadOnly:  true,
				MountPath: mountTOSPath(parseBucketFromPVCName(pvcName)),
			})
		}
	}
}

func (i *Impl) addMountTOSEnv(podTemplate *corev1.PodTemplateSpec, localTask *localstore.Task) {
	if len(localTask.AccelerateNames) == 0 {
		return
	}
	for i := range podTemplate.Spec.Containers {
		podTemplate.Spec.Containers[i].Env = append(podTemplate.Spec.Containers[i].Env, corev1.EnvVar{
			Name:  consts.IsMountTOS,
			Value: strconv.FormatBool(true),
		})
	}
}

// CronCleanFunc ...
func (i *Impl) CronCleanFunc() (func(), *time.Duration) {
	return nil, nil
}

func mountTOSPath(bucket string) string {
	return fmt.Sprintf("/tos-data/%s", bucket)
}
