package mounttos

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8sapierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/GBA-BI/tes-k8s-agent/pkg/consts"
	"github.com/GBA-BI/tes-k8s-agent/pkg/utils"
)

const deletingWaitDuration = time.Second * 5

func (i *Impl) createPVAndPVC(ctx context.Context, pvcName, secretName string, fusePodResources FusePodResources, fuseAdditionalArgs string) (deleting bool, err error) {
	gotPV := &corev1.PersistentVolume{}
	pvExist := true
	if err = i.kubeClient.Get(ctx, ctrlclient.ObjectKey{Name: pvcName}, gotPV); err != nil {
		if !k8sapierrors.IsNotFound(err) {
			return false, fmt.Errorf("failed to get pv: %w", err)
		}
		pvExist = false
	}
	if pvExist && !gotPV.DeletionTimestamp.IsZero() {
		return true, nil
	}

	gotPVC := &corev1.PersistentVolumeClaim{}
	pvcExist := true
	if err = i.kubeClient.Get(ctx, ctrlclient.ObjectKey{Namespace: i.namespace, Name: pvcName}, gotPVC); err != nil {
		if !k8sapierrors.IsNotFound(err) {
			return false, fmt.Errorf("failed to get pvc: %w", err)
		}
		pvcExist = false
	}
	if pvcExist && !gotPVC.DeletionTimestamp.IsZero() {
		return true, nil
	}

	// Only if both PV and PVC are not deleting, we can create them. Otherwise, a new created
	// PV may bound to a deleting PVC.
	if !pvExist {
		if err = i.kubeClient.Create(ctx, i.newPV(pvcName, secretName, fusePodResources, fuseAdditionalArgs)); err != nil {
			if k8sapierrors.IsAlreadyExists(err) {
				return false, nil
			}
			return false, fmt.Errorf("failed to create pv: %w", err)
		}
	}
	if !pvcExist {
		if err = i.kubeClient.Create(ctx, i.newPVC(pvcName)); err != nil {
			if k8sapierrors.IsAlreadyExists(err) {
				return false, nil
			}
			return false, fmt.Errorf("failed to create pvc: %w", err)
		}
	}

	return false, nil
}

func (i *Impl) newPV(pvcName, secretName string, fusePodResources FusePodResources, fuseAdditionalArgs string) *corev1.PersistentVolume {
	bucket := parseBucketFromPVCName(pvcName)

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvcName,
		},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver: "tos.csi.volcengine.com",
					NodePublishSecretRef: &corev1.SecretReference{
						Name:      secretName,
						Namespace: i.namespace,
					},
					NodeStageSecretRef: &corev1.SecretReference{
						Name:      secretName,
						Namespace: i.namespace,
					},
					VolumeAttributes: map[string]string{
						"bucket":                  bucket,
						"path":                    "/",
						"url":                     i.opts.TOSS3URL,
						"fuse_pod_cpu_request":    fusePodResources.Requests.CPU,
						"fuse_pod_cpu_limit":      fusePodResources.Limits.CPU,
						"fuse_pod_memory_request": fusePodResources.Requests.Memory,
						"fuse_pod_memory_limit":   fusePodResources.Limits.Memory,
					},
					VolumeHandle: pvcName,
				},
			},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			VolumeMode:                    utils.Point(corev1.PersistentVolumeFilesystem),
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: storageQuantity,
			},
		},
	}

	if fuseAdditionalArgs != "" {
		pv.Spec.PersistentVolumeSource.CSI.VolumeAttributes["additional_args"] = fuseAdditionalArgs
	}

	pv.Labels = map[string]string{consts.LabelBucketName: bucket}

	return pv
}

func (i *Impl) newPVC(pvcName string) *corev1.PersistentVolumeClaim {
	bucket := parseBucketFromPVCName(pvcName)

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: i.namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: storageQuantity,
				},
			},
			VolumeMode: utils.Point(corev1.PersistentVolumeFilesystem),
			VolumeName: pvcName,
		},
	}

	pvc.Labels = map[string]string{consts.LabelBucketName: bucket}

	return pvc
}

func (i *Impl) deletePVAndPVC(ctx context.Context, pvcName string) error {
	if err := i.kubeClient.Delete(ctx, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: pvcName, Namespace: i.namespace,
	}}, ctrlclient.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
		if k8sapierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to delete pvc: %w", err)
	}

	if err := i.kubeClient.Delete(ctx, &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{
		Name: pvcName,
	}}, ctrlclient.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
		if k8sapierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to delete pv: %w", err)
	}
	return nil
}

func pvcNameFromExternalBucket(submissionID, bucket string) string {
	return fmt.Sprintf("%s-%s", submissionID, bucket)
}

func pvcNameFromBucket(bucket string) string {
	return fmt.Sprintf("workflow-%s", bucket)
}

func parseBucketFromPVCName(pvcName string) string {
	splited := strings.SplitN(pvcName, "-", 2)
	if len(splited) < 2 {
		return pvcName
	}
	// workflow-<bucket>
	// <submissionID>-<bucket>
	return splited[1]
}

var storageQuantity = resource.MustParse("20Gi")
