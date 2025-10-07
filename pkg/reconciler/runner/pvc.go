package runner

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	k8sapierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/GBA-BI/tes-k8s-agent/pkg/consts"
	"github.com/GBA-BI/tes-k8s-agent/pkg/filelog"
	"github.com/GBA-BI/tes-k8s-agent/pkg/utils"
)

func pvcName(taskID string) string {
	return fmt.Sprintf("%s-pvc", taskID)
}

func (r *Runner) createPVC(ctx context.Context, logger filelog.Logger, taskID string, diskGB float64) error {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.namespace,
			Name:      pvcName(taskID),
			Labels: map[string]string{
				consts.LabelTaskID: taskID,
			},
			Finalizers: []string{consts.ProcessTaskFinalizer},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceStorage: resource.MustParse(fmt.Sprintf("%fGi", diskGB)),
				},
			},
			StorageClassName: utils.Point(r.opts.StorageClass),
		},
	}
	if err := r.kubeClient.Create(ctx, pvc); err != nil {
		if k8sapierrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("failed to create pvc: %w", err)
	}
	logger.Infof("created pvc %s", pvc.Name)
	return nil
}

func (r *Runner) deletePVC(ctx context.Context, logger filelog.Logger, pvcName string) error {
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.kubeClient.Get(ctx, ctrlclient.ObjectKey{Namespace: r.namespace, Name: pvcName}, pvc); err != nil {
		if k8sapierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get pvc: %w", err)
	}

	if err := utils.RemoveObjectFinalizer(ctx, r.kubeClient, pvc, consts.ProcessTaskFinalizer); err != nil {
		return err
	}

	if err := r.kubeClient.Delete(ctx, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: r.namespace,
		Name:      pvcName,
	}}, ctrlclient.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
		if k8sapierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to delete pvc %s: %w", pvcName, err)
	}
	logger.Infof("deleted pvc %s", pvcName)
	return nil
}
