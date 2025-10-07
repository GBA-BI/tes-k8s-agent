package mounttos

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	k8sapierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/GBA-BI/tes-k8s-agent/pkg/consts"
)

const (
	// tosSecretKeyAK is secret data key of ak in tos secret
	tosSecretKeyAK = "akId"
	// tosSecretKeySK is secret data key of sk in tos secret
	tosSecretKeySK = "akSecret"
)

func (i *Impl) storeKubeSecret(ctx context.Context, name, ak, sk string) (deleting bool, err error) {
	kubeSecret := &corev1.Secret{}
	if err := i.kubeClient.Get(ctx, ctrlclient.ObjectKey{Namespace: i.namespace, Name: name}, kubeSecret); err != nil {
		if k8sapierrors.IsNotFound(err) {
			return false, i.createKubeSecret(ctx, name, ak, sk)
		}
		return false, fmt.Errorf("failed to get secret from k8s: %w", err)
	}
	// not managed by vetes-k8s-agent, do noting
	if kubeSecret.Labels == nil || kubeSecret.Labels[consts.LabelManagedBy] != consts.ManagedByVeTESK8SAgent {
		return false, nil
	}
	if !kubeSecret.DeletionTimestamp.IsZero() {
		return true, nil
	}

	// compare and update secret
	if ak == string(kubeSecret.Data[tosSecretKeyAK]) && sk == string(kubeSecret.Data[tosSecretKeySK]) {
		return false, nil
	}
	patch := ctrlclient.StrategicMergeFrom(kubeSecret.DeepCopy())
	kubeSecret.Data[tosSecretKeyAK] = []byte(ak)
	kubeSecret.Data[tosSecretKeySK] = []byte(sk)
	if err := i.kubeClient.Patch(ctx, kubeSecret, patch); err != nil {
		return false, fmt.Errorf("failed to upate tos secret aksk: %w", err)
	}
	return false, nil
}

func (i *Impl) createKubeSecret(ctx context.Context, name, ak, sk string) error {
	kubeSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: i.namespace,
			Name:      name,
			Labels: map[string]string{
				consts.LabelManagedBy: consts.ManagedByVeTESK8SAgent,
			},
		},
		Data: map[string][]byte{
			tosSecretKeyAK: []byte(ak),
			tosSecretKeySK: []byte(sk),
		},
	}
	if err := i.kubeClient.Create(ctx, kubeSecret); err != nil {
		if k8sapierrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("failed to create secret in k8s: %w", err)
	}
	return nil
}

func (i *Impl) deleteKubeSecret(ctx context.Context, namespace, name string) error {
	if err := i.kubeClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}},
		ctrlclient.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
		if k8sapierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to delete secret in k8s: %w", err)
	}
	return nil
}

func externalBucketTOSSecretName(submissionID, bucket string) string {
	return fmt.Sprintf("%s-%s", submissionID, bucket)
}
