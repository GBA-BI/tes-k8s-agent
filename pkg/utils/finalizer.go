package utils

import (
	"context"
	"fmt"

	k8sapierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/cluster-api/util/patch"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// AddObjectFinalizer ...
func AddObjectFinalizer(ctx context.Context, kubeClient ctrlclient.Client, obj ctrlclient.Object, finalizerName string) error {
	if controllerutil.ContainsFinalizer(obj, finalizerName) {
		return nil
	}
	patchHelper, err := patch.NewHelper(obj, kubeClient)
	if err != nil {
		return fmt.Errorf("failed to create object patchHelper: %w", err)
	}
	controllerutil.AddFinalizer(obj, finalizerName)
	if err = patchHelper.Patch(ctx, obj); err != nil {
		return fmt.Errorf("failed to add finalizer to %s: %w", obj.GetName(), err)
	}
	return nil
}

// RemoveObjectFinalizer ...
func RemoveObjectFinalizer(ctx context.Context, kubeClient ctrlclient.Client, obj ctrlclient.Object, finalizerName string) error {
	if !controllerutil.ContainsFinalizer(obj, finalizerName) {
		return nil
	}
	patchHelper, err := patch.NewHelper(obj, kubeClient)
	if err != nil {
		return fmt.Errorf("failed to create object patchHelper: %w", err)
	}
	controllerutil.RemoveFinalizer(obj, finalizerName)
	if err = patchHelper.Patch(ctx, obj); err != nil {
		if k8sapierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to remove finalizer from %s: %w", obj.GetName(), err)
	}
	return nil
}
