package reconciler

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/GBA-BI/tes-k8s-agent/pkg/consts"
	"github.com/GBA-BI/tes-k8s-agent/pkg/localstore"
	"github.com/GBA-BI/tes-k8s-agent/pkg/reconciler/runner"
)

type reconciler struct {
	runner *runner.Runner
}

type podReconciler struct {
	runner *runner.Runner
}

// RegisterReconciler ...
func RegisterReconciler(mgr ctrl.Manager, localStoreHelper localstore.Helper, runnerImpl *runner.Runner, opts *Options) error {
	r := &reconciler{
		runner: runnerImpl,
	}
	pr := &podReconciler{
		runner: runnerImpl,
	}

	if _, err := ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: opts.Concurrency,
			CacheSyncTimeout:        opts.SyncTimeout,
		}).
		WithEventFilter(predicate.ResourceVersionChangedPredicate{}).
		Watches(&batchv1.Job{}, handler.EnqueueRequestsFromMapFunc(r.enqueueTaskObject)).
		Watches(localStoreHelper.StoreType(), handler.EnqueueRequestsFromMapFunc(r.enqueueTaskObject)).
		Named("task-reconciler").
		Build(r); err != nil {
		return fmt.Errorf("failed to set up with manager: %w", err)
	}
	if _, err := ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: opts.Concurrency,
			CacheSyncTimeout:        opts.SyncTimeout,
		}).
		WithEventFilter(predicate.ResourceVersionChangedPredicate{}).
		For(&corev1.Pod{}, builder.WithPredicates(predicate.NewPredicateFuncs(pr.enqueuePod))).
		Named("pod-reconciler").
		Build(pr); err != nil {
		return fmt.Errorf("failed to set up with manager: %w", err)
	}
	return nil
}

// Reconcile ...
func (r *reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	taskID := req.Name
	return r.runner.ProcessTask(ctx, taskID)
}

// Reconcile ...
func (pr *podReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	return pr.runner.ProcessPod(ctx, req.Name)
}

func (r *reconciler) enqueueTaskObject(_ context.Context, object ctrlclient.Object) []reconcile.Request {
	if object.GetLabels() == nil {
		return nil
	}
	taskID, ok := object.GetLabels()[consts.LabelTaskID]
	if !ok {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: taskID}}}
}

func (pr *podReconciler) enqueuePod(object ctrlclient.Object) bool {
	if object.GetLabels() == nil {
		return false
	}
	_, ok := object.GetLabels()[consts.LabelTaskID]
	return ok
}
