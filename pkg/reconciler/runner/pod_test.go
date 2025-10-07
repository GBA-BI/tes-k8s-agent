package runner

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/GBA-BI/tes-k8s-agent/pkg/consts"
	"github.com/GBA-BI/tes-k8s-agent/pkg/utils"
	vetesclientfake "github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient/fake"
	"github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient/models"
)

var fakeInputsFilerJobName = "task-xxxx-inputs-filer"
var fakeInputsFilerPodName = "task-xxxx-inputs-filer"
var fakeExecutorJobName = "task-xxxx-ex-00"
var fakeExecutorPodName = "task-xxxx-ex-00-abcde"

var fakeInputsFilerPod = &corev1.Pod{
	ObjectMeta: metav1.ObjectMeta{
		Namespace: fakeNamespace,
		Name:      fakeInputsFilerPodName,
		UID:       "inputs-filer-pod-uid",
		Labels: map[string]string{
			consts.LabelTaskID:  fakeTaskID,
			consts.LabelJobName: fakeInputsFilerJobName,
			consts.LabelType:    fmt.Sprintf("%s%s", consts.InputsMode, consts.FilerTypeSuffix),
		},
	},
}

var fakeInputsFilerJob = &batchv1.Job{
	ObjectMeta: metav1.ObjectMeta{
		Namespace: fakeNamespace,
		Name:      fakeInputsFilerJobName,
		Labels: map[string]string{
			consts.LabelTaskID: fakeTaskID,
			consts.LabelType:   fmt.Sprintf("%s%s", consts.InputsMode, consts.FilerTypeSuffix),
		},
	},
}

var fakeExecutorPod = &corev1.Pod{
	ObjectMeta: metav1.ObjectMeta{
		Namespace: fakeNamespace,
		Name:      fakeExecutorPodName,
		UID:       "executor-pod-uid",
		Labels: map[string]string{
			consts.LabelTaskID:     fakeTaskID,
			consts.LabelJobName:    fakeExecutorJobName,
			consts.LabelType:       consts.ExecutorType,
			consts.LabelExecutorNo: "0",
		},
	},
}

func TestProcessPodNotFound(t *testing.T) {
	g := gomega.NewWithT(t)

	fakeKubeClient := ctrlfake.NewClientBuilder().Build()
	r := &Runner{
		kubeClient:     fakeKubeClient,
		clusterID:      fakeClusterID,
		namespace:      fakeNamespace,
		taskProcessing: map[string]struct{}{},
		opts:           &Options{},
	}
	resp, err := r.ProcessPod(context.Background(), fakeExecutorPodName)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(resp).To(gomega.Equal(ctrl.Result{}))
}

func TestProcessPodWithoutTaskIDLabel(t *testing.T) {
	g := gomega.NewWithT(t)

	fakeKubeClient := ctrlfake.NewClientBuilder().WithObjects(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: fakeNamespace,
		Name:      fakeExecutorPodName,
	}}).Build()
	r := &Runner{
		kubeClient:     fakeKubeClient,
		clusterID:      fakeClusterID,
		namespace:      fakeNamespace,
		taskProcessing: map[string]struct{}{},
		opts:           &Options{},
	}
	resp, err := r.ProcessPod(context.Background(), fakeExecutorPodName)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(resp).To(gomega.Equal(ctrl.Result{}))
}

func TestProcessPodOtherProcessing(t *testing.T) {
	g := gomega.NewWithT(t)

	fakeKubeClient := ctrlfake.NewClientBuilder().WithObjects(fakeExecutorPod).Build()
	r := &Runner{
		kubeClient: fakeKubeClient,
		clusterID:  fakeClusterID,
		namespace:  fakeNamespace,
		taskProcessing: map[string]struct{}{
			fakeTaskID: {},
		},
		opts: &Options{},
	}
	resp, err := r.ProcessPod(context.Background(), fakeExecutorPodName)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(resp).To(gomega.Equal(ctrl.Result{RequeueAfter: tryProcessLatency}))
}

func TestProcessPodRecordExecutorLog(t *testing.T) {
	g := gomega.NewWithT(t)

	executorPod := fakeExecutorPod.DeepCopy()
	delete(executorPod.Labels, consts.LabelExecutorNo) // to avoid process executor time
	executorPod.Status = corev1.PodStatus{Phase: corev1.PodFailed}

	fakeKubeClient := ctrlfake.NewClientBuilder().WithObjects(executorPod).Build()
	fakeKubeClientNative := kubernetesfake.NewSimpleClientset(executorPod)

	r := &Runner{
		kubeClient:       fakeKubeClient,
		kubeClientNative: fakeKubeClientNative,
		clusterID:        fakeClusterID,
		namespace:        fakeNamespace,
		taskProcessing:   map[string]struct{}{},
		opts: &Options{
			TaskLog: TaskLogOptions{
				OutputDir: "/app/log",
			},
		},
	}

	resp, err := r.ProcessPod(context.Background(), fakeExecutorPodName)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(resp).To(gomega.Equal(ctrl.Result{}))
}

func TestProcessPodProcessExecutorTimeWaiting(t *testing.T) {
	g := gomega.NewWithT(t)

	executorPod := fakeExecutorPod.DeepCopy()
	executorPod.Status = corev1.PodStatus{
		Phase:     corev1.PodPending,
		StartTime: utils.Point(metav1.Now()),
		ContainerStatuses: []corev1.ContainerStatus{{
			State: corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{},
			},
		}},
	}

	fakeKubeClient := ctrlfake.NewClientBuilder().WithObjects(executorPod).Build()

	r := &Runner{
		kubeClient:     fakeKubeClient,
		clusterID:      fakeClusterID,
		namespace:      fakeNamespace,
		taskProcessing: map[string]struct{}{},
		opts:           &Options{},
	}

	resp, err := r.ProcessPod(context.Background(), fakeExecutorPodName)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(resp).To(gomega.Equal(ctrl.Result{}))
}

func TestProcessPodProcessExecutorTimeRunning(t *testing.T) {
	g := gomega.NewWithT(t)
	mockctrl := gomock.NewController(t)
	defer mockctrl.Finish()

	now := time.Now()

	executorPod := fakeExecutorPod.DeepCopy()
	executorPod.Status = corev1.PodStatus{
		Phase:     corev1.PodRunning,
		StartTime: utils.Point(metav1.NewTime(now)),
		ContainerStatuses: []corev1.ContainerStatus{{
			State: corev1.ContainerState{
				Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(now)},
			},
		}},
	}

	fakeKubeClient := ctrlfake.NewClientBuilder().WithObjects(executorPod).Build()

	fakeVeTESClient := vetesclientfake.NewFakeClient(mockctrl)
	fakeVeTESClient.EXPECT().GetTask(gomock.Any(), &models.GetTaskRequest{ID: fakeTaskID, View: consts.BasicView}).
		Return(&models.GetTaskResponse{Task: &models.Task{
			ID:           fakeTaskID,
			State:        consts.TaskRunning,
			ClusterID:    fakeClusterID,
			Logs:         nil,
			CreationTime: time.Now().Add(-time.Hour).Format(time.RFC3339),
		}}, nil)
	fakeVeTESClient.EXPECT().UpdateTask(gomock.Any(), &models.UpdateTaskRequest{
		ID: fakeTaskID,
		Logs: []*models.TaskLog{{
			ClusterID: fakeClusterID,
			StartTime: utils.Point(now.Format(time.RFC3339)),
			Logs: [][]*models.ExecutorLog{{{
				ExecutorID: fakeExecutorPodName,
				StartTime:  utils.Point(now.Format(time.RFC3339)),
			}}},
		}},
	}).Return(&models.UpdateTaskResponse{}, nil)

	r := &Runner{
		vetesClient:    fakeVeTESClient,
		kubeClient:     fakeKubeClient,
		clusterID:      fakeClusterID,
		namespace:      fakeNamespace,
		taskProcessing: map[string]struct{}{},
		opts:           &Options{},
	}

	resp, err := r.ProcessPod(context.Background(), fakeExecutorPodName)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(resp).To(gomega.Equal(ctrl.Result{}))

	gotPod := &corev1.Pod{}
	err = r.kubeClient.Get(context.Background(), ctrlclient.ObjectKey{Name: fakeExecutorPodName, Namespace: fakeNamespace}, gotPod)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(controllerutil.ContainsFinalizer(gotPod, consts.ProcessExecutorTimeFinalizer)).To(gomega.BeTrue())
}

func TestProcessPodProcessExecutorTimeTerminated(t *testing.T) {
	g := gomega.NewWithT(t)
	mockctrl := gomock.NewController(t)
	defer mockctrl.Finish()

	now := time.Now()

	executorPod := fakeExecutorPod.DeepCopy()
	controllerutil.AddFinalizer(executorPod, consts.ProcessExecutorTimeFinalizer)
	executorPod.Status = corev1.PodStatus{
		Phase:     corev1.PodRunning,
		StartTime: utils.Point(metav1.NewTime(now)),
		ContainerStatuses: []corev1.ContainerStatus{{
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					StartedAt:  metav1.NewTime(now),
					FinishedAt: metav1.NewTime(now),
				},
			},
		}},
	}

	fakeKubeClient := ctrlfake.NewClientBuilder().WithObjects(executorPod).Build()

	fakeVeTESClient := vetesclientfake.NewFakeClient(mockctrl)
	fakeVeTESClient.EXPECT().GetTask(gomock.Any(), &models.GetTaskRequest{ID: fakeTaskID, View: consts.BasicView}).
		Return(&models.GetTaskResponse{Task: &models.Task{
			ID:        fakeTaskID,
			State:     consts.TaskRunning,
			ClusterID: fakeClusterID,
			Logs: []*models.TaskLog{{
				ClusterID: fakeClusterID,
				StartTime: utils.Point(now.Add(-time.Minute).Format(time.RFC3339)),
				Logs: [][]*models.ExecutorLog{{{
					ExecutorID: fakeExecutorPodName,
					StartTime:  utils.Point(now.Add(-time.Minute).Format(time.RFC3339)),
				}}},
			}},
			CreationTime: time.Now().Add(-time.Hour).Format(time.RFC3339),
		}}, nil)
	fakeVeTESClient.EXPECT().UpdateTask(gomock.Any(), &models.UpdateTaskRequest{
		ID: fakeTaskID,
		Logs: []*models.TaskLog{{
			ClusterID: fakeClusterID,
			Logs: [][]*models.ExecutorLog{{{
				ExecutorID: fakeExecutorPodName,
				EndTime:    utils.Point(now.Format(time.RFC3339)),
			}}},
		}},
	}).Return(&models.UpdateTaskResponse{}, nil)

	r := &Runner{
		vetesClient:    fakeVeTESClient,
		kubeClient:     fakeKubeClient,
		clusterID:      fakeClusterID,
		namespace:      fakeNamespace,
		taskProcessing: map[string]struct{}{},
		opts:           &Options{},
	}

	resp, err := r.ProcessPod(context.Background(), fakeExecutorPodName)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(resp).To(gomega.Equal(ctrl.Result{}))

	gotPod := &corev1.Pod{}
	err = r.kubeClient.Get(context.Background(), ctrlclient.ObjectKey{Name: fakeExecutorPodName, Namespace: fakeNamespace}, gotPod)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(controllerutil.ContainsFinalizer(gotPod, consts.ProcessExecutorTimeFinalizer)).To(gomega.BeFalse())
}

func TestProcessPodImagePullBackoffTimeout(t *testing.T) {
	g := gomega.NewWithT(t)
	mockctrl := gomock.NewController(t)
	defer mockctrl.Finish()

	now := time.Now()

	inputsFilerPod := fakeInputsFilerPod.DeepCopy()
	inputsFilerPod.Status = corev1.PodStatus{
		Phase:     corev1.PodPending,
		StartTime: utils.Point(metav1.NewTime(now.Add(-time.Minute * 10))),
		ContainerStatuses: []corev1.ContainerStatus{{
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
				Reason: "ImagePullBackOff",
			}},
		}},
	}

	fakeEvent := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: fakeNamespace,
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:       "Pod",
			Namespace:  fakeNamespace,
			Name:       fakeInputsFilerPodName,
			UID:        "inputs-filer-pod-uid",
			APIVersion: "v1",
		},
		Reason:  "Failed",
		Message: "Failed to pull image xxx",
		Type:    corev1.EventTypeWarning,
	}

	fakeKubeClient := ctrlfake.NewClientBuilder().WithObjects(inputsFilerPod, fakeInputsFilerJob).Build()
	fakeKubeClientNative := kubernetesfake.NewSimpleClientset(fakeEvent)

	r := &Runner{
		kubeClient:       fakeKubeClient,
		kubeClientNative: fakeKubeClientNative,
		clusterID:        fakeClusterID,
		namespace:        fakeNamespace,
		taskProcessing:   map[string]struct{}{},
		opts: &Options{
			PodImagePullBackoffTimeout: time.Minute * 10,
		},
	}

	resp, err := r.ProcessPod(context.Background(), fakeInputsFilerPodName)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(resp).To(gomega.Equal(ctrl.Result{}))

	gotJob := &batchv1.Job{}
	err = fakeKubeClient.Get(context.Background(), ctrlclient.ObjectKeyFromObject(fakeInputsFilerJob), gotJob)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(*gotJob.Spec.ActiveDeadlineSeconds).To(gomega.Equal(int64(0)))
}

func TestProcessPodImagePullBackoffNotTimeout(t *testing.T) {
	g := gomega.NewWithT(t)
	mockctrl := gomock.NewController(t)
	defer mockctrl.Finish()

	now := time.Now()

	inputsFilerPod := fakeInputsFilerPod.DeepCopy()
	inputsFilerPod.Status = corev1.PodStatus{
		Phase:     corev1.PodPending,
		StartTime: utils.Point(metav1.NewTime(now.Add(-time.Minute * 5))),
		ContainerStatuses: []corev1.ContainerStatus{{
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
				Reason: "ImagePullBackOff",
			}},
		}},
	}

	fakeEvent := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: fakeNamespace,
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:       "Pod",
			Namespace:  fakeNamespace,
			Name:       fakeInputsFilerPodName,
			UID:        "inputs-filer-pod-uid",
			APIVersion: "v1",
		},
		Reason:  "Failed",
		Message: "Failed to pull image xxx",
		Type:    corev1.EventTypeWarning,
	}

	fakeKubeClient := ctrlfake.NewClientBuilder().WithObjects(inputsFilerPod, fakeInputsFilerJob, fakeEvent).Build()

	r := &Runner{
		kubeClient:     fakeKubeClient,
		clusterID:      fakeClusterID,
		namespace:      fakeNamespace,
		taskProcessing: map[string]struct{}{},
		opts: &Options{
			PodPollInterval:            time.Minute,
			PodImagePullBackoffTimeout: time.Minute * 10,
		},
	}

	resp, err := r.ProcessPod(context.Background(), fakeInputsFilerPodName)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(resp).To(gomega.Equal(ctrl.Result{RequeueAfter: time.Minute}))

	gotJob := &batchv1.Job{}
	err = fakeKubeClient.Get(context.Background(), ctrlclient.ObjectKeyFromObject(fakeInputsFilerJob), gotJob)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(gotJob.Spec.ActiveDeadlineSeconds).To(gomega.BeNil())
}
