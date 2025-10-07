package vetesclient

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/GBA-BI/tes-k8s-agent/pkg/consts"
	"github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient/models"
)

func TestClient(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "vetes client")
}

var fakeEndpoint = "http://vetes-api:8080"
var fakeClient = NewClient(&Options{
	Endpoint: fakeEndpoint,
})
var (
	fakeTaskID    = "task-xxxx"
	fakeClusterID = "cluster-xxxx"
)

var _ = ginkgo.BeforeSuite(func() {
	httpmock.ActivateNonDefault(fakeClient.(*impl).cli)
})
var _ = ginkgo.AfterSuite(func() {
	httpmock.DeactivateAndReset()
})
var _ = ginkgo.AfterEach(func() {
	httpmock.Reset()
})

var _ = ginkgo.It("ListTasks", func() {
	fakeResp := &models.ListTasksResponse{
		Tasks: []*models.Task{{
			ID:    fakeTaskID,
			State: consts.TaskQueued,
		}},
		NextPageToken: "next-token",
	}
	responder, _ := httpmock.NewJsonResponder(200, fakeResp)
	httpmock.RegisterResponder(http.MethodGet, fmt.Sprintf("%s%s/tasks?cluster_id=%s&name_prefix=%s&page_size=%s&page_token=%s&state=%s&state=%s&view=%s&without_cluster=%s",
		fakeEndpoint, ga4ghAPIPrefix, "cluster-01", "task-", "256", "last-token", consts.TaskQueued, consts.TaskCanceling, consts.MinimalView, "true"), responder)
	resp, err := fakeClient.ListTasks(context.Background(), &models.ListTasksRequest{
		NamePrefix:     "task-",
		State:          []string{consts.TaskQueued, consts.TaskCanceling},
		ClusterID:      "cluster-01",
		WithoutCluster: true, // this is invalid, but we just test query param here
		View:           consts.MinimalView,
		PageSize:       256,
		PageToken:      "last-token",
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(resp).To(gomega.BeEquivalentTo(fakeResp))
})

var _ = ginkgo.It("GetTask", func() {
	fakeResp := &models.GetTaskResponse{Task: &models.Task{
		ID:    fakeTaskID,
		State: consts.TaskCanceling,
	}}
	responder, _ := httpmock.NewJsonResponder(200, fakeResp)
	httpmock.RegisterResponder(http.MethodGet, fmt.Sprintf("%s%s/tasks/%s?view=%s", fakeEndpoint, ga4ghAPIPrefix, fakeTaskID, consts.MinimalView), responder)
	resp, err := fakeClient.GetTask(context.Background(), &models.GetTaskRequest{
		ID:   fakeTaskID,
		View: consts.MinimalView,
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(resp).To(gomega.BeEquivalentTo(fakeResp))
})

var _ = ginkgo.It("UpdateTask", func() {
	fakeResp := &models.UpdateTaskResponse{}
	responder, _ := httpmock.NewJsonResponder(200, fakeResp)
	httpmock.RegisterResponder(http.MethodPatch, fmt.Sprintf("%s%s/tasks/%s", fakeEndpoint, otherAPIPrefix, fakeTaskID), responder)
	_, err := fakeClient.UpdateTask(context.Background(), &models.UpdateTaskRequest{ID: fakeTaskID})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
})

var _ = ginkgo.It("PutCluster", func() {
	fakeResp := &models.PutClusterResponse{}
	responder, _ := httpmock.NewJsonResponder(200, fakeResp)
	httpmock.RegisterResponder(http.MethodPut, fmt.Sprintf("%s%s/clusters/%s", fakeEndpoint, otherAPIPrefix, fakeClusterID), responder)
	_, err := fakeClient.PutCluster(context.Background(), &models.PutClusterRequest{ID: fakeClusterID})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
})
