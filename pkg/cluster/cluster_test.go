package cluster

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/onsi/gomega"

	"github.com/GBA-BI/tes-k8s-agent/pkg/utils"
	vetesclientfake "github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient/fake"
	"github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient/models"
)

var (
	fakeClusterID = "cluster-01"
	fakeConfig    = &Config{
		Capacity: &Capacity{
			Count:       utils.Point(10),
			CPUCores:    utils.Point(10),
			RamGB:       utils.Point[float64](100),
			DiskGB:      utils.Point[float64](1000),
			GPUCapacity: &GPUCapacity{GPU: map[string]float64{"type-01": 5}},
		},
		Limits: &Limits{
			CPUCores: utils.Point(4),
			RamGB:    utils.Point[float64](10),
			GPULimit: &GPULimit{GPU: map[string]float64{"type-01": 1}},
		},
	}
	fakePutClusterReq = &models.PutClusterRequest{
		ID: fakeClusterID,
		Capacity: &models.Capacity{
			Count:       utils.Point(10),
			CPUCores:    utils.Point(10),
			RamGB:       utils.Point[float64](100),
			DiskGB:      utils.Point[float64](1000),
			GPUCapacity: &models.GPUCapacity{GPU: map[string]float64{"type-01": 5}},
		},
		Limits: &models.Limits{
			CPUCores: utils.Point(4),
			RamGB:    utils.Point[float64](10),
			GPULimit: &models.GPULimit{GPU: map[string]float64{"type-01": 1}},
		},
	}
)

func TestReportCluster(t *testing.T) {
	g := gomega.NewWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fakeVeTESClient := vetesclientfake.NewFakeClient(ctrl)
	fakeVeTESClient.EXPECT().PutCluster(gomock.Any(), fakePutClusterReq).Return(&models.PutClusterResponse{}, nil)

	r := &reporter{
		vetesClient: fakeVeTESClient,
		id:          fakeClusterID,
		cfg:         fakeConfig,
	}
	g.Expect(func() { r.reportCluster() }).NotTo(gomega.Panic())
}
