package cluster

import "github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient/models"

// Config ...
type Config struct {
	Capacity *Capacity `yaml:"capacity,omitempty"`
	Limits   *Limits   `yaml:"limits,omitempty"`
}

// Capacity ...
type Capacity struct {
	Count       *int         `yaml:"count,omitempty"`
	CPUCores    *int         `yaml:"cpu_cores,omitempty"`
	RamGB       *float64     `yaml:"ram_gb,omitempty"` // nolint
	DiskGB      *float64     `yaml:"disk_gb,omitempty"`
	GPUCapacity *GPUCapacity `yaml:"gpu_capacity,omitempty"`
}

// GPUCapacity ...
type GPUCapacity struct {
	GPU map[string]float64 `yaml:"gpu,omitempty"`
}

// Limits ...
type Limits struct {
	CPUCores *int      `yaml:"cpu_cores,omitempty"`
	RamGB    *float64  `yaml:"ram_gb,omitempty"` // nolint
	GPULimit *GPULimit `yaml:"gpu_limit,omitempty"`
}

// GPULimit ...
type GPULimit struct {
	GPU map[string]float64 `yaml:"gpu,omitempty"`
}

func convertToClientCluster(id string, cfg *Config) *models.PutClusterRequest {
	res := &models.PutClusterRequest{ID: id}
	if cfg == nil {
		return res
	}
	if cfg.Capacity != nil {
		res.Capacity = &models.Capacity{
			Count:    cfg.Capacity.Count,
			CPUCores: cfg.Capacity.CPUCores,
			RamGB:    cfg.Capacity.RamGB,
			DiskGB:   cfg.Capacity.DiskGB,
		}
		if cfg.Capacity.GPUCapacity != nil {
			res.Capacity.GPUCapacity = &models.GPUCapacity{GPU: cfg.Capacity.GPUCapacity.GPU}
		}
	}
	if cfg.Limits != nil {
		res.Limits = &models.Limits{
			CPUCores: cfg.Limits.CPUCores,
			RamGB:    cfg.Limits.RamGB,
		}
		if cfg.Limits.GPULimit != nil {
			res.Limits.GPULimit = &models.GPULimit{GPU: cfg.Limits.GPULimit.GPU}
		}
	}
	return res
}
