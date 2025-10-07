package offload

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/pflag"
	corev1 "k8s.io/api/core/v1"

	"github.com/GBA-BI/tes-k8s-agent/pkg/consts"
)

// PVCOptions ...
type PVCOptions struct {
	PVCName string `mapstructure:"pvcName"`
	Path    string `mapstructure:"path"`
}

// NewPVCOptions ...
func NewPVCOptions() *PVCOptions {
	return &PVCOptions{
		Path: "/offload",
	}
}

// Validate ...
func (o *PVCOptions) Validate() error {
	if !filepath.IsAbs(o.Path) {
		return fmt.Errorf("offload pvc path %s should be absolute path", o.Path)
	}
	s, err := os.Stat(o.Path)
	if err != nil {
		return fmt.Errorf("invalid offload pvc path %s: %w", o.Path, err)
	}
	if !s.IsDir() {
		return fmt.Errorf("offload pvc path %s should be a directory", o.Path)
	}
	if o.PVCName == "" {
		return errors.New("offload pvc name should not be empty")
	}
	return nil
}

// AddFlags ...
func (o *PVCOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.PVCName, "offload-pvc-name", o.PVCName, "offload pvc name")
	fs.StringVar(&o.Path, "offload-pvc-path", o.Path, "offload pvc path, which should be absolute")
}

type pvcHelper struct {
	path    string
	pvcName string
}

// NewPVCHelper ...
func NewPVCHelper(opts *PVCOptions) Helper {
	return &pvcHelper{
		path:    opts.Path,
		pvcName: opts.PVCName,
	}
}

var _ Helper = (*pvcHelper)(nil)

// OffloadInputs ...
func (p *pvcHelper) OffloadInputs(taskID string, inputsJSON []byte) (string, error) {
	return p.offload(taskID, inputsJSON, inputsFileName)
}

// OffloadOutputs ...
func (p *pvcHelper) OffloadOutputs(taskID string, outputsJSON []byte) (string, error) {
	return p.offload(taskID, outputsJSON, outputsFileName)
}

func (p *pvcHelper) offload(taskID string, content []byte, fileName string) (string, error) {
	offloadDir := filepath.Join(p.path, taskID)
	if err := os.MkdirAll(offloadDir, 0755); err != nil {
		return "", fmt.Errorf("failed to mkdir %s: %w", offloadDir, err)
	}
	offloadPath := filepath.Join(offloadDir, fileName)
	if err := os.WriteFile(offloadPath, content, 0755); err != nil {
		return "", fmt.Errorf("failed to write file %s: %w", offloadPath, err)
	}
	return offloadPath, nil
}

// DeleteOffloadFile ...
func (p *pvcHelper) DeleteOffloadFile(taskID string) {
	offloadDir := filepath.Join(p.path, taskID)
	_ = os.RemoveAll(offloadDir)
}

// ModifyInputsFiler ...
func (p *pvcHelper) ModifyInputsFiler(taskID string, podTemplate *corev1.PodTemplateSpec) {
	p.modifyFiler(taskID, podTemplate)
}

// ModifyOutputsFiler ...
func (p *pvcHelper) ModifyOutputsFiler(taskID string, podTemplate *corev1.PodTemplateSpec) {
	p.modifyFiler(taskID, podTemplate)
}

func (p *pvcHelper) modifyFiler(taskID string, podTemplate *corev1.PodTemplateSpec) {
	podTemplate.Spec.Volumes = append(podTemplate.Spec.Volumes, corev1.Volume{
		Name: offloadVolumeName,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: p.pvcName,
				ReadOnly:  true,
			},
		},
	})
	for i := range podTemplate.Spec.Containers {
		podTemplate.Spec.Containers[i].VolumeMounts = append(podTemplate.Spec.Containers[i].VolumeMounts, corev1.VolumeMount{
			Name:      offloadVolumeName,
			ReadOnly:  true,
			MountPath: filepath.Join(p.path, taskID),
			SubPath:   taskID,
		})
		podTemplate.Spec.Containers[i].Env = append(podTemplate.Spec.Containers[i].Env, corev1.EnvVar{
			Name:  consts.OffloadType,
			Value: consts.PVCOffloadType,
		}, corev1.EnvVar{
			Name:  consts.OffloadPVCName,
			Value: p.pvcName,
		}, corev1.EnvVar{
			Name:  consts.OffloadPath,
			Value: p.path,
		})
	}
}

const (
	offloadVolumeName = "offload-volume"
	inputsFileName    = "inputs.json"
	outputsFileName   = "outputs.json"
)
