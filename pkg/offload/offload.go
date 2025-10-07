package offload

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"

	"github.com/GBA-BI/tes-k8s-agent/pkg/consts"
)

// Helper ...
type Helper interface {
	OffloadInputs(taskID string, inputsJSON []byte) (string, error)
	OffloadOutputs(taskID string, outputsJSON []byte) (string, error)
	DeleteOffloadFile(taskID string)
	ModifyInputsFiler(taskID string, podTemplate *corev1.PodTemplateSpec)
	ModifyOutputsFiler(taskID string, podTemplate *corev1.PodTemplateSpec)
}

// NewHelper ...
func NewHelper(opts *Options) (Helper, error) {
	switch opts.Type {
	case consts.PVCOffloadType:
		return NewPVCHelper(opts.PVC), nil
	default:
		return nil, fmt.Errorf("unsupported offload type: %s", opts.Type)
	}
}
