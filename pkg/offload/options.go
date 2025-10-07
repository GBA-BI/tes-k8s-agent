package offload

import (
	"fmt"

	"github.com/spf13/pflag"

	"github.com/GBA-BI/tes-k8s-agent/pkg/consts"
)

// Options ...
type Options struct {
	Type string      `mapstructure:"type"`
	PVC  *PVCOptions `mapstructure:"pvc"`
}

// NewOptions ...
func NewOptions() *Options {
	return &Options{
		Type: consts.PVCOffloadType,
		PVC:  NewPVCOptions(),
	}
}

// Validate ...
func (o *Options) Validate() error {
	switch o.Type {
	case consts.PVCOffloadType:
		return o.PVC.Validate()
	default:
		return fmt.Errorf("unsupported offload type: %s", o.Type)
	}
}

// AddFlags ...
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.Type, "offload-type", o.Type, "offload type")
	o.PVC.AddFlags(fs)
}
