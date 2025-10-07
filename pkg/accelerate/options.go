package accelerate

import (
	"fmt"

	"github.com/spf13/pflag"

	"github.com/GBA-BI/tes-k8s-agent/pkg/accelerate/mounttos"
	"github.com/GBA-BI/tes-k8s-agent/pkg/consts"
)

// Options ...
type Options struct {
	Type     string            `mapstructure:"type"`
	MountTOS *mounttos.Options `mappstructure:"mounttos"`
}

// NewOptions ...
func NewOptions() *Options {
	return &Options{
		Type:     consts.NullAccelerateType,
		MountTOS: mounttos.NewOptions(),
	}
}

// Validate ...
func (o *Options) Validate() error {
	switch o.Type {
	case consts.NullAccelerateType:
	case consts.MountTOSAccelerateType:
		if err := o.MountTOS.Validate(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid accelerate type: %s", o.Type)
	}
	return nil
}

// AddFlags ...
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.Type, "accelerate-type", o.Type, "accelerate type")
	o.MountTOS.AddFlags(fs)
}
