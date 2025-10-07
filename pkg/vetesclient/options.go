package vetesclient

import (
	"time"

	"github.com/spf13/pflag"
)

// Options ...
type Options struct {
	Endpoint string        `mapstructure:"endpoint"`
	Timeout  time.Duration `mapstructure:"timeout"`
}

// NewOptions ...
func NewOptions() *Options {
	return &Options{
		Endpoint: "http://vetes-api.vetes-system:8080",
		Timeout:  10 * time.Minute, // for huge inputs/outputs
	}
}

// Validate ...
func (o *Options) Validate() error {
	return nil
}

// AddFlags ...
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.Endpoint, "vetes-client-endpoint", o.Endpoint, "endpoint of the vetes-client")
	fs.DurationVar(&o.Timeout, "vetes-client-timeout", o.Timeout, "timeout of the vetes-client")
}
