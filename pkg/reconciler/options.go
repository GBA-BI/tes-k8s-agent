package reconciler

import (
	"fmt"
	"time"

	"github.com/spf13/pflag"
)

// Options ...
type Options struct {
	SyncTimeout time.Duration `mapstructure:"syncTimeout"`
	Concurrency int           `mapstructure:"concurrency"`
}

// NewOptions ...
func NewOptions() *Options {
	return &Options{
		SyncTimeout: time.Minute,
		Concurrency: 10,
	}
}

// Validate ...
func (o *Options) Validate() error {
	if o.SyncTimeout < time.Minute {
		return fmt.Errorf("reconciler sync timeout %s should not less than 1m", o.SyncTimeout.String())
	}
	if o.Concurrency <= 0 {
		return fmt.Errorf("reconciler concurrency %d should be positive", o.Concurrency)
	}
	return nil
}

// AddFlags ...
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.DurationVar(&o.SyncTimeout, "reconciler-sync-timeout", o.SyncTimeout, "reconciler sync timeout")
	fs.IntVar(&o.Concurrency, "reconciler-concurrency", o.Concurrency, "reconciler concurrency")
}
