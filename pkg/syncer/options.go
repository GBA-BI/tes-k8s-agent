package syncer

import (
	"fmt"
	"time"

	"github.com/spf13/pflag"
)

// Options ...
type Options struct {
	Period      time.Duration `mapstructure:"period"`
	Concurrency int           `mapstructure:"concurrency"`
}

// NewOptions ...
func NewOptions() *Options {
	return &Options{
		Period:      time.Second * 10,
		Concurrency: 10,
	}
}

// Validate ...
func (o *Options) Validate() error {
	if o.Period < time.Second {
		return fmt.Errorf("task sync period %s should not less than 1s", o.Period.String())
	}
	if o.Concurrency <= 0 {
		return fmt.Errorf("sync concurrency %d should be positive", o.Concurrency)
	}
	return nil
}

// AddFlags ...
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.DurationVar(&o.Period, "syncer-period", o.Period, "period of sync tasks")
	fs.IntVar(&o.Concurrency, "syncer-concurrency", o.Concurrency, "concurrency of sync tasks")
}
