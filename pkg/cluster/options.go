package cluster

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/spf13/pflag"
)

// Options ...
type Options struct {
	ID           string        `mapstructure:"id"`
	ConfigPath   string        `mapstructure:"configPath"`
	ReportPeriod time.Duration `mapstructure:"reportPeriod"`
}

// NewOptions ...
func NewOptions() *Options {
	return &Options{
		ConfigPath:   "/app/conf/cluster.yaml",
		ReportPeriod: time.Second * 15,
	}
}

// Validate ...
func (o *Options) Validate() error {
	if o.ID == "" {
		return errors.New("empty cluster id")
	}
	if _, err := os.Stat(o.ConfigPath); err != nil {
		return fmt.Errorf("cluster config path %s not exist", o.ConfigPath)
	}
	if o.ReportPeriod < time.Second {
		return fmt.Errorf("cluster report period %s should not less than 1s", o.ReportPeriod.String())
	}
	return nil
}

// AddFlags ...
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.ID, "cluster-id", o.ID, "cluster id")
	fs.StringVar(&o.ConfigPath, "cluster-config-path", o.ConfigPath, "cluster config path, should be yaml")
	fs.DurationVar(&o.ReportPeriod, "cluster-report-period", o.ReportPeriod, "period of reporting cluster")
}
