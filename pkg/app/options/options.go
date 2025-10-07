package options

import (
	"github.com/GBA-BI/tes-k8s-agent/pkg/log"
	"github.com/spf13/pflag"

	"github.com/GBA-BI/tes-k8s-agent/pkg/accelerate"
	"github.com/GBA-BI/tes-k8s-agent/pkg/cluster"
	"github.com/GBA-BI/tes-k8s-agent/pkg/offload"
	"github.com/GBA-BI/tes-k8s-agent/pkg/reconciler"
	"github.com/GBA-BI/tes-k8s-agent/pkg/reconciler/runner"
	"github.com/GBA-BI/tes-k8s-agent/pkg/syncer"
	"github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient"
)

// Options ...
type Options struct {
	Log            *log.Options           `mapstructure:"log"`
	VeTESClient    *vetesclient.Options   `mapstructure:"vetesClient"`
	LeaderElection *LeaderElectionOptions `mapstructure:"leaderElection"`
	Server         *ServerOptions         `mapstructure:"server"`
	Cluster        *cluster.Options       `mapstructure:"cluster"`
	Syncer         *syncer.Options        `mapstructure:"syncer"`
	Reconciler     *reconciler.Options    `mapstructure:"reconciler"`
	Offload        *offload.Options       `mapstructure:"offload"`
	Runner         *runner.Options        `mapstructure:"runner"`
	Accelerate     *accelerate.Options    `mapstructure:"accelerate"`
	Namespace      string                 `mapstructure:"namespace"`
}

// NewOptions ...
func NewOptions() *Options {
	return &Options{
		Log:            log.NewOptions(),
		VeTESClient:    vetesclient.NewOptions(),
		LeaderElection: NewLeaderElectionOptions(),
		Server:         NewServerOptions(),
		Cluster:        cluster.NewOptions(),
		Syncer:         syncer.NewOptions(),
		Reconciler:     reconciler.NewOptions(),
		Offload:        offload.NewOptions(),
		Runner:         runner.NewOptions(),
		Accelerate:     accelerate.NewOptions(),
		Namespace:      "vetes-system",
	}
}

// Validate ...
func (o *Options) Validate() error {
	if err := o.Log.Validate(); err != nil {
		return err
	}
	if err := o.VeTESClient.Validate(); err != nil {
		return err
	}
	if err := o.LeaderElection.Validate(); err != nil {
		return err
	}
	if err := o.Server.Validate(); err != nil {
		return err
	}
	if err := o.Cluster.Validate(); err != nil {
		return err
	}
	if err := o.Syncer.Validate(); err != nil {
		return err
	}
	if err := o.Reconciler.Validate(); err != nil {
		return err
	}
	if err := o.Offload.Validate(); err != nil {
		return err
	}
	if err := o.Runner.Validate(); err != nil {
		return err
	}
	if err := o.Accelerate.Validate(); err != nil {
		return err
	}
	return nil
}

// AddFlags ...
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.Log.AddFlags(fs)
	o.VeTESClient.AddFlags(fs)
	o.LeaderElection.AddFlags(fs)
	o.Server.AddFlags(fs)
	o.Cluster.AddFlags(fs)
	o.Syncer.AddFlags(fs)
	o.Reconciler.AddFlags(fs)
	o.Offload.AddFlags(fs)
	o.Runner.AddFlags(fs)
	o.Accelerate.AddFlags(fs)
	fs.StringVarP(&o.Namespace, "namespace", "n", o.Namespace, "namespace for running tasks")
}
