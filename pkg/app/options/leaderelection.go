package options

import (
	"errors"

	"github.com/spf13/pflag"
)

// LeaderElectionOptions ...
type LeaderElectionOptions struct {
	Enable    bool   `mapstructure:"enable"`
	Namespace string `mapstructure:"namespace"`
	Name      string `mapstructure:"name"`
}

// NewLeaderElectionOptions ...
func NewLeaderElectionOptions() *LeaderElectionOptions {
	return &LeaderElectionOptions{
		Enable:    true,
		Namespace: "vetes-system",
		Name:      "vetes-k8s-agent",
	}
}

// Validate ...
func (o *LeaderElectionOptions) Validate() error {
	if o.Enable == false {
		return nil
	}
	if o.Namespace == "" || o.Name == "" {
		return errors.New("namespace and name must be specified")
	}
	return nil
}

// AddFlags ...
func (o *LeaderElectionOptions) AddFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&o.Enable, "leader-election", o.Enable, "enable leader election")
	fs.StringVar(&o.Namespace, "leader-election-namespace", o.Namespace, "namespace of the leader election")
	fs.StringVar(&o.Name, "leader-election-name", o.Name, "name of the leader election")
}
