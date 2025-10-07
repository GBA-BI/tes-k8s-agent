package options

import (
	"github.com/spf13/pflag"
)

// ServerOptions ...
type ServerOptions struct {
	HealthzPort uint16 `mapstructure:"healthzPort"`
	MetricsPort uint16 `mapstructure:"metricsPort"`
}

// NewServerOptions ...
func NewServerOptions() *ServerOptions {
	return &ServerOptions{
		HealthzPort: 9440,
		MetricsPort: 8081,
	}
}

// Validate ...
func (o *ServerOptions) Validate() error {
	return nil
}

// AddFlags ...
func (o *ServerOptions) AddFlags(fs *pflag.FlagSet) {
	fs.Uint16Var(&o.HealthzPort, "http-healthz-port", o.HealthzPort, "http port to listen on for healthz")
	fs.Uint16Var(&o.MetricsPort, "http-metrics-port", o.MetricsPort, "http port to listen on for metrics")
}
