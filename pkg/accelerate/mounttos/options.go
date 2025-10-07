package mounttos

import (
	"fmt"
	"net/url"
	"time"

	"github.com/spf13/pflag"
)

// Options ...
type Options struct {
	TOSS3URL         string                 `mapstructure:"tosS3URL"`
	BucketNumPerTask int                    `mapstructure:"bucketNumPerTask"`
	CleanPeriod      time.Duration          `mapstructure:"cleanPeriod"`
	StaticTOSSecret  StaticTOSSecretOptions `mapstructure:"staticTOSSecret"`
	FusePodResources FusePodResources       `mapstructure:"fusePodResources"`
	AdditionalArgs   string                 `mapstructure:"additionalArgs"`
}

// StaticTOSSecretOptions ...
type StaticTOSSecretOptions struct {
	Enable bool   `mapstructure:"enable"`
	Name   string `mapstructure:"name"`
}

// FusePodResources ...
type FusePodResources struct {
	Requests FusePodResource `mapstructure:"requests"`
	Limits   FusePodResource `mapstructure:"limits"`
}

// FusePodResource ...
type FusePodResource struct {
	CPU    string `mapstructure:"cpu"`
	Memory string `mapstructure:"memory"`
}

// NewOptions ...
func NewOptions() *Options {
	return &Options{
		BucketNumPerTask: 10,
		CleanPeriod:      10 * time.Minute,
		FusePodResources: FusePodResources{
			Requests: FusePodResource{
				CPU:    "100m",
				Memory: "200Mi",
			},
			Limits: FusePodResource{
				CPU:    "2",
				Memory: "8Gi",
			},
		},
	}
}

// Validate ...
func (o *Options) Validate() error {
	u, err := url.Parse(o.TOSS3URL)
	if err != nil {
		return fmt.Errorf("invalid tosS3URl: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("invalid tosS3URl scheme: %s", u.Scheme)
	}
	if o.BucketNumPerTask <= 0 {
		return fmt.Errorf("bucketNumPerTask %d must be greater than 0", o.BucketNumPerTask)
	}
	if o.CleanPeriod < time.Second {
		return fmt.Errorf("mountTOS clean period %s should not less than 1s", o.CleanPeriod.String())
	}
	if o.StaticTOSSecret.Enable {
		if o.StaticTOSSecret.Name == "" {
			return fmt.Errorf("empty static tos secret name")
		}
	}
	return nil
}

// AddFlags ...
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.TOSS3URL, "mount-tos-s3-url", o.TOSS3URL, "tos s3 url with scheme")
	fs.IntVar(&o.BucketNumPerTask, "mount-tos-bucket-num-per-task", o.BucketNumPerTask, "the number of mounting bucket per task")
	fs.DurationVar(&o.CleanPeriod, "mount-tos-clean-period", o.CleanPeriod, "mount tos clean period")
	fs.BoolVar(&o.StaticTOSSecret.Enable, "mount-tos-static-secret-enable", o.StaticTOSSecret.Enable, "use static secret to mount tos")
	fs.StringVar(&o.StaticTOSSecret.Name, "mount-tos-static-secret-name", o.StaticTOSSecret.Name, "static secret name to mount tos")
	fs.StringVar(&o.FusePodResources.Requests.CPU, "mount-tos-fuse-pod-requests-cpu", o.FusePodResources.Requests.CPU, "mount tos fuse pod requests cpu")
	fs.StringVar(&o.FusePodResources.Requests.Memory, "mount-tos-fuse-pod-requests-memory", o.FusePodResources.Requests.Memory, "mount tos fuse pod requests memory")
	fs.StringVar(&o.FusePodResources.Limits.CPU, "mount-tos-fuse-pod-limits-cpu", o.FusePodResources.Limits.CPU, "mount tos fuse pod limits cpu")
	fs.StringVar(&o.FusePodResources.Limits.Memory, "mount-tos-fuse-pod-limits-memory", o.FusePodResources.Limits.Memory, "mount tos fuse pod limits memory")
	fs.StringVar(&o.AdditionalArgs, "mount-tos-additional-args", o.AdditionalArgs, "mount tos additional args")
}
