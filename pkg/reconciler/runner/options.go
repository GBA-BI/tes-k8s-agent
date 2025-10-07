package runner

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/pflag"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/GBA-BI/tes-k8s-agent/pkg/consts"
)

// Options ...
type Options struct {
	S3                         S3Options                      `mapstructure:"s3"`
	ExecutorImagePullSecret    ExecutorImagePullSecretOptions `mapstructure:"executorImagePullSecret"`
	ExecutorBasePath           string                         `mapstructure:"executorBasePath"`
	FilerImage                 FilerImageOptions              `mapstructure:"filerImage"`
	FilerResources             ResourcesOptions               `mapstructure:"filerResources"`
	StorageClass               string                         `mapstructure:"storageClass"`
	ExecutorRetries            int                            `mapstructure:"executorRetries"`
	FilerRetries               int                            `mapstructure:"filerRetries"`
	PodPollInterval            time.Duration                  `mapstructure:"podPollInterval"`
	PodImagePullBackoffTimeout time.Duration                  `mapstructure:"podImagePullBackoffTimeout"`
	FilerPodLabels             map[string]string              `mapstructure:"filerPodLabels"`
	FilerPodAnnotations        map[string]string              `mapstructure:"filerPodAnnotations"`
	ExecutorECSPodLabels       map[string]string              `mapstructure:"executorECSPodLabels"`
	ExecutorECSPodAnnotations  map[string]string              `mapstructure:"executorECSPodAnnotations"`
	ExecutorPodEnv             map[string]string              `mapstructure:"executorPodEnv"`
	FilerPodEnv                map[string]string              `mapstructure:"filerPodEnv"`
	TaskLog                    TaskLogOptions                 `mapstructure:"taskLog"`
	Transfer                   TransferOptions                `mapstructure:"transfer"`
}

// S3Options ...
type S3Options struct {
	Enable           bool   `mapstructure:"enable"`
	Type             string `mapstructure:"type"`
	StaticSecretName string `mapstructure:"staticSecretName"`
	SDKConfigmapName string `maptructure:"sdkConfigmapName"`
}

// ExecutorImagePullSecretOptions ...
type ExecutorImagePullSecretOptions struct {
	StaticName string `mapstructure:"staticName"`
}

// FilerImageOptions ...
type FilerImageOptions struct {
	Image               string `mapstructure:"image"`
	ImagePullSecretName string `mapstructure:"imagePullSecretName"`
}

// ResourcesOptions ...
type ResourcesOptions struct {
	Limits   map[string]string `mapstructure:"limits"`
	Requests map[string]string `mapstructure:"requests"`
}

// TaskLogOptions ...
type TaskLogOptions struct {
	OutputDir     string `mapstructure:"outputDir"`
	PVCName       string `mapstructure:"pvcName"`
	FilerLogLevel string `mapstructure:"filerLogLevel"`
}

// TransferOptions ...
type TransferOptions struct {
	Enable      bool   `mapstructure:"enable"`
	WESBasePath string `mapstructure:"wesBasePath"`
	TESBasePath string `mapstructure:"tesBasePath"`
	PVCName     string `mapstructure:"pvcName"`
}

// NewOptions ...
func NewOptions() *Options {
	return &Options{
		S3: S3Options{
			Enable: true,
			Type:   consts.TOSType,
		},
		ExecutorBasePath: "/cromwell-executions/",
		FilerResources: ResourcesOptions{
			Limits: map[string]string{
				string(corev1.ResourceCPU):    "1",
				string(corev1.ResourceMemory): "2Gi",
			},
			Requests: map[string]string{
				string(corev1.ResourceCPU):    "500m",
				string(corev1.ResourceMemory): "1Gi",
			},
		},
		StorageClass:               "ebs-ssd",
		ExecutorRetries:            2,
		FilerRetries:               2,
		PodPollInterval:            time.Minute,
		PodImagePullBackoffTimeout: 10 * time.Minute,
		TaskLog: TaskLogOptions{
			OutputDir:     "/app/log",
			FilerLogLevel: "info",
		},
		Transfer: TransferOptions{
			Enable:      false,
			WESBasePath: "/data",
			TESBasePath: "/transfer",
		},
	}
}

// Validate ...
func (o *Options) Validate() error {
	if o.S3.Enable {
		switch o.S3.Type {
		case consts.TOSType, consts.S3Type:
		default:
			return fmt.Errorf("invalid s3 type: %s", o.S3.Type)
		}
		if o.S3.StaticSecretName == "" {
			return errors.New("s3 staticSecretName must be set")
		}
		if o.S3.SDKConfigmapName == "" {
			return errors.New("empty s3 sdkConfigmapName")
		}
	}

	if !filepath.IsAbs(o.ExecutorBasePath) {
		return fmt.Errorf("executorBasePath %s should be absolute path", o.ExecutorBasePath)
	}
	if !strings.HasSuffix(o.ExecutorBasePath, "/") {
		return fmt.Errorf("executorBasePath %s should ends with slash", o.ExecutorBasePath)
	}

	if o.FilerImage.Image == "" {
		return errors.New("empty filerImage")
	}

	for _, quantity := range o.FilerResources.Limits {
		if _, err := resource.ParseQuantity(quantity); err != nil {
			return fmt.Errorf("filerResources.Limits %s is not a valid quantity", quantity)
		}
	}
	for _, quantity := range o.FilerResources.Requests {
		if _, err := resource.ParseQuantity(quantity); err != nil {
			return fmt.Errorf("filerResources.Requests %s is not a valid quantity", quantity)
		}
	}

	if o.StorageClass == "" {
		return errors.New("empty storageClass")
	}
	if o.ExecutorRetries < 0 {
		return errors.New("executorRetries must be greater than or equal to 0")
	}
	if o.FilerRetries < 0 {
		return errors.New("filerRetries must be greater than or equal to 0")
	}
	if o.PodPollInterval <= 0 {
		return errors.New("podPollInterval must be greater than 0")
	}
	if o.PodImagePullBackoffTimeout <= 0 {
		return errors.New("imagePullBackoffTimeout must be greater than 0")
	}

	s, err := os.Stat(o.TaskLog.OutputDir)
	if err != nil {
		return fmt.Errorf("invalid taskLog outputDir: %w", err)
	}
	if !s.IsDir() {
		return errors.New("taskLog outputDir is not a directory")
	}
	if o.TaskLog.PVCName == "" {
		return errors.New("pvcName is required")
	}
	switch strings.ToLower(o.TaskLog.FilerLogLevel) {
	case "debug", "info", "warn", "error", "panic", "fatal":
	default:
		return errors.New("invalid filer log level")
	}

	if o.Transfer.Enable {
		if !path.IsAbs(o.Transfer.WESBasePath) {
			return errors.New("transfer.wesBasePath must be an absolute path")
		}
		if !path.IsAbs(o.Transfer.TESBasePath) {
			return errors.New("transfer.tesBasePath must be an absolute path")
		}
		if o.Transfer.WESBasePath == o.Transfer.TESBasePath {
			return errors.New("transfer.wesBasePath and transfer.tesBasePath must not be the same")
		}
		if o.Transfer.PVCName == "" {
			return errors.New("transfer.pvcName must be set")
		}
	}

	return nil
}

// AddFlags ...
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&o.S3.Enable, "s3-enable", o.S3.Enable, "enable s3")
	fs.StringVar(&o.S3.Type, "s3-type", o.S3.Type, "s3 type")
	fs.StringVar(&o.S3.StaticSecretName, "s3-static-secret-name", o.S3.StaticSecretName, "s3 static secret name")
	fs.StringVar(&o.S3.SDKConfigmapName, "s3-sdk-configmap-name", o.S3.SDKConfigmapName, "s3 sdk configmap name")
	fs.StringVar(&o.ExecutorImagePullSecret.StaticName, "executor-image-pull-secret-static-name", o.ExecutorImagePullSecret.StaticName, "executor imagePullSecret static name")
	fs.StringVar(&o.ExecutorBasePath, "executor-base-path", o.ExecutorBasePath, "executor base path to mount")
	fs.StringVar(&o.FilerImage.Image, "filer-image", o.FilerImage.Image, "filer image")
	fs.StringVar(&o.FilerImage.ImagePullSecretName, "filer-image-pull-secret-name", o.FilerImage.ImagePullSecretName, "filer imagePullSecret name")
	fs.StringToStringVar(&o.FilerResources.Limits, "filer-resources-limits", o.FilerResources.Limits, "filer resources limits")
	fs.StringToStringVar(&o.FilerResources.Requests, "filer-resources-requests", o.FilerResources.Requests, "filer resources requests")
	fs.StringVar(&o.StorageClass, "storage-class", o.StorageClass, "storageClass name")
	fs.IntVar(&o.ExecutorRetries, "executor-retries", o.ExecutorRetries, "executor retries times")
	fs.IntVar(&o.FilerRetries, "filer-retries", o.FilerRetries, "filer retries times")
	fs.DurationVar(&o.PodPollInterval, "pod-poll-interval", o.PodPollInterval, "pod poll interval")
	fs.DurationVar(&o.PodImagePullBackoffTimeout, "pod-image-pull-backoff-timeout", o.PodImagePullBackoffTimeout, "pod ImagePullBackOff timeout")
	fs.StringToStringVar(&o.FilerPodLabels, "filer-pod-labels", o.FilerPodLabels, "filer pod labels")
	fs.StringToStringVar(&o.FilerPodAnnotations, "filer-pod-annotations", o.FilerPodAnnotations, "filer pod annotations")
	fs.StringToStringVar(&o.ExecutorECSPodLabels, "executor-ecs-pod-labels", o.ExecutorECSPodLabels, "executor ECS pod labels")
	fs.StringToStringVar(&o.ExecutorECSPodAnnotations, "executor-ecs-pod-annotations", o.ExecutorECSPodAnnotations, "executor ECS pod annotations")
	fs.StringToStringVar(&o.ExecutorPodEnv, "executor-pod-env", o.ExecutorPodEnv, "executor pod env")
	fs.StringToStringVar(&o.FilerPodEnv, "filer-pod-env", o.FilerPodEnv, "filer pod env")
	fs.StringVar(&o.TaskLog.OutputDir, "task-log-output-dir", o.TaskLog.OutputDir, "taskLog outputDir")
	fs.StringVar(&o.TaskLog.PVCName, "task-log-pvc-name", o.TaskLog.PVCName, "taskLog pvcName")
	fs.StringVar(&o.TaskLog.FilerLogLevel, "task-log-filer-log-level", o.TaskLog.FilerLogLevel, "taskLog filerLogLevel")
	fs.BoolVar(&o.Transfer.Enable, "transfer-enable", o.Transfer.Enable, "enable transfer")
	fs.StringVar(&o.Transfer.WESBasePath, "transfer-wes-base-path", o.Transfer.WESBasePath, "transfer wes base path")
	fs.StringVar(&o.Transfer.TESBasePath, "transfer-tes-base-path", o.Transfer.TESBasePath, "transfer tes base path")
	fs.StringVar(&o.Transfer.PVCName, "transfer-pvc-name", o.Transfer.PVCName, "transfer pvc name")
}
