package consts

// task state
const (
	TaskQueued        = "QUEUED"
	TaskInitializing  = "INITIALIZING"
	TaskRunning       = "RUNNING"
	TaskComplete      = "COMPLETE"
	TaskSystemError   = "SYSTEM_ERROR"
	TaskExecutorError = "EXECUTOR_ERROR"
	TaskCanceling     = "CANCELING"
	TaskCanceled      = "CANCELED"
)

// task view types
const (
	MinimalView = "MINIMAL"
	BasicView   = "BASIC"
	FullView    = "FULL"
)

// ListTasks pageSize
const (
	DefaultPageSize = 256
	MaximumPageSize = 2048
)

// offload constants
const (
	// PVCOffloadType ...
	PVCOffloadType = "pvc"
	// OffloadThreshold is 100KiB
	OffloadThreshold = 102400
)

// s3 type
const (
	TOSType = "tos"
	S3Type  = "s3"
)

// filer mode
const (
	InputsMode  = "inputs"
	OutputsMode = "outputs"
)

// filer env
const (
	PodInfoAnnotationsFile        = "POD_INFO_ANNOTATIONS_FILE"
	OffloadType                   = "OFFLOAD_TYPE"
	OffloadPVCName                = "OFFLOAD_PVC_NAME"
	OffloadPath                   = "OFFLOAD_PATH"
	HostBasePath                  = "HOST_BASE_PATH"
	ContainerBasePath             = "CONTAINER_BASE_PATH"
	AWSSharedCredentialsFile      = "AWS_SHARED_CREDENTIALS_FILE"
	AWSCredentialsExpiredTimeFile = "AWS_CREDENTIALS_EXPIRED_TIME_FILE"
	S3SDKConfigFile               = "S3SDK_CONFIG_FILE"
	IsMountTOS                    = "IS_MOUNT_TOS"
	AAIPassport                   = "AAI_PASSPORT"
)

// accelerate constants
const (
	// NullAccelerateType ...
	NullAccelerateType = "null"
	// MountTOSAccelerateType ...
	MountTOSAccelerateType = "mount-tos"
)
