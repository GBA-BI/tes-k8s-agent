package consts

// LabelTaskID is label key of the task id
const LabelTaskID = "vetes.bioos.volcengine.com/task-id"

// LabelJobName is label key of the job name on pod
const LabelJobName = "job-name"

// AnnoStop is annotation key of stop on configmap
const AnnoStop = "vetes.bioos.volcengine.com/stop"

// AnnoStage is annotations key of task stage on configmap
const AnnoStage = "vetes.bioos.volcengine.com/stage"

// AnnoExecutorStage is annotation key of executor stage on configmap
const AnnoExecutorStage = "vetes.bioos.volcengine.com/executor-stage"

// LabelType is label key of the job/pod type
const LabelType = "vetes.bioos.volcengine.com/type"

// types of job/pod
const (
	FilerTypeSuffix = "-filer"
	ExecutorType    = "executor"
)

// LabelExecutorNo is label key of the executor number on job/pod
const LabelExecutorNo = "vetes.bioos.volcengine.com/executor-no"

// AnnoTESTaskName is annotation key of the passed task name on executor job/pod
const AnnoTESTaskName = "tes-task-name"

// bioosinfo labels on executor pod
const (
	LabelAccountID    = "vetes.bioos.volcengine.com/account-id"
	LabelUserID       = "vetes.bioos.volcengine.com/user-id"
	LabelSubmissionID = "vetes.bioos.volcengine.com/submission-id"
	LabelRunID        = "vetes.bioos.volcengine.com/run-id"
)

// metering annotations on executor pod
// typo of volcegine follows bio-metering, should not change
const (
	AnnoMeteringResource = "pod.bioos.volcegine.com/metering-resource"
	AnnoMeteringUserInfo = "pod.bioos.volcegine.com/metering-user-info"
)

// annotations on filer pod for inputs/outputs
const (
	AnnoTaskInputs     = "task-inputs"
	AnnoTaskOutputs    = "task-outputs"
	AnnoTaskInputsRef  = "task-inputs-ref"
	AnnoTaskOutputsRef = "task-outputs-ref"
)

// GPUNameAffinityKey is key of gpu name affinity
const GPUNameAffinityKey = "machine.cluster.vke.volcengine.com/gpu-name"

// NvidiaGPUResource ...
const NvidiaGPUResource = "nvidia.com/gpu"

// finalizers
const (
	ProcessTaskFinalizer         = "vetes.bioos.volcengine.com/task"
	ProcessExecutorTimeFinalizer = "vetes.bioos.volcengine.com/executor-time"
)

const (
	// LabelManagedBy is label key of resource managed by. Currently, it is on tos-secret created by vetes-k8s-agent
	LabelManagedBy = "vetes.bioos.volcengine.com/managed-by"
	// ManagedByVeTESK8SAgent is label value of resource managed by veTES k8s agent
	ManagedByVeTESK8SAgent = "vetes-k8s-agent"
)

const (
	// LabelBucketName is label key of bucket name on tos pv/pvc
	LabelBucketName = "vetes.bioos.volcengine.com/bucket-name"
)
