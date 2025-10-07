package localstore

// TaskInfo ...
type TaskInfo struct {
	Task
	Stop          *string
	Stage         *int
	ExecutorStage *int
}

// Task ...
type Task struct {
	ID              string      `yaml:"id"`
	Name            string      `yaml:"name,omitempty"`
	Resources       *Resources  `yaml:"resources,omitempty"`
	Executors       []*Executor `yaml:"executors,omitempty"`
	BioosInfo       *BioosInfo  `yaml:"bioos_info,omitempty"`
	Volumes         []string    `yaml:"volumes,omitempty"`
	InputsJSON      string      `yaml:"inputs_json,omitempty"`
	OutputsJSON     string      `yaml:"outputs_json,omitempty"`
	InputsRef       string      `yaml:"inputs_ref,omitempty"`
	OutputsRef      string      `yaml:"outputs_ref,omitempty"`
	AccelerateNames []string    `yaml:"accelerate_names,omitempty"`
}

// Resources ...
type Resources struct {
	CPUCores int          `yaml:"cpu_cores,omitempty"`
	RamGB    float64      `yaml:"ram_gb,omitempty"` // nolint
	DiskGB   float64      `yaml:"disk_gb,omitempty"`
	GPU      *GPUResource `yaml:"gpu,omitempty"`
}

// GPUResource ...
type GPUResource struct {
	Type  string  `yaml:"type,omitempty"`
	Count float64 `yaml:"count,omitempty"`
}

// Executor ...
type Executor struct {
	Image   string            `yaml:"image"`
	Command []string          `yaml:"command"`
	Workdir string            `yaml:"workdir,omitempty"`
	Stdin   string            `yaml:"stdin,omitempty"`
	Stdout  string            `yaml:"stdout,omitempty"`
	Stderr  string            `yaml:"stderr,omitempty"`
	Env     map[string]string `yaml:"env,omitempty"`
}

// BioosInfo ...
type BioosInfo struct {
	AccountID    string         `yaml:"account_id,omitempty"`
	UserID       string         `yaml:"user_id,omitempty"`
	SubmissionID string         `yaml:"submission_id,omitempty"`
	RunID        string         `yaml:"run_id,omitempty"`
	Meta         *BioosInfoMeta `yaml:"meta,omitempty"`
}

type BioosInfoMeta struct {
	AAIPassport     *string          `yaml:"aai_passport,omitempty"`
	MountTOS        *bool            `yaml:"mount_tos,omitempty"`
	BucketsAuthInfo *BucketsAuthInfo `yaml:"buckets_auth_info,omitempty"`
}

// BucketsAuthInfo ...
type BucketsAuthInfo struct {
	ReadOnly  []string                  `yaml:"read_only,omitempty"`
	ReadWrite []string                  `yaml:"read_write,omitempty"`
	External  []*ExternalBucketAuthInfo `yaml:"external,omitempty"`
}

// ExternalBucketAuthInfo ...
type ExternalBucketAuthInfo struct {
	Bucket string `yaml:"bucket,omitempty"`
	AK     string `yaml:"ak,omitempty"`
	SK     string `yaml:"sk,omitempty"`
}
