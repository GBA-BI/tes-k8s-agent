package models

// ListTasksRequest ...
type ListTasksRequest struct {
	NamePrefix     string   `query:"name_prefix"`
	State          []string `query:"state"`
	ClusterID      string   `query:"cluster_id"`
	WithoutCluster bool     `query:"without_cluster"`
	View           string   `query:"view"`
	PageSize       int      `query:"page_size"`
	PageToken      string   `query:"page_token"`
}

// ListTasksResponse ...
type ListTasksResponse struct {
	Tasks         []*Task `json:"tasks"`
	NextPageToken string  `json:"next_page_token,omitempty"`
}

// GetTaskRequest ...
type GetTaskRequest struct {
	ID   string `path:"id"`
	View string `query:"view"`
}

// GetTaskResponse ...
type GetTaskResponse struct {
	*Task `json:",inline"`
}

// UpdateTaskRequest ...
type UpdateTaskRequest struct {
	ID        string     `path:"id" json:"-"`
	ClusterID *string    `json:"cluster_id,omitempty"`
	State     *string    `json:"state,omitempty"`
	Logs      []*TaskLog `json:"logs,omitempty"`
}

// UpdateTaskResponse ...
type UpdateTaskResponse struct{}

// Task ...
type Task struct {
	ID            string      `json:"id"`
	State         string      `json:"state"`
	Name          string      `json:"name,omitempty"`
	Description   string      `json:"description,omitempty"`
	Inputs        []*Input    `json:"inputs,omitempty"`
	Outputs       []*Output   `json:"outputs,omitempty"`
	Resources     *Resources  `json:"resources,omitempty"`
	Executors     []*Executor `json:"executors,omitempty"`
	Volumes       []string    `json:"volumes,omitempty"`
	Logs          []*TaskLog  `json:"logs,omitempty"`
	CreationTime  string      `json:"creation_time,omitempty"`
	BioosInfo     *BioosInfo  `json:"bioos_info,omitempty"`
	PriorityValue int         `json:"priority_value,omitempty"`
	ClusterID     string      `json:"cluster_id,omitempty"`
}

// Input ...
type Input struct {
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	Path        string `json:"path"`
	Type        string `json:"type"`
	URL         string `json:"url,omitempty"`
	Content     string `json:"content,omitempty"`
}

// Output ...
type Output struct {
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	Path        string `json:"path"`
	Type        string `json:"type"`
	URL         string `json:"url"`
}

// Resources ...
type Resources struct {
	CPUCores int          `json:"cpu_cores,omitempty"`
	RamGB    float64      `json:"ram_gb,omitempty"` // nolint
	DiskGB   float64      `json:"disk_gb,omitempty"`
	GPU      *GPUResource `json:"gpu,omitempty"`
}

// GPUResource ...
type GPUResource struct {
	Count float64 `json:"count"`
	Type  string  `json:"type"`
}

// Executor ...
type Executor struct {
	Image   string            `json:"image"`
	Command []string          `json:"command"`
	Workdir string            `json:"workdir,omitempty"`
	Stdin   string            `json:"stdin,omitempty"`
	Stdout  string            `json:"stdout,omitempty"`
	Stderr  string            `json:"stderr,omitempty"`
	Env     map[string]string `json:"env,omitempty"`
}

// BioosInfo ...
type BioosInfo struct {
	AccountID    string         `json:"account_id,omitempty"`
	UserID       string         `json:"user_id,omitempty"`
	SubmissionID string         `json:"submission_id,omitempty"`
	RunID        string         `json:"run_id,omitempty"`
	Meta         *BioosInfoMeta `json:"meta,omitempty"`
}

// BioosInfoMeta ...
type BioosInfoMeta struct {
	AAIPassport     *string          `json:"aai_passport,omitempty"`
	MountTOS        *bool            `json:"mount_tos,omitempty"`
	BucketsAuthInfo *BucketsAuthInfo `json:"buckets_auth_info,omitempty"`
}

// BucketsAuthInfo ...
type BucketsAuthInfo struct {
	ReadOnly  []string                  `json:"read_only,omitempty"`
	ReadWrite []string                  `json:"read_write,omitempty"`
	External  []*ExternalBucketAuthInfo `json:"external,omitempty"`
}

// ExternalBucketAuthInfo ...
type ExternalBucketAuthInfo struct {
	Bucket string `json:"bucket,omitempty"`
	AK     string `json:"ak,omitempty"`
	SK     string `json:"sk,omitempty"`
}

// TaskLog ...
type TaskLog struct {
	ClusterID  string           `json:"cluster_id"`
	Logs       [][]*ExecutorLog `json:"logs,omitempty"`
	StartTime  *string          `json:"start_time,omitempty"`
	EndTime    *string          `json:"end_time,omitempty"`
	SystemLogs []string         `json:"system_logs,omitempty"`
}

// ExecutorLog ...
type ExecutorLog struct {
	ExecutorID string  `json:"executor_id"`
	StartTime  *string `json:"start_time,omitempty"`
	EndTime    *string `json:"end_time,omitempty"`
}
