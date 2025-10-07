package runner

// Init
//  |
// Initializing = PVCToCreate
//  |
// PVCCreated = InputsFilerToCreate
//  |
// InputsFilerCreated
//  |
// InputsFilerFinished
//  |
// Running = ExecutorsToCreate
//  |
// ExecutorsFinished = OutputsFilerToCreate
//  |
// OutputsFilerCreated
//  |
// OutputsFilerFinished

const (
	taskStageInit = iota
	taskStageInitializing
	taskStagePVCCreated
	taskStageInputsFilerCreated
	taskStageInputsFilerFinished
	taskStageRunning
	taskStageExecutorsFinished
	taskStageOutputsFilerCreated
	taskStageOutputsFilerFinished
)

const (
	taskStagePVCToCreate          = taskStageInitializing
	taskStageInputsFilerToCreate  = taskStagePVCCreated
	taskStageExecutorsToCreate    = taskStageRunning
	taskStageOutputsFilerToCreate = taskStageExecutorsFinished
)

type executorStatus int

const (
	executorStatusToCreate executorStatus = iota
	executorStatusCreated
	executorStatusFailed
	executorStatusSuccess
)

type executorStage struct {
	index  int
	status executorStatus
}

func newExecutorStageValue(index int, status executorStatus) int {
	return executorStage{index: index, status: status}.value()
}

func (e executorStage) value() int {
	return e.index*10 + int(e.status)
}

func parseExecutorStageValue(v int) executorStage {
	return executorStage{
		index:  v / 10,
		status: executorStatus(v % 10),
	}
}
