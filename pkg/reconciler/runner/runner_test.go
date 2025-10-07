package runner

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/onsi/gomega"

	"github.com/GBA-BI/tes-k8s-agent/pkg/localstore"
	localstorefake "github.com/GBA-BI/tes-k8s-agent/pkg/localstore/fake"
)

func TestCleanTaskLogFiles(t *testing.T) {
	g := gomega.NewWithT(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	path, err := os.MkdirTemp("", "test-log")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(path)

	if err = os.MkdirAll(filepath.Join(path, "task-not-exist"), 0755); err != nil {
		panic(err)
	}
	if err = os.MkdirAll(filepath.Join(path, "task-exist"), 0755); err != nil {
		panic(err)
	}
	f, err := os.Create(filepath.Join(path, "app.log"))
	if err != nil {
		panic(err)
	}
	defer f.Close()

	fakeLocalStoreHelper := localstorefake.NewFakeHelper(ctrl)
	fakeLocalStoreHelper.EXPECT().GetTask(gomock.Any(), "task-not-exist").Return(nil, localstore.ErrNotFound)
	fakeLocalStoreHelper.EXPECT().GetTask(gomock.Any(), "task-exist").Return(&localstore.TaskInfo{}, nil)

	r := &Runner{
		opts: &Options{TaskLog: TaskLogOptions{
			OutputDir: path,
		}},
		localStoreHelper: fakeLocalStoreHelper,
	}

	r.cleanTaskLogFiles()

	remainFiles, err := os.ReadDir(path)
	if err != nil {
		panic(err)
	}

	names := make([]string, 0, len(remainFiles))
	for _, file := range remainFiles {
		names = append(names, file.Name())
	}
	g.Expect(names).To(gomega.ConsistOf("app.log", "task-exist"))
}
