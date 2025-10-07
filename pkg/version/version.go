package version

import (
	"fmt"
	"os"
	"runtime"

	"github.com/gosuri/uitable"
)

var (
	module       string
	version      string
	branch       string
	gitCommit    string
	gitTreeState string
	buildTime    string
)

// Info contains versioning information.
type Info struct {
	Module       string `json:"module"`
	Version      string `json:"version"`
	Branch       string `json:"branch"`
	GitCommit    string `json:"gitCommit"`
	GitTreeState string `json:"gitTreeState"`
	BuildTime    string `json:"buildTime"`
	GoVersion    string `json:"goVersion"`
	Compiler     string `json:"compiler"`
	Platform     string `json:"platform"`
}

// String return the string of Info.
func (i Info) String() string {
	table := uitable.New()
	table.RightAlign(0)
	table.MaxColWidth = 80
	table.Separator = " "
	table.AddRow("module:", i.Module)
	table.AddRow("version:", i.Version)
	table.AddRow("branch:", i.Branch)
	table.AddRow("gitCommit:", i.GitCommit)
	table.AddRow("gitTreeState:", i.GitTreeState)
	table.AddRow("buildTime:", i.BuildTime)
	table.AddRow("goVersion:", i.GoVersion)
	table.AddRow("compiler:", i.Compiler)
	table.AddRow("platform:", i.Platform)

	return table.String()
}

// Get ...
func Get() Info {
	// These variables typically come from -ldflags settings
	return Info{
		Module:       module,
		Version:      version,
		Branch:       branch,
		GitCommit:    gitCommit,
		GitTreeState: gitTreeState,
		BuildTime:    buildTime,
		GoVersion:    runtime.Version(),
		Compiler:     runtime.Compiler,
		Platform:     fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
	}
}

// PrintVersionOrContinue will print git commit and exit with os.Exit(0) if CLI v flag is present.
func PrintVersionOrContinue() {
	fmt.Printf("%s\n", Get())
	if versionFlag {
		os.Exit(0)
	}
}
