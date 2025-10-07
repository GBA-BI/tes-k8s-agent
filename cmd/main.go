package main

import (
	"fmt"
	"os"

	"github.com/GBA-BI/tes-k8s-agent/pkg/app"
)

func main() {
	command, err := app.NewAgentCommand()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	if err = command.Execute(); err != nil {
		os.Exit(1)
	}
}
