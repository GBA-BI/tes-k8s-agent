package version

import (
	"github.com/spf13/pflag"
)

var versionFlag bool

// AddFlags ...
func AddFlags(fs *pflag.FlagSet) {
	fs.BoolVarP(&versionFlag, "version", "v", false, "version")
}
