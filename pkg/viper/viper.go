package viper

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// ConfigFlagName is the flag for config
const ConfigFlagName = "config"

var cfgFile string

func init() {
	pflag.StringVarP(&cfgFile, ConfigFlagName, "c", cfgFile, "Read configuration from specified `FILE`, "+
		"support JSON, YAML formats.")
}

// LoadConfig ...
func LoadConfig(conf interface{}) error {
	// to avoid split podLabels/podAnnotations key
	v := viper.NewWithOptions(viper.KeyDelimiter("::"))

	v.SetConfigName("config") // name of config file (without extension)
	if cfgFile != "" {
		v.SetConfigFile(cfgFile)
		configDir := filepath.Dir(cfgFile)
		if configDir != "." {
			v.AddConfigPath(configDir)
		}
	}

	v.AddConfigPath("conf")
	v.AddConfigPath(".")
	v.AddConfigPath("$HOME")

	v.SetEnvKeyReplacer(strings.NewReplacer("::", "_"))
	v.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := v.ReadInConfig(); err != nil {
		return err
	}

	fmt.Println("Using config file:", v.ConfigFileUsed())

	return v.Unmarshal(conf, viper.DecodeHook(
		mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToTimeHookFunc(time.RFC3339),
		)))
}
