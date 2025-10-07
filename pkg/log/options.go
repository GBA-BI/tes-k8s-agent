package log

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/pflag"
)

// Options ...
type Options struct {
	Level       string   `mapstructure:"level"`
	OutputPath  string   `mapstructure:"output-path,omitempty"`
	EncoderType string   `mapstructure:"encoder-type,omitempty"`
	MaxSize     int      `mapstructure:"max-size,omitempty"`
	MaxBackups  int      `mapstructure:"max-backups,omitempty"`
	MaxAge      int      `mapstructure:"max-age,omitempty"`
	Compress    bool     `mapstructure:"compress,omitempty"`
	MessageKey  string   `mapstructure:"message-key,omitempty"`
	LevelKey    string   `mapstructure:"level-key,omitempty"`
	CallerKey   string   `mapstructure:"caller-key,omitempty"`
	TimeKey     string   `mapstructure:"time-key,omitempty"`
	ExtraKeys   []string `mapstructure:"extra-keys,omitempty"`
}

// NewOptions new a log option.
func NewOptions() *Options {
	return &Options{
		Level:       "info",
		OutputPath:  "",
		EncoderType: EncoderConsole,
		MaxSize:     100,
		MaxBackups:  5,
		MaxAge:      1,
		Compress:    true,
	}
}

// Validate validate log options is valid.
func (o *Options) Validate() error {
	switch strings.ToLower(o.Level) {
	case "debug", "info", "warn", "error", "panic", "fatal":
	default:
		return errors.New("invalid log level")
	}
	if o.EncoderType != EncoderConsole && o.EncoderType != EncoderJson {
		return errors.New("invalid log encoder type")
	}
	if o.OutputPath != "" {
		dirName := filepath.Dir(o.OutputPath)
		if dirName != "" {
			if _, err := os.Stat(dirName); err != nil {
				return fmt.Errorf("invalid log output path: %w", err)
			}
		}
	}
	return nil
}

// AddFlags ...
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.StringVarP(&o.Level, "log-level", "", "info", "log level")
	fs.StringVarP(&o.OutputPath, "log-path", "", "", "log path")
	fs.StringVarP(&o.EncoderType, "log-encode-type", "", EncoderConsole, "log encoder console/json")
	fs.IntVarP(&o.MaxSize, "log-max-size", "", 100, "the maximum size in megabytes of the log file")
	fs.IntVarP(&o.MaxBackups, "log-max-backups", "", 5, "the maximum number of old log files to retain")
	fs.IntVarP(&o.MaxAge, "log-max-age", "", 1, "log maximum number of days")
	fs.BoolVarP(&o.Compress, "log-compress", "", true, "log compress")
	fs.StringVarP(&o.MessageKey, "log-message-key", "", "msg", "log message key")
	fs.StringVarP(&o.LevelKey, "log-level-key", "", "level", "log level key")
	fs.StringVarP(&o.CallerKey, "log-caller-key", "", "caller", "log caller key")
	fs.StringVarP(&o.TimeKey, "log-time-key", "", "", "log time key")
	fs.StringArrayVarP(&o.ExtraKeys, "log-extra-keys", "", []string{}, "log extra key")
}
