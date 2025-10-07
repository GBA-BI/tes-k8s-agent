package filelog

import (
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger ...
type Logger interface {
	Debugf(msg string, keysAndValues ...interface{})
	Infof(msg string, keysAndValues ...interface{})
	Warnf(msg string, keysAndValues ...interface{})
	Errorf(msg string, keysAndValues ...interface{})
	Panicf(msg string, keysAndValues ...interface{})
	Fatalf(msg string, keysAndValues ...interface{})
	Sync()
	GetFileContent() ([]byte, error)
}

type writeToFileLogger struct {
	*zap.Logger
	writeToFile string
}

// NewLoggerWithWriteToFile returns a logger which writes logs to file
func NewLoggerWithWriteToFile(filename string) Logger {
	writer := &fileWriter{fileName: filename}
	logger := zap.New(zapcore.NewTee(zapcore.NewCore(
		zapcore.NewConsoleEncoder(getEncoderConfig()),
		zapcore.AddSync(writer),
		zapcore.InfoLevel,
	)), zap.WithCaller(false), zap.AddStacktrace(zapcore.FatalLevel+1))

	return &writeToFileLogger{Logger: logger, writeToFile: filename}
}

func (w *writeToFileLogger) Debugf(msg string, keysAndValues ...interface{}) {
	w.Sugar().Debugf(msg, keysAndValues...)
}

func (w *writeToFileLogger) Infof(msg string, keysAndValues ...interface{}) {
	w.Sugar().Infof(msg, keysAndValues...)
}

func (w *writeToFileLogger) Warnf(msg string, keysAndValues ...interface{}) {
	w.Sugar().Warnf(msg, keysAndValues...)
}

func (w *writeToFileLogger) Errorf(msg string, keysAndValues ...interface{}) {
	w.Sugar().Errorf(msg, keysAndValues...)
}

func (w *writeToFileLogger) Panicf(msg string, keysAndValues ...interface{}) {
	w.Sugar().Panicf(msg, keysAndValues...)
}

func (w *writeToFileLogger) Fatalf(msg string, keysAndValues ...interface{}) {
	w.Sugar().Fatalf(msg, keysAndValues...)
}

func (w *writeToFileLogger) Sync() {
	_ = w.Sugar().Sync()
}

// GetFileContent returns log file content, if the Logger is generated
// by NewLoggerWithWriteToFile
func (w *writeToFileLogger) GetFileContent() ([]byte, error) {
	content, err := os.ReadFile(w.writeToFile)
	if err != nil {
		return nil, err
	}
	return content, nil
}

func getEncoderConfig() zapcore.EncoderConfig {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	encoderConfig.EncodeTime = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(t.Format("2006-01-02 15:04:05.000"))
	}
	return encoderConfig
}

type fileWriter struct {
	fileName string
}

// Write always append the file. We cannot use lumberjack, because it will open the file, record the position,
// and write logs to the position. But the file is written by both this agent and filer, so lumberjack will
// write over filer logs.
func (f *fileWriter) Write(p []byte) (n int, err error) {
	var file *os.File
	if _, err = os.Stat(f.fileName); err != nil {
		if !os.IsNotExist(err) {
			return 0, err
		}
		if err = os.MkdirAll(filepath.Dir(f.fileName), 0755); err != nil {
			return 0, err
		}
		file, err = os.OpenFile(f.fileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	} else {
		file, err = os.OpenFile(f.fileName, os.O_WRONLY|os.O_APPEND, 0644)
	}
	if err != nil {
		return 0, err
	}
	defer file.Close()
	return file.Write(p)
}
