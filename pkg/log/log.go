package log

import (
	"context"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var defaultLogger Logger = &emptyLogger{}

// Logger ...
type Logger interface {
	Debugw(msg string, keysAndValues ...interface{})
	Infow(msg string, keysAndValues ...interface{})
	Warnw(msg string, keysAndValues ...interface{})
	Errorw(msg string, keysAndValues ...interface{})
	Panicw(msg string, keysAndValues ...interface{})
	Fatalw(msg string, keysAndValues ...interface{})

	CtxDebugw(ctx context.Context, template string, args ...interface{})
	CtxInfow(ctx context.Context, template string, args ...interface{})
	CtxWarnw(ctx context.Context, template string, args ...interface{})
	CtxErrorw(ctx context.Context, template string, args ...interface{})
	CtxPanicw(ctx context.Context, template string, args ...interface{})
	CtxFatalw(ctx context.Context, template string, args ...interface{})

	Sync()
}

// Debugw ...
func Debugw(msg string, keysAndValues ...interface{}) {
	defaultLogger.Debugw(msg, keysAndValues...)
}

// Infow ...
func Infow(msg string, keysAndValues ...interface{}) {
	defaultLogger.Infow(msg, keysAndValues...)
}

// Warnw ...
func Warnw(msg string, keysAndValues ...interface{}) {
	defaultLogger.Warnw(msg, keysAndValues...)
}

// Errorw ...
func Errorw(msg string, keysAndValues ...interface{}) {
	defaultLogger.Errorw(msg, keysAndValues...)
}

// Panicw ...
func Panicw(msg string, keysAndValues ...interface{}) {
	defaultLogger.Panicw(msg, keysAndValues...)
}

// Fatalw ...
func Fatalw(msg string, keysAndValues ...interface{}) {
	defaultLogger.Fatalw(msg, keysAndValues...)
}

// CtxDebugw ...
func CtxDebugw(ctx context.Context, msg string, keysAndValues ...interface{}) {
	defaultLogger.CtxDebugw(ctx, msg, keysAndValues...)
}

// CtxInfow ...
func CtxInfow(ctx context.Context, msg string, keysAndValues ...interface{}) {
	defaultLogger.CtxInfow(ctx, msg, keysAndValues...)
}

// CtxWarnw ...
func CtxWarnw(ctx context.Context, msg string, keysAndValues ...interface{}) {
	defaultLogger.CtxWarnw(ctx, msg, keysAndValues...)
}

// CtxErrorw ...
func CtxErrorw(ctx context.Context, msg string, keysAndValues ...interface{}) {
	defaultLogger.CtxErrorw(ctx, msg, keysAndValues...)
}

// CtxPanicw ...
func CtxPanicw(ctx context.Context, msg string, keysAndValues ...interface{}) {
	defaultLogger.CtxPanicw(ctx, msg, keysAndValues...)
}

// CtxFatalw ...
func CtxFatalw(ctx context.Context, msg string, keysAndValues ...interface{}) {
	defaultLogger.CtxFatalw(ctx, msg, keysAndValues...)
}

// Sync ...
func Sync() {
	defaultLogger.Sync()
}

var registerLogger sync.Once

// RegisterLogger register global logger.
func RegisterLogger(options *Options) {
	registerLogger.Do(func() {
		defaultLogger = NewLogger(options)
	})
}

// NewLogger new a logger.
func NewLogger(opts *Options) Logger {
	if opts == nil {
		opts = NewOptions()
	}

	cores := getZapCores(opts)

	zapTee := zapcore.NewTee(cores...)
	logger := zap.New(zapTee, zap.AddCaller(), zap.AddCallerSkip(2), zap.AddStacktrace(zapcore.FatalLevel))

	return &zapLogger{logger, opts}
}

func getZapCores(opts *Options) []zapcore.Core {
	zapLevel, err := zapcore.ParseLevel(opts.Level)
	if err != nil {
		zapLevel = zapcore.InfoLevel
	}
	encoderConfig := getEncoderConfig(opts)
	var zapCores []zapcore.Core
	if opts.EncoderType == EncoderConsole {
		zapCores = []zapcore.Core{zapcore.NewCore(zapcore.NewConsoleEncoder(encoderConfig), zapcore.AddSync(os.Stdout), zapLevel)}
	} else {
		zapCores = []zapcore.Core{zapcore.NewCore(zapcore.NewJSONEncoder(encoderConfig), zapcore.AddSync(os.Stdout), zapLevel)}
	}
	if opts.OutputPath != "" {
		writer := &lumberjack.Logger{
			Filename:   opts.OutputPath, // make sure logfile parent dir exist
			MaxSize:    opts.MaxSize,
			MaxBackups: opts.MaxBackups,
			MaxAge:     opts.MaxAge,
			Compress:   opts.Compress,
		}
		zapCores = append(zapCores, zapcore.NewCore(zapcore.NewJSONEncoder(encoderConfig), zapcore.AddSync(writer), zapLevel))
	}
	return zapCores
}

func getEncoderConfig(opts *Options) zapcore.EncoderConfig {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.StacktraceKey = ""
	if opts.MessageKey != "" {
		encoderConfig.MessageKey = opts.MessageKey
	}
	if opts.LevelKey != "" {
		encoderConfig.LevelKey = opts.LevelKey
	}
	if opts.CallerKey != "" {
		encoderConfig.CallerKey = opts.CallerKey
	}
	if opts.TimeKey != "" {
		encoderConfig.TimeKey = opts.TimeKey
	}
	encoderConfig.EncodeTime = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(t.Format(time.RFC3339))
	}
	return encoderConfig
}

type zapLogger struct {
	*zap.Logger
	opts *Options
}

// Debugw ...
func (z *zapLogger) Debugw(msg string, keysAndValues ...interface{}) {
	z.Sugar().Debugw(msg, keysAndValues...)
}

// Infow ...
func (z *zapLogger) Infow(msg string, keysAndValues ...interface{}) {
	z.Sugar().Infow(msg, keysAndValues...)
}

// Warnw ...
func (z *zapLogger) Warnw(msg string, keysAndValues ...interface{}) {
	z.Sugar().Warnw(msg, keysAndValues...)
}

// Errorw ...
func (z *zapLogger) Errorw(msg string, keysAndValues ...interface{}) {
	z.Sugar().Errorw(msg, keysAndValues...)
}

// Panicw ...
func (z *zapLogger) Panicw(msg string, keysAndValues ...interface{}) {
	z.Sugar().Panicw(msg, keysAndValues...)
}

// Fatalw ...
func (z *zapLogger) Fatalw(msg string, keysAndValues ...interface{}) {
	z.Sugar().Fatalw(msg, keysAndValues...)
}

// CtxDebugw ...
func (z *zapLogger) CtxDebugw(ctx context.Context, msg string, keysAndValues ...interface{}) {
	log := z.ctxLogger(ctx)
	log.Debugw(msg, keysAndValues...)
}

// CtxInfow ...
func (z *zapLogger) CtxInfow(ctx context.Context, msg string, keysAndValues ...interface{}) {
	log := z.ctxLogger(ctx)
	log.Infow(msg, keysAndValues...)
}

// CtxWarnw ...
func (z *zapLogger) CtxWarnw(ctx context.Context, msg string, keysAndValues ...interface{}) {
	log := z.ctxLogger(ctx)
	log.Warnw(msg, keysAndValues...)
}

// CtxErrorw ...
func (z *zapLogger) CtxErrorw(ctx context.Context, msg string, keysAndValues ...interface{}) {
	log := z.ctxLogger(ctx)
	log.Errorw(msg, keysAndValues...)
}

// CtxPanicw ...
func (z *zapLogger) CtxPanicw(ctx context.Context, msg string, keysAndValues ...interface{}) {
	log := z.ctxLogger(ctx)
	log.Panicw(msg, keysAndValues...)
}

// CtxFatalw ...
func (z *zapLogger) CtxFatalw(ctx context.Context, msg string, keysAndValues ...interface{}) {
	log := z.ctxLogger(ctx)
	log.Fatalw(msg, keysAndValues...)
}

func (z *zapLogger) ctxLogger(ctx context.Context) *zap.SugaredLogger {
	log := z.Sugar()
	if len(z.opts.ExtraKeys) > 0 {
		for _, k := range z.opts.ExtraKeys {
			log = log.With(k, ctx.Value(k))
		}
	}
	return log
}

// Sync ...
func (z *zapLogger) Sync() {
	_ = z.Sugar().Sync()
}

// GetZapLogger ...
func GetZapLogger() *zap.Logger {
	log, ok := defaultLogger.(*zapLogger)
	if !ok {
		defaultLogger = NewLogger(nil)
		log = defaultLogger.(*zapLogger)
	}
	return log.Logger.WithOptions(zap.AddCallerSkip(-2))
}

var _ Logger = &zapLogger{}
