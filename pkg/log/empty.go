package log

import (
	"context"
)

type emptyLogger struct{}

// Debugw ...
func (*emptyLogger) Debugw(msg string, keysAndValues ...interface{}) {}

// Infow ...
func (*emptyLogger) Infow(msg string, keysAndValues ...interface{}) {}

// Warnw ...
func (*emptyLogger) Warnw(msg string, keysAndValues ...interface{}) {}

// Errorw ...
func (*emptyLogger) Errorw(msg string, keysAndValues ...interface{}) {}

// Panicw ...
func (*emptyLogger) Panicw(msg string, keysAndValues ...interface{}) {}

// Fatalw ...
func (*emptyLogger) Fatalw(msg string, keysAndValues ...interface{}) {}

// CtxDebugw ...
func (*emptyLogger) CtxDebugw(ctx context.Context, msg string, keysAndValues ...interface{}) {}

// CtxInfow ...
func (*emptyLogger) CtxInfow(ctx context.Context, msg string, keysAndValues ...interface{}) {}

// CtxWarnw ...
func (*emptyLogger) CtxWarnw(ctx context.Context, msg string, keysAndValues ...interface{}) {}

// CtxErrorw ...
func (*emptyLogger) CtxErrorw(ctx context.Context, msg string, keysAndValues ...interface{}) {}

// CtxPanicw ...
func (*emptyLogger) CtxPanicw(ctx context.Context, msg string, keysAndValues ...interface{}) {}

// CtxFatalw ...
func (*emptyLogger) CtxFatalw(ctx context.Context, msg string, keysAndValues ...interface{}) {}

// Sync ...
func (*emptyLogger) Sync() {}
