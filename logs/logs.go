package logs

import (
	"context"
	"log/slog"
	"os"
)

// Level represents the severity of a log message
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

// Logger is the interface that wraps the basic logging methods.
type Logger interface {
	Debug(ctx context.Context, msg string, keysAndValues ...interface{})
	Info(ctx context.Context, msg string, keysAndValues ...interface{})
	Warn(ctx context.Context, msg string, keysAndValues ...interface{})
	Error(ctx context.Context, msg string, keysAndValues ...interface{})
	WithFields(fields map[string]interface{}) Logger
}

type defaultLogger struct {
	logger *slog.Logger
}

func NewDefaultLogger(level slog.Leveler) Logger {
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})
	l := slog.New(handler)
	return &defaultLogger{
		logger: l,
	}
}

func (l *defaultLogger) Debug(ctx context.Context, msg string, keysAndValues ...interface{}) {
	l.logger.DebugContext(ctx, msg, keysAndValues...)
}

func (l *defaultLogger) Info(ctx context.Context, msg string, keysAndValues ...interface{}) {
	l.logger.InfoContext(ctx, msg, keysAndValues...)
}

func (l *defaultLogger) Warn(ctx context.Context, msg string, keysAndValues ...interface{}) {
	l.logger.WarnContext(ctx, msg, keysAndValues...)
}

func (l *defaultLogger) Error(ctx context.Context, msg string, keysAndValues ...interface{}) {
	l.logger.ErrorContext(ctx, msg, keysAndValues...)
}

func (l *defaultLogger) WithFields(fields map[string]interface{}) Logger {
	args := make([]any, 0, len(fields)*2)
	for k, v := range fields {
		args = append(args, k, v)
	}
	return &defaultLogger{logger: l.logger.With(args...)}
}

func Initialize(level Level) {
	var slogLevel slog.Level

	switch level {
	case LevelDebug:
		slogLevel = slog.LevelDebug
	case LevelInfo:
		slogLevel = slog.LevelInfo
	case LevelWarn:
		slogLevel = slog.LevelWarn
	case LevelError:
		slogLevel = slog.LevelError
	}

	Log = NewDefaultLogger(slogLevel)
}

var Log Logger

func Debug(ctx context.Context, msg string, keysAndValues ...interface{}) {
	Log.Debug(ctx, msg, keysAndValues...)
}

func Info(ctx context.Context, msg string, keysAndValues ...interface{}) {
	Log.Info(ctx, msg, keysAndValues...)
}

func Warn(ctx context.Context, msg string, keysAndValues ...interface{}) {
	Log.Warn(ctx, msg, keysAndValues...)
}

func Error(ctx context.Context, msg string, keysAndValues ...interface{}) {
	Log.Error(ctx, msg, keysAndValues...)
}
