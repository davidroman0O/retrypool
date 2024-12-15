// logs.go
package retrypool

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"sync/atomic"
)

// Logger provides structured logging with source tracking
type Logger interface {
	Debug(ctx context.Context, msg string, keysAndValues ...any)
	Info(ctx context.Context, msg string, keysAndValues ...any)
	Warn(ctx context.Context, msg string, keysAndValues ...any)
	Error(ctx context.Context, msg string, keysAndValues ...any)
	WithFields(fields map[string]any) Logger
	Enable()
	Disable()
}

type LogFormat string
type LogLevel = slog.Level

const (
	TextFormat LogFormat = "text"
	JSONFormat LogFormat = "json"
)

// loggerConfig holds configuration for the logger
type loggerConfig struct {
	format LogFormat
	level  LogLevel
	output *os.File // Allows for custom output (defaults to os.Stdout)
}

// LogOption defines functional options for logger configuration
type LogOption func(*loggerConfig)

// WithFormat sets the log format
func WithFormat(format LogFormat) LogOption {
	return func(cfg *loggerConfig) {
		cfg.format = format
	}
}

// WithOutput sets a custom output file
func WithOutput(output *os.File) LogOption {
	return func(cfg *loggerConfig) {
		cfg.output = output
	}
}

// defaultLogger implements Logger with slog
type defaultLogger struct {
	logger  *slog.Logger
	enabled atomic.Bool // Allows for runtime enable/disable
}

// NewLogger creates a new logger with the given options
func NewLogger(level LogLevel, opts ...LogOption) Logger {
	cfg := &loggerConfig{
		format: TextFormat,
		level:  level,
		output: os.Stdout,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	handlerOpts := &slog.HandlerOptions{
		Level:     cfg.level,
		AddSource: false,
	}

	var handler slog.Handler
	switch cfg.format {
	case JSONFormat:
		handler = slog.NewJSONHandler(cfg.output, handlerOpts)
	default:
		handler = slog.NewTextHandler(cfg.output, handlerOpts)
	}

	l := &defaultLogger{
		logger: slog.New(handler),
	}
	l.enabled.Store(true)
	return l
}

// addSource adds file:line information to log entries
func (l *defaultLogger) addSource(keysAndValues []any) []any {
	if pc, file, line, ok := runtime.Caller(2); ok {
		if fn := runtime.FuncForPC(pc); fn != nil {
			// Add function name, file and line number
			return append(keysAndValues,
				"caller", fmt.Sprintf("%s:%d", file, line),
				"function", fn.Name(),
			)
		}
		return append(keysAndValues, "source", fmt.Sprintf("%s:%d", file, line))
	}
	return keysAndValues
}

func (l *defaultLogger) Debug(ctx context.Context, msg string, keysAndValues ...any) {
	if !l.enabled.Load() {
		return
	}
	l.logger.DebugContext(ctx, msg, l.addSource(keysAndValues)...)
}

func (l *defaultLogger) Info(ctx context.Context, msg string, keysAndValues ...any) {
	if !l.enabled.Load() {
		return
	}
	l.logger.InfoContext(ctx, msg, l.addSource(keysAndValues)...)
}

func (l *defaultLogger) Warn(ctx context.Context, msg string, keysAndValues ...any) {
	if !l.enabled.Load() {
		return
	}
	l.logger.WarnContext(ctx, msg, l.addSource(keysAndValues)...)
}

func (l *defaultLogger) Error(ctx context.Context, msg string, keysAndValues ...any) {
	if !l.enabled.Load() {
		return
	}
	l.logger.ErrorContext(ctx, msg, l.addSource(keysAndValues)...)
}

func (l *defaultLogger) WithFields(fields map[string]any) Logger {
	if len(fields) == 0 {
		return l
	}

	args := make([]any, 0, len(fields)*2)
	for k, v := range fields {
		args = append(args, k, v)
	}

	return &defaultLogger{
		logger: l.logger.With(args...),
	}
}

// Enable enables logging
func (l *defaultLogger) Enable() {
	l.enabled.Store(true)
}

// Disable disables logging
func (l *defaultLogger) Disable() {
	l.enabled.Store(false)
}
