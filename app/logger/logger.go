package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type ILogger interface {
	Debug(msg string, fields ...FieldAlias)
	Info(msg string, fields ...FieldAlias)
	Warn(msg string, fields ...FieldAlias)
	Error(msg string, fields ...FieldAlias)
	Fatal(msg string, fields ...FieldAlias)
	Sync() error
}

type FieldAlias = zapcore.Field

func New() ILogger {
	config := zap.Config{
		Level:       zap.NewAtomicLevelAt(zap.InfoLevel),
		Development: true,
		Encoding:    "console", // Use console encoding for string format
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "time",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			MessageKey:     "msg",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.CapitalLevelEncoder, // Capital letters for level
			EncodeTime:     zapcore.ISO8601TimeEncoder,  // ISO8601 time format
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}

	logger, err := config.Build()
	if err != nil {
		return nil
	}

	return logger
}

func String(key string, val string) FieldAlias {
	return zap.String(key, val)
}

func Int(key string, val int) FieldAlias {
	return zap.Int(key, val)
}

var Logger ILogger

func init() {
	Logger = New()
}
