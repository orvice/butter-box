package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/orvice/butter-box/internal/app"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	if err := app.Run(context.Background(), logger); err != nil {
		logger.Error("butter box server exited", slog.Any("error", err))
		os.Exit(1)
	}
}
