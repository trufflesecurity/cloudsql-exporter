package main

import (
	"log/slog"
	"os"

	"github.com/fr12k/cloudsql-exporter/cmd"
)

func main() {
	logOpts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, logOpts))
	slog.SetDefault(logger)
	opts := cmd.NewCommand()
	cmd.Backup(opts)
}
