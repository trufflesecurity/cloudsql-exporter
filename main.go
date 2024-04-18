package main

import (
	"log/slog"
	"os"

	"github.com/fr12k/cloudsql-exporter/cmd"
	_ "github.com/fr12k/cloudsql-exporter/cmd/backup"
	_ "github.com/fr12k/cloudsql-exporter/cmd/restore"
)

func main() {
	logOpts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, logOpts))
	slog.SetDefault(logger)

	cmd.Execute()
}
