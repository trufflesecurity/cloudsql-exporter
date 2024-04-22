package main

import (
	"log/slog"
	"os"

	"github.com/fr12k/cloudsql-exporter/cmd"
	_ "github.com/fr12k/cloudsql-exporter/cmd/backup"
	_ "github.com/fr12k/cloudsql-exporter/cmd/restore"

	"github.com/dusted-go/logging/prettylog"
)

func main() {

	prettyHandler := prettylog.NewHandler(&slog.HandlerOptions{
		Level:       slog.LevelInfo,
		AddSource:   false,
		ReplaceAttr: nil,
	})
	logger := slog.New(prettyHandler)
	slog.SetDefault(logger)

	err := cmd.Execute()
	if err != nil {
		slog.Error("error executing command", "error", err)
		os.Exit(1)
	}
}
