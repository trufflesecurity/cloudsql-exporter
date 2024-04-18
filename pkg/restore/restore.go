package restore

import (
	"context"
	"log/slog"

	secretmanager "cloud.google.com/go/secretmanager/apiv1beta2"
	"cloud.google.com/go/storage"
	"github.com/fr12k/cloudsql-exporter/pkg/cloudsql"
	"google.golang.org/api/sqladmin/v1"
)

func Restore(opts *cloudsql.RestoreOptions) ([]string, error) {
	var backupPaths []string

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sqlAdminSvc, err := sqladmin.NewService(ctx)
	if err != nil {
		slog.Error("error init sqladmin.Service client", "error", err)
		return nil, err
	}

	storageSvc, err := storage.NewClient(ctx)
	if err != nil {
		slog.Error("init storage.Service client", "error", err)
		return nil, err
	}

	secretSvc, err := secretmanager.NewClient(ctx)
	if err != nil {
		slog.Error("init secretmanager.Service client", "error", err)
		return nil, err
	}

	cls := cloudsql.NewCloudSQL(ctx, sqlAdminSvc, storageSvc, secretSvc, opts.Project)

	password, err := cls.Restore(opts)
	if err != nil {
		slog.Error("error validate cloudsql database", "instance", opts.Instance, "error", err)
		return nil, err
	}

	slog.Info("Backup complete", "backups", backupPaths, "password", *password)

	return backupPaths, nil
}
