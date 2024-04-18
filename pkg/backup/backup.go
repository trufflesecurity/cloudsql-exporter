package backup

import (
	"context"
	"log/slog"
	"time"

	secretmanager "cloud.google.com/go/secretmanager/apiv1beta2"
	"cloud.google.com/go/storage"
	"google.golang.org/api/sqladmin/v1"

	"github.com/fr12k/cloudsql-exporter/pkg/cloudsql"
)

type BackupOptions struct {
	Bucket   string
	Project  string
	Instance string
	User     string
	Password string

	Compression           bool
	EnsureIamBindings     bool
	EnsureIamBindingsTemp bool
	Validate              bool

	Version string
}

func Backup(opts *BackupOptions) ([]string, error) {
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

	instances, err := cls.EnumerateCloudSQLDatabaseInstances(opts.Instance)
	if err != nil {
		slog.Error("error reading cloudsql instances", "error", err)
		return nil, err
	}

	for instance, databases := range instances {
		slog.Info("Exporting backup for instance", "instance", string(instance))

		if opts.EnsureIamBindings || opts.EnsureIamBindingsTemp {
			sqlAdminSvcAccount, err := cls.GetSvcAcctForCloudSQLInstance(string(instance), "")
			if err != nil {
				slog.Error("error get service account for instance", "instance", string(instance), "error", err)
				return nil, err
			}
			if opts.EnsureIamBindingsTemp {
				defer func() {
					err = cls.RemoveRoleBindingToGCSBucket(opts.Bucket, "roles/storage.objectCreator", sqlAdminSvcAccount, string(instance))
					if err != nil {
						slog.Error("error remove role binding roles/storage.objectCreator", "service_account", sqlAdminSvcAccount, "error", err)
					}
					err = cls.RemoveRoleBindingToGCSBucket(opts.Bucket, "roles/storage.objectViewer", sqlAdminSvcAccount, string(instance))
					if err != nil {
						slog.Error("error remove role binding roles/storage.objectViewer", "service_account", sqlAdminSvcAccount, "error", err)
					}
				}()
			}
			err = cls.AddRoleBindingToGCSBucket(opts.Bucket, "roles/storage.objectCreator", sqlAdminSvcAccount, string(instance))
			if err != nil {
				slog.Error("error add role binding roles/storage.objectCreator", "service_account", sqlAdminSvcAccount, "error", err)
			}
			err = cls.AddRoleBindingToGCSBucket(opts.Bucket, "roles/storage.objectViewer", sqlAdminSvcAccount, string(instance))
			if err != nil {
				slog.Error("error add role binding roles/storage.objectViewer", "service_account", sqlAdminSvcAccount, "error", err)
			}
		}

		var objectName string

		backupTime := time.Now()

		if opts.Compression {
			objectName = backupTime.Format("20060102T150405") + ".sql.gz"
		} else {
			objectName = backupTime.Format("20060102T150405") + ".sql"
		}

		users, err := cls.ExportCloudSQLUser(string(instance), opts.Bucket, backupTime.Format("20060102T150405"))
		if err != nil {
			slog.Error("error export cloudsql user", "databases", databases, "instance", string(instance), "error", err)
			return nil, err
		}

		slog.Info("Exported cloudsql users", "users", users)

		stats, err := cls.ExportCloudSQLStatistics(databases, string(instance), opts.Bucket, backupTime.Format("20060102T150405"), opts.User, opts.Password)
		if err != nil {
			slog.Error("error export cloudsql statistics", "databases", databases, "instance", string(instance), "error", err)
			return nil, err
		}

		slog.Info("Exported cloudsql statistics", "stats", stats)

		locations, err := cls.ExportCloudSQLDatabase(databases, string(instance), opts.Bucket, objectName)
		if err != nil {
			slog.Error("error export cloudsql database", "databases", databases, "instance", string(instance), "error", err)
			return nil, err
		}
		backupPaths = append(backupPaths, locations...)

		if opts.Validate {
			for _, location := range locations {
				//TODO only supports one database export not multiple
				opts := &cloudsql.RestoreOptions {
					Instance: string(instance),
					Bucket: opts.Bucket,
					User: opts.User,
					File: location,
				}
				password, err := cls.Restore(opts)
				if err != nil {
					slog.Error("error validate cloudsql database", "databases", databases, "instance", string(instance), "error", err)
					return nil, err
				}
				slog.Info("Successfully validated backup", "instance", string(instance), "database", databases, "password", *password)
			}
		}
	}

	slog.Info("Backup complete", "backups", backupPaths)

	return backupPaths, nil
}
