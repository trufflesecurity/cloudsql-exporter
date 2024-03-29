package cmd

import (
	"context"
	"log/slog"
	"os"
	"time"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"google.golang.org/api/sqladmin/v1"
	"google.golang.org/api/storage/v1"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/fr12k/cloudsql-exporter/pkg/cloudsql"
	"github.com/fr12k/cloudsql-exporter/pkg/version"
)

var (
	app = kingpin.New("cloudsql-backup", "Export Cloud SQL databases to Google Cloud Storage")

	bucket                = app.Flag("bucket", "Google Cloud Storage bucket name").Required().String()
	project               = app.Flag("project", "GCP project ID").Required().String()
	instance              = app.Flag("instance", "Cloud SQL instance name, if not specified all within the project will be enumerated").String()
	compression           = app.Flag("compression", "Enable compression for exported SQL files").Bool()
	ensureIamBindings     = app.Flag("ensure-iam-bindings", "Ensure that the Cloud SQL service account has the required IAM role binding to export and validate the backup").Bool()
	ensureIamBindingsTemp = app.Flag("ensure-iam-bindings-temp", "Ensure that the Cloud SQL service account has the required IAM role binding to export and validate the backup").Bool()
	validate              = app.Flag("validate", "Will try to import the exported data into a new created CloudSQL instance").Bool()
)

type BackupOptions struct {
	Bucket                string
	Project               string
	Instance              string
	Compression           bool
	EnsureIamBindings     bool
	EnsureIamBindingsTemp bool
	Validate              bool

	Version string
	logger  *slog.Logger
}

func NewBackupOptions(logger *slog.Logger) *BackupOptions {
	return &BackupOptions{
		logger: logger,
	}
}

func NewCommand(logger *slog.Logger) *BackupOptions {
	kingpin.MustParse(app.Parse(os.Args[1:]))
	app.Version("cloudsql-exporter " + version.BuildVersion)

	opts := NewBackupOptions(logger)
	opts.Bucket = *bucket
	opts.Project = *project
	opts.Instance = *instance
	opts.Compression = *compression
	opts.EnsureIamBindings = *ensureIamBindings
	opts.EnsureIamBindingsTemp = *ensureIamBindingsTemp
	opts.Validate = *validate
	opts.Version = version.BuildVersion
	return opts
}

func Backup(opts *BackupOptions) ([]string, error) {
	var backupPaths []string

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hc, err := google.DefaultClient(ctx, sqladmin.SqlserviceAdminScope)
	if err != nil {
		opts.logger.Error("error init http.Client", "error", err)
		return nil, err
	}

	sqlAdminSvc, err := sqladmin.NewService(ctx, option.WithHTTPClient(hc))
	if err != nil {
		opts.logger.Error("error init sqladmin.Service client", "error", err)
		return nil, err
	}

	storageSvc, err := storage.NewService(ctx, option.WithHTTPClient(hc))
	if err != nil {
		opts.logger.Error("init storage.Service client", "error", err)
		return nil, err
	}

	cls := cloudsql.NewCloudSQL(ctx, opts.logger, sqlAdminSvc, storageSvc, opts.Project)

	instances, err := cls.EnumerateCloudSQLDatabaseInstances(opts.Instance)
	if err != nil {
		opts.logger.Error("error reading cloudsql instances", "error", err)
		return nil, err
	}

	for instance, databases := range instances {
		opts.logger.Info("Exporting backup for instance", "instance", string(instance))

		if opts.EnsureIamBindings || opts.EnsureIamBindingsTemp {
			sqlAdminSvcAccount, err := cls.GetSvcAcctForCloudSQLInstance(string(instance), "")
			if err != nil {
				opts.logger.Error("error get service account for instance", "instance", string(instance), "error", err)
				return nil, err
			}
			if opts.EnsureIamBindingsTemp {
				defer func() {
					err = cls.RemoveRoleBindingToGCSBucket(opts.Bucket, "roles/storage.objectCreator", sqlAdminSvcAccount, string(instance))
					if err != nil {
						opts.logger.Error("error remove role binding roles/storage.objectCreator", "service_account", sqlAdminSvcAccount, "error", err)
					}
					err = cls.RemoveRoleBindingToGCSBucket(opts.Bucket, "roles/storage.objectViewer", sqlAdminSvcAccount, string(instance))
					if err != nil {
						opts.logger.Error("error remove role binding roles/storage.objectViewer", "service_account", sqlAdminSvcAccount, "error", err)
					}
				}()
			}
			err = cls.AddRoleBindingToGCSBucket(opts.Bucket, "roles/storage.objectCreator", sqlAdminSvcAccount, string(instance))
			if err != nil {
				opts.logger.Error("error add role binding roles/storage.objectCreator", "service_account", sqlAdminSvcAccount, "error", err)
			}
			err = cls.AddRoleBindingToGCSBucket(opts.Bucket, "roles/storage.objectViewer", sqlAdminSvcAccount, string(instance))
			if err != nil {
				opts.logger.Error("error add role binding roles/storage.objectViewer", "service_account", sqlAdminSvcAccount, "error", err)
			}
		}

		var objectName string

		if opts.Compression {
			objectName = time.Now().Format("20060102T150405") + ".sql.gz"
		} else {
			objectName = time.Now().Format("20060102T150405") + ".sql"
		}

		locations, err := cls.ExportCloudSQLDatabase(databases,string(instance), opts.Bucket, objectName)
		if err != nil {
			opts.logger.Error("error export cloudsql database", "databases", databases, "instance", string(instance), "error", err)
			return nil, err
		}
		backupPaths = append(backupPaths, locations...)

		if opts.Validate {
			//TODO
			err = cls.Validate(string(instance), opts.Bucket, locations[1])
			if err != nil {
				opts.logger.Error("error validate cloudsql database", "databases", databases, "instance", string(instance), "error", err)
				return nil, err
			}
		}
	}

	opts.logger.Info("Backup complete", "backups", backupPaths)

	return backupPaths, nil
}
