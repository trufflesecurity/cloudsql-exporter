package cmd

import (
	"context"
	"log"
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
}

func NewBackupOptions() *BackupOptions {
	return &BackupOptions{}
}

func NewCommand() *BackupOptions {
	kingpin.MustParse(app.Parse(os.Args[1:]))
	app.Version("cloudsql-exporter " + version.BuildVersion)

	opts := NewBackupOptions()
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

func Backup(opts *BackupOptions) []string {
	var backupPaths []string

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hc, err := google.DefaultClient(ctx, sqladmin.SqlserviceAdminScope)
	if err != nil {
		log.Fatal(err)
	}

	sqlAdminSvc, err := sqladmin.NewService(ctx, option.WithHTTPClient(hc))
	if err != nil {
		log.Fatal(err)
	}

	storageSvc, err := storage.NewService(ctx, option.WithHTTPClient(hc))
	if err != nil {
		log.Fatal(err)
	}

	instances, err := cloudsql.EnumerateCloudSQLDatabaseInstances(ctx, sqlAdminSvc, opts.Project, opts.Instance)
	if err != nil {
		log.Fatal(err)
	}

	for instance, databases := range instances {
		log.Printf("Exporting backup for instance %s", instance)

		if opts.EnsureIamBindings || opts.EnsureIamBindingsTemp{
			sqlAdminSvcAccount, err := cloudsql.GetSvcAcctForCloudSQLInstance(ctx, sqlAdminSvc, opts.Project, string(instance), "")
			if err != nil {
				log.Fatal(err)
			}
			if opts.EnsureIamBindingsTemp {
				defer func() {
					err = cloudsql.RemoveRoleBindingToGCSBucket(ctx, storageSvc, opts.Project, opts.Bucket, "roles/storage.objectCreator", sqlAdminSvcAccount, string(instance))
					if err != nil {
						log.Fatal(err)
					}
					err = cloudsql.RemoveRoleBindingToGCSBucket(ctx, storageSvc, opts.Project, opts.Bucket, "roles/storage.objectViewer", sqlAdminSvcAccount, string(instance))
					if err != nil {
						log.Fatal(err)
					}
				}()
			}
			err = cloudsql.AddRoleBindingToGCSBucket(ctx, storageSvc, opts.Project, opts.Bucket, "roles/storage.objectCreator", sqlAdminSvcAccount, string(instance))
			if err != nil {
				log.Fatal(err)
			}
			err = cloudsql.AddRoleBindingToGCSBucket(ctx, storageSvc, opts.Project, opts.Bucket, "roles/storage.objectViewer", sqlAdminSvcAccount, string(instance))
			if err != nil {
				log.Fatal(err)
			}
		}

		var objectName string

		if opts.Compression {
			objectName = time.Now().Format("20060102T150405") + ".sql.gz"
		} else {
			objectName = time.Now().Format("20060102T150405") + ".sql"
		}

		locations, err := cloudsql.ExportCloudSQLDatabase(ctx, sqlAdminSvc, databases, opts.Project, string(instance), opts.Bucket, objectName)
		if err != nil {
			log.Fatal(err)
		}
		backupPaths = append(backupPaths, locations...)

		if opts.Validate {
			err = cloudsql.Validate(ctx, sqlAdminSvc, storageSvc, opts.Project, string(instance), opts.Bucket, locations[1])
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	log.Println("Backup complete")
	return backupPaths
}
