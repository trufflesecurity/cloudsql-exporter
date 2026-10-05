package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"google.golang.org/api/sqladmin/v1"
	"google.golang.org/api/storage/v1"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/trufflesecurity/cloudsql-exporter/pkg/cloudsql"
	"github.com/trufflesecurity/cloudsql-exporter/pkg/version"
)

var (
	app = kingpin.New("cloudsql-exporter", "Export or restore Cloud SQL databases using Google Cloud Storage")

	bucket            = app.Flag("bucket", "Google Cloud Storage bucket name").Required().String()
	project           = app.Flag("project", "GCP project ID").Required().String()
	instance          = app.Flag("instance", "Cloud SQL instance name, if not specified all within the project will be enumerated").String()
	compression       = app.Flag("compression", "Enable compression for exported SQL files").Bool()
	fileType          = app.Flag("fileType", "Export format: SQL for MySQL/PostgreSQL or BAK for SQL Server").Default("SQL").String()
	ensureIamBindings = app.Flag("ensure-iam-bindings", "Ensure that the Cloud SQL service account has the bucket IAM roles needed to export or restore").Bool()
	restore           = app.Flag("restore", "Restore a SQL backup (requires --instance and --database)").Bool()
	database          = app.Flag("database", "Destination database for restoration; must already exist").String()
	backup            = app.Flag("backup", "GCS object name to restore, bypassing the backup selector (requires --restore)").String()
	yes               = app.Flag("yes", "Skip written restoration confirmation (requires --restore)").Bool()
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	app.Version("cloudsql-exporter " + version.BuildVersion)
	kingpin.MustParse(app.Parse(os.Args[1:]))
	if err := validateOptions(); err != nil {
		log.Fatal(err)
	}

	hc, err := google.DefaultClient(ctx, sqladmin.SqlserviceAdminScope, storage.DevstorageFullControlScope)
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

	if *restore {
		if err := restoreDatabase(ctx, sqlAdminSvc, storageSvc, os.Stdin, os.Stdout); err != nil {
			log.Fatal(err)
		}
		log.Println("Restoration complete")
		return
	}

	instances, err := cloudsql.EnumerateCloudSQLDatabaseInstances(ctx, sqlAdminSvc, *project, *instance)
	if err != nil {
		log.Fatal(err)
	}

	for instance, databases := range instances {
		log.Printf("Exporting backup for instance %s", instance)

		if *ensureIamBindings {
			sqlAdminSvcAccount, err := cloudsql.GetSvcAcctForCloudSQLInstance(ctx, sqlAdminSvc, *project, string(instance), "")
			if err != nil {
				log.Fatal(err)
			}
			err = cloudsql.AddRoleBindingToGCSBucket(ctx, storageSvc, *project, *bucket, "roles/storage.objectCreator", sqlAdminSvcAccount, string(instance))
			if err != nil {
				log.Fatal(err)
			}
			err = cloudsql.AddRoleBindingToGCSBucket(ctx, storageSvc, *project, *bucket, "roles/storage.objectViewer", sqlAdminSvcAccount, string(instance))
			if err != nil {
				log.Fatal(err)
			}
		}

		objectName := time.Now().Format(time.RFC3339Nano) + "." + strings.ToLower(*fileType)
		if *compression {
			objectName += ".gz"
		}

		err := cloudsql.ExportCloudSQLDatabase(ctx, sqlAdminSvc, databases, *project, string(instance), *bucket, objectName, *fileType)
		if err != nil {
			log.Fatal(err)
		}
	}

	log.Println("Backup complete")

}

func validateOptions() error {
	*fileType = strings.ToUpper(*fileType)
	if *fileType != "SQL" && *fileType != "BAK" {
		return fmt.Errorf("--fileType must be SQL or BAK")
	}
	if *restore {
		if *instance == "" || *database == "" {
			return fmt.Errorf("--restore requires --instance and --database")
		}
		if *compression || *fileType != "SQL" {
			return fmt.Errorf("--compression and --fileType BAK apply only to exports")
		}
	} else if *backup != "" || *database != "" || *yes {
		return fmt.Errorf("--backup, --database and --yes require --restore")
	}
	if *compression && *fileType == "BAK" {
		return fmt.Errorf("--compression is supported only for SQL exports")
	}
	return nil
}

func restoreDatabase(ctx context.Context, sqlAdminSvc *sqladmin.Service, storageSvc *storage.Service, input io.Reader, output io.Writer) error {
	reader := bufio.NewReader(input)
	objectName := *backup
	if objectName == "" {
		backups, err := cloudsql.ListDatabaseBackups(ctx, storageSvc, *bucket, *project, *instance, *database)
		if err != nil {
			return err
		}
		if len(backups) == 0 {
			return fmt.Errorf("no SQL backups found for %s/%s/%s", *project, *instance, *database)
		}
		for i, name := range backups {
			if _, err := fmt.Fprintf(output, "%d) %q\n", i+1, "gs://"+*bucket+"/"+name); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprint(output, "Select a backup number: "); err != nil {
			return err
		}
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("reading backup selection: %w", err)
		}
		selection, err := strconv.Atoi(strings.TrimSpace(line))
		if err != nil || selection < 1 || selection > len(backups) {
			return fmt.Errorf("invalid backup selection")
		}
		objectName = backups[selection-1]
	}
	if strings.HasPrefix(objectName, "gs://") || !strings.HasSuffix(objectName, ".sql") && !strings.HasSuffix(objectName, ".sql.gz") {
		return fmt.Errorf("backup must be a .sql or .sql.gz GCS object name")
	}
	parts := strings.Split(objectName, "/")
	if len(parts) != 4 || parts[0] == "" || parts[1] == "" || parts[2] != *database {
		return fmt.Errorf("backup must use PROJECT/INSTANCE/%s/FILE.sql[.gz]; renaming a database during SQL import is not supported", *database)
	}
	if _, err := storageSvc.Objects.Get(*bucket, objectName).Context(ctx).Do(); err != nil {
		return fmt.Errorf("checking backup: %w", err)
	}
	if _, err := sqlAdminSvc.Databases.Get(*project, *instance, *database).Context(ctx).Do(); err != nil {
		return fmt.Errorf("checking destination database: %w", err)
	}
	if _, err := fmt.Fprintf(output, "Restore %q into %s/%s/%s.\nThis imports SQL and may overwrite existing data; SQL statements can also affect other databases.\n", "gs://"+*bucket+"/"+objectName, *project, *instance, *database); err != nil {
		return err
	}
	if !*yes {
		confirmation := fmt.Sprintf("RESTORE gs://%s/%s INTO %s/%s/%s", *bucket, objectName, *project, *instance, *database)
		if _, err := fmt.Fprintf(output, "Type %q to confirm: ", confirmation); err != nil {
			return err
		}
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("reading confirmation: %w", err)
		}
		if strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r") != confirmation {
			return fmt.Errorf("restoration cancelled: confirmation did not match")
		}
	}
	if *ensureIamBindings {
		account, err := cloudsql.GetSvcAcctForCloudSQLInstance(ctx, sqlAdminSvc, *project, *instance, "")
		if err != nil {
			return err
		}
		if err := cloudsql.AddRoleBindingToGCSBucket(ctx, storageSvc, *project, *bucket, "roles/storage.objectViewer", account, *instance); err != nil {
			return err
		}
	}
	return cloudsql.RestoreCloudSQLDatabase(ctx, sqlAdminSvc, *project, *instance, *database, *bucket, objectName)
}
