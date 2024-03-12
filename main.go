package main

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

	"github.com/trufflesecurity/cloudsql-exporter/pkg/cloudsql"
	"github.com/trufflesecurity/cloudsql-exporter/pkg/version"
)

var (
	app = kingpin.New("cloudsql-backup", "Export Cloud SQL databases to Google Cloud Storage")

	bucket                = app.Flag("bucket", "Google Cloud Storage bucket name").Required().String()
	project               = app.Flag("project", "GCP project ID").Required().String()
	instance              = app.Flag("instance", "Cloud SQL instance name, if not specified all within the project will be enumerated").String()
	compression           = app.Flag("compression", "Enable compression for exported SQL files").Bool()
	ensureIamBindings     = app.Flag("ensure-iam-bindings", "Ensure that the Cloud SQL service account has the required IAM role binding to export and validate the backup").Bool()
	ensureIamBindingsTemp = app.Flag("ensure-iam-bindings-temp", "Ensure that the Cloud SQL service account has the required IAM role binding to export and validate the backup").Bool()
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kingpin.MustParse(app.Parse(os.Args[1:]))
	app.Version("cloudsql-exporter " + version.BuildVersion)

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

	instances, err := cloudsql.EnumerateCloudSQLDatabaseInstances(ctx, sqlAdminSvc, *project, *instance)
	if err != nil {
		log.Fatal(err)
	}

	for instance, databases := range instances {
		log.Printf("Exporting backup for instance %s", instance)

		if *ensureIamBindings || *ensureIamBindingsTemp {
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

		var objectName string

		if *compression {
			objectName = time.Now().Format(time.RFC3339Nano) + ".sql.gz"
		} else {
			objectName = time.Now().Format(time.RFC3339Nano) + ".sql"
		}

		err := cloudsql.ExportCloudSQLDatabase(ctx, sqlAdminSvc, databases, *project, string(instance), *bucket, objectName)

		if err != nil {
			log.Fatal(err)
		}

		if *ensureIamBindingsTemp {
			sqlAdminSvcAccount, err := cloudsql.GetSvcAcctForCloudSQLInstance(ctx, sqlAdminSvc, *project, string(instance), "")
			if err != nil {
				log.Fatal(err)
			}
			err = cloudsql.RemoveRoleBindingToGCSBucket(ctx, storageSvc, *project, *bucket, "roles/storage.objectCreator", sqlAdminSvcAccount, string(instance))
			if err != nil {
				log.Fatal(err)
			}
			err = cloudsql.RemoveRoleBindingToGCSBucket(ctx, storageSvc, *project, *bucket, "roles/storage.objectViewer", sqlAdminSvcAccount, string(instance))
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	log.Println("Backup complete")

}
