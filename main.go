package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"google.golang.org/api/sqladmin/v1"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	app = kingpin.New("cloudsql-backup", "Export Cloud SQL databases to Google Cloud Storage")

	bucket   = app.Flag("bucket", "Google Cloud Storage bucket name").Required().String()
	project  = app.Flag("project", "GCP project ID").Required().String()
	instance = app.Flag("instance", "Cloud SQL instance name, if not specified all within the project will be enumerated").String()
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kingpin.MustParse(app.Parse(os.Args[1:]))

	hc, err := google.DefaultClient(ctx, sqladmin.SqlserviceAdminScope)
	if err != nil {
		log.Fatal(err)
	}

	service, err := sqladmin.NewService(ctx, option.WithHTTPClient(hc))
	if err != nil {
		log.Fatal(err)
	}

	instances, err := EnumerateCloudSQLDatabaseInstances(ctx, service, *project, *instance)
	if err != nil {
		log.Fatal(err)
	}

	for instance, databases := range instances {
		log.Printf("Exporting backup for instance %s", instance)
		err := ExportCloudSQLDatabase(ctx, service, databases, *project, string(instance), *bucket, time.Now().Format(time.RFC3339Nano))
		if err != nil {
			log.Fatal(err)
		}
	}

}

type InstanceID string
type Databases []string

func (d Databases) Items() []string {
	return d
}

type Instances map[InstanceID]Databases

// EnumerateCloudSQLDatabaseInstances enumerates Cloud SQL database instances in the given project.
func EnumerateCloudSQLDatabaseInstances(ctx context.Context, service *sqladmin.Service, projectID, instanceID string) (Instances, error) {
	instances := Instances{}

	enumerated := []string{}

	if instanceID == "" {
		instanceList, err := service.Instances.List(projectID).Do()
		if err != nil {
			return nil, err
		}
		for _, instance := range instanceList.Items {
			enumerated = append(enumerated, string(instance.Name))
		}
	} else {
		enumerated = append(enumerated, instanceID)
	}

	for _, instance := range enumerated {
		log.Printf("Found instance %s", instance)
		databases, err := ListDatabasesForCloudSQLInstance(ctx, service, projectID, instance)
		if err != nil {
			return nil, err
		}
		instances[InstanceID(instance)] = databases
	}

	return instances, nil
}

// ListDatabasesForCloudSQLInstance lists the databases for a given Cloud SQL instance.
func ListDatabasesForCloudSQLInstance(ctx context.Context, service *sqladmin.Service, projectID, instanceID string) (Databases, error) {
	var databases Databases

	list, err := service.Databases.List(projectID, instanceID).Do()
	if err != nil {
		return nil, err
	}

	for _, database := range list.Items {
		if database.Name == "mysql" {
			log.Printf("Skipping database %s", database.Name)
			continue
		}
		databases = append(databases, database.Name)
	}

	return databases, nil
}

// ExportCloudSQLDatabase exports a Cloud SQL database to a Google Cloud Storage bucket.
func ExportCloudSQLDatabase(ctx context.Context, service *sqladmin.Service, databases []string, projectID, instanceID, bucketName, objectName string) error {
	for _, database := range databases {
		log.Printf("Exporting database %s for instance %s", database, instanceID)

		req := &sqladmin.InstancesExportRequest{
			ExportContext: &sqladmin.ExportContext{
				FileType:  "SQL",
				Kind:      "sql#exportContext",
				Databases: []string{database},
				Uri:       fmt.Sprintf("gs://%s/%s/%s/%s/%s.sql", bucketName, projectID, instanceID, database, objectName),
			},
		}

		op, err := service.Instances.Export(projectID, instanceID, req).Do()
		if err != nil {
			return err
		}

		err = WaitForSQLOperation(ctx, service, time.Minute*10, projectID, op)
		if err != nil {
			return err
		}
	}

	return nil
}

func WaitForSQLOperation(ctx context.Context, service *sqladmin.Service, timeout time.Duration, gcpProject string, op *sqladmin.Operation) error {
	if op == nil {
		return errors.New("got nil op")
	}

	for {
		select {
		case <-ctx.Done():
			return errors.New("timeout reached")
		default:
			time.Sleep(time.Second * 10)
			op, err := service.Operations.Get(gcpProject, op.Name).Do()
			if err != nil {
				return err
			}
			if op.Status == "DONE" {
				return nil
			}
		}
	}

}
