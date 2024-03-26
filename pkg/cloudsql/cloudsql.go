package cloudsql

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"google.golang.org/api/sqladmin/v1"
	"google.golang.org/api/storage/v1"
)

type InstanceID string
type Databases []string

func (d Databases) Items() []string {
	return d
}

type Instances map[InstanceID]Databases

// EnumerateCloudSQLDatabaseInstances enumerates Cloud SQL database instances in the given project.
func EnumerateCloudSQLDatabaseInstances(ctx context.Context, sqlAdminSvc *sqladmin.Service, projectID, instanceID string) (Instances, error) {
	log.Printf("Enumerating Cloud SQL instances in project %s", projectID)

	instances := Instances{}

	enumerated := []string{}

	if instanceID == "" {
		instanceList, err := sqlAdminSvc.Instances.List(projectID).Do()
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
		databases, err := ListDatabasesForCloudSQLInstance(ctx, sqlAdminSvc, projectID, instance)
		if err != nil {
			return nil, err
		}
		instances[InstanceID(instance)] = databases
	}

	return instances, nil
}

// GetSvcAcctForCloudSQLInstance returns the service account for the given Cloud SQL sqladmin.Database
func GetSvcAcctForCloudSQLInstance(ctx context.Context, sqlAdminSvc *sqladmin.Service, projectID, instanceID, database string) (string, error) {
	instance, err := sqlAdminSvc.Instances.Get(projectID, instanceID).Do()
	if err != nil {
		return "", err
	}

	return instance.ServiceAccountEmailAddress, nil
}

// AddRoleBindingToGCSBucket adds a role binding to a GCS bucket.
func AddRoleBindingToGCSBucket(ctx context.Context, storageSvc *storage.Service, projectID, bucketName, role, sqlAdminSvcAccount, instance string) error {
	log.Printf("Ensuring role %s to bucket %s for service account %s used by instance %s", role, bucketName, sqlAdminSvcAccount, instance)

	svcAcctMember := fmt.Sprintf("serviceAccount:%s", sqlAdminSvcAccount)

	policy, err := storageSvc.Buckets.GetIamPolicy(bucketName).Do()
	if err != nil {
		return err
	}

	for i, binding := range policy.Bindings {
		if binding.Role == role {
			for _, member := range binding.Members {
				if member == svcAcctMember {
					log.Printf("Role %s already exists for service account %s", role, sqlAdminSvcAccount)
					return nil
				}
			}
			binding.Members = append(binding.Members, svcAcctMember)
			policy.Bindings[i] = binding
			break
		}
	}

	_, err = storageSvc.Buckets.SetIamPolicy(bucketName, policy).Do()
	if err != nil {
		return err
	}

	return nil
}

// ListDatabasesForCloudSQLInstance lists the databases for a given Cloud SQL instance.
func ListDatabasesForCloudSQLInstance(ctx context.Context, sqlAdminSvc *sqladmin.Service, projectID, instanceID string) (Databases, error) {
	var databases Databases

	list, err := sqlAdminSvc.Databases.List(projectID, instanceID).Do()
	if err != nil {
		return nil, err
	}

	for _, database := range list.Items {
		if database.Name == "mysql" {
			log.Printf("Skipping database %s", database.Name)
			continue
		}
		log.Printf("Found database %s for instance %s", database.Name, instanceID)
		databases = append(databases, database.Name)
	}

	return databases, nil
}

// ExportCloudSQLDatabase exports a Cloud SQL database to a Google Cloud Storage bucket.
func ExportCloudSQLDatabase(ctx context.Context, sqlAdminSvc *sqladmin.Service, databases []string, projectID, instanceID, bucketName, objectName string) error {
	for _, database := range databases {
		log.Printf("Exporting database %s for instance %s", database, instanceID)

		req := &sqladmin.InstancesExportRequest{
			ExportContext: &sqladmin.ExportContext{
				FileType:  "SQL",
				Kind:      "sql#exportContext",
				Databases: []string{database},
				Uri:       fmt.Sprintf("gs://%s/%s/%s/%s/%s", bucketName, projectID, instanceID, database, objectName),
			},
		}

		op, err := sqlAdminSvc.Instances.Export(projectID, instanceID, req).Do()
		if err != nil {
			return err
		}

		err = WaitForSQLOperation(ctx, sqlAdminSvc, time.Minute*10, projectID, op)
		if err != nil {
			return err
		}
	}

	return nil
}

func WaitForSQLOperation(ctx context.Context, sqlAdminSvc *sqladmin.Service, timeout time.Duration, gcpProject string, op *sqladmin.Operation) error {
	if op == nil {
		return errors.New("got nil op")
	}

	for {
		select {
		case <-ctx.Done():
			return errors.New("timeout reached")
		default:
			time.Sleep(time.Second * 10)
			op, err := sqlAdminSvc.Operations.Get(gcpProject, op.Name).Do()
			if err != nil {
				return err
			}
			if op.Status == "DONE" {
				return nil
			}
		}
	}

}
