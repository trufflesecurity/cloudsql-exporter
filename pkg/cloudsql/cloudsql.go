package cloudsql

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
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
	instance, err := sqlAdminSvc.Instances.Get(projectID, instanceID).Context(ctx).Do()
	if err != nil {
		return "", err
	}

	return instance.ServiceAccountEmailAddress, nil
}

// AddRoleBindingToGCSBucket adds a role binding to a GCS bucket.
func AddRoleBindingToGCSBucket(ctx context.Context, storageSvc *storage.Service, projectID, bucketName, role, sqlAdminSvcAccount, instance string) error {
	log.Printf("Ensuring role %s to bucket %s for service account %s used by instance %s", role, bucketName, sqlAdminSvcAccount, instance)

	svcAcctMember := fmt.Sprintf("serviceAccount:%s", sqlAdminSvcAccount)

	policy, err := storageSvc.Buckets.GetIamPolicy(bucketName).OptionsRequestedPolicyVersion(3).Context(ctx).Do()
	if err != nil {
		return err
	}

	found := false
	for i, binding := range policy.Bindings {
		if binding.Role == role {
			// Conditional grants do not ensure access to this backup.
			if binding.Condition != nil {
				continue
			}
			found = true
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
	if !found {
		policy.Bindings = append(policy.Bindings, &storage.PolicyBindings{Role: role, Members: []string{svcAcctMember}})
	}

	_, err = storageSvc.Buckets.SetIamPolicy(bucketName, policy).Context(ctx).Do()
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
func ExportCloudSQLDatabase(ctx context.Context, sqlAdminSvc *sqladmin.Service, databases []string, projectID, instanceID, bucketName, objectName, fileType string) error {
	for _, database := range databases {
		if fileType == "BAK" {
			switch strings.ToLower(database) {
			case "master", "model", "msdb", "tempdb":
				log.Printf("Skipping SQL Server system database %s", database)
				continue
			}
		}
		log.Printf("Exporting database %s for instance %s", database, instanceID)

		req := &sqladmin.InstancesExportRequest{
			ExportContext: &sqladmin.ExportContext{
				FileType:  fileType,
				Kind:      "sql#exportContext",
				Databases: []string{database},
				Uri:       fmt.Sprintf("gs://%s/%s/%s/%s/%s", bucketName, projectID, instanceID, database, objectName),
			},
		}

		op, err := sqlAdminSvc.Instances.Export(projectID, instanceID, req).Context(ctx).Do()
		if err != nil {
			return err
		}

		err = WaitForSQLOperation(ctx, sqlAdminSvc, 0, projectID, op)
		if err != nil {
			return err
		}
	}

	return nil
}

// ListDatabaseBackups lists SQL dumps in the exporter's database prefix, newest first.
func ListDatabaseBackups(ctx context.Context, storageSvc *storage.Service, bucketName, projectID, instanceID, database string) ([]string, error) {
	var objects []*storage.Object
	prefix := fmt.Sprintf("%s/%s/%s/", projectID, instanceID, database)
	err := storageSvc.Objects.List(bucketName).Prefix(prefix).Delimiter("/").Pages(ctx, func(page *storage.Objects) error {
		for _, object := range page.Items {
			if strings.HasSuffix(object.Name, ".sql") || strings.HasSuffix(object.Name, ".sql.gz") {
				objects = append(objects, object)
			}
		}
		return nil
	})
	sort.Slice(objects, func(i, j int) bool {
		left, _ := time.Parse(time.RFC3339Nano, objects[i].TimeCreated)
		right, _ := time.Parse(time.RFC3339Nano, objects[j].TimeCreated)
		if left.Equal(right) {
			return objects[i].Name > objects[j].Name
		}
		return left.After(right)
	})
	var backups []string
	for _, object := range objects {
		backups = append(backups, object.Name)
	}
	return backups, err
}

// RestoreCloudSQLDatabase imports an exported SQL dump into an existing database.
func RestoreCloudSQLDatabase(ctx context.Context, sqlAdminSvc *sqladmin.Service, projectID, instanceID, database, bucketName, objectName string) error {
	req := &sqladmin.InstancesImportRequest{
		ImportContext: &sqladmin.ImportContext{
			FileType: "SQL",
			Database: database,
			Uri:      fmt.Sprintf("gs://%s/%s", bucketName, objectName),
		},
	}
	op, err := sqlAdminSvc.Instances.Import(projectID, instanceID, req).Context(ctx).Do()
	if err != nil {
		return err
	}
	if op != nil {
		log.Printf("Restore submitted as Cloud SQL operation %s; if waiting stops, check this operation before retrying the import", op.Name)
	}
	return WaitForSQLOperation(ctx, sqlAdminSvc, 0, projectID, op)
}

func WaitForSQLOperation(ctx context.Context, sqlAdminSvc *sqladmin.Service, timeout time.Duration, gcpProject string, op *sqladmin.Operation) error {
	if op == nil {
		return errors.New("got nil op")
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	for {
		if op.Status == "DONE" {
			if op.Error != nil && len(op.Error.Errors) > 0 {
				return fmt.Errorf("cloud SQL operation %s failed: %s: %s", op.Name, op.Error.Errors[0].Code, op.Error.Errors[0].Message)
			}
			return nil
		}
		timer := time.NewTimer(10 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("waiting for cloud SQL operation %s: %w; operation may still be running, check its status before retrying", op.Name, ctx.Err())
		case <-timer.C:
		}
		updated, err := sqlAdminSvc.Operations.Get(gcpProject, op.Name).Context(ctx).Do()
		if err != nil {
			return fmt.Errorf("waiting for cloud SQL operation %s: %w; operation may still be running, check its status before retrying", op.Name, err)
		}
		op = updated
	}

}
