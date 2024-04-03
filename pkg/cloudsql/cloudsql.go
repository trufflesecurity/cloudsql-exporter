package cloudsql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"google.golang.org/api/googleapi"
	"google.golang.org/api/sqladmin/v1"

	"cloud.google.com/go/iam"
	"cloud.google.com/go/storage"
)

type InstanceID string
type Databases []string

func (d Databases) Items() []string {
	return d
}

type Instances map[InstanceID]Databases

type CloudSQL struct {
	ProjectID string

	ctx         context.Context
	sqlAdminSvc *sqladmin.Service
	storageSvc  *storage.Client
}

func NewCloudSQL(ctx context.Context, sqlAdminSvc *sqladmin.Service, storageSvc *storage.Client, projectID string) *CloudSQL {
	return &CloudSQL{
		ProjectID:   projectID,
		ctx:         ctx,
		sqlAdminSvc: sqlAdminSvc,
		storageSvc:  storageSvc,
	}
}

// EnumerateCloudSQLDatabaseInstances enumerates Cloud SQL database instances in the given project.
func (c *CloudSQL) EnumerateCloudSQLDatabaseInstances(instanceID string) (Instances, error) {
	slog.Info("Enumerating Cloud SQL instances in project", "projectId", c.ProjectID)

	instances := Instances{}

	enumerated := []string{}

	if instanceID == "" {
		instanceList, err := c.sqlAdminSvc.Instances.List(c.ProjectID).Do()
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
		slog.Info("Found instance", "instance", instance)
		databases, err := c.ListDatabasesForCloudSQLInstance(instance)
		if err != nil {
			return nil, err
		}
		instances[InstanceID(instance)] = databases
	}

	return instances, nil
}

// GetSvcAcctForCloudSQLInstance returns the service account for the given Cloud SQL sqladmin.Database
func (c *CloudSQL) GetSvcAcctForCloudSQLInstance(instanceID, database string) (string, error) {
	instance, err := c.sqlAdminSvc.Instances.Get(c.ProjectID, instanceID).Do()
	if err != nil {
		return "", err
	}

	return instance.ServiceAccountEmailAddress, nil
}

// AddRoleBindingToGCSBucket adds a role binding to a GCS bucket.
func (c *CloudSQL) AddRoleBindingToGCSBucket(bucketName, role, sqlAdminSvcAccount, instance string) error {
	slog.Info("Ensuring role to bucket for service account used by instance", "role", role, "bucket", bucketName, "service_account", sqlAdminSvcAccount, "instance", instance)

	svcAcctMember := fmt.Sprintf("serviceAccount:%s", sqlAdminSvcAccount)

	bucket := c.storageSvc.Bucket(bucketName)
	policy, err := bucket.IAM().Policy(c.ctx)
	if err != nil {
		return err
	}

	var iamRole iam.RoleName = iam.RoleName(role)

	if policy.HasRole(svcAcctMember, iamRole) {
		return nil
	}

	policy.Add(svcAcctMember, iamRole)

	if err := bucket.IAM().SetPolicy(c.ctx, policy); err != nil {
		return err
	}

	return nil
}

// RemoveRoleBindingToGCSBucket remove a role binding to a GCS bucket.
func (c *CloudSQL) RemoveRoleBindingToGCSBucket(bucketName, role, sqlAdminSvcAccount, instance string) error {
	slog.Info("Deleting role to bucket for service account used by instance", "role", role, "bucket", bucketName, "service_account", sqlAdminSvcAccount, "instance", instance)

	svcAcctMember := fmt.Sprintf("serviceAccount:%s", sqlAdminSvcAccount)

	bucket := c.storageSvc.Bucket(bucketName)
	policy, err := bucket.IAM().Policy(c.ctx)
	if err != nil {
		return err
	}

	var iamRole iam.RoleName = iam.RoleName(role)

	if !policy.HasRole(svcAcctMember, iamRole) {
		return nil
	}

	policy.Remove(svcAcctMember, iamRole)

	if err := bucket.IAM().SetPolicy(c.ctx, policy); err != nil {
		return err
	}

	return nil
}

// ListDatabasesForCloudSQLInstance lists the databases for a given Cloud SQL instance.
func (c *CloudSQL) ListDatabasesForCloudSQLInstance(instanceID string) (Databases, error) {
	var databases Databases

	list, err := c.sqlAdminSvc.Databases.List(c.ProjectID, instanceID).Do()
	if err != nil {
		return nil, err
	}

	for _, database := range list.Items {
		if database.Name == "mysql" || database.Name == "postgres" {
			slog.Info("Skipping database", "database", database.Name)
			continue
		}
		slog.Info("Found database for instance", "database", database.Name, "instance", instanceID)
		databases = append(databases, database.Name)
	}

	return databases, nil
}

// ExportCloudSQLDatabase exports a Cloud SQL database to a Google Cloud Storage bucket.
func (c *CloudSQL) ExportCloudSQLDatabase(databases []string, instanceID, bucketName, objectName string) ([]string, error) {
	locations := make([]string, 0)
	for _, database := range databases {
		objectName := fmt.Sprintf("%s-%s", database, objectName)
		//TODO make this configurable

		location := fmt.Sprintf("gs://%s/%s/cloudsql/%s", bucketName, instanceID, objectName)

		locations = append(locations, location)
		slog.Info("Exporting database for instance", "database", database, "instance", instanceID, "location", location)

		req := &sqladmin.InstancesExportRequest{
			ExportContext: &sqladmin.ExportContext{
				FileType:  "SQL",
				Kind:      "sql#exportContext",
				Databases: []string{database},
				Uri:       location,
			},
		}

		op, err := c.sqlAdminSvc.Instances.Export(c.ProjectID, instanceID, req).Do()
		if err != nil {
			return nil, err
		}

		err = c.WaitForSQLOperation(time.Minute*1, op)
		if err != nil {
			return nil, err
		}
	}

	return locations, nil
}

func (c *CloudSQL) WaitForSQLOperation(timeout time.Duration, op *sqladmin.Operation) error {
	if op == nil {
		return errors.New("got nil op")
	}

	for {
		select {
		case <-c.ctx.Done():
			return errors.New("timeout reached")
		default:
			time.Sleep(timeout)
			op, err := c.sqlAdminSvc.Operations.Get(c.ProjectID, op.Name).Do()
			if err != nil {
				return err
			}
			if op.Status == "DONE" {
				return nil
			}
		}
	}

}

func generatePassword(length int) string {
	// Define the character set
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()-_=+[]{},.<>?;:"

	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())

	// Create a byte slice to store the password characters
	password := make([]byte, length)

	// Fill the byte slice with random characters from the charset
	for i := range password {
		password[i] = charset[rand.Intn(len(charset))]
	}

	return string(password)
}

func (c *CloudSQL) Validate(instance string, bucket string, file string) error {
	// Define the database instance parameters
	password := generatePassword(12)
	dbinstance := &sqladmin.DatabaseInstance{
		Name:         fmt.Sprintf("restore-%s", instance),
		InstanceType: "CLOUD_SQL_INSTANCE",
		Region:       "europe-west3",
		Settings: &sqladmin.Settings{
			Tier:             "db-f1-micro", // Change as needed
			ActivationPolicy: "ALWAYS",
			DatabaseFlags: []*sqladmin.DatabaseFlags{
				{Name: "cloudsql.iam_authentication", Value: "on"},
			},
			InsightsConfig: &sqladmin.InsightsConfig{
				QueryInsightsEnabled: true,
			},
			UserLabels: map[string]string{
				"service": fmt.Sprintf("restore-%s", instance),
			},
		},
		RootPassword:    password,
		DatabaseVersion: "POSTGRES_13",
	}

	db, err := c.sqlAdminSvc.Instances.Get(c.ProjectID, dbinstance.Name).Do()
	if err != nil && err.(*googleapi.Error).Code != 404 {
		slog.Error("Failed to get PostgreSQL instance", "instance", dbinstance.Name, "error", err)
		return err
	}

	if db == nil {
		// Create the PostgreSQL instance
		operation, err := c.sqlAdminSvc.Instances.Insert(c.ProjectID, dbinstance).Context(c.ctx).Do()
		if err != nil {
			slog.Error("Failed to create PostgreSQL instance", "instance", dbinstance.Name, "error", err)
			return err
		}

		// Wait for the operation to complete
		if err := c.WaitForSQLOperation(time.Minute*1, operation); err != nil {
			slog.Error("Failed to create PostgreSQL instance", "instance", dbinstance.Name, "error", err)
			return err
		}

		fmt.Println("PostgreSQL instance created successfully.")
	}

	sqlAdminSvcAccount, err := c.GetSvcAcctForCloudSQLInstance(fmt.Sprintf("restore-%s", instance), "")
	if err != nil {
		slog.Error("Failed to get service account for instance", "instance", fmt.Sprintf("restore-%s", instance), "error", err)
		return err
	}

	defer func() {
		err = c.RemoveRoleBindingToGCSBucket(bucket, "roles/storage.legacyBucketReader", sqlAdminSvcAccount, string(instance))
		if err != nil {
			slog.Error("Failed to remove role binding roles/storage.legacyBucketReader", "service_account", sqlAdminSvcAccount, "error", err)
		}
		err = c.AddRoleBindingToGCSBucket(bucket, "roles/storage.objectViewer", sqlAdminSvcAccount, string(instance))
		if err != nil {
			slog.Error("Failed to remove role binding roles/storage.objectViewer", "service_account", sqlAdminSvcAccount, "error", err)
		}
	}()

	err = c.AddRoleBindingToGCSBucket(bucket, "roles/storage.legacyBucketReader", sqlAdminSvcAccount, string(instance))
	if err != nil {
		slog.Error("Failed to add role binding roles/storage.legacyBucketReader", "service_account", sqlAdminSvcAccount, "error", err)
		return err
	}
	err = c.AddRoleBindingToGCSBucket(bucket, "roles/storage.objectViewer", sqlAdminSvcAccount, string(instance))
	if err != nil {
		slog.Error("Failed to add role binding roles/storage.objectViewer", "service_account", sqlAdminSvcAccount, "error", err)
		return err
	}

	slog.Info("Import data", "file", file)
	// Import data from SQL file
	importReq := &sqladmin.InstancesImportRequest{
		ImportContext: &sqladmin.ImportContext{
			Kind:     "sql#importContext",
			Database: "your_database_name",
			FileType: "SQL",
			Uri:      file, // You can also use local file path here
		},
	}

	importOp, err := c.sqlAdminSvc.Instances.Import(c.ProjectID, fmt.Sprintf("restore-%s", instance), importReq).Context(c.ctx).Do()
	if err != nil {
		slog.Error("Failed to import data", "file", file, "error", err)
	}

	// Wait for the import operation to complete
	if err := c.WaitForSQLOperation(time.Minute*1, importOp); err != nil {
		slog.Error("Failed to import data", "error", err)
	}

	slog.Info("Data imported successfully.")
	return nil
}
