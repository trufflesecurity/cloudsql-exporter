package cloudsql

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"google.golang.org/api/googleapi"
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

	roleExists := false
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
			roleExists = true
			break
		}
	}

	if !roleExists {
		policy.Bindings = append(policy.Bindings, &storage.PolicyBindings{
			Role:    role,
			Members: []string{svcAcctMember},
		})
	}

	_, err = storageSvc.Buckets.SetIamPolicy(bucketName, policy).Do()
	if err != nil {
		return err
	}

	return nil
}

// RemoveRoleBindingToGCSBucket remove a role binding to a GCS bucket.
func RemoveRoleBindingToGCSBucket(ctx context.Context, storageSvc *storage.Service, projectID, bucketName, role, sqlAdminSvcAccount, instance string) error {
	log.Printf("Deleting role %s to bucket %s for service account %s used by instance %s", role, bucketName, sqlAdminSvcAccount, instance)

	svcAcctMember := fmt.Sprintf("serviceAccount:%s", sqlAdminSvcAccount)

	policy, err := storageSvc.Buckets.GetIamPolicy(bucketName).Do()
	if err != nil {
		return err
	}

	for i, binding := range policy.Bindings {
		if binding.Role == role {
			for j, member := range binding.Members {
				if member == svcAcctMember {
					if len(binding.Members) == 1 {
						binding.Members = []string{}
					} else {
						binding.Members = append(binding.Members[:j], binding.Members[j+1:]...)
					}
					policy.Bindings[i] = binding
					break
				}
			}
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
func ExportCloudSQLDatabase(ctx context.Context, sqlAdminSvc *sqladmin.Service, databases []string, projectID, instanceID, bucketName, objectName string) ([]string, error) {
	locations := make([]string, 0)
	for _, database := range databases {
		objectName := fmt.Sprintf("%s-%s", database ,objectName)
		//TODO make this configurable

		location := fmt.Sprintf("gs://%s/%s/cloudsql/%s", bucketName, instanceID, objectName)

		locations = append(locations, location)
		log.Printf("Exporting database %s for instance %s to %s", database, instanceID, location)

		req := &sqladmin.InstancesExportRequest{
			ExportContext: &sqladmin.ExportContext{
				FileType:  "SQL",
				Kind:      "sql#exportContext",
				Databases: []string{database},
				Uri: location,
			},
		}

		op, err := sqlAdminSvc.Instances.Export(projectID, instanceID, req).Do()
		if err != nil {
			return nil, err
		}

		err = WaitForSQLOperation(ctx, sqlAdminSvc, time.Minute*1, projectID, op)
		if err != nil {
			return nil, err
		}
	}

	return locations, nil
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
			time.Sleep(timeout)
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

func Validate(ctx context.Context, admin *sqladmin.Service, storageSvc *storage.Service, project string, instance string, bucket string, file string) error {
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
				RootPassword: password,
				DatabaseVersion: "POSTGRES_13",
    }

		db, err := admin.Instances.Get(project, dbinstance.Name).Do()
		if err != nil && err.(*googleapi.Error).Code != 404 {
        log.Fatalf("Failed to get PostgreSQL instance: %v", err)
    }

		if db == nil {
			// Create the PostgreSQL instance
			operation, err := admin.Instances.Insert(project, dbinstance).Context(ctx).Do()
			if err != nil {
					log.Fatalf("Failed to create PostgreSQL instance: %v", err)
			}

			// Wait for the operation to complete
			if err := WaitForSQLOperation(ctx, admin, time.Minute*1, project, operation); err != nil {
					log.Fatalf("Failed to create PostgreSQL instance: %v", err)
			}

			fmt.Println("PostgreSQL instance created successfully.")
		}

		log.Printf("PostgreSQL instance exists %+v.", db)

		sqlAdminSvcAccount, err := GetSvcAcctForCloudSQLInstance(ctx, admin, project, fmt.Sprintf("restore-%s", instance), "")
		if err != nil {
			log.Fatal(err)
		}

		defer func() {
			err = RemoveRoleBindingToGCSBucket(ctx, storageSvc, project, bucket, "roles/storage.legacyBucketReader", sqlAdminSvcAccount, string(instance))
			if err != nil {
				log.Fatal(err)
			}
			err = RemoveRoleBindingToGCSBucket(ctx, storageSvc, project, bucket, "roles/storage.objectViewer", sqlAdminSvcAccount, string(instance))
			if err != nil {
				log.Fatal(err)
			}
		}()

		err = AddRoleBindingToGCSBucket(ctx, storageSvc, project, bucket, "roles/storage.legacyBucketReader", sqlAdminSvcAccount, string(instance))
		if err != nil {
			log.Fatal(err)
		}
		err = AddRoleBindingToGCSBucket(ctx, storageSvc, project, bucket, "roles/storage.objectViewer", sqlAdminSvcAccount, string(instance))
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("Import data from %s", file)
    // Import data from SQL file
    importReq := &sqladmin.InstancesImportRequest{
        ImportContext: &sqladmin.ImportContext{
					  Kind:      "sql#importContext",
            Database:  "your_database_name",
            FileType:  "SQL",
            Uri:       file, // You can also use local file path here
        },
    }

    importOp, err := admin.Instances.Import(project, fmt.Sprintf("restore-%s", instance), importReq).Context(ctx).Do()
    if err != nil {
        log.Fatalf("Failed to initialize import data: %+v", err)
    }

    // Wait for the import operation to complete
    if err := WaitForSQLOperation(ctx, admin, time.Minute*1, project, importOp); err != nil {
        log.Fatalf("Failed to import data: %+v", err)
    }

    fmt.Println("Data imported successfully.")
		return nil
}
