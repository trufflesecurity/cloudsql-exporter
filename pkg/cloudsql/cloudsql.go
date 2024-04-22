package cloudsql

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math"
	"math/rand"
	"strings"
	"time"

	bakstorage "github.com/fr12k/cloudsql-exporter/pkg/storage"

	"google.golang.org/api/googleapi"
	"google.golang.org/api/sqladmin/v1"
	"gopkg.in/yaml.v3"

	"cloud.google.com/go/iam"
	"cloud.google.com/go/secretmanager/apiv1beta2"
	"cloud.google.com/go/secretmanager/apiv1beta2/secretmanagerpb"
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
	secretSvc   *secretmanager.Client
}

func NewCloudSQL(ctx context.Context, sqlAdminSvc *sqladmin.Service, storageSvc *storage.Client, secretSvc *secretmanager.Client, projectID string) *CloudSQL {
	return &CloudSQL{
		ProjectID:   projectID,
		ctx:         ctx,
		sqlAdminSvc: sqlAdminSvc,
		storageSvc:  storageSvc,
		secretSvc:   secretSvc,
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

func (c *CloudSQL) ExportUsers(instanceID, database string) ([]*sqladmin.User, error) {
	users, err := c.sqlAdminSvc.Users.List(c.ProjectID, instanceID).Do()
	if err != nil {
		return nil, err
	}
	return users.Items, nil
}

// ExportCloudSQLUser exports a Cloud SQL users to Google Cloud Storage bucket.
func (c *CloudSQL) ExportCloudSQLUser(instanceID, bucketName, backupTime string) ([]string, error) {
	location := fmt.Sprintf("%s/cloudsql/users-%s.txt", instanceID, backupTime)
	slog.Info("Exporting users for instance", "instance", instanceID, "location", location)

	users, err := c.ExportUsers(instanceID, "")
	if err != nil {
		return nil, err
	}

	bucket := c.storageSvc.Bucket(bucketName)
	writer := bucket.Object(location).NewWriter(c.ctx)
	defer writer.Close()

	userNames := []string{}
	for _, user := range users {
		if user.Name == "mysql" || user.Name == "postgres" {
			slog.Info("Skipping user", "users", user.Name)
			continue
		}
		userNames = append(userNames, user.Name)
		_, err := writer.Write([]byte(fmt.Sprintf("%s\n", user.Name)))
		if err != nil {
			return nil, err
		}
	}
	return userNames, nil
}

type CloudSQLStatistic struct {
	FullTableName                string `yaml:"full_table_name"`
	TableSizeBytes               int64  `yaml:"table_size_bytes"`
	TableSizeBytesWithoutIndexes int64  `yaml:"table_size_bytes_without_indexes"`
	TotalSizeBytes               int64  `yaml:"total_size_bytes"`
	RowCount                     int64  `yaml:"row_count"`
}

func (c *CloudSQL) GetCloudSQLStatistic(instanceID, user, password, database string) (map[string]*CloudSQLStatistic, error) {
	conn := Connection{
		User: user,
		//TODO get the password
		Password: password,
		Database: database,
		URL:      fmt.Sprintf("%s:%s:%s", c.ProjectID, "europe-west3", instanceID),
	}

	dbConn, err := conn.Connect()
	if err != nil {
		slog.Error("Failed to connect to database", "instance", conn.URL, "database", conn.Database, "error", err)
		return nil, err
	}

	defer dbConn.Close()
	statsSQL := `
	SELECT
	schemaname || '.' || tablename AS full_table_name,
	pg_table_size(schemaname || '.' || tablename) AS table_size_bytes,
	pg_relation_size(schemaname || '.' || tablename) AS table_size_bytes_without_indexes,
	pg_total_relation_size(schemaname || '.' || tablename) AS total_size_bytes,
	reltuples AS row_count
FROM
	pg_catalog.pg_tables
JOIN
	pg_catalog.pg_class ON pg_tables.tablename = pg_class.relname
WHERE
	pg_catalog.pg_tables.schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY
	schemaname,
	tablename;`

	_, err = dbConn.Exec("ANALYZE VERBOSE;")

	if err != nil {
		slog.Error("Failed to execute analyze query", "instance", conn.URL, "database", conn.Database, "error", err)
		return nil, err
	}

	rows, err := dbConn.Query(statsSQL)
	if err != nil {
		slog.Error("Failed to execute query", "instance", conn.URL, "database", conn.Database, "error", err)
		return nil, err
	}

	// Iterate over the rows
	stats := make(map[string]*CloudSQLStatistic)
	for rows.Next() {
		stat := &CloudSQLStatistic{}
		var rowCount float64
		err := rows.Scan(&stat.FullTableName, &stat.TableSizeBytes, &stat.TableSizeBytesWithoutIndexes, &stat.TotalSizeBytes, &rowCount)
		if err != nil {
			return nil, err
		}
		stat.RowCount = int64(math.Round(rowCount))
		stats[stat.FullTableName] = stat
	}

	// Check for errors from iterating over rows
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return stats, nil
}

// ExportCloudSQLStatistics exports statistics like tables and size Google Cloud Storage bucket.
func (c *CloudSQL) ExportCloudSQLStatistics(databases []string, instanceID, bucketName, backupTime string, user string, password string) (map[string]*CloudSQLStatistic, error) {
	stats := make(map[string]*CloudSQLStatistic)

	for _, database := range databases {
		dbStats, err := c.GetCloudSQLStatistic(instanceID, user, password, database)
		if err != nil {
			return nil, err
		}

		location := fmt.Sprintf("%s/cloudsql/stats-%s-%s.yaml", instanceID, database, backupTime)
		slog.Info("Exporting statistics for instance", "instance", instanceID, "location", location)

		bucket := c.storageSvc.Bucket(bucketName)
		writer := bucket.Object(location).NewWriter(c.ctx)
		defer writer.Close()

		err = yaml.NewEncoder(writer).Encode(dbStats)
		if err != nil {
			slog.Error("Failed yaml encode statistics", "error", err)
			return nil, err
		}
		maps.Copy(stats, dbStats)
	}
	return stats, nil
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
			if op.Error != nil {
				var errors []string
				for _, e := range op.Error.Errors {
					errors = append(errors, e.Message)
				}
				return fmt.Errorf("operation failed: %s", errors)
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
	rand.New(rand.NewSource(time.Now().UnixNano()))

	// Create a byte slice to store the password characters
	password := make([]byte, length)

	// Fill the byte slice with random characters from the charset
	for i := range password {
		password[i] = charset[rand.Intn(len(charset))]
	}

	return string(password)
}

type RestoreOptions struct {
	Bucket   string
	Project  string
	Instance string
	File     string
	User     string

	StoreSecret   bool

	Version string
}

func (c *CloudSQL) savePassword(password string, dbInstance string) error {
	// Create the secret
	secret, err := c.secretSvc.GetSecret(c.ctx, &secretmanagerpb.GetSecretRequest{
		Name: fmt.Sprintf("projects/%s/secrets/%s", c.ProjectID, strings.ToUpper(dbInstance)),
	})

	if err != nil /*&& err.(*apierror.APIError).Code != 404 */ {
		slog.Error("Failed to get secret", "instance", dbInstance, "error", err)
	}

	if secret != nil {
		err := c.secretSvc.DeleteSecret(c.ctx, &secretmanagerpb.DeleteSecretRequest{
			Name: secret.Name,
		})
		if err != nil {
			slog.Error("Failed to delete secret", "instance", dbInstance, "error", err)
			return err
		}
		secret = nil
	}

	if secret == nil {
		secret, err = c.secretSvc.CreateSecret(c.ctx, &secretmanagerpb.CreateSecretRequest{
			Parent:   fmt.Sprintf("projects/%s", c.ProjectID),
			SecretId: strings.ToUpper(dbInstance),
			Secret: &secretmanagerpb.Secret{
				Replication: &secretmanagerpb.Replication{
					Replication: &secretmanagerpb.Replication_UserManaged_{
						UserManaged: &secretmanagerpb.Replication_UserManaged{
							Replicas: []*secretmanagerpb.Replication_UserManaged_Replica{
								{
									Location: "europe-west3",
								},
							},
						},
					},
				},
				Name: strings.ToUpper(dbInstance),
			},
		})

		if err != nil {
			slog.Error("Failed to create secret", "instance", dbInstance, "error", err)
			return err
		}
		_, err := c.secretSvc.AddSecretVersion(c.ctx, &secretmanagerpb.AddSecretVersionRequest{
			Parent: secret.Name,
			Payload: &secretmanagerpb.SecretPayload{
				Data: []byte(password),
			},
		})
		if err != nil {
			slog.Error("Failed to add secret version", "secret", secret.Name, "error", err)
			return err
		}
	}
	return nil
}

func (c *CloudSQL) Restore(opts *RestoreOptions) (*string, error) {
	// Define the database instance parameters
	password := generatePassword(12)
	dbinstance := &sqladmin.DatabaseInstance{
		Name:         fmt.Sprintf("restore-%s", opts.Instance),
		InstanceType: "CLOUD_SQL_INSTANCE",
		//TODO make this configurable
		Region: "europe-west3",
		Settings: &sqladmin.Settings{
			Tier:             "db-f1-micro", //TODO make it configurable Change as needed
			ActivationPolicy: "ALWAYS",
			DatabaseFlags: []*sqladmin.DatabaseFlags{
				{Name: "cloudsql.iam_authentication", Value: "on"},
			},
			InsightsConfig: &sqladmin.InsightsConfig{
				QueryInsightsEnabled: true,
			},
			UserLabels: map[string]string{
				"service": fmt.Sprintf("restore-%s", opts.Instance),
				"kind":    "restore",
			},
		},
		RootPassword:    password,
		DatabaseVersion: "POSTGRES_13", //TODO get version from backup file
	}

	slog.Info("Check if restore instance exists", "instance", dbinstance.Name)
	db, err := c.sqlAdminSvc.Instances.Get(c.ProjectID, dbinstance.Name).Do()
	if err != nil && err.(*googleapi.Error).Code != 404 {
		slog.Error("Failed to get PostgreSQL instance", "instance", dbinstance.Name, "error", err)
		return nil, err
	}

	if db == nil {
		// Store the password for the new created database instance if requested
		if opts.StoreSecret {
			err := c.savePassword(password, dbinstance.Name)
			if err != nil {
				return nil, err
			}
		}

		// Create the PostgreSQL instance
		slog.Info("Create PostgreSQL instance", "instance", dbinstance.Name)
		operation, err := c.sqlAdminSvc.Instances.Insert(c.ProjectID, dbinstance).Context(c.ctx).Do()
		if err != nil {
			slog.Error("Failed to create PostgreSQL instance", "instance", dbinstance.Name, "error", err)
			return nil, err
		}

		// Wait for the operation to complete
		if err := c.WaitForSQLOperation(time.Minute*1, operation); err != nil {
			slog.Error("Failed to create PostgreSQL instance", "instance", dbinstance.Name, "error", err)
			return nil, err
		}
		slog.Info("Successfully created PostgreSQL instance", "instance", dbinstance.Name)
	} else {
		secretVersion, err := c.secretSvc.AccessSecretVersion(c.ctx, &secretmanagerpb.AccessSecretVersionRequest{
			Name: fmt.Sprintf("projects/%s/secrets/%s/versions/latest", c.ProjectID, strings.ToUpper(dbinstance.Name)),
		})

		if err != nil {
			slog.Error("Failed to get secret version", "instance", dbinstance.Name, "error", err)
			return nil, err
		}
		password = string(secretVersion.Payload.Data)
	}

	backLocation := bakstorage.NewLocation(opts.File)

	database := &sqladmin.Database{
		Name: backLocation.Database,
	}

	dbase, err := c.sqlAdminSvc.Databases.Get(c.ProjectID, dbinstance.Name, database.Name).Do()
	if err != nil && err.(*googleapi.Error).Code != 404 {
		slog.Error("Failed to get PostgreSQL instance database", "instance", dbinstance.Name, "database", database.Name, "error", err)
	}

	if dbase == nil {
		operation, err := c.sqlAdminSvc.Databases.Insert(c.ProjectID, dbinstance.Name, database).Context(c.ctx).Do()
		if err != nil {
			slog.Error("Failed to create PostgreSQL instance database", "instance", dbinstance.Name, "database", database.Name, "error", err)
			return nil, err
		}

		// Wait for the operation to complete
		if err := c.WaitForSQLOperation(time.Second*10, operation); err != nil {
			slog.Error("Failed to create PostgreSQL instance database", "instance", dbinstance.Name, "database", database.Name, "error", err)
			return nil, err
		}
		slog.Info("Successfully created PostgreSQL instance database", "instance", dbinstance.Name, "database", database.Name)
	}

	reader, err := c.storageSvc.Bucket(backLocation.Bucket).Object(backLocation.UserLocation()).NewReader(c.ctx)
	if err != nil {
		slog.Error("Failed to open file", "location", backLocation.UserLocation(), "error", err)
		return nil, err
	}
	defer reader.Close()

	// Read file contents into a string
	data, err := io.ReadAll(reader)
	if err != nil {
		slog.Error("Failed to read content from file", "location", backLocation.UserLocation(), "error", err)
		return nil, fmt.Errorf("failed to read file contents: %v", err)
	}

	// Split file contents into an array of strings (assuming lines are separated by newlines)
	users := strings.Split(string(data), "\n")

	for _, user := range users {
		sqlUser := &sqladmin.User{
			Name:     user,
			Password: generatePassword(12),
		}
		u, err := c.sqlAdminSvc.Users.Get(c.ProjectID, dbinstance.Name, sqlUser.Name).Context(c.ctx).Do()
		if err != nil && err.(*googleapi.Error).Code != 404 {
			slog.Error("Failed to get user", "user", user, "error", err)
			return nil, err
		}
		if u == nil {
			operation, err := c.sqlAdminSvc.Users.Insert(c.ProjectID, dbinstance.Name, sqlUser).Context(c.ctx).Do()
			if err != nil {
				slog.Error("Failed to create PostgreSQL user", "instance", dbinstance.Name, "database", database.Name, "user", sqlUser.Name, "error", err)
				return nil, err
			}
			// Wait for the operation to complete
			if err := c.WaitForSQLOperation(time.Second*10, operation); err != nil {
				slog.Error("Failed to create PostgreSQL user", "instance", dbinstance.Name, "database", database.Name, "user", sqlUser.Name, "error", err)
				return nil, err
			}
			slog.Info("Successfully created PostgreSQL user", "instance", dbinstance.Name, "database", database.Name, "user", sqlUser.Name)
		}
	}

	sqlAdminSvcAccount, err := c.GetSvcAcctForCloudSQLInstance(fmt.Sprintf("restore-%s", opts.Instance), "")
	if err != nil {
		slog.Error("Failed to get service account for instance", "instance", fmt.Sprintf("restore-%s", opts.Instance), "error", err)
		return nil, err
	}

	defer func() {
		err = c.RemoveRoleBindingToGCSBucket(opts.Bucket, "roles/storage.legacyBucketReader", sqlAdminSvcAccount, opts.Instance)
		if err != nil {
			slog.Error("Failed to remove role binding roles/storage.legacyBucketReader", "service_account", sqlAdminSvcAccount, "error", err)
		}
		err = c.AddRoleBindingToGCSBucket(opts.Bucket, "roles/storage.objectViewer", sqlAdminSvcAccount, opts.Instance)
		if err != nil {
			slog.Error("Failed to remove role binding roles/storage.objectViewer", "service_account", sqlAdminSvcAccount, "error", err)
		}
	}()

	err = c.AddRoleBindingToGCSBucket(opts.Bucket, "roles/storage.legacyBucketReader", sqlAdminSvcAccount, opts.Instance)
	if err != nil {
		slog.Error("Failed to add role binding roles/storage.legacyBucketReader", "service_account", sqlAdminSvcAccount, "error", err)
		return nil, err
	}
	err = c.AddRoleBindingToGCSBucket(opts.Bucket, "roles/storage.objectViewer", sqlAdminSvcAccount, opts.Instance)
	if err != nil {
		slog.Error("Failed to add role binding roles/storage.objectViewer", "service_account", sqlAdminSvcAccount, "error", err)
		return nil, err
	}

	slog.Info("Import data", "instance", dbinstance.Name, "file", opts.File)
	// Import data from SQL file
	importReq := &sqladmin.InstancesImportRequest{
		ImportContext: &sqladmin.ImportContext{
			Kind:       "sql#importContext",
			Database:   database.Name,
			FileType:   "SQL",
			ImportUser: opts.User,
			Uri:        opts.File, // You can also use local file path here
			//TODO check what bak import and export is capable of
			// BakImportOptions: &sqladmin.ImportContextBakImportOptions{

			// },
		},
	}

	importOp, err := c.sqlAdminSvc.Instances.Import(c.ProjectID, fmt.Sprintf("restore-%s", opts.Instance), importReq).Context(c.ctx).Do()
	if err != nil {
		slog.Error("Failed to import data", "file", opts.File, "error", err)
		return nil, err
	}

	// Wait for the import operation to complete
	if err := c.WaitForSQLOperation(time.Minute*1, importOp); err != nil {
		slog.Error("Failed to import data", "error", err)
		return nil, err
	}

	slog.Info("Data imported successfully", "instance", dbinstance.Name, "file", opts.File)

	//TODO make the system user be configurable
	stats, err := c.GetCloudSQLStatistic(dbinstance.Name, "postgres", password, database.Name)

	statsBackup := make(map[string]*CloudSQLStatistic)

	object := c.storageSvc.
		Bucket(backLocation.Bucket).
		Object(backLocation.StatsLocation())

	_, err = object.Attrs(c.ctx)
	if err != nil && err != storage.ErrObjectNotExist {
		slog.Error("Failed to retrieve bucket object", "location", backLocation.StatsLocation(), "error", err)
		return nil, err
	}

	//Only check restore integrity when stats yaml file exists. If not, skip the check
	//The stats will be created during the backup process if ExportStats is enabled
	if err != storage.ErrObjectNotExist {
		reader, err = object.NewReader(c.ctx)
		if err != nil {
			slog.Error("Failed to read user file", "location", backLocation.StatsLocation(), "error", err)
			return nil, err
		}
		defer reader.Close()

		err = yaml.NewDecoder(reader).Decode(&statsBackup)
		if err != nil {
			slog.Error("Failed to decode stats", "error", err)
			return nil, err
		}

		var validationErrors []error
		for key, value := range stats {
			if _, ok := statsBackup[key]; !ok {
				slog.Error("Stats not found", "key", key)
				return nil, errors.New("stats not found")
			}
			if value.RowCount != statsBackup[key].RowCount {
				slog.Error("Row count mismatch", "key", key, "value", value.RowCount, "backup", statsBackup[key].RowCount)
				validationErrors = append(validationErrors, fmt.Errorf("row count mismatch key: %s, value: %d, backup: %d", key, value.RowCount, statsBackup[key].RowCount))
			}
		}
		if validationErrors != nil {
			return nil, errors.Join(validationErrors...)
		}
	} else {
		slog.Info("Stats file not found, skipping validation", "location", backLocation.StatsLocation())
	}

	return &dbinstance.Name, nil
}
