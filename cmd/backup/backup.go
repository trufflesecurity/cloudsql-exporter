package backup

import (
	"fmt"

	"github.com/fr12k/cloudsql-exporter/cmd"
	"github.com/fr12k/cloudsql-exporter/pkg/backup"

	"github.com/spf13/cobra"
)

type BackupOptions struct {
	ExportStats bool
	Password    string

	Compression           bool
	EnsureIamBindings     bool
	EnsureIamBindingsTemp bool
}

var backupOpts = &BackupOptions{}

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "This export data from Cloud SQL instance to a GCS bucket.",
	Long:  `This export data from Cloud SQL instance to a GCS bucket.`,
	RunE:  execute,
}

func init() {
	cmd.RootCmd.AddCommand(backupCmd)

	backupCmd.Flags().BoolVar(&backupOpts.ExportStats, "stats", false, "Extract tables statistics to be able to validate restored data integrity")
	backupCmd.Flags().StringVar(&backupOpts.Password, "password", "", "Cloud SQL password for the user to connect to the database to export tables statistics")
	backupCmd.MarkFlagsRequiredTogether("stats", "password")

	backupCmd.Flags().BoolVar(&backupOpts.Compression, "compression", false, "Enable compression for the backup file")
	backupCmd.Flags().BoolVar(&backupOpts.EnsureIamBindings, "ensure-iam-bindings", false, "Ensure permanent IAM bindings for the Cloud SQL instance")
	backupCmd.Flags().BoolVar(&backupOpts.EnsureIamBindingsTemp, "ensure-iam-bindings-temp", false, "Ensure IAM bindings temp for the Cloud SQL instance temp ")
}

func execute(ccmd *cobra.Command, args []string) error {
	fmt.Printf("Backup command %v+", backupOpts)
	bucket := GetString(ccmd, "bucket")
	project := GetString(ccmd, "project")
	instance := GetString(ccmd, "instance")
	user := GetString(ccmd, "user")

	fmt.Printf("Backup command %v %v %v %v", bucket, project, instance, user)

	opts := backup.BackupOptions{
		Bucket:   bucket,
		Project:  project,
		Instance: instance,
		User:     user,

		ExportStats: true,
		Password:    backupOpts.Password,

		Compression:           backupOpts.Compression,
		EnsureIamBindings:     backupOpts.EnsureIamBindings,
		EnsureIamBindingsTemp: backupOpts.EnsureIamBindingsTemp,
	}

	locations, err := backup.Backup(&opts)
	if err != nil {
		return err
	}

	fmt.Printf("Backup locations %v", locations)
	return nil
}

func GetString(ccmd *cobra.Command, name string) string {
	bucket, err := ccmd.Flags().GetString(name)
	if err != nil {
		panic(err)
	}
	return bucket
}
