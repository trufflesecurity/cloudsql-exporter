package restore

import (
	"github.com/fr12k/cloudsql-exporter/cmd"
	"github.com/fr12k/cloudsql-exporter/pkg/cloudsql"
	"github.com/fr12k/cloudsql-exporter/pkg/restore"

	"github.com/spf13/cobra"
)

type RestoreOptions struct {
	File string
}

var restoreOpts = &RestoreOptions{}

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Example: "cloudsql-exporter restore --bucket=database-backup-bucket --project=f**********g --instance=db-instance-to-backup  --user ******** --file gs://database-backup-bucket/db-instance-to-backup/cloudsql/dbname-20240422T173358.sql.gz",
	Short: "This import data from a GCS bucket to Cloud SQL instance.",
	Long:  `This import data from a GCS bucket to Cloud SQL instance.`,
	RunE:  execute,
}

func init() {
	cmd.RootCmd.AddCommand(restoreCmd)
	cmd.AddRequiredFlag(restoreCmd, &restoreOpts.File, "file", "The full location of the file to restore cloudsql instance from. (required)")
}

func execute(ccmd *cobra.Command, args []string) error {
	bucket := GetString(ccmd, "bucket")
	project := GetString(ccmd, "project")
	instance := GetString(ccmd, "instance")
	user := GetString(ccmd, "user")

	opts := &cloudsql.RestoreOptions{
		Bucket:   bucket,
		Project:  project,
		Instance: instance,
		File:     restoreOpts.File,
		User:     user,
	}

	_, err := restore.Restore(opts)
	if err != nil {
		return err
	}

	return nil
}

func GetString(ccmd *cobra.Command, name string) string {
	bucket, err := ccmd.Flags().GetString(name)
	if err != nil {
		panic(err)
	}
	return bucket
}
