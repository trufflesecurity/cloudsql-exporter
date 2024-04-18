package restore

import (
	"fmt"

	"github.com/fr12k/cloudsql-exporter/cmd"
	"github.com/fr12k/cloudsql-exporter/pkg/cloudsql"
	"github.com/fr12k/cloudsql-exporter/pkg/restore"

	"github.com/spf13/cobra"
)

type RestoreOptions struct {
	File     string
}

var restoreOpts = &RestoreOptions{}

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "This export data from Cloud SQL instance to a GCS bucket.",
	Long: `This export data from Cloud SQL instance to a GCS bucket.`,
	RunE: execute,
}

func init() {
	cmd.RootCmd.AddCommand(restoreCmd)
	cmd.AddRequiredFlag(restoreCmd, &restoreOpts.File, "file", "The bucket file to restore the cloudsql instance from")
}

func execute(ccmd *cobra.Command, args []string) error {
	fmt.Printf("Restore command %v+", restoreOpts)

	bucket := GetString(ccmd, "bucket")
	project := GetString(ccmd, "project")
	instance := GetString(ccmd, "instance")
	user := GetString(ccmd, "user")

	fmt.Printf("Restore command %v %v %v %v", bucket, project, instance, user)

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
