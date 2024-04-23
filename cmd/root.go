package cmd

import (
	"github.com/fr12k/cloudsql-exporter/pkg/version"
	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "cloudsql-exporter",
	Short: "This is tool to export/import data from/to Cloud SQL instances.",
	Long:  `This is tool to export/import data from/to Cloud SQL instances.`,

	SilenceUsage:  true,
	SilenceErrors: true,
}

func init() {
	AddRequiredPersistentFlagShort(RootCmd, "bucket", "b", "The GCP bucket name to export/import data to.")
	AddRequiredPersistentFlagShort(RootCmd, "project", "p", "The GCP project name that contains the Cloud SQL instance.")
	AddRequiredPersistentFlagShort(RootCmd, "instance", "i", "The GCP Cloud SQL instance name to export/import data from.")
	RootCmd.PersistentFlags().String("user", "", "The Cloud SQL user to connect to the database.")

	RootCmd.Version = version.BuildVersion
}

func AddRequiredPersistentFlagShort(ccmd *cobra.Command, name, shorthand, usage string) {
	ccmd.PersistentFlags().StringP(name, shorthand, "", usage)
	err := ccmd.MarkPersistentFlagRequired(name)
	if err != nil {
		panic(err)
	}
}

func AddRequiredFlag(ccmd *cobra.Command, ref *string, name, usage string) {
	ccmd.Flags().StringVar(ref, name, "", usage)
	err := ccmd.MarkFlagRequired(name)
	if err != nil {
		panic(err)
	}
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() error {
	return RootCmd.Execute()
}
