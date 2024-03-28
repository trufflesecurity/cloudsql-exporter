package main

import (
	"github.com/fr12k/cloudsql-exporter/cmd"
)

func main() {
	opts := cmd.NewCommand()
	cmd.Backup(opts)
}
