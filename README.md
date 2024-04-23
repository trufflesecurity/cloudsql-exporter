# cloudsql-exporter

cloudsql-exporter automatically exports CloudSQL databases in a given project to a GCS bucket.
It supports automatic enumeration of CloudSQL instances and their databases, and can even ensure the correct IAM role bindings are in place for a successful export.

![Demo](./docs/images/backup.gif)

### Why

CloudSQL includes automatic backup functionality, so why might you want to use this?

CloudSQL backups are tied to the CloudSQL instance. So, if the instance itself gets deleted, so do the backups.
Similarly if the GCP project were deleted, the instance and the backups would too.
Exporting your database to a separate Google Cloud Storage bucket, preferably in another GCP project within another account can provide extra assurance of data retention in these scenarios. Additionally you can have much better control over data retention. It's a good supplement to the built-in backup functionality.

## Usage

```bash
$ cloudsql-exporter --help
This is tool to export/import data from/to Cloud SQL instances.

Usage:
  cloudsql-exporter [command]

Available Commands:
  backup      This export data from Cloud SQL instance to a GCS bucket.
  completion  Generate the autocompletion script for the specified shell
  help        Help about any command
  restore     This import data from a GCS bucket to Cloud SQL instance.

Flags:
  -b, --bucket string     GCS bucket name
  -h, --help              help for cloudsql-exporter
  -i, --instance string   Cloud SQL instance name
  -p, --project string    GCP project name
      --user string       Cloud SQL user
  -v, --version           version for cloudsql-exporter

Use "cloudsql-exporter [command] --help" for more information about a command.
```

### Backup

Example usage:

```bash
cloudsql-exporter backup --bucket=database-backup-bucket --project=f**********g --instance=db-instance-to-backup \
  --ensure-iam-bindings-temp   --compression --user ******** --stats --password ${CLOUDSQL_PASSWORD}
```

![backup](./docs/images/backup.gif)

```bash
cloudsql-exporter backup --help
This export data from Cloud SQL instance to a GCS bucket.

Usage:
  cloudsql-exporter backup [flags]

Flags:
      --compression                Enable gz compression for the exported backup data file. (default: false)
      --ensure-iam-bindings        Ensure needed IAM permission on the target bucket are set for the Cloud SQL instance service account. (default: false)
      --ensure-iam-bindings-temp   Ensure needed IAM permission on the target bucket are set and removed afterwards. (default: false)
  -h, --help                       help for backup
      --password string            Cloud SQL password for the user to connect to the database to export tables statistics. (required if stats flag is set)
      --stats                      Extract tables statistics to be able to validate restored data integrity. (default: false)

Global Flags:
  -b, --bucket string     The GCP bucket name to export/import data to.
  -i, --instance string   The GCP Cloud SQL instance name to export/import data from.
  -p, --project string    The GCP project name that contains the Cloud SQL instance.
      --user string       The Cloud SQL user to connect to the database.
```

### Restore

Example usage:

```bash
cloudsql-exporter restore --bucket=database-backup-bucket --project=f**********g --instance=db-instance-to-backup  --user ******** --file gs://
Usage:
  cloudsql-exporter restore [flags]

Examples:
cloudsql-exporter restore --bucket=database-backup-bucket --project=f**********g --instance=db-instance-to-backup  --user ******** --file gs://database-backup-bucket/db-instance-to-backup/cloudsql/dbname-20240422T173358.sql.gz

Flags:
      --file string   The full location of the file to restore cloudsql instance from. (required)
  -h, --help          help for restore

Global Flags:
  -b, --bucket string     The GCP bucket name to export/import data to.
  -i, --instance string   The GCP Cloud SQL instance name to export/import data from.
  -p, --project string    The GCP project name that contains the Cloud SQL instance.
      --user string       The Cloud SQL user to connect to the database.
```

## Installation

### 1. Compile with Go

```
go install github.com/fr12k/cloudsql-exporter
```

### 2. [Release binaries](https://github.com/fr12k/cloudsql-exporter/releases)
