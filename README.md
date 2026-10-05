# cloudsql-exporter

cloudsql-exporter automatically exports CloudSQL databases in a given project to a GCS bucket and can restore those SQL dumps.
It supports automatic enumeration of CloudSQL instances and their databases, and can even ensure the correct IAM role bindings are in place for a successful export.

![Demo](demo.svg)

### Why

CloudSQL includes automatic backup functionality, so why might you want to use this?

CloudSQL backups are tied to the CloudSQL instance. So, if the instance itself gets deleted, so do the backups.
Similarly if the GCP project were deleted, the instance and the backups would too.
Exporting your database to a separate Google Cloud Storage bucket, preferrably in another GCP project within another account can provide extra assurance of data retention in these scenarios. Additionally you can have much better control over data retention. It's a good supplement to the built-in backup functionality.

## Usage

```bash
$ cloudsql-exporter --help
usage: cloudsql-backup --bucket=BUCKET --project=PROJECT [<flags>]

Export or restore Cloud SQL databases using Google Cloud Storage

Flags:
  --help                 Show context-sensitive help (also try --help-long and
                         --help-man).
  --bucket=BUCKET        Google Cloud Storage bucket name
  --project=PROJECT      GCP project ID
  --instance=INSTANCE    Cloud SQL instance name, if not specified all within
                         the project will be enumerated
  --ensure-iam-bindings  Ensure that the Cloud SQL service account has the
                         bucket IAM roles needed to export or restore
  --compression          Enable compression for exported SQL files
  --restore              Restore a SQL backup (requires --instance and --database)
  --database=DATABASE    Destination database for restoration; must already exist
  --backup=BACKUP        GCS object name to restore, bypassing the backup selector
  --yes                  Skip written restoration confirmation
```

### Restore a backup

```bash
cloudsql-exporter --restore --bucket my-cloudsql-backups --project my-project \
  --instance my-instance --database my-database
```

Restoration lists `.sql` and `.sql.gz` objects under `PROJECT/INSTANCE/DATABASE/`
in the bucket, ordered by creation time, newest first. Enter the number of the
backup to restore, then type the full `RESTORE gs://BUCKET/OBJECT INTO PROJECT/INSTANCE/DATABASE`
phrase shown in the prompt exactly to confirm that backup and destination.
Invalid input or EOF cancels restoration before any import or IAM change.

Use `--backup` with the full object name within the bucket to skip the selector.
Use `--yes` to skip written confirmation. Each flag bypasses only its own prompt;
combine them for unattended restoration:

```bash
cloudsql-exporter --restore --bucket my-cloudsql-backups --project my-project \
  --instance my-instance --database my-database \
  --backup 'my-project/my-instance/my-database/2026-01-01T00:00:00Z.sql.gz' --yes
```

The destination instance and database must already exist. `--backup` can point
to a dump exported from another project or instance in the same bucket, but must
use the exporter's `PROJECT/INSTANCE/DATABASE/FILE.sql[.gz]` layout and match the
destination database name. Renaming databases during import is not supported. The dump
must be compatible with the destination engine/version. This uses Cloud SQL's
[SQL import API](https://cloud.google.com/sql/docs/mysql/import-export/import-export-sql),
so SQL statements may overwrite data or select another database regardless of
`--database`; review the dump and take a fresh backup before restoring.
Database users and instance configuration are not restored.
For PostgreSQL, restore into a new, empty database with the original name;
existing objects can cause the import to fail after partially applying the dump.
Imports are not guaranteed to be atomic for either engine.

The CLI waits until the Cloud SQL operation completes and reports SQL errors.
It logs the operation ID as soon as the import is submitted. If you interrupt
the CLI or polling fails, the server operation may continue: check it with
`gcloud sql operations describe OPERATION_ID --project PROJECT` before retrying
an import. The CLI does not cancel or roll back the server operation.

The caller needs Cloud SQL import/get permissions and GCS object read permissions
(also list permissions when using the selector). The destination instance's service
account needs read access to the dump. `--ensure-iam-bindings` grants that service
account `roles/storage.objectViewer` on the bucket after confirmation; the caller
must also be allowed to read and update the bucket IAM policy.
That grant persists and covers all objects in the bucket. IAM changes can take
time to propagate; if the initial import is denied, allow propagation and check
the operation status before retrying.

## Installation
### 1. Compile with Go

```
go install github.com/trufflesecurity/cloudsql-exporter
```

### 2. [Release binaries](https://github.com/trufflesecurity/cloudsql-exporter/releases)

### 3. Docker

> Note: Apple M1 hardware users should run with `docker run --platform linux/arm64` for better performance.

#### **Most users**

```bash
docker run -v "$HOME/.config/gcloud/application_default_credentials.json:/gcloud.json" -e GOOGLE_APPLICATION_CREDENTIALS=/gcloud.json trufflesecurity/cloudsql-exporter:latest --bucket my-cloudsql-backups --project my-project  --ensure-iam-bindings
```

#### **Apple M1 users**

The `linux/arm64` image is better to run on the M1 than the amd64 image.
Even better is running the native darwin binary avilable, but there is not container image for that.

```bash
docker run --platform linux/arm64 -v "$HOME/.config/gcloud/application_default_credentials.json:/gcloud.json" -e GOOGLE_APPLICATION_CREDENTIALS=/gcloud.json trufflesecurity/cloudsql-exporter:latest --bucket my-cloudsql-backups --project my-project  --ensure-iam-bindings
```

### 4. Brew

```bash
brew tap trufflesecurity/cloudsql-exporter
brew install cloudsql-exporter
```

## Todo (help wanted!)

- Provide a terraform module for [running in Cloud Run on a schedule](https://cloud.google.com/run/docs/triggering/using-scheduler)
