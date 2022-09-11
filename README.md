# cloudsql-exporter

cloudsql-exporter automatically exports CloudSQL databases in a given project to a GCS bucket.
It supports automatic enumeration of CloudSQL instances and their databases, and can even ensure the correct IAM role bindings are in place for a successful export.

## Usage

```bash
$ cloudsql-exporter --help
usage: cloudsql-backup --bucket=BUCKET --project=PROJECT [<flags>]

Export Cloud SQL databases to Google Cloud Storage

Flags:
  --help                 Show context-sensitive help (also try --help-long and
                         --help-man).
  --bucket=BUCKET        Google Cloud Storage bucket name
  --project=PROJECT      GCP project ID
  --instance=INSTANCE    Cloud SQL instance name, if not specified all within
                         the project will be enumerated
  --ensure-iam-bindings  Ensure that the Cloud SQL service account has the
                         required IAM role binding to export and validate the
                         backup
```

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
