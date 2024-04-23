package storage

import (
	"fmt"
	"path/filepath"
	"strings"
)

func databaseFromFile(file string) string {
	name := filepath.Base(file)
	ss := strings.Split(name, "-")
	return strings.Join(ss[:len(ss)-1], "-")
}

type Location struct {
	Bucket      string
	Database    string
	Instance    string
	Path        string
	Time        string
	Compression bool
}

func (b Location) UserLocation() string {
	return fmt.Sprintf("%susers-%s.txt", b.Path, b.Time)
}

func (b Location) StatsLocation(database string) string {
	return fmt.Sprintf("%sstats-%s-%s.yaml", b.Path, database, b.Time)
}

func (b Location) DatabaseLocation(database string) string {
	suffix := "sql"
	if b.Compression {
		suffix = "sql.gz"
	}
	return fmt.Sprintf("gs://%s/%s/cloudsql/%s-%s.%s", b.Bucket, b.Instance, database, b.Time, suffix)
}

// NewLocation parse the location metadata from the file path.
// Valid file path should be in the format of gs://flink-backup-bucket-flink-platform-staging/payment-events/cloudsql/payment-service-20240417T150207.sql.gz
func NewLocation(file string) Location {
	bucket := strings.Split(file, "/")[2]
	ss := strings.Split(filepath.Dir(file), "/")
	instance := ss[2]
	path := strings.Join(ss[2:], "/") + "/"
	ss = strings.Split(filepath.Base(file), "-")
	time := strings.Split(ss[len(ss)-1], ".")[0]
	database := databaseFromFile(file)
	return Location{
		Bucket:   bucket,
		Path:     path,
		Instance: instance,
		Database: database,
		Time:     time,
	}
}
