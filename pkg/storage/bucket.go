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
	Bucket   string
	Database string
	Path string
	Time     string
}

func (b Location) UserLocation() string {
	return fmt.Sprintf("%susers-%s.txt", b.Path, b.Time)
}

func (b Location) StatsLocation() string {
	return fmt.Sprintf("%sstats-%s-%s.yaml", b.Path, b.Database, b.Time)
}

//NewLocation parse the location metadata from the file path.
//Valid file path should be in the format of gs://flink-backup-bucket-flink-platform-staging/payment-events/cloudsql/payment-service-20240417T150207.sql.gz
func NewLocation(file string) Location {
	bucket := strings.Split(file, "/")[2]
	ss := strings.Split(filepath.Dir(file), "/")
	path := strings.Join(ss[2:], "/") + "/"
	ss = strings.Split(filepath.Base(file), "-")
	time := strings.Split(ss[len(ss)-1],".")[0]
	database := databaseFromFile(file)
	return Location{
		Bucket:   bucket,
		Path: path,
		Database: database,
		Time:     time,
	}
}
