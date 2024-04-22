package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDbFromFileName(t *testing.T) {
	test := []struct {
		file     string
		expected string
	}{
		{
			file:     "gs://flink-backup-bucket-flink-platform-staging/dc-stock-level-service/cloudsql/dc-stock-level-service-20240404T152957.sql.gz",
			expected: "dc-stock-level-service",
		},
		{
			file:     "gs://flink-backup-bucket-flink-platform-staging/pricing/cloudsql/pricing-20240404T152957.sql.gz",
			expected: "pricing",
		},
	}

	for _, tt := range test {
		assert.Equal(t, tt.expected, databaseFromFile(tt.file))
	}
}

func TestFileTemplate(t *testing.T) {
	test := []struct {
		file     string
		expected Location
	}{
		{
			file: "gs://flink-backup-bucket-flink-platform-staging/dc-stock-level-service/cloudsql/dc-stock-level-service-20240404T152957.sql.gz",
			expected: Location{
				Bucket:   "flink-backup-bucket-flink-platform-staging",
				Path:     "dc-stock-level-service/cloudsql/",
				Time:     "20240404T152957",
				Database: "dc-stock-level-service",
			},
		},
		{
			file: "gs://flink-backup-bucket-flink-platform-staging/pricing/cloudsql/pricing-20240404T152957.sql.gz",
			expected: Location{
				Bucket:   "flink-backup-bucket-flink-platform-staging",
				Path:     "pricing/cloudsql/",
				Time:     "20240404T152957",
				Database: "pricing",
			},
		},
	}

	for _, tt := range test {
		assert.Equal(t, tt.expected, NewLocation(tt.file))
	}
}

func TestUserLocation(t *testing.T) {
	loc := Location{
		Bucket:   "flink-backup-bucket-flink-platform-staging",
		Path:     "pricing/cloudsql/",
		Time:     "20240404T152957",
		Database: "pricing",
	}

	assert.Equal(t, "pricing/cloudsql/users-20240404T152957.txt", loc.UserLocation())
}

func TestStatsLocation(t *testing.T) {
	loc := Location{
		Bucket:   "flink-backup-bucket-flink-platform-staging",
		Path:     "pricing/cloudsql/",
		Time:     "20240404T152957",
		Database: "pricing",
	}

	assert.Equal(t, "pricing/cloudsql/stats-pricing-20240404T152957.yaml", loc.StatsLocation())
}
