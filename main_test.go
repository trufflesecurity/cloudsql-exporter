package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/trufflesecurity/cloudsql-exporter/pkg/cloudsql"
	"google.golang.org/api/option"
	"google.golang.org/api/sqladmin/v1"
	"google.golang.org/api/storage/v1"
)

func TestValidateOptions(t *testing.T) {
	savedFormat, savedInstance, savedDatabase, savedBackup := *fileType, *instance, *database, *backup
	savedRestore, savedCompression, savedYes := *restore, *compression, *yes
	t.Cleanup(func() {
		*fileType, *instance, *database, *backup = savedFormat, savedInstance, savedDatabase, savedBackup
		*restore, *compression, *yes = savedRestore, savedCompression, savedYes
	})
	for _, tc := range []struct {
		name, format, inst, db, object, wantError string
		restoring, compressed, skipConfirmation   bool
	}{
		{name: "default SQL", format: "SQL"},
		{name: "case-insensitive SQL", format: "sQl", compressed: true},
		{name: "case-insensitive BAK", format: "bak"},
		{name: "CSV needs a query", format: "CSV", wantError: "--fileType must be SQL or BAK"},
		{name: "unspecified format", format: "SQL_FILE_TYPE_UNSPECIFIED", wantError: "--fileType must be SQL or BAK"},
		{name: "invalid format", format: "../backup", wantError: "--fileType must be SQL or BAK"},
		{name: "empty format", wantError: "--fileType must be SQL or BAK"},
		{name: "BAK is not gzip", format: "BAK", compressed: true, wantError: "--compression is supported only for SQL exports"},
		{name: "SQL restore", format: "SQL", restoring: true, inst: "inst", db: "db"},
		{name: "missing restore instance", format: "SQL", restoring: true, db: "db", wantError: "--restore requires --instance and --database"},
		{name: "missing restore database", format: "SQL", restoring: true, inst: "inst", wantError: "--restore requires --instance and --database"},
		{name: "BAK restore is unsupported", format: "BAK", restoring: true, inst: "inst", db: "db", wantError: "apply only to exports"},
		{name: "restore with compression", format: "SQL", restoring: true, compressed: true, inst: "inst", db: "db", wantError: "apply only to exports"},
		{name: "export with restore backup", format: "SQL", object: "backup.sql", wantError: "require --restore"},
		{name: "export with restore database", format: "SQL", db: "db", wantError: "require --restore"},
		{name: "export with yes", format: "SQL", skipConfirmation: true, wantError: "require --restore"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			*fileType, *instance, *database, *backup = tc.format, tc.inst, tc.db, tc.object
			*restore, *compression, *yes = tc.restoring, tc.compressed, tc.skipConfirmation
			err := validateOptions()
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("validation error = %v, want %q", err, tc.wantError)
				}
			} else if err != nil || *fileType != strings.ToUpper(tc.format) {
				t.Fatalf("format = %q, error = %v", *fileType, err)
			}
		})
	}
}

func TestExportFileTypes(t *testing.T) {
	for _, format := range []string{"SQL", "BAK"} {
		t.Run(format, func(t *testing.T) {
			var exported []string
			object := "backup." + strings.ToLower(format)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" || r.URL.Path != "/v1/projects/proj/instances/inst/export" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
				}
				var req sqladmin.InstancesExportRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
				}
				if req.ExportContext == nil || len(req.ExportContext.Databases) != 1 {
					t.Error("export must name exactly one database")
					http.Error(w, "invalid export", http.StatusBadRequest)
					return
				}
				db := req.ExportContext.Databases[0]
				exported = append(exported, db)
				if req.ExportContext.FileType != format || req.ExportContext.Uri != "gs://backups/proj/inst/"+db+"/"+object {
					t.Errorf("wrong export context: %+v", req.ExportContext)
				}
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(sqladmin.Operation{Name: "export-op", Status: "DONE"}); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			svc, err := sqladmin.NewService(context.Background(), option.WithEndpoint(server.URL+"/"), option.WithoutAuthentication())
			if err != nil {
				t.Fatal(err)
			}
			databases := []string{"master", "model", "MSDB", "tempdb", "app"}
			if err := cloudsql.ExportCloudSQLDatabase(context.Background(), svc, databases, "proj", "inst", "backups", object, format); err != nil {
				t.Fatal(err)
			}
			want := strings.Join(databases, ",")
			if format == "BAK" {
				want = "app"
			}
			if strings.Join(exported, ",") != want {
				t.Fatalf("exported databases = %v, want %s", exported, want)
			}
		})
	}
}

func TestRestoreDatabase(t *testing.T) {
	savedBucket, savedProject, savedInstance, savedDatabase := *bucket, *project, *instance, *database
	savedBackup, savedYes, savedIAM := *backup, *yes, *ensureIamBindings
	t.Cleanup(func() {
		*bucket, *project, *instance, *database = savedBucket, savedProject, savedInstance, savedDatabase
		*backup, *yes, *ensureIamBindings = savedBackup, savedYes, savedIAM
	})
	// Names deliberately sort opposite to creation time.
	const oldBackup = "proj/inst/db/2026-02-01.sql"
	const newBackup = "proj/inst/db/2026-01-01.sql.gz"
	const confirmation = "RESTORE gs://backups/" + newBackup + " INTO proj/inst/db\n"
	for _, tc := range []struct {
		name, object, input, failure, wantObject string
		yes, iam, wantImport                     bool
	}{
		{name: "selector and confirmation", input: "2\nRESTORE gs://backups/" + oldBackup + " INTO proj/inst/db\n", wantObject: oldBackup, wantImport: true},
		{name: "explicit backup still confirms", object: newBackup, input: confirmation, wantObject: newBackup, wantImport: true},
		{name: "both bypass flags", object: newBackup, yes: true, wantObject: newBackup, wantImport: true},
		{name: "yes still selects", yes: true, input: "1\n", wantObject: newBackup, wantImport: true},
		{name: "IAM after confirmation", object: newBackup, input: confirmation, iam: true, wantObject: newBackup, wantImport: true},
		{name: "wrong confirmation", input: "1\nyes\n", iam: true},
		{name: "wrong destination confirmation", object: newBackup, input: "RESTORE proj/other/db\n"},
		{name: "wrong backup confirmation", object: newBackup, input: "RESTORE gs://backups/" + oldBackup + " INTO proj/inst/db\n"},
		{name: "CRLF prompts", input: "1\r\n" + strings.ReplaceAll(confirmation, "\n", "\r\n"), wantObject: newBackup, wantImport: true},
		{name: "source database mismatch", object: "proj/inst/other/backup.sql", yes: true},
		{name: "nested backup", object: "proj/inst/db/nested/backup.sql", yes: true},
		{name: "another source instance", object: "other-project/other-instance/db/backup.sql", yes: true, wantObject: "other-project/other-instance/db/backup.sql", wantImport: true},
		{name: "confirmation EOF", object: newBackup},
		{name: "unterminated confirmation", object: newBackup, input: strings.TrimSuffix(confirmation, "\n")},
		{name: "selection EOF"},
		{name: "zero selection", input: "0\n"},
		{name: "negative selection", input: "-1\n"},
		{name: "out of range selection", input: "3\n"},
		{name: "invalid selection", input: "latest\n"},
		{name: "not a SQL backup", object: "backup.csv", yes: true},
		{name: "URI instead of object name", object: "gs://backups/backup.sql", yes: true},
		{name: "no backups", failure: "empty", input: "1\n"},
		{name: "listing fails", failure: "list", input: "1\n"},
		{name: "missing backup", object: newBackup, yes: true, failure: "object"},
		{name: "missing database", object: newBackup, yes: true, failure: "database"},
		{name: "import API failure", object: newBackup, yes: true, failure: "import"},
		{name: "completed operation failure", object: newBackup, yes: true, failure: "operation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encode := func(w http.ResponseWriter, value interface{}) {
				t.Helper()
				if err := json.NewEncoder(w).Encode(value); err != nil {
					t.Error(err)
				}
			}
			*bucket, *project, *instance, *database = "backups", "proj", "inst", "db"
			*backup, *yes, *ensureIamBindings = tc.object, tc.yes, tc.iam
			imports, iamWrites, lists := 0, 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == "GET" && r.URL.Path == "/b/backups/o":
					lists++
					if r.URL.Query().Get("prefix") != "proj/inst/db/" || r.URL.Query().Get("delimiter") != "/" {
						t.Errorf("wrong backup prefix: %s", r.URL.RawQuery)
					}
					if tc.failure == "list" {
						http.Error(w, "listing failed", http.StatusForbidden)
					} else if tc.failure == "empty" {
						encode(w, storage.Objects{})
					} else if r.URL.Query().Get("pageToken") == "next" {
						encode(w, storage.Objects{Items: []*storage.Object{{Name: newBackup, TimeCreated: "2026-01-01T00:00:00.12Z"}}})
					} else {
						encode(w, storage.Objects{NextPageToken: "next", Items: []*storage.Object{{Name: oldBackup, TimeCreated: "2026-01-01T00:00:00.1Z"}, {Name: "proj/inst/db/ignore.txt"}}})
					}
				case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/b/backups/o/"):
					if tc.failure == "object" {
						http.Error(w, "missing object", http.StatusNotFound)
					} else {
						encode(w, storage.Object{Name: strings.TrimPrefix(r.URL.Path, "/b/backups/o/")})
					}
				case r.Method == "GET" && r.URL.Path == "/v1/projects/proj/instances/inst/databases/db":
					if tc.failure == "database" {
						http.Error(w, "missing database", http.StatusNotFound)
					} else {
						encode(w, sqladmin.Database{Name: "db"})
					}
				case r.Method == "GET" && r.URL.Path == "/v1/projects/proj/instances/inst":
					encode(w, sqladmin.DatabaseInstance{ServiceAccountEmailAddress: "sql@example.com"})
				case r.URL.Path == "/b/backups/iam":
					if r.Method == "PUT" {
						iamWrites++
						var policy storage.Policy
						if err := json.NewDecoder(r.Body).Decode(&policy); err != nil {
							t.Error(err)
						}
						if len(policy.Bindings) != 1 || policy.Bindings[0].Role != "roles/storage.objectViewer" || policy.Bindings[0].Members[0] != "serviceAccount:sql@example.com" {
							t.Errorf("wrong IAM policy: %+v", policy)
						}
					}
					encode(w, storage.Policy{})
				case r.Method == "POST" && r.URL.Path == "/v1/projects/proj/instances/inst/import":
					imports++
					if tc.failure == "import" {
						http.Error(w, "import failed", http.StatusBadRequest)
						return
					}
					var req sqladmin.InstancesImportRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
					}
					if req.ImportContext == nil || req.ImportContext.Database != "db" || req.ImportContext.FileType != "SQL" || req.ImportContext.Uri != "gs://backups/"+tc.wantObject && tc.wantImport {
						t.Errorf("wrong import request: %+v", req.ImportContext)
					}
					op := sqladmin.Operation{Name: "restore-op", Status: "DONE"}
					if tc.failure == "operation" {
						op.Error = &sqladmin.OperationErrors{Errors: []*sqladmin.OperationError{{Code: "ERROR_RDBMS", Message: "SQL failed"}}}
					}
					encode(w, op)
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					http.Error(w, "unexpected request", http.StatusNotFound)
				}
			}))
			defer server.Close()
			ctx := context.Background()
			sqlSvc, err := sqladmin.NewService(ctx, option.WithEndpoint(server.URL+"/"), option.WithoutAuthentication())
			if err != nil {
				t.Fatal(err)
			}
			storageSvc, err := storage.NewService(ctx, option.WithEndpoint(server.URL+"/"), option.WithoutAuthentication())
			if err != nil {
				t.Fatal(err)
			}
			var output bytes.Buffer
			err = restoreDatabase(ctx, sqlSvc, storageSvc, strings.NewReader(tc.input), &output)
			if (err == nil) != tc.wantImport {
				t.Fatalf("restore error = %v, want success = %v", err, tc.wantImport)
			}
			wantRequests := 0
			if tc.wantImport || tc.failure == "import" || tc.failure == "operation" {
				wantRequests = 1
			}
			if imports != wantRequests {
				t.Errorf("imports = %d, want %d", imports, wantRequests)
			}
			if tc.object != "" && lists != 0 {
				t.Error("explicit backup did not bypass listing")
			}
			if tc.yes && strings.Contains(output.String(), "to confirm") {
				t.Error("--yes did not bypass confirmation")
			}
			if tc.iam && tc.wantImport && iamWrites != 1 || !tc.wantImport && iamWrites != 0 {
				t.Errorf("unexpected IAM writes: %d", iamWrites)
			}
		})
	}
}

func TestOperationWait(t *testing.T) {
	if err := cloudsql.WaitForSQLOperation(context.Background(), nil, time.Second, "proj", nil); err == nil {
		t.Fatal("nil operation succeeded")
	}
	if err := cloudsql.WaitForSQLOperation(context.Background(), nil, time.Millisecond, "proj", &sqladmin.Operation{Name: "op", Status: "RUNNING"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cloudsql.WaitForSQLOperation(ctx, nil, time.Second, "proj", &sqladmin.Operation{Name: "op", Status: "RUNNING"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error = %v", err)
	}
}

func TestOperationPolling(t *testing.T) {
	for _, scenario := range []string{"success", "SQL failure", "poll failure"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/projects/proj/operations/op" {
					t.Errorf("wrong operation path: %s", r.URL.Path)
				}
				w.Header().Set("Content-Type", "application/json")
				if scenario == "poll failure" {
					http.Error(w, "unavailable", http.StatusServiceUnavailable)
					return
				}
				op := sqladmin.Operation{Name: "op", Status: "DONE"}
				if scenario == "SQL failure" {
					op.Error = &sqladmin.OperationErrors{Errors: []*sqladmin.OperationError{{Code: "ERROR_RDBMS", Message: "SQL failed"}}}
				}
				if err := json.NewEncoder(w).Encode(op); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			svc, err := sqladmin.NewService(context.Background(), option.WithEndpoint(server.URL+"/"), option.WithoutAuthentication())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			err = cloudsql.WaitForSQLOperation(ctx, svc, 0, "proj", &sqladmin.Operation{Name: "op", Status: "RUNNING"})
			if scenario == "success" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "operation op") {
				t.Fatalf("failure did not name the operation: %v", err)
			} else if scenario == "poll failure" && !strings.Contains(err.Error(), "may still be running") {
				t.Fatalf("polling error did not explain server state: %v", err)
			}
		})
	}
}
