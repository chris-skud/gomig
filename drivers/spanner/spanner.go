package spanner

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strings"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/chris-skud/gomig/driver"
	"github.com/chris-skud/gomig/file"
	"google.golang.org/api/iterator"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

func init() {
	driver.Register("spanner", "spanner", nil, Open)
}

// DefaultMigrationsTable is used if no custom table is specified
const DefaultMigrationsTable = "SchemaMigrations"

// Driver errors
var (
	ErrNilConfig      = fmt.Errorf("no config")
	ErrNoDatabaseName = fmt.Errorf("no database name")
	ErrNoSchema       = fmt.Errorf("no schema")
	ErrDatabaseDirty  = fmt.Errorf("database is dirty")
	ErrNotImplemented = fmt.Errorf("operation not supported")
)

// Config used for a Spanner instance
type Config struct {
	MigrationsTable string
	DatabaseName    string
}

// Spanner implements database.Driver for Google Cloud Spanner
type Spanner struct {
	db *DB

	config *Config
}

type DB struct {
	admin *database.DatabaseAdminClient
	data  *spanner.Client
}

func Open(migrateURL string) (driver.Driver, error) {
	purl, err := url.Parse(migrateURL)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return nil, err
	}
	dbname := strings.Replace(purl.String(), "spanner://", "", 1)
	dataClient, err := spanner.NewClient(ctx, dbname)
	if err != nil {
		log.Fatal(err)
	}

	migrationsTable := purl.Query().Get("x-migrations-table")
	if len(migrationsTable) == 0 {
		migrationsTable = DefaultMigrationsTable
	}

	db := &DB{admin: adminClient, data: dataClient}
	driver := &Spanner{db: db, config: &Config{
		DatabaseName:    dbname,
		MigrationsTable: migrationsTable,
	}}

	return driver, nil
}

func (s *Spanner) Close() error {
	s.db.data.Close()
	return s.db.admin.Close()
}

// Migrate is the heart of the driver.
// It will receive a file which the driver should apply
// to its backend or whatever. The migration function should use
// the pipe channel to return any errors or other useful information.
func (s *Spanner) Migrate(file file.File) error {
	err := file.ReadContent()

	// run migration
	stmts := migrationStatements(file.Content)
	ctx := context.Background()

	fmt.Printf("stmts: %s", string(stmts[0]))
	fmt.Println("s.config.DatabaseName ", s.config.DatabaseName)
	fmt.Println("s.config.DatabaseName ", s.config.DatabaseName)

	op, err := s.db.admin.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   s.config.DatabaseName,
		Statements: stmts,
	})
	if err != nil {
		fmt.Println("error 1: ", err.Error())
		return err
	}
	if err := op.Wait(ctx); err != nil {
		fmt.Println("op.Wait err ", err.Error())
		return err
	}

	return nil
}

// Version returns the current migration version.
func (s *Spanner) Version() (file.Version, error) {
	var version file.Version
	ctx := context.Background()
	// iter := s.db.data.ReadOnlyTransaction().Read(ctx, tableName, spanner.AllKeys, "version")
	// row, err := iter.Next()
	stmt := spanner.NewStatement("SELECT version FROM " + DefaultMigrationsTable + " ORDER BY version DESC LIMIT 1")
	iter := s.db.data.Single().Query(ctx, stmt)

	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return version, nil
		}
		if err != nil {
			return version, err
		}

		err = row.ToStruct(&version)
		if err != nil {
			return version, err
		}
	}
}

// Versions returns the list of applied migrations.
func (s *Spanner) Versions() (file.Versions, error) {
	var versions file.Versions
	ctx := context.Background()

	iter := s.db.data.Single().Read(ctx, "Albums", spanner.AllKeys(), []string{"version"})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return versions, nil
		}
		if err != nil {
			return versions, nil
		}

		var version file.Version
		err = row.ToStruct(&version)
		if err != nil {
			return versions, err
		}

		versions = append(versions, version)
	}
}

// Execute a statement
func (s *Spanner) Execute(statement string) error {
	return ErrNotImplemented
}

func migrationStatements(migration []byte) []string {
	regex := regexp.MustCompile(";$")
	migrationString := string(migration[:])
	migrationString = strings.TrimSpace(migrationString)
	migrationString = regex.ReplaceAllString(migrationString, "")

	statements := strings.Split(migrationString, ";")
	return statements
}
