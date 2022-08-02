package testpgx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"

	pgxstdlib "github.com/jackc/pgx/v4/stdlib"
)

type Env struct {
	postgresCid string
	opts        *Options

	dbUser      string
	dbPassword  string
	dbSocketDir string

	canCreateNMoreDbs int
	createDBLock      sync.RWMutex
	dbs               chan *pgx.Conn
}

type Migrator interface {
	Migrate(*sql.DB) error
}

type StepMigrator interface {
	Migrator
	StepUp(*sql.DB) error
	StepDown(*sql.DB) error
}

const (
	testDbHost = "0.0.0.0"
	testDBUser = "postgres"
	testDBPass = "anypassword"

	defaultPostgresImage = "postgres:14.4"
	// Test errors can occur if there are too many DBs created (because we hit memory limits)
	maxNumDbsPerPostgresInstance = 10
)

const docker = "docker"

type Options struct {
	// PostgresDockerImage is the Docker image to use for running PostgreSQL, e.g.
	// 'postgres:14.4'
	PostgresDockerImage string
	// DockerBinaryPath is the path to the local Docker binary. If blank, looks
	// in $PATH.
	DockerBinaryPath string
	Migrator         Migrator
}

type Option func(*Options)

func WithPostgresDockerImage(img string) Option {
	return func(o *Options) {
		o.PostgresDockerImage = img
	}
}

func WithDockerBinaryPath(p string) Option {
	return func(o *Options) {
		o.DockerBinaryPath = p
	}
}

func WithMigrator(m Migrator) Option {
	return func(o *Options) {
		o.Migrator = m
	}
}

func New(ctx context.Context, opts ...Option) (*Env, error) {
	o := &Options{
		PostgresDockerImage: defaultPostgresImage,
	}
	for _, opt := range opts {
		opt(o)
	}

	if o.DockerBinaryPath == "" {
		p, err := exec.LookPath("docker")
		if err != nil {
			return nil, fmt.Errorf("error looking for 'docker' binary in path: %w", err)
		}
		o.DockerBinaryPath = p
	}

	tmpDir, err := ioutil.TempDir("", "testpgx-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create socket temp dir: %w", err)
	}

	// We put the socket in a subdirectory, because the postgres Docker image takes
	// ownership of this directory, and if we own the parent, we can still manage
	// it appropriately.
	socketDir := filepath.Join(tmpDir, "sub")
	if err := os.Chmod(tmpDir, 0766); err != nil {
		return nil, fmt.Errorf("failed to change permissions on socket temp dir: %w", err)
	}

	args := []string{
		"run",
		"--rm",
		"--detach",
		"--env", "POSTGRES_PASSWORD=" + testDBPass,
		"--volume", socketDir + ":/var/run/postgresql",
		o.PostgresDockerImage, "-c", "listen_addresses=",
	}
	cmd := exec.CommandContext(ctx, o.DockerBinaryPath, args...)

	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("while starting postgres: %w", err)
	}

	cID, err := getPostgresContainerID(out)
	if err != nil {
		return nil, fmt.Errorf("when getting postgres cid: %w", err)
	}

	env := &Env{
		postgresCid:       cID,
		dbUser:            testDBUser,
		dbPassword:        testDBPass,
		dbSocketDir:       socketDir,
		opts:              o,
		canCreateNMoreDbs: maxNumDbsPerPostgresInstance,
		dbs:               make(chan *pgx.Conn, maxNumDbsPerPostgresInstance),
	}

	if _, err := env.waitForPostgresToBeReady(ctx, "" /* dbName */); err != nil {
		return nil, fmt.Errorf("waiting for container to be ready: %w", err)
	}

	return env, nil
}

func (e *Env) GetMigratedDB(ctx context.Context, t testing.TB) *pgx.Conn {
	conn, err := e.aquireMigratedDB(ctx)
	if err != nil {
		t.Fatalf("aquiring pool: %v", err)
	}
	t.Cleanup(func() { e.freeConn(conn) })
	return conn
}

func (e *Env) WithMigratedDB(ctx context.Context, fn func(*pgx.Conn) error) error {
	conn, err := e.aquireMigratedDB(ctx)
	if err != nil {
		return fmt.Errorf("aquiring pool: %v", err)
	}
	if err := fn(conn); err != nil {
		return fmt.Errorf("running fn: %w", err)
	}
	e.freeConn(conn)
	return nil
}

type RemoveFunc func() error

func (e *Env) makeTempPasswordFile() (string, RemoveFunc, error) {
	f, err := ioutil.TempFile(e.dbSocketDir, "pgpassfile-*")
	if err != nil {
		return "", nil, fmt.Errorf("failed to create temp password file: %w", err)
	}
	defer f.Close()
	fn := f.Name()

	if _, err := io.WriteString(f, e.dbPassword); err != nil {
		return "", nil, fmt.Errorf("failed to write test DB password to file: %w", err)
	}

	if err := f.Close(); err != nil {
		return "", nil, fmt.Errorf("failed to close test DB password file: %w", err)
	}

	removeFn := func() error { return os.Remove(fn) }
	return fn, removeFn, nil
}

type modification struct {
	name string
	fn   func(string) string
}

type DumpOptions struct {
	modifications []modification
}

func (do *DumpOptions) addOrReplaceModification(newM modification) {
	for _, m := range do.modifications {
		if m.name == newM.name {
			m.fn = newM.fn
			return
		}
	}
	// If we're here, add the modification to the end.
	do.modifications = append(do.modifications, newM)
}

type DumpOption func(*DumpOptions)

func WithHumanReadableSchema() DumpOption {
	return func(do *DumpOptions) {
		ms := []modification{
			{name: "removeOwnershipCommands", fn: removeOwnershipCommands},
			{name: "removeComments", fn: removeComments},
			{name: "removeSchemaMigrationInfo", fn: removeSchemaMigrationInfo},
			{name: "removeNewlineMidAlter", fn: removeNewlineMidAlter},
			{name: "sortByTableAndRemoveNonTableStatements", fn: sortByTableAndRemoveNonTableStatements},
			{name: "removePublicPrefix", fn: removePublicPrefix},
			{name: "removeRepeatedNewlines", fn: removeRepeatedNewlines},
			{name: "removeRepeatedSpaces", fn: removeRepeatedSpaces},
			{name: "addExtraNewlineBeforeCreateTableStatements", fn: addExtraNewlineBeforeCreateTableStatements},
			{name: "indentAtBeginningOfLine", fn: indentAtBeginningOfLine},
			{name: "addDisclaimer", fn: addDisclaimer},
		}
		for _, m := range ms {
			do.addOrReplaceModification(m)
		}
	}
}

func WithCustomDisclaimer(disclaimer string) DumpOption {
	return func(do *DumpOptions) {
		do.addOrReplaceModification(modification{
			name: "addDisclaimer",
			fn:   addCustomDisclaimer(disclaimer),
		})
	}
}

func (e *Env) DumpDatabaseSchema(ctx context.Context, dbName string, opts ...DumpOption) (string, error) {
	do := &DumpOptions{}
	for _, opt := range opts {
		opt(do)
	}
	schema, err := e.dumpDatabaseSchema(ctx, dbName)
	if err != nil {
		return "", err
	}
	return simplifySchema(schema, do.modifications), nil
}

func (e *Env) dumpDatabaseSchema(ctx context.Context, dbName string) (string, error) {
	// passwdFile, remove, err := e.makeTempPasswordFile()
	// defer remove()

	args := []string{
		"exec",
		// "--env", "PGPASSFILE=" + passwdFile,
		"--env", "PGPASSWORD=" + e.dbPassword,
		e.postgresCid,
		"pg_dump",
		"--schema-only",
		"--username", e.dbUser,
	}

	cmd := exec.CommandContext(ctx, e.opts.DockerBinaryPath, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("when getting db schema for %q: %w: %s", dbName, err, string(out))
	}
	return string(out), nil
}

func (e *Env) TearDown(ctx context.Context) error {
	if e != nil && e.postgresCid != "" {
		if err := exec.CommandContext(ctx, e.opts.DockerBinaryPath, "kill", e.postgresCid).Run(); err != nil {
			return err
		}
	}
	return nil
}

func (e *Env) aquireMigratedDB(ctx context.Context) (*pgx.Conn, error) {
	err := e.createMigratedDBIfUnderCap(ctx)
	if err != nil {
		return nil, fmt.Errorf("aquiring db conn: %w", err)
	}
	conn := <-e.dbs
	return conn, nil
}

func (e *Env) createMigratedDBIfUnderCap(ctx context.Context) error {
	e.createDBLock.Lock()
	if e.canCreateNMoreDbs > 0 {
		e.canCreateNMoreDbs--
		e.createDBLock.Unlock()
		newDB, err := e.createMigratedDB(ctx)
		if err != nil {
			return fmt.Errorf("when creating new db under cap: %w", err)
		}
		e.dbs <- newDB
		return nil
	}
	e.createDBLock.Unlock()
	return nil
}

func (e *Env) freeConn(p *pgx.Conn) {
	e.dbs <- p
}

func createTestDBName() string {
	return fmt.Sprintf("test_db_%s", strings.ReplaceAll(uuid.New().String(), "-", ""))
}

func (e *Env) CreateDB(ctx context.Context) (*pgx.Conn, error) {
	testDBName := createTestDBName()
	conn, err := e.createDatabaseAndWaitForReady(ctx, testDBName)
	if err != nil {
		return nil, fmt.Errorf("waiting for database to be ready: %w", err)
	}

	return conn, nil
}

func connToDB(conn *pgx.Conn) *sql.DB {
	return pgxstdlib.OpenDB(*conn.Config())
}

func (e *Env) createMigratedDB(ctx context.Context) (*pgx.Conn, error) {
	conn, err := e.CreateDB(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create DB: %w", err)
	}

	if e.opts.Migrator == nil {
		return nil, errors.New("a migrated DB was requested, but no migrator was given")
	}

	if err := e.opts.Migrator.Migrate(connToDB(conn)); err != nil {
		return nil, fmt.Errorf("an error occurred while applying migrations: %w", err)
	}

	return conn, nil
}

func getPostgresContainerID(upOutput []byte) (string, error) {
	result := strings.TrimSpace(string(upOutput))
	if len(result) != 64 {
		return "", fmt.Errorf("expected cid container ID length 64, got length %d: %q", len(result), result)
	}
	return result, nil
}

func (e *Env) dsn(dbName string) string {
	dsn := fmt.Sprintf("user=%s password=%s host=%s sslmode=disable", e.dbUser, e.dbPassword, e.dbSocketDir)
	if dbName != "" {
		dsn += " dbname=" + dbName
	}
	return dsn
}

func (e *Env) waitForPostgresToBeReady(ctx context.Context, dbName string) (*pgx.Conn, error) {
	var (
		waitFor     = 1 * time.Millisecond
		waitingFor  = 0 * time.Millisecond
		maxWaitTime = 15 * time.Second

		lastErr error
	)

	for waitingFor < maxWaitTime {
		conn, err := pgx.Connect(ctx, e.dsn(dbName))
		if err == nil {
			if err = conn.Ping(ctx); err == nil {
				return conn, nil
			}
		}
		time.Sleep(waitFor)
		waitingFor += waitFor
		waitFor *= 2
		lastErr = err
	}
	return nil, fmt.Errorf("wasn't ready: %w", lastErr)
}

func (e *Env) createDatabaseAndWaitForReady(ctx context.Context, dbName string) (*pgx.Conn, error) {
	conn, err := e.waitForPostgresToBeReady(ctx, "" /* db name */)
	if err != nil {
		return nil, fmt.Errorf("failed to get DB connection: %w", err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s;", dbName)); err != nil {
		return nil, fmt.Errorf("failed to create database %q: %w", dbName, err)
	}
	return conn, nil
}

func simplifySchema(schema string, ms []modification) string {
	for _, m := range ms {
		schema = m.fn(schema)
	}
	return schema
}

func addDisclaimer(in string) string {
	standardDisclaimer := `-- Human Readable Schema

-- This schema is heavily simplified to assist human comprehension it is
-- presented out of order and should not be used for anything other than
-- reference. It is auto-generated by the testpgx package.

`
	return addCustomDisclaimer(standardDisclaimer)(in)
}

func addCustomDisclaimer(disclaimer string) func(string) string {
	return func(in string) string {
		return disclaimer + in
	}
}

func indentAtBeginningOfLine(in string) string {
	r := regexp.MustCompile("\\n ")
	return r.ReplaceAllString(in, "\n    ")
}

func addExtraNewlineBeforeCreateTableStatements(in string) string {
	r := regexp.MustCompile("\\nCREATE TABLE")
	return r.ReplaceAllString(in, "\n\n\nCREATE TABLE")
}

func removeNewlineMidAlter(in string) string {
	r := regexp.MustCompile("([^;,\\()])\\n")
	return r.ReplaceAllString(in, "$1")
}

func removeOwnershipCommands(in string) string {
	r := regexp.MustCompile("ALTER (TABLE|TYPE|FUNCTION|SEQUENCE) [^ ]+ OWNER TO postgres;")
	return r.ReplaceAllString(in, "")
}

func removeComments(in string) string {
	r := regexp.MustCompile("\\n--[^\\n]*")
	return r.ReplaceAllString(in, "")
}

func removeRepeatedSpaces(in string) string {
	r := regexp.MustCompile(" +")
	return r.ReplaceAllString(in, " ")
}

func removeRepeatedNewlines(in string) string {
	r := regexp.MustCompile("\\n+")
	return r.ReplaceAllString(in, "\n")
}

func removeSchemaMigrationInfo(in string) string {
	r := regexp.MustCompile(";[^;]*(schema|applied)_migration[^;]*;")
	return r.ReplaceAllString(in, ";")
}

func removePublicPrefix(in string) string {
	r := regexp.MustCompile("public\\.")
	return r.ReplaceAllString(in, " ")
}

func sortByTableAndRemoveNonTableStatements(in string) string {
	tableRegexp := regexp.MustCompile("public\\.[a-z_]*")
	statements := strings.Split(in, ";")

	tableStatements := make(map[string][]string)
	for _, statement := range statements {
		match := tableRegexp.FindString(statement)
		if match != "" {
			if _, ok := tableStatements[match]; !ok {
				tableStatements[match] = []string{}
			}
			if strings.Contains(statement, "CREATE TABLE") {
				statement = sortCreateTableStatement(statement)
			}
			tableStatements[match] = append(tableStatements[match], statement)
		}
	}
	result := ""
	for _, statement := range statements {
		match := tableRegexp.FindString(statement)
		if match != "" {
			if tss, ok := tableStatements[match]; ok {
				for _, ts := range tss {
					result += ts + ";"
				}
				delete(tableStatements, match)
			}
		}
		// Uncomment this to preserve non-table statements
		// else { result += statement + ";" }
	}
	return result
}

func sortCreateTableStatement(in string) string {
	r := regexp.MustCompile(`CREATE TABLE ([a-z_.]+)\s*\(((\n|.*)*)\)`)
	matches := r.FindStringSubmatch(in)
	if len(matches) == 0 {
		panic(fmt.Errorf("statement didn't match regex: %q", in))
	}
	columns := strings.Split(matches[2], ",\n")
	for i, c := range columns {
		columns[i] = strings.TrimSpace(c)
	}
	sort.Strings(columns)
	out := fmt.Sprintf("\n\nCREATE TABLE %s (\n\t%s)", matches[1], strings.Join(columns, ",\n\t"))
	return out
}
