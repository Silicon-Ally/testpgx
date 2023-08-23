package testpgx

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
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
	"github.com/hashicorp/go-multierror"
	"github.com/jackc/pgx/v4"

	"github.com/jackc/pgx/v4/pgxpool"
	pgxstdlib "github.com/jackc/pgx/v4/stdlib"
)

type TestDB struct {
	name string
	conn *pgxpool.Pool
	e    *Env

	once sync.Once
}

func (tdb *TestDB) close(ctx context.Context) error {
	if tdb == nil {
		return errors.New("testdb was nil for close")
	}
	var rErr error
	tdb.once.Do(func() {
		tdb.conn.Close()
		// We opt to drop/re-create databases instead of truncating tables because its
		// just too messy. Even clearing all the data may not be desireable for something
		// like a migration-tracking table. Plus, there's sequences and triggers and all
		// sorts of nuance that's lost with a simple truncation.
		// In the future, we could provide a configurable 'truncate' option to address this.
		if _, err := tdb.e.conn.Exec(ctx, fmt.Sprintf("DROP DATABASE %s;", tdb.name)); err != nil {
			rErr = multierror.Append(rErr, fmt.Errorf("failed to drop database %q: %w", tdb.name, err))
		}

		// A new DB can now be created.
		tdb.e.canCreateDB <- struct{}{}
	})
	return rErr
}

type Env struct {
	postgresCid string
	opts        *options
	canCreateDB chan struct{}
	conn        *pgxpool.Pool

	dbUser      string
	dbPassword  string
	dbSocketDir string
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
	testDBUser = "postgres"
	testDBPass = "anypassword"

	defaultPostgresImage = "postgres:14.6"
	defaultMaxDBs        = 10
)

type options struct {
	postgresDockerImage string
	dockerBinaryPath    string
	migrator            Migrator
	maxDBs              int
}

// Option allows configuring the test environment created by testpgx.
type Option func(*options)

// WithPostgresDockerImage csets the Docker image to use for running
// PostgreSQL. It defaults to 'postgres:14.4'
func WithPostgresDockerImage(img string) Option {
	return func(o *options) {
		o.postgresDockerImage = img
	}
}

// WithDockerBinaryPath sets the path to the local Docker binary. If blank, the
// default is to look in $PATH for the executable.
func WithDockerBinaryPath(p string) Option {
	return func(o *options) {
		o.dockerBinaryPath = p
	}
}

// WithMigrator allows specifing a migration system to populate the database
// schema. If not specified, the database will be empty (e.g. have no tables
// or anything).
func WithMigrator(m Migrator) Option {
	return func(o *options) {
		o.migrator = m
	}
}

// WithMaxDBs controls the number of DBs, which corresponds to the number of
// parallel executions. Test errors can occur if there are too many DBs created
// (because of memory limits). Defaults to 10.
func WithMaxDBs(n int) Option {
	return func(o *options) {
		o.maxDBs = n
	}
}

func New(ctx context.Context, opts ...Option) (*Env, error) {
	o := &options{
		postgresDockerImage: defaultPostgresImage,
		maxDBs:              defaultMaxDBs,
	}
	for _, opt := range opts {
		opt(o)
	}

	if o.dockerBinaryPath == "" {
		p, err := exec.LookPath("docker")
		if err != nil {
			return nil, fmt.Errorf("error looking for 'docker' binary in path: %w", err)
		}
		o.dockerBinaryPath = p
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

	if err := exec.CommandContext(ctx, o.dockerBinaryPath, "pull", o.postgresDockerImage).Run(); err != nil {
		return nil, fmt.Errorf("failed to pull postgres docker image %q: %w", o.postgresDockerImage, err)
	}

	args := []string{
		"run",
		"--rm",
		"--detach",
		"--env", "POSTGRES_PASSWORD=" + testDBPass,
		"--volume", socketDir + ":/var/run/postgresql",
		o.postgresDockerImage, "-c", "listen_addresses=",
	}
	cmd := exec.CommandContext(ctx, o.dockerBinaryPath, args...)

	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("while starting postgres: %w", err)
	}

	cID, err := getPostgresContainerID(out)
	if err != nil {
		return nil, fmt.Errorf("when getting postgres cid: %w", err)
	}

	// Postgres starts up twice, first as an initialization phase, and then for
	// real. See this thread [1] for more info.
	// [1] https://github.com/docker-library/postgres/issues/146
	pgLogCtx, pgLogDone := context.WithCancel(ctx)
	defer pgLogDone()
	pgLogCmd := exec.CommandContext(pgLogCtx, o.dockerBinaryPath, "logs", "-f", cID)
	pgLogs, err := pgLogCmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to get stdout pipe for docker logs: %w", err)
	}
	if err := pgLogCmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start docker logs: %w", err)
	}
	initDone := make(chan struct{})
	go func() {
		sc := bufio.NewScanner(pgLogs)
		for sc.Scan() {
			if strings.Contains(sc.Text(), "PostgreSQL init process complete; ready for start up.") {
				break
			}
		}
		close(initDone)
		pgLogDone()
	}()

	env := &Env{
		postgresCid: cID,
		canCreateDB: make(chan struct{}, o.maxDBs),
		dbUser:      testDBUser,
		dbPassword:  testDBPass,
		dbSocketDir: socketDir,
		opts:        o,
	}

	// Fill the channel with tokens.
	for i := 0; i < o.maxDBs; i++ {
		env.canCreateDB <- struct{}{}
	}

	err = waitForPostgresToBeReady(ctx, func(ctx context.Context) error {
		// Don't try to connect until initialization is done. Postgres starts up twice
		// in Docker, the first is just an initialization.
		<-initDone

		pool, err := pgxpool.Connect(ctx, env.dsn("" /* dbName */))
		if err != nil {
			return fmt.Errorf("failed to connect to database instance: %w", err)
		}
		if err := pool.Ping(ctx); err != nil {
			return fmt.Errorf("failed to ping database instance: %w", err)
		}
		env.conn = pool
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to wait for postgres to be ready: %w", err)
	}

	return env, nil
}

func (e *Env) GetMigratedDB(ctx context.Context, t testing.TB) *pgxpool.Pool {
	db, err := e.createMigratedDB(ctx)
	if err != nil {
		t.Fatalf("acquiring pool: %v", err)
	}
	t.Cleanup(func() {
		if err := db.close(ctx); err != nil {
			t.Errorf("error cleaning up DB: %v", err)
		}
	})
	return db.conn
}

func (e *Env) WithMigratedDB(ctx context.Context, fn func(*pgxpool.Pool) error) error {
	tdb, err := e.createMigratedDB(ctx)
	if err != nil {
		return fmt.Errorf("aquiring pool: %v", err)
	}
	defer tdb.close(ctx) // Best effort close in the event of a failure.
	if err := fn(tdb.conn); err != nil {
		return fmt.Errorf("running fn: %w", err)
	}

	if err := tdb.close(ctx); err != nil {
		return fmt.Errorf("failed to clean up DB: %w", err)
	}
	return nil
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
		"--dbname", dbName,
	}

	cmd := exec.CommandContext(ctx, e.opts.dockerBinaryPath, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("when getting db schema for %q: %w: %s", dbName, err, string(out))
	}
	return string(out), nil
}

func (e *Env) TearDown(ctx context.Context) error {
	e.conn.Close()
	if e.postgresCid != "" {
		if err := exec.CommandContext(ctx, e.opts.dockerBinaryPath, "kill", e.postgresCid).Run(); err != nil {
			return fmt.Errorf("failed to kill Postgres docker container: %w", err)
		}
	}
	return nil
}

func (e *Env) createMigratedDB(ctx context.Context) (*TestDB, error) {
	<-e.canCreateDB
	tdb, err := e.CreateDB(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create DB: %w", err)
	}

	if e.opts.migrator == nil {
		return nil, errors.New("a migrated DB was requested, but no migrator was given")
	}

	err = tdb.conn.AcquireFunc(ctx, func(conn *pgxpool.Conn) error {
		if err := e.opts.migrator.Migrate(connToDB(conn)); err != nil {
			return fmt.Errorf("an error occurred while applying migrations: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return tdb, nil
}

// See comment on truncateDB for more info.
//
//nolint:unused
func resetSequences(ctx context.Context, conn *pgx.Conn) error {
	listSequencesQuery := `SELECT c.relname FROM pg_class c WHERE c.relkind = 'S';`
	rows, err := conn.Query(ctx, listSequencesQuery)
	if err != nil {
		return fmt.Errorf("failed to load sequences: %w", err)
	}
	defer rows.Close()

	batch := &pgx.Batch{}
	for rows.Next() {
		var seqName string
		if err := rows.Scan(&seqName); err != nil {
			return fmt.Errorf("failed to load sequence: %w", err)
		}
		batch.Queue(`ALTER SEQUENCE ` + seqName + ` RESTART;`)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error while loading sequences: %w", err)
	}

	batchLen := batch.Len()

	res := conn.SendBatch(ctx, batch)
	defer res.Close()

	for i := 0; i < batchLen; i++ {
		if _, err := res.Exec(); err != nil {
			return fmt.Errorf("failed to execute sequence reset: %w", err)
		}
	}

	return nil
}

// truncateDB does a best-effort removal of all the data in the database,
// without deleting any of the schema. It isn't currently used, see comments on
// (*TestDB).close for more information.
//
//nolint:unused
func (e *Env) truncateDB(ctx context.Context, conn *pgx.Conn) error {
	if err := resetSequences(ctx, conn); err != nil {
		return fmt.Errorf("failed to list sequences: %v", err)
	}

	listTablesQuery := `SELECT tablename FROM pg_catalog.pg_tables
WHERE schemaname != 'information_schema' AND
schemaname != 'pg_catalog';`
	rows, err := conn.Query(ctx, listTablesQuery)
	if err != nil {
		return fmt.Errorf("failed to load table list: %w", err)
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var tblName string
		if err := rows.Scan(&tblName); err != nil {
			return fmt.Errorf("failed to load table name: %w", err)
		}
		tableNames = append(tableNames, tblName)
	}

	truncQuery := `TRUNCATE TABLE ` + strings.Join(tableNames, ",") + ";"
	if _, err := conn.Exec(ctx, truncQuery); err != nil {
		return fmt.Errorf("failed to truncate all DB tables: %w", err)
	}
	return nil
}

func createTestDBName() string {
	return fmt.Sprintf("test_db_%s", strings.ReplaceAll(uuid.New().String(), "-", ""))
}

func (e *Env) CreateDB(ctx context.Context) (*TestDB, error) {
	testDBName := createTestDBName()
	tdb, err := e.createDatabaseAndWaitForReady(ctx, testDBName)
	if err != nil {
		return nil, fmt.Errorf("waiting for database to be ready: %w", err)
	}

	return tdb, nil
}

func connToDB(conn *pgxpool.Conn) *sql.DB {
	return pgxstdlib.OpenDB(*conn.Conn().Config())
}

func getPostgresContainerID(upOutput []byte) (string, error) {
	result := strings.TrimSpace(string(upOutput))
	if len(result) != 64 {
		return "", fmt.Errorf("expected cid container ID length 64, got length %d: %q", len(result), result)
	}
	return result, nil
}

func (e *Env) dsn(dbName string) string {
	dsn := fmt.Sprintf("user=%s password=%s host=%s sslmode=disable pool_max_conns=20", e.dbUser, e.dbPassword, e.dbSocketDir)
	if dbName != "" {
		dsn += " dbname=" + dbName
	}
	return dsn
}

// connFn connects to a Postgres instance.
type connFn func(ctx context.Context) error

func waitForPostgresToBeReady(ctx context.Context, cFn connFn) error {
	var (
		waitFor     = 1 * time.Millisecond
		waitingFor  = 0 * time.Millisecond
		maxWaitTime = 15 * time.Second

		lastErr error
	)

	for waitingFor < maxWaitTime {
		err := cFn(ctx)
		if err == nil {
			return nil
		}
		time.Sleep(waitFor)
		waitingFor += waitFor
		waitFor *= 2
		lastErr = err
	}
	return fmt.Errorf("wasn't ready: %w", lastErr)
}

func (e *Env) createDatabaseAndWaitForReady(ctx context.Context, dbName string) (*TestDB, error) {
	if _, err := e.conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s;", dbName)); err != nil {
		return nil, fmt.Errorf("failed to create database %q: %w", dbName, err)
	}

	// Now connect to the actual database, since we can't make the original `conn`
	// connect to it, see https://stackoverflow.com/a/10338367.
	var pgxConn *pgxpool.Pool
	cFn := func(ctx context.Context) error {
		conn, err := pgxpool.Connect(ctx, e.dsn(dbName))
		if err != nil {
			return fmt.Errorf("failed to connect to database %q: %w", dbName, err)
		}
		if err := conn.Ping(ctx); err != nil {
			return fmt.Errorf("failed to ping database %q: %w", dbName, err)
		}
		pgxConn = conn
		return nil
	}

	if err := waitForPostgresToBeReady(ctx, cFn); err != nil {
		return nil, fmt.Errorf("failed to wait for connection to DB %q to be ready: %w", dbName, err)
	}

	if pgxConn == nil {
		return nil, errors.New("pgxConn was nil even though database came up successfully")
	}

	return &TestDB{
		name: dbName,
		conn: pgxConn,
		e:    e,
	}, nil
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
	r := regexp.MustCompile(`\n `)
	return r.ReplaceAllString(in, "\n    ")
}

func addExtraNewlineBeforeCreateTableStatements(in string) string {
	r := regexp.MustCompile(`\nCREATE TABLE`)
	return r.ReplaceAllString(in, "\n\n\nCREATE TABLE")
}

func removeNewlineMidAlter(in string) string {
	r := regexp.MustCompile(`([^;,\(\)])\n`)
	return r.ReplaceAllString(in, "$1")
}

func removeOwnershipCommands(in string) string {
	r := regexp.MustCompile(`ALTER (TABLE|TYPE|FUNCTION|SEQUENCE) [^ ]+ OWNER TO postgres;`)
	return r.ReplaceAllString(in, "")
}

func removeComments(in string) string {
	r := regexp.MustCompile(`\n--[^\n]*`)
	return r.ReplaceAllString(in, "")
}

func removeRepeatedSpaces(in string) string {
	r := regexp.MustCompile(" +")
	return r.ReplaceAllString(in, " ")
}

func removeRepeatedNewlines(in string) string {
	r := regexp.MustCompile(`\n+`)
	return r.ReplaceAllString(in, "\n")
}

func removeSchemaMigrationInfo(in string) string {
	r := regexp.MustCompile(";[^;]*(schema|applied)_migration[^;]*;")
	return r.ReplaceAllString(in, ";")
}

func removePublicPrefix(in string) string {
	r := regexp.MustCompile(`public\.`)
	return r.ReplaceAllString(in, " ")
}

func sortByTableAndRemoveNonTableStatements(in string) string {
	tableRegexp := regexp.MustCompile(`public\.[a-z_]*`)
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
