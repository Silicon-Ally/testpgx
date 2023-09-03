package main

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/Silicon-Ally/testpgx"
	"github.com/Silicon-Ally/testpgx/migrate"
	"github.com/google/go-cmp/cmp"
)

type testMigrateLogger struct{}

func (l *testMigrateLogger) Printf(format string, args ...any) {
	log.Printf(format, args...)
}

var (
	env *testpgx.Env
)

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	ctx := context.Background()

	mgr, err := migrate.New("./migrations", migrate.WithLogger(&testMigrateLogger{}))
	if err != nil {
		log.Fatalf("migrate.New: %v", err)
	}

	opts := []testpgx.Option{
		testpgx.WithMigrator(mgr),
		// The default of 10 seems to consume too many resources on GitHub Actions and
		// cause timeouts.
		testpgx.WithMaxDBs(3),
	}

	if env, err = testpgx.New(ctx, opts...); err != nil {
		log.Fatalf("failed to init the test env: %v", err)
	}
	defer func() {
		if err := env.TearDown(ctx); err != nil {
			log.Fatalf("failed to teardown env: %v", err)
		}
	}()

	return m.Run()
}

func TestDumpSchema(t *testing.T) {
	ctx := context.Background()
	db := env.GetMigratedDB(ctx, t)
	sqlDump, err := env.DumpDatabaseSchema(ctx, db.Config().ConnConfig.Database)
	if err != nil {
		t.Fatalf("failed to dump database schema: %v", err)
	}

	diffFile(t, sqlDump, "golden/raw_dump.sql")
}

func TestDumpHumanReadableSchema(t *testing.T) {
	ctx := context.Background()
	db := env.GetMigratedDB(ctx, t)
	sqlDump, err := env.DumpDatabaseSchema(ctx, db.Config().ConnConfig.Database, testpgx.WithHumanReadableSchema())
	if err != nil {
		t.Fatalf("failed to dump database schema: %v", err)
	}

	diffFile(t, sqlDump, "golden/simplified_dump.sql")
}

func diffFile(t *testing.T, got, wantFile string) {
	want, err := os.ReadFile(wantFile)
	if err != nil {
		t.Fatalf("failed to load golden file %q to diff against: %v", wantFile, err)
	}

	if diff := cmp.Diff(string(want), got); diff != "" {
		t.Errorf("output didn't match golden file %q (-want +got)\n%s", wantFile, diff)
	}
}

func TestSchemaHistory(t *testing.T) {
	ctx := context.Background()
	db := env.GetMigratedDB(ctx, t)

	q := `SELECT id, version FROM schema_migrations_history ORDER BY id`
	rows, err := db.Query(ctx, q)
	if err != nil {
		t.Fatalf("failed to query schema migrations history: %v", err)
	}

	type versionHistory struct {
		ID      int
		Version int
	}

	var got []versionHistory
	for rows.Next() {
		var vh versionHistory
		if err := rows.Scan(&vh.ID, &vh.Version); err != nil {
			t.Fatalf("failed to load version history entry: %v", err)
		}
		got = append(got, vh)
	}

	want := []versionHistory{
		{ID: 1, Version: 1}, // 0001_create_schema_migrations_history
		{ID: 2, Version: 2}, // 0002_create_user_table
		{ID: 3, Version: 3}, // 0003_create_todo_table
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected schema version history (-want +got)\n%s", diff)
	}
}

func TestTodos(t *testing.T) {
	ctx := context.Background()
	db := env.GetMigratedDB(ctx, t)

	addUserQuery := `INSERT INTO users (name) VALUES ('Test User') RETURNING id;`

	var userID int
	if err := db.QueryRow(ctx, addUserQuery).Scan(&userID); err != nil {
		t.Fatalf("failed to insert user: %v", err)
	}

	addTODOsQuery := `INSERT INTO todos (title, body, created_by) VALUES
	('Take out trash', 'The truck arrives at 7:30 am', $1),
	('Eat your wheaties', 'There''s six boxes in the cabinet', $1);`

	if _, err := db.Exec(ctx, addTODOsQuery, userID); err != nil {
		t.Fatalf("failed to add todos: %v", err)
	}

	type Todo struct {
		ID        int
		Title     string
		Body      string
		CreatedBy int
	}
	rows, err := db.Query(ctx, `SELECT id, title, body, created_by FROM todos ORDER BY id`)
	if err != nil {
		t.Fatalf("failed to query todos: %v", err)
	}
	defer rows.Close()

	var todos []Todo
	for rows.Next() {
		var todo Todo
		if err := rows.Scan(&todo.ID, &todo.Title, &todo.Body, &todo.CreatedBy); err != nil {
			t.Fatalf("failed to scan todo: %v", err)
		}
		todos = append(todos, todo)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("error while loading todos: %v", err)
	}

	want := []Todo{
		{
			ID:        1,
			Title:     "Take out trash",
			Body:      "The truck arrives at 7:30 am",
			CreatedBy: userID,
		},
		{
			ID:        2,
			Title:     "Eat your wheaties",
			Body:      "There's six boxes in the cabinet",
			CreatedBy: userID,
		},
	}

	if diff := cmp.Diff(want, todos); diff != "" {
		t.Errorf("unexpected todos returned (-want +got)\n%s", diff)
	}
}
