package migrate

import (
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/hashicorp/go-multierror"

	_ "github.com/golang-migrate/migrate/v4/source/file"
)

type Logger interface {
	Printf(format string, args ...any)
}

type noopLogger struct{}

func (*noopLogger) Printf(_ string, _ ...any) { /* no-op */ }

type Migrator struct {
	migrationsPath string
	logger         Logger
}

type Options struct {
	logger Logger
}

type Option func(*Options)

func WithLogger(lgr Logger) Option {
	return func(o *Options) {
		o.logger = lgr
	}
}

func New(migrationsPath string, opts ...Option) (*Migrator, error) {
	o := &Options{
		logger: &noopLogger{},
	}
	for _, opt := range opts {
		opt(o)
	}

	fi, err := os.Stat(migrationsPath)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("given migrations path %q doesn't exist", migrationsPath)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to check migrations path: %w", err)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("given migrations path %q is not a directory", migrationsPath)
	}

	return &Migrator{
		migrationsPath: migrationsPath,
		logger:         o.logger,
	}, nil
}

func (m *Migrator) Migrate(db *sql.DB) (err error) {
	// Pings the database to distinguish between migration and connection errors
	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	driver, err := pgx.WithInstance(db, &pgx.Config{})
	if err != nil {
		return fmt.Errorf("failed to init driver instance: %w", err)
	}

	mgr, err := migrate.NewWithDatabaseInstance(
		"file://"+m.migrationsPath,
		"pgx",
		driver,
	)
	if err != nil {
		return fmt.Errorf("failed to init migrate instance: %w", err)
	}
	defer func() {
		closeErr := closeMigrator(mgr)
		if closeErr == nil {
			return
		}
		err = multierror.Append(err, fmt.Errorf("failed to close migrator: %w", closeErr))
	}()

	if err := mgr.Up(); errors.Is(err, migrate.ErrNoChange) {
		m.logger.Printf("no new migrations to apply")
	} else if err != nil {
		return fmt.Errorf("failed to apply migrations: %w", err)
	}

	return nil
}

func closeMigrator(m *migrate.Migrate) error {
	srcErr, dbErr := m.Close()
	var err error
	if srcErr != nil {
		err = multierror.Append(err, fmt.Errorf("failed to close file source: %w", srcErr))
	}
	if dbErr != nil {
		err = multierror.Append(err, fmt.Errorf("failed to close DB source: %w", dbErr))
	}
	return err
}
