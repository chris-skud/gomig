package commands

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"

	"github.com/chris-skud/gomig/file"
	"github.com/chris-skud/gomig/migrate"
	"github.com/urfave/cli"
)

func Commands() cli.Commands {
	return cli.Commands{
		{
			Name:        "migrate",
			Aliases:     []string{"m"},
			Usage:       "migrate database",
			Subcommands: MigrateCommands(),
		},
	}
}

func Flags() []cli.Flag {
	return []cli.Flag{
		cli.StringFlag{
			Name:   "url, u",
			Usage:  "Driver URL",
			Value:  "",
			EnvVar: "DRIVER_URL",
		},
		cli.StringFlag{
			Name:   "path, p",
			Usage:  "Files path",
			Value:  "./files",
			EnvVar: "FILES_PATH",
		},
	}
}

var MigrateFlags = []cli.Flag{}

//Commands returns the application cli commands:
//create <name>  Create a new migration
//up             Apply all -up- migrations
//down           Apply all -down- migrations
//reset          Down followed by Up
//redo           Roll back most recent migration, then apply it again
//version        Show current migration version
//migrate <n>    Apply migrations -n|+n
//goto <v>       Migrate to version v
func MigrateCommands() cli.Commands {
	return cli.Commands{
		createCommand,
		upCommand,
		downCommand,
		resetCommand,
		redoCommand,
		versionCommand,
		migrateCommand,
		applyCommand,
		rollbackCommand,
		gotoCommand,
	}
}

var createCommand = cli.Command{
	Name:      "create",
	Aliases:   []string{"c"},
	Usage:     "Create a new migration",
	ArgsUsage: "<name>",
	Flags:     MigrateFlags,
	Action: func(ctx *cli.Context) error {
		name := ctx.Args().First()
		if name == "" {
			log.Fatal("Please specify a name for the new migration")
		}
		// if more than one param is passed, create a concat name
		if ctx.NArg() != 1 {
			name = strings.Join(ctx.Args(), "_")
		}

		migrate, _, cancel := newMigrateWithCtx(ctx.GlobalString("url"), ctx.GlobalString("path"))
		defer cancel()

		migrationFile, err := migrate.Create(name)
		if err != nil {
			log.Fatalf("Migration failed")
		}
		log.Printf("Version %v migration files created in %v:\n", migrationFile.Version, ctx.GlobalString("path"))
		return nil
	},
}

var upCommand = cli.Command{
	Name:  "up",
	Usage: "Apply all -up- migrations",
	Flags: MigrateFlags,
	Action: func(ctx *cli.Context) error {
		log.Println("Applying all -up- migrations")
		migrate, mctx, cancel := newMigrateWithCtx(ctx.GlobalString("url"), ctx.GlobalString("path"))
		defer cancel()
		err := migrate.Up(mctx)
		if err != nil {
			log.Fatalf("Failed to apply all -up- migrations: %s", err.Error())
		}
		logCurrentVersion(mctx, migrate)
		return nil
	},
}

var downCommand = cli.Command{
	Name:  "down",
	Usage: "Apply all -down- migrations",
	Flags: MigrateFlags,
	Action: func(ctx *cli.Context) error {
		log.Println("Applying all -down- migrations")
		migrate, mctx, cancel := newMigrateWithCtx(ctx.GlobalString("url"), ctx.GlobalString("path"))
		defer cancel()
		err := migrate.Down(mctx)
		if err != nil {
			log.Fatalf("Failed to apply all -down- migrations: %s", err.Error())
		}
		logCurrentVersion(mctx, migrate)
		return nil
	},
}

var redoCommand = cli.Command{
	Name:    "redo",
	Aliases: []string{"r"},
	Usage:   "Roll back most recent migration, then apply it again",
	Flags:   MigrateFlags,
	Action: func(ctx *cli.Context) error {
		log.Println("Redoing last migration")
		migrate, mctx, cancel := newMigrateWithCtx(ctx.GlobalString("url"), ctx.GlobalString("path"))
		defer cancel()
		err := migrate.Redo(mctx)
		if err != nil {
			log.Fatalf("Failed to redo last migration: %s", err.Error())
		}
		logCurrentVersion(mctx, migrate)
		return nil
	},
}

var versionCommand = cli.Command{
	Name:    "version",
	Aliases: []string{"v"},
	Usage:   "Show current migration version",
	Flags:   MigrateFlags,
	Action: func(ctx *cli.Context) error {
		migrate, mctx, cancel := newMigrateWithCtx(ctx.GlobalString("url"), ctx.GlobalString("path"))
		defer cancel()
		version, err := migrate.Version(mctx)
		if err != nil {
			log.Fatalf("Unable to fetch version: %s", err.Error())
		}

		log.Printf("Current version: %d", version)
		return nil
	},
}

var resetCommand = cli.Command{
	Name:  "reset",
	Usage: "Down followed by Up",
	Flags: MigrateFlags,
	Action: func(ctx *cli.Context) error {
		log.Println("Reseting database")
		migrate, mctx, cancel := newMigrateWithCtx(ctx.GlobalString("url"), ctx.GlobalString("path"))
		defer cancel()
		err := migrate.Redo(mctx)
		if err != nil {
			log.Fatalf("Failed to reset database: %s", err.Error())
		}
		logCurrentVersion(mctx, migrate)
		return nil
	},
}

var migrateCommand = cli.Command{
	Name:            "migrate",
	Aliases:         []string{"m"},
	Usage:           "Apply migrations -n|+n",
	ArgsUsage:       "<n>",
	Flags:           MigrateFlags,
	SkipFlagParsing: true,
	Action: func(ctx *cli.Context) error {
		relativeN := ctx.Args().First()
		relativeNInt, err := strconv.Atoi(relativeN)
		if err != nil {
			log.Fatalf("Unable to parse param <n>: %s", err.Error())
		}

		log.Printf("Applying %d migrations", relativeNInt)

		migrate, mctx, cancel := newMigrateWithCtx(ctx.GlobalString("url"), ctx.GlobalString("path"))
		defer cancel()
		err = migrate.Migrate(mctx, relativeNInt)
		if err != nil {
			log.Fatalf("Failed to apply %d migrations: %s", relativeNInt, err.Error())
		}
		logCurrentVersion(mctx, migrate)
		return nil
	},
}

var applyCommand = cli.Command{
	Name:            "apply",
	Aliases:         []string{"a"},
	Usage:           "Run up migration for specific version",
	ArgsUsage:       "<version>",
	Flags:           MigrateFlags,
	SkipFlagParsing: true,
	Action: func(ctx *cli.Context) error {
		version := ctx.Args().First()
		versionInt, err := strconv.Atoi(version)
		if err != nil {
			log.Fatalf("Unable to parse param <n>: %s", err.Error())
		}

		log.Printf("Applying version %d", versionInt)

		migrate, mctx, cancel := newMigrateWithCtx(ctx.GlobalString("url"), ctx.GlobalString("path"))
		defer cancel()
		err = migrate.ApplyVersion(mctx, file.Version(versionInt))
		if err != nil {
			log.Fatalf("Failed to apply version %d: %s", versionInt, err.Error())
		}
		logCurrentVersion(mctx, migrate)
		return nil
	},
}

var rollbackCommand = cli.Command{
	Name:            "rollback",
	Aliases:         []string{"r"},
	Usage:           "Run down migration for specific version",
	ArgsUsage:       "<version>",
	Flags:           MigrateFlags,
	SkipFlagParsing: true,
	Action: func(ctx *cli.Context) error {
		version := ctx.Args().First()
		versionInt, err := strconv.Atoi(version)
		if err != nil {
			log.Fatalf("Unable to parse param <n>: %s", err.Error())
		}

		log.Printf("Applying version %d", versionInt)

		migrate, mctx, cancel := newMigrateWithCtx(ctx.GlobalString("url"), ctx.GlobalString("path"))
		defer cancel()
		err = migrate.RollbackVersion(mctx, file.Version(versionInt))
		if err != nil {
			log.Fatalf("Failed to rollback version %d: %s", versionInt, err.Error())
		}
		logCurrentVersion(mctx, migrate)
		return nil
	},
}

var gotoCommand = cli.Command{
	Name:      "goto",
	Aliases:   []string{"g"},
	Usage:     "Migrate to version <v>",
	ArgsUsage: "<v>",
	Flags:     MigrateFlags,
	Action: func(ctx *cli.Context) error {
		toVersion := ctx.Args().First()
		toVersionInt, err := strconv.Atoi(toVersion)
		if err != nil || toVersionInt < 0 {
			log.Fatalf("Unable to parse param <v>")
		}

		log.Printf("Migrating to version %d", toVersionInt)

		migrate, mctx, cancel := newMigrateWithCtx(ctx.GlobalString("url"), ctx.GlobalString("path"))
		defer cancel()
		currentVersion, err := migrate.Version(mctx)
		if err != nil {
			log.Fatalf("failed to migrate to version %d: %s", toVersionInt, err.Error())
		}

		relativeNInt := toVersionInt - int(currentVersion)

		err = migrate.Migrate(mctx, relativeNInt)
		if err != nil {
			log.Fatalf("Failed to migrate to vefrsion %d: %s", toVersionInt, err.Error())
		}
		logCurrentVersion(mctx, migrate)
		return nil
	},
}

func newMigrateWithCtx(url, migrationsPath string) (*migrate.Handle, context.Context, func()) {
	done := make(chan struct{})
	m, err := migrate.Open(url, migrationsPath, migrate.WithHooks(
		func(f file.File) error {
			log.Printf("Applying %s migration for version %d (%s)", f.Direction, f.Version, f.Name)
			return nil
		},
		func(f file.File) error {
			done <- struct{}{}
			return nil
		},
	))
	if err != nil {
		log.Fatalf("Initialization failed: %s", err)
	}
	ctx, cancel := newOsInterruptCtx(done)
	return m, ctx, cancel
}

// newOsInterruptCtx returns new context that will be cancelled
// on os.Interrupt signal.
func newOsInterruptCtx(done <-chan struct{}) (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		exit := false
		for loop := true; loop; {
			select {
			case <-done:
				if exit {
					loop = false
				}
			case <-c:
				if exit {
					os.Exit(5)
				}
				cancel()
				exit = true
				log.Println("Aborting after this migration... Hit again to force quit.")
			}
		}
		signal.Stop(c)
	}()
	return ctx, cancel
}

func logCurrentVersion(ctx context.Context, migrate *migrate.Handle) {
	version, err := migrate.Version(ctx)
	if err != nil {
		log.Fatalf("Unable to fetch version: %s", err.Error())
	}
	log.Printf("done: current-version %d", version)
}
