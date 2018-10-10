package main

import (
	"log"
	"os"

	"github.com/chris-skud/gomig/commands"
	_ "github.com/chris-skud/gomig/drivers/spanner"
	"github.com/urfave/cli"
)

func main() {
	app := App()
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func App() *cli.App {
	app := cli.NewApp()
	app.Usage = "Migrations and cronjobs for databases"
	app.Version = "2.1.1"

	app.Flags = commands.Flags()

	app.Commands = commands.Commands()
	return app
}
