package main

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/DavidGamba/go-getoptions"
	"github.com/cyverse-de/dbutil"
	"github.com/pkg/errors"

	_ "github.com/lib/pq"
)

// commandLineOptionValues represents the option values that are accepted by this utility.
type commandLineOptionValues struct {
	Source string
	Dest   string
}

// parseCommandLine parses the command-line and returns a structure containging the option
// values specified on the command line. If the user requests help or a usage error is detected
// then a usage message will be displayed and the program will exit.
func parseCommandLine() *commandLineOptionValues {
	optionValues := &commandLineOptionValues{}
	opt := getoptions.New()

	// Define the command-line options.
	opt.Bool("help", false, opt.Alias("h", "?"))
	opt.StringVar(&optionValues.Source, "source", "",
		opt.Alias("s"),
		opt.Required(),
		opt.Description("the connection URI for the source database"))
	opt.StringVar(&optionValues.Dest, "dest", "",
		opt.Alias("d"),
		opt.Required(),
		opt.Description("the connection URI for the destination database"))

	// Parse the command line, handling requests for help and usage errors.
	_, err := opt.Parse(os.Args[1:])
	if opt.Called("help") {
		fmt.Fprintf(os.Stderr, opt.Help())
		os.Exit(0)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n\n", err)
		fmt.Fprintf(os.Stderr, opt.Help(getoptions.HelpSynopsis))
	}

	return optionValues
}

// initDatabase establishes a database connection and verifies that the database can be reached.
func initDatabase(driverName, databaseURI string) (*sql.DB, error) {
	wrapMsg := "unable to initialize the database"

	// Create a database connector to establish the connection for us.
	connector, err := dbutil.NewDefaultConnector("1m")
	if err != nil {
		return nil, errors.Wrap(err, wrapMsg)
	}

	// Establish the database connection.
	db, err := connector.Connect(driverName, databaseURI)
	if err != nil {
		return nil, errors.Wrap(err, wrapMsg)
	}

	return db, nil
}

func main() {
	// Parse the command-line optons.
	optionValues := parseCommandLine()

	// Establish the source database connection.
	sourceDB, err := initDatabase("postgres", optionValues.Source)
	if err != nil {
		fmt.Fprintf(os.Stderr, "source database: %s\n", err.Error())
		os.Exit(1)
	}
	defer sourceDB.Close()

	// Establish the destination database connection.
	destDB, err := initDatabase("postgres", optionValues.Dest)
	if err != nil {
		fmt.Fprintf(os.Stderr, "destination database: %s\n", err.Error())
		os.Exit(1)
	}
	defer destDB.Close()

	// Start a transaction for the source database to keep query results consistent.
	sourceTx, err := sourceDB.Begin()
	if err != nil {
		fmt.Fprintf(os.Stderr, "source database: %s\n", err.Error())
		os.Exit(1)
	}
	defer sourceTx.Rollback()

	// Start a transaction for the destination database.
	destTx, err := destDB.Begin()
	if err != nil {
		fmt.Fprintf(os.Stderr, "destination database: %s\n", err.Error())
		os.Exit(1)
	}
	defer destTx.Rollback()

	// Run the database migration.
	err = runMigration(sourceTx, destTx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	// Commit the transaction in the destination database.
	err = destTx.Commit()
	if err != nil {
		fmt.Fprintf(os.Stderr, "destination database commit failed: %s\n", err.Error())
		os.Exit(1)
	}
}
