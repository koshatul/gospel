package testing

import (
	"github.com/spf13/cobra"
)

// TestingCommand is the "gospel test" command.
var TestingCommand = &cobra.Command{
	Use:   "testing",
	Short: "Testing utilities",
}

func init() {
	TestingCommand.AddCommand(LoadTestCommand)
}
