package di

import (
	"sync"

	"github.com/jmalloc/fabricate/src/fabricate"
	"github.com/spf13/cobra"
	"github.com/uber-go/multierr"
)

// Container is the commad's global DI container.
var Container = &fabricate.Container{}

var m sync.Mutex

// After registers fn to be called after cmd is run.
func After(cmd *cobra.Command, fn func() error) {
	m.Lock()
	defer m.Unlock()

	next := cmd.PostRunE
	cmd.PostRunE = func(cmd *cobra.Command, args []string) error {
		err := fn()

		if next != nil {
			return multierr.Append(
				err,
				next(cmd, args),
			)
		}

		return err
	}
}
