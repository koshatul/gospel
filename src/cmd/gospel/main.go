package main

import (
	"context"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/jmalloc/gospel/src/cmd/gospel/commands"
	"github.com/jmalloc/gospel/src/cmd/gospel/commands/testing"
	"github.com/jmalloc/gospel/src/cmd/gospel/di"
	"github.com/spf13/cobra"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	var root = &cobra.Command{
		Use:   "gospel",
		Short: "Manage Gospel event stores",
	}

	root.PersistentFlags().String(
		"mariadb-dsn",
		"",
		"the DSN used to connect to MariaDB",
	)

	root.PersistentFlags().String(
		"mariadb-store",
		"default",
		"the name of the event store to use when using MariaDB",
	)

	root.PersistentFlags().Duration(
		"timeout",
		0,
		"maximum allowed execution time",
	)

	var cancelCtx func()

	root.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		timeout, err := cmd.Flags().GetDuration("timeout")
		if err != nil {
			return err
		}

		ctx := context.Background()

		if timeout > 0 {
			// setup the deadline for the context as of now, not when it's first
			// requested from the container, then store the cancel func to be
			// invoked in the post-run handler
			var cancel func()
			ctx, cancel = context.WithTimeout(ctx, timeout)
			cancelCtx = cancel
		} else {
			var cancel func()
			ctx, cancel = context.WithCancel(ctx)
			cancelCtx = cancel
		}

		go func() {
			signals := make(chan os.Signal)
			signal.Notify(signals, os.Interrupt)

			select {
			case <-signals:
				cancelCtx()
			case <-ctx.Done():
			}
		}()

		di.Container.Define(
			func() *cobra.Command {
				return cmd
			},
		)

		di.Container.Define(
			func() context.Context {
				return ctx
			},
		)

		return nil
	}

	root.PersistentPostRun = func(cmd *cobra.Command, _ []string) {
		if cancelCtx != nil {
			cancelCtx()
		}
	}

	root.AddCommand(commands.WatchCommand)
	root.AddCommand(testing.TestingCommand)

	silenceUsageForContextErrors(root)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func silenceUsageForContextErrors(cmd *cobra.Command) {
	if cmd.RunE != nil {
		next := cmd.RunE
		cmd.RunE = func(cmd *cobra.Command, args []string) error {
			err := next(cmd, args)
			if err == context.Canceled || err == context.DeadlineExceeded {
				cmd.SilenceUsage = true
			}
			return err
		}
	}

	for _, child := range cmd.Commands() {
		silenceUsageForContextErrors(child)
	}
}
