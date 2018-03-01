package di

import (
	"context"

	"github.com/jmalloc/gospel/src/gospel"
	"github.com/jmalloc/gospel/src/gospelmaria"
	"github.com/jmalloc/twelf/src/twelf"
	"github.com/spf13/cobra"
)

func init() {
	Container.Define(
		func(ctx context.Context, cmd *cobra.Command, c *gospelmaria.Client) (gospel.EventStore, error) {
			name, err := cmd.Flags().GetString("mariadb-store")
			if err != nil {
				return nil, err
			}

			return c.OpenStore(ctx, name)
		},
	)

	Container.Define(
		func(cmd *cobra.Command, logger twelf.Logger) (*gospelmaria.Client, error) {
			dsn, err := cmd.Flags().GetString("mariadb-dsn")
			if err != nil {
				return nil, err
			}

			opts := []gospel.Option{
				gospel.Logger(logger),
			}

			var c *gospelmaria.Client

			if dsn == "" {
				c, err = gospelmaria.OpenEnv(opts...)
			} else {
				c, err = gospelmaria.Open(dsn, opts...)
			}

			if err != nil {
				return nil, err
			}

			After(cmd, func() error {
				return c.Close()
			})

			return c, err
		},
	)
}
