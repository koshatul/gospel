package commands

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/jmalloc/gospel/src/cmd/gospel/di"
	"github.com/jmalloc/gospel/src/gospel"
	"github.com/spf13/cobra"
)

// WatchCommand is the "gospel testing load-test" command.
var WatchCommand = &cobra.Command{
	Use:   "watch",
	Short: "Watch an event stream in real-time.",
}

func init() {
	WatchCommand.Flags().StringP(
		"stream", "s",
		"",
		"the name of the stream to watch, defaults to the ε-stream",
	)

	WatchCommand.Flags().Uint64P(
		"offset", "o",
		0,
		"the offset to begin reading from",
	)

	WatchCommand.Flags().BoolP(
		"show-body", "b",
		false,
		"include the event body in the output",
	)

	di.Container.Bind(&WatchCommand.RunE, func(
		ctx context.Context,
		cmd *cobra.Command,
		args []string,
		es gospel.EventStore,
	) error {
		stream, err := cmd.Flags().GetString("stream")
		if err != nil {
			return err
		}

		offset, err := cmd.Flags().GetUint64("offset")
		if err != nil {
			return err
		}

		showBody, err := cmd.Flags().GetBool("show-body")
		if err != nil {
			return err
		}

		r, err := es.Open(ctx, gospel.Address{
			Stream: stream,
			Offset: offset,
		})
		if err != nil {
			return err
		}

		w := cmd.OutOrStdout()

		for {
			_, err := r.Next(ctx)
			if err != nil {
				return err
			}

			fact := r.Get()

			if showBody {
				// indent to align with hex dump
				fmt.Print("          ")
			}

			fmt.Fprintf(
				w,
				"%s  %s  %s (%s)\n",
				fact.Addr,
				fact.Time.Format(time.RFC3339),
				fact.Event,
				fact.Event.ContentType,
			)

			if showBody {
				fmt.Fprintln(w, strings.Repeat("-", 78))

				h := hex.Dumper(w)
				h.Write(fact.Event.Body)
				h.Close()
				fmt.Fprint(w, "\n")
				fmt.Fprint(w, "\n")
			}
		}
	})
}
