package testing

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"time"

	"github.com/jmalloc/gospel/src/cmd/gospel/di"
	"github.com/jmalloc/gospel/src/gospel"
	"github.com/jmalloc/gospel/src/internal/metrics"
	"github.com/spf13/cobra"
)

// LoadTestCommand is the "gospel testing load-test" command.
var LoadTestCommand = &cobra.Command{
	Use:   "load-test",
	Short: "Append random events as fast as possible.",
}

func init() {
	LoadTestCommand.Flags().Uint16P(
		"num-streams", "n",
		100,
		"the number of streams to append to",
	)

	di.Container.Bind(&LoadTestCommand.RunE, func(
		ctx context.Context,
		cmd *cobra.Command,
		args []string,
		es gospel.EventStore,
	) error {
		n, err := cmd.Flags().GetUint16("num-streams")
		if err != nil {
			return err
		}

		rc := metrics.NewRateCounter()
		w := cmd.OutOrStdout()

		go showAppendRate(
			ctx,
			w,
			rc,
		)

		return loadTest(
			ctx,
			w,
			es,
			rc,
			int(n),
		)
	})
}

func loadTest(
	ctx context.Context,
	w io.Writer,
	es gospel.EventStore,
	rc *metrics.RateCounter,
	n int,
) error {
	names := make([]string, n)
	for i := 0; i < n; i++ {
		names[i] = randomName()
	}

	for {
		if err := loadTestStream(
			ctx,
			w,
			es,
			rc,
			names[rand.Intn(n)],
		); err != nil {
			return err
		}
	}
}

func loadTestStream(
	ctx context.Context,
	w io.Writer,
	es gospel.EventStore,
	rc *metrics.RateCounter,
	stream string,
) error {
	n := 1 + rand.Intn(5000)

	fmt.Fprintf(
		w,
		"target stream changed to %s, appending %d event(s)\n",
		stream,
		n,
	)

	next, err := es.AppendUnchecked(
		ctx,
		stream,
		gospel.Event{
			EventType:   "load-test.stream-changed",
			ContentType: "text/plain",
			Body: []byte(fmt.Sprintf(
				"pid %d changed the target stream to %s",
				os.Getpid(),
				stream,
			)),
		},
	)
	if err != nil {
		return err
	}

	rc.Tick()

	for i := 1; i <= n; i++ {
		next, err = es.Append(
			ctx,
			next,
			gospel.Event{
				EventType:   "load-test.test-event-appended",
				ContentType: "text/plain",
				Body: []byte(fmt.Sprintf(
					"pid %d appended event %d of %d to %s",
					os.Getpid(),
					i,
					n,
					stream,
				)),
			},
		)

		if gospel.IsConflict(err) {
			fmt.Fprintln(w, err)
			return nil // find a new stream
		} else if err != nil {
			return err
		}

		rc.Tick()
	}

	return nil
}

func showAppendRate(
	ctx context.Context,
	w io.Writer,
	rc *metrics.RateCounter,
) {
	for {
		select {
		case <-time.After(2500 * time.Millisecond):
		case <-ctx.Done():
			return
		}

		fmt.Fprintf(
			w,
			"avg. append rate: %.02f/s\n",
			rc.Rate(),
		)
	}
}
