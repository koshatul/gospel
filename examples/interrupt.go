package examples

import (
	"context"
	"os"
	"os/signal"
)

// WithCancelOnInterrupt returns a new cancelable context derived from ctx,
// and cancels it if an interrupt signal (CTRL-C) is received.
func WithCancelOnInterrupt(ctx context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		signals := make(chan os.Signal)
		signal.Notify(signals, os.Interrupt)

		select {
		case <-signals:
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx, cancel
}
