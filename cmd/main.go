package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-logr/logr"
	"github.com/spf13/cobra"

	"k8s.io/klog/v2"
)

func main() {
	logger := createLogger(0)
	var logVerbosity int

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cmd := &cobra.Command{
		Use:           "cfa",
		Short:         "CI Failure Atlas command line interface.",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRun: func(cmd *cobra.Command, _ []string) {
			klog.EnableContextualLogging(true)
			ctx = klog.NewContext(ctx, createLogger(logVerbosity))
			cmd.SetContext(ctx)
		},
	}

	cmd.PersistentFlags().IntVarP(&logVerbosity, "verbosity", "v", 0, "set the verbosity level")

	commands := []func() (*cobra.Command, error){
		NewRunCommand,
		NewRunOnceCommand,
		NewSyncOnceCommand,
		NewReportCommand,
		NewWorkflowCommand,
		NewMetricsCommand,
	}

	for _, newCmd := range commands {
		c, err := newCmd()
		if err != nil {
			logger.Error(err, "Failed to create subcommand.")
			os.Exit(1)
		}
		cmd.AddCommand(c)
	}

	if err := cmd.Execute(); err != nil {
		logger.Error(err, "Command failed.")
		os.Exit(1)
	}
}

func createLogger(verbosity int) logr.Logger {
	return logr.FromSlogHandler(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:       slog.Level(verbosity * -1),
		AddSource:   false,
		ReplaceAttr: nil,
	}))
}
