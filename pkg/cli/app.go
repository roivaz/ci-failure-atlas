package cli

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"ci-failure-atlas/pkg/frontend"
	postgresoptions "ci-failure-atlas/pkg/store/postgres/options"
)

func NewAppCommand() (*cobra.Command, error) {
	listen := "127.0.0.1:8082"
	defaultWeek := ""
	historyWeeks := 4
	servePostgresRaw := postgresoptions.DefaultCLIOptions()

	cmd := &cobra.Command{
		Use:           "app",
		Short:         "Run the unified app.",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			postgresCompleted, err := completePostgresForCommand(cmd.Context(), servePostgresRaw)
			if err != nil {
				return err
			}
			defer postgresCompleted.Cleanup()

			handler, err := frontend.NewHandler(frontend.HandlerOptions{
				DefaultWeek:         defaultWeek,
				HistoryHorizonWeeks: historyWeeks,
				PostgresPool:        postgresCompleted.Connection,
			})
			if err != nil {
				return err
			}

			listenAddress := strings.TrimSpace(listen)
			if listenAddress == "" {
				listenAddress = "127.0.0.1:8082"
			}
			listener, err := net.Listen("tcp", listenAddress)
			if err != nil {
				return fmt.Errorf("listen on %q: %w", listenAddress, err)
			}
			defer func() {
				_ = listener.Close()
			}()

			server := &http.Server{Handler: handler}
			cmd.Printf(
				"Serving unified app at %s (Ctrl+C to stop)\n",
				siteRunURLFromListenAddress(listenAddress),
			)

			go func() {
				<-cmd.Context().Done()
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = server.Shutdown(shutdownCtx)
			}()
			if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return fmt.Errorf("serve unified app: %w", err)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&listen, "app.listen", listen, "listen address for unified app (host:port)")
	cmd.Flags().StringVar(&defaultWeek, "week", defaultWeek, "default week to open when no week query is provided (YYYY-MM-DD)")
	cmd.Flags().IntVar(&historyWeeks, "history.weeks", historyWeeks, "number of most recent semantic weeks used for history scoring")
	if err := postgresoptions.BindOptions(servePostgresRaw, cmd); err != nil {
		return nil, err
	}
	return cmd, nil
}

func siteRunURLFromListenAddress(listenAddress string) string {
	trimmed := strings.TrimSpace(listenAddress)
	if trimmed == "" {
		return "http://127.0.0.1:8080"
	}
	host, port, err := net.SplitHostPort(trimmed)
	if err != nil {
		return "http://" + trimmed
	}
	normalizedHost := strings.Trim(strings.TrimSpace(host), "[]")
	if normalizedHost == "" || normalizedHost == "0.0.0.0" || normalizedHost == "::" {
		normalizedHost = "localhost"
	}
	return fmt.Sprintf("http://%s:%s", normalizedHost, port)
}
