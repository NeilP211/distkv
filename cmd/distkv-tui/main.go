// Command distkv-tui is a live terminal-UI dashboard for a DistKV cluster.
//
// Usage:
//
//	distkv-tui [--endpoints h:p,h:p,...] [--refresh <duration>]
//
// The dashboard polls every node in the cluster every --refresh interval and
// displays a table showing each node's role, Raft term, commit index, and
// member count.  The leader's row is highlighted.  An input panel below the
// table accepts ad-hoc commands:
//
//	p <key> <value>  – Put
//	g <key>          – linearizable Get
//	q                – quit
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/NeilP211/distkv/internal/tui"
	"github.com/NeilP211/distkv/pkg/client"
)

const defaultEndpoints = "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "distkv-tui:", err)
		os.Exit(1)
	}
}

func run() error {
	endpointsFlag := flag.String("endpoints", defaultEndpoints,
		"comma-separated host:port addresses of cluster nodes")
	refreshFlag := flag.Duration("refresh", time.Second,
		"how often to poll node status (e.g. 500ms, 2s)")
	flag.Parse()

	eps := splitEndpoints(*endpointsFlag)
	if len(eps) == 0 {
		return fmt.Errorf("--endpoints must contain at least one host:port")
	}

	cl := client.New(eps)
	defer cl.Close()

	dash := tui.New(cl, eps, *refreshFlag)
	defer dash.Close()

	if err := dash.Run(); err != nil {
		return fmt.Errorf("dashboard: %w", err)
	}
	return nil
}

// splitEndpoints splits a comma-separated endpoint string, trimming blanks.
func splitEndpoints(s string) []string {
	var out []string
	for _, e := range strings.Split(s, ",") {
		if e = strings.TrimSpace(e); e != "" {
			out = append(out, e)
		}
	}
	return out
}
