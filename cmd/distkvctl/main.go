// Command distkvctl is the DistKV command-line client.  It wraps pkg/client
// to run one-shot key-value operations against a cluster:
//
//	distkvctl [--endpoints h:p,h:p,...] put <key> <value>
//	distkvctl [--endpoints ...]         get <key>
//	distkvctl [--endpoints ...]         del <key>
//	distkvctl [--endpoints ...]         cas <key> <expect> <value>
//	distkvctl [--endpoints ...]         status
//
// It prints results plainly and exits non-zero on any error.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/NeilP211/distkv/pkg/client"
)

const defaultEndpoints = "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "distkvctl:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	// Manual flag handling so the --endpoints flag may precede the
	// subcommand without the flag package consuming subcommand arguments.
	endpoints := defaultEndpoints
	for len(args) > 0 {
		switch {
		case args[0] == "--endpoints" || args[0] == "-endpoints":
			if len(args) < 2 {
				return fmt.Errorf("--endpoints requires a value")
			}
			endpoints, args = args[1], args[2:]
		case strings.HasPrefix(args[0], "--endpoints="):
			endpoints = strings.TrimPrefix(args[0], "--endpoints=")
			args = args[1:]
		case strings.HasPrefix(args[0], "-endpoints="):
			endpoints = strings.TrimPrefix(args[0], "-endpoints=")
			args = args[1:]
		default:
			goto parsed
		}
	}
parsed:
	if len(args) == 0 {
		return fmt.Errorf("usage: distkvctl [--endpoints h:p,...] <put|get|del|cas|status> [args]")
	}

	eps := splitEndpoints(endpoints)
	if len(eps) == 0 {
		return fmt.Errorf("no endpoints configured")
	}
	cl := client.New(eps)
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd, rest := args[0], args[1:]
	switch cmd {
	case "put":
		if len(rest) != 2 {
			return fmt.Errorf("usage: distkvctl put <key> <value>")
		}
		if err := cl.Put(ctx, rest[0], rest[1]); err != nil {
			return err
		}
		fmt.Println("OK")

	case "get":
		if len(rest) != 1 {
			return fmt.Errorf("usage: distkvctl get <key>")
		}
		v, found, err := cl.Get(ctx, rest[0])
		if err != nil {
			return err
		}
		if !found {
			fmt.Println("(not found)")
			return nil
		}
		fmt.Println(v)

	case "del":
		if len(rest) != 1 {
			return fmt.Errorf("usage: distkvctl del <key>")
		}
		if err := cl.Delete(ctx, rest[0]); err != nil {
			return err
		}
		fmt.Println("OK")

	case "cas":
		if len(rest) != 3 {
			return fmt.Errorf("usage: distkvctl cas <key> <expect> <value>")
		}
		ok, err := cl.CAS(ctx, rest[0], rest[1], rest[2])
		if err != nil {
			return err
		}
		if ok {
			fmt.Println("OK (swapped)")
		} else {
			fmt.Println("MISMATCH (not swapped)")
		}

	case "status":
		if len(rest) != 0 {
			return fmt.Errorf("usage: distkvctl status")
		}
		info, err := cl.Status(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("leader:  %s\n", info.LeaderID)
		fmt.Printf("term:    %d\n", info.Term)
		fmt.Printf("role:    %s\n", info.Role)
		fmt.Printf("commit:  %d\n", info.CommitIndex)
		fmt.Printf("members: %s\n", strings.Join(info.Members, ", "))

	default:
		return fmt.Errorf("unknown command %q (want put|get|del|cas|status)", cmd)
	}
	return nil
}

// splitEndpoints splits a comma-separated endpoint list, trimming blanks.
func splitEndpoints(s string) []string {
	var out []string
	for _, e := range strings.Split(s, ",") {
		if e = strings.TrimSpace(e); e != "" {
			out = append(out, e)
		}
	}
	return out
}
