// Package tui provides a live terminal-UI dashboard for a DistKV cluster.
//
// The Dashboard polls every cluster node independently via the gRPC KV Status
// RPC at a configurable interval and renders a table showing each node's role,
// Raft term, commit index, and member count.  The leader's row is highlighted
// in yellow.  Below the table an input panel accepts ad-hoc commands:
//
//	p <key> <value>  – issues a Put
//	g <key>          – issues a linearizable Get and shows the result
//	q                – quits the application
package tui

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/NeilP211/distkv/api"
	"github.com/NeilP211/distkv/pkg/client"
)

// nodeStatus holds the latest polled status for one cluster endpoint.
type nodeStatus struct {
	Endpoint    string
	Role        string
	Term        uint64
	CommitIndex uint64
	MemberCount int
	LeaderID    string
	Err         error
}

// Dashboard is a live TUI cluster monitor.  Build one with New, then call Run.
type Dashboard struct {
	app       *tview.Application
	table     *tview.Table
	output    *tview.TextView
	input     *tview.InputField
	leaderBar *tview.TextView

	cl        *client.Client
	endpoints []string
	refresh   time.Duration

	mu       sync.Mutex
	statuses []nodeStatus

	conns map[string]*grpc.ClientConn
}

// New builds a Dashboard that polls the given endpoints every refresh interval.
// The supplied client is used for ad-hoc Put/Get commands.
func New(cl *client.Client, endpoints []string, refresh time.Duration) *Dashboard {
	d := &Dashboard{
		app:       tview.NewApplication(),
		cl:        cl,
		endpoints: endpoints,
		refresh:   refresh,
		statuses:  make([]nodeStatus, len(endpoints)),
		conns:     make(map[string]*grpc.ClientConn),
	}
	for i, ep := range endpoints {
		d.statuses[i] = nodeStatus{Endpoint: ep, Role: "unknown"}
	}
	d.buildUI()
	return d
}

// buildUI assembles the tview layout.
func (d *Dashboard) buildUI() {
	// ── leader header ─────────────────────────────────────────────────────────
	d.leaderBar = tview.NewTextView().
		SetDynamicColors(true).
		SetText("[yellow]Leader: (discovering…)[-]")
	d.leaderBar.SetBorder(false)

	// ── status table ──────────────────────────────────────────────────────────
	d.table = tview.NewTable().
		SetFixed(1, 0). // freeze the header row
		SetSelectable(false, false)
	d.table.SetBorder(true).SetTitle(" Cluster Nodes ").SetTitleColor(tcell.ColorAqua)
	d.populateTableHeader()

	// ── output view ───────────────────────────────────────────────────────────
	d.output = tview.NewTextView().
		SetDynamicColors(true).
		SetScrollable(true).
		SetWordWrap(true)
	d.output.SetBorder(true).SetTitle(" Output ").SetTitleColor(tcell.ColorGreen)
	fmt.Fprintln(d.output, "[white]Ready. Type commands below.[-]")

	// ── command input ─────────────────────────────────────────────────────────
	d.input = tview.NewInputField().
		SetLabel("cmd> ").
		SetFieldWidth(0).
		SetFieldBackgroundColor(tcell.ColorDefault)
	d.input.SetBorder(true).
		SetTitle(" Commands: p <key> <val>  g <key>  q=quit ").
		SetTitleColor(tcell.ColorYellow)
	d.input.SetDoneFunc(d.handleCommand)

	// ── layout ────────────────────────────────────────────────────────────────
	tableWithHeader := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(d.leaderBar, 1, 0, false).
		AddItem(d.table, 0, 1, false)

	layout := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(tableWithHeader, 0, 3, false).
		AddItem(d.output, 0, 2, false).
		AddItem(d.input, 3, 0, true)

	d.app.SetRoot(layout, true).EnableMouse(false)

	// Global key-binding: 'q' quits.
	d.app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyRune && event.Rune() == 'q' {
			d.app.Stop()
			return nil
		}
		return event
	})
}

// populateTableHeader writes the fixed header row.
func (d *Dashboard) populateTableHeader() {
	headers := []string{"Node", "Role", "Term", "Commit", "Members"}
	for col, h := range headers {
		d.table.SetCell(0, col, tview.NewTableCell(h).
			SetTextColor(tcell.ColorAqua).
			SetSelectable(false).
			SetExpansion(1).
			SetAttributes(tcell.AttrBold))
	}
}

// Run starts the polling goroutine and the tview event loop.  It blocks until
// the application exits.
func (d *Dashboard) Run() error {
	go d.pollLoop()
	return d.app.Run()
}

// pollLoop fetches status from every endpoint in parallel at the refresh interval.
func (d *Dashboard) pollLoop() {
	ticker := time.NewTicker(d.refresh)
	defer ticker.Stop()

	// First poll immediately.
	d.poll()
	for range ticker.C {
		d.poll()
	}
}

// poll queries every endpoint concurrently and refreshes the table.
func (d *Dashboard) poll() {
	var wg sync.WaitGroup
	results := make([]nodeStatus, len(d.endpoints))

	for i, ep := range d.endpoints {
		wg.Add(1)
		go func(i int, ep string) {
			defer wg.Done()
			results[i] = d.queryNode(ep)
		}(i, ep)
	}
	wg.Wait()

	d.mu.Lock()
	d.statuses = results
	d.mu.Unlock()

	d.app.QueueUpdateDraw(func() {
		d.refreshTable()
	})
}

// queryNode dials a single endpoint's gRPC KV service and returns its status.
func (d *Dashboard) queryNode(ep string) nodeStatus {
	ns := nodeStatus{Endpoint: ep}
	conn, err := d.getConn(ep)
	if err != nil {
		ns.Err = err
		ns.Role = "unreachable"
		return ns
	}

	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()

	kv := api.NewKVClient(conn)
	resp, err := kv.Status(ctx, &api.StatusReq{})
	if err != nil {
		ns.Err = err
		ns.Role = "unreachable"
		return ns
	}

	ns.Role = resp.GetRole()
	ns.Term = resp.GetTerm()
	ns.CommitIndex = resp.GetCommitIndex()
	ns.MemberCount = len(resp.GetMemberIds())
	ns.LeaderID = resp.GetLeaderId()
	return ns
}

// getConn returns a cached gRPC connection for ep, dialing lazily.
func (d *Dashboard) getConn(ep string) (*grpc.ClientConn, error) {
	if cc, ok := d.conns[ep]; ok {
		return cc, nil
	}
	cc, err := grpc.NewClient(ep, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	d.conns[ep] = cc
	return cc, nil
}

// refreshTable rewrites table rows from the latest polled statuses.
// Must be called from the tview draw goroutine (via QueueUpdateDraw).
func (d *Dashboard) refreshTable() {
	d.mu.Lock()
	statuses := make([]nodeStatus, len(d.statuses))
	copy(statuses, d.statuses)
	d.mu.Unlock()

	// Figure out leader id for the header bar.
	leaderID := ""
	for _, s := range statuses {
		if s.LeaderID != "" {
			leaderID = s.LeaderID
			break
		}
	}

	// Clear existing data rows (keep header at row 0).
	for d.table.GetRowCount() > 1 {
		d.table.RemoveRow(d.table.GetRowCount() - 1)
	}

	for row, s := range statuses {
		isLeader := s.Role == "Leader"
		color := tcell.ColorWhite
		if isLeader {
			color = tcell.ColorYellow
		}
		if s.Err != nil || s.Role == "unreachable" {
			color = tcell.ColorRed
		}

		// Short node label: strip the host, show port or full endpoint.
		label := s.Endpoint

		role := s.Role
		if role == "" {
			role = "unknown"
		}

		cells := []string{
			label,
			role,
			fmt.Sprintf("%d", s.Term),
			fmt.Sprintf("%d", s.CommitIndex),
			fmt.Sprintf("%d", s.MemberCount),
		}
		for col, text := range cells {
			cell := tview.NewTableCell(text).
				SetTextColor(color).
				SetExpansion(1)
			if isLeader {
				cell.SetAttributes(tcell.AttrBold)
			}
			d.table.SetCell(row+1, col, cell)
		}
	}

	// Update leader bar.
	if leaderID == "" {
		leaderID = "(unknown)"
	}
	d.leaderBar.SetText(fmt.Sprintf("[yellow]Leader: %s[-]  [gray]refresh every %s[-]  [white]q=quit  p=put  g=get[-]", leaderID, d.refresh))
}

// handleCommand is called when the user presses Enter in the input field.
func (d *Dashboard) handleCommand(key tcell.Key) {
	if key != tcell.KeyEnter {
		return
	}
	line := strings.TrimSpace(d.input.GetText())
	d.input.SetText("")

	if line == "" {
		return
	}

	parts := strings.Fields(line)
	cmd := parts[0]

	switch cmd {
	case "q":
		d.app.Stop()

	case "p":
		if len(parts) < 3 {
			d.logOutput("[red]usage: p <key> <value>[-]")
			return
		}
		key, value := parts[1], strings.Join(parts[2:], " ")
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := d.cl.Put(ctx, key, value)
			d.app.QueueUpdateDraw(func() {
				if err != nil {
					d.logOutput(fmt.Sprintf("[red]Put(%q) error: %v[-]", key, err))
				} else {
					d.logOutput(fmt.Sprintf("[green]Put(%q, %q) OK[-]", key, value))
				}
			})
		}()

	case "g":
		if len(parts) < 2 {
			d.logOutput("[red]usage: g <key>[-]")
			return
		}
		key := parts[1]
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			val, found, err := d.cl.Get(ctx, key)
			d.app.QueueUpdateDraw(func() {
				if err != nil {
					d.logOutput(fmt.Sprintf("[red]Get(%q) error: %v[-]", key, err))
				} else if !found {
					d.logOutput(fmt.Sprintf("[yellow]Get(%q) → (not found)[-]", key))
				} else {
					d.logOutput(fmt.Sprintf("[green]Get(%q) → %q[-]", key, val))
				}
			})
		}()

	default:
		d.logOutput(fmt.Sprintf("[red]unknown command %q — use p <key> <val>, g <key>, or q[-]", cmd))
	}
}

// logOutput appends a timestamped line to the output panel.
func (d *Dashboard) logOutput(msg string) {
	ts := time.Now().Format("15:04:05")
	fmt.Fprintf(d.output, "[gray]%s[-] %s\n", ts, msg)
	d.output.ScrollToEnd()
}

// Close releases the per-node gRPC connections held by the Dashboard.
// The client passed to New is NOT closed here; the caller owns its lifetime.
func (d *Dashboard) Close() {
	for _, cc := range d.conns {
		_ = cc.Close()
	}
}
