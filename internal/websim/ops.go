package websim

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"time"

	"github.com/NeilP211/distkv/internal/raft"
	"github.com/NeilP211/distkv/internal/server"
	"github.com/NeilP211/distkv/internal/store"
)

// retryInterval is how long the leader-aware ops wait before re-resolving the
// leader after a transient failure (no leader yet, or a stale leader).
const retryInterval = 20 * time.Millisecond

// LogEntryView is a display-friendly projection of one Raft log entry.
type LogEntryView struct {
	Index   uint64 `json:"index"`
	Term    uint64 `json:"term"`
	Kind    string `json:"kind"`    // "cmd" | "noop" | "conf"
	Summary string `json:"summary"` // e.g. "PUT a=1", "DEL a", "CAS a (x→y)"
}

// leaderNode returns the current leader's RaftNode when exactly one node both
// claims the Leader role and names itself leader.
func (c *Cluster) leaderNode() (*server.RaftNode, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, nd := range c.nodes {
		s := nd.rn.Status()
		if s.Role == "Leader" && s.Leader == id {
			return nd.rn, true
		}
	}
	return nil, false
}

// isTransient reports whether err is a retryable leadership error (as opposed
// to a definitive application error like a CAS mismatch).
func isTransient(err error) bool {
	return errors.Is(err, raft.ErrNotLeader) || errors.Is(err, server.ErrLeadershipLost)
}

// sleepCtx waits for d or until ctx is done; it returns false if ctx is done.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// propose issues cmd against the current leader, retrying on transient
// leadership errors until ctx is done. Definitive application errors (CAS
// mismatch, unknown op) and context errors are returned immediately.
func (c *Cluster) propose(ctx context.Context, cmd store.Command) (string, error) {
	for {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		rn, ok := c.leaderNode()
		if !ok {
			if !sleepCtx(ctx, retryInterval) {
				return "", errors.New("no leader: cluster has no quorum")
			}
			continue
		}
		v, err := rn.Propose(ctx, cmd)
		if err == nil {
			return v, nil
		}
		if isTransient(err) {
			if !sleepCtx(ctx, retryInterval) {
				return "", errors.New("no leader: cluster has no quorum")
			}
			continue
		}
		return "", err
	}
}

// Put sets key=value via the leader and returns the committed value.
func (c *Cluster) Put(ctx context.Context, key, value string) (string, error) {
	return c.propose(ctx, store.Command{Op: store.OpPut, Key: key, Value: value})
}

// Delete removes key via the leader.
func (c *Cluster) Delete(ctx context.Context, key string) (string, error) {
	return c.propose(ctx, store.Command{Op: store.OpDelete, Key: key})
}

// CAS sets key=value only if its current value equals expect. A mismatch
// returns store.ErrCASMismatch.
func (c *Cluster) CAS(ctx context.Context, key, expect, value string) (string, error) {
	return c.propose(ctx, store.Command{Op: store.OpCAS, Key: key, ExpectValue: expect, Value: value})
}

// Get performs a linearizable read of key against the leader, retrying on
// transient leadership errors until ctx is done.
func (c *Cluster) Get(ctx context.Context, key string) (string, bool, error) {
	for {
		if ctx.Err() != nil {
			return "", false, ctx.Err()
		}
		rn, ok := c.leaderNode()
		if !ok {
			if !sleepCtx(ctx, retryInterval) {
				return "", false, errors.New("no leader: cluster has no quorum")
			}
			continue
		}
		v, found, err := rn.LinearizableGet(ctx, key)
		if err == nil {
			return v, found, nil
		}
		if isTransient(err) {
			if !sleepCtx(ctx, retryInterval) {
				return "", false, errors.New("no leader: cluster has no quorum")
			}
			continue
		}
		return "", false, err
	}
}

// NodeLog returns the display views of every retained log entry for node id.
func (c *Cluster) NodeLog(id string) []LogEntryView {
	c.mu.Lock()
	nd := c.nodes[raft.NodeID(id)]
	c.mu.Unlock()
	if nd == nil {
		return nil
	}
	first := nd.rn.FirstIndex()
	last := nd.rn.LastIndex()
	if last < first {
		return nil
	}
	entries := nd.rn.LogEntries(first, last+1) // half-open [first, last+1)
	views := make([]LogEntryView, 0, len(entries))
	for _, e := range entries {
		views = append(views, entryView(e))
	}
	return views
}

// entryView projects a raft.LogEntry into a display view, decoding the command
// payload for normal entries.
func entryView(e raft.LogEntry) LogEntryView {
	v := LogEntryView{Index: e.Index, Term: e.Term}
	switch e.Type {
	case raft.EntryNoop:
		v.Kind, v.Summary = "noop", "noop"
	case raft.EntryConfChange:
		v.Kind, v.Summary = "conf", "conf change"
	default: // EntryNormal
		v.Kind = "cmd"
		v.Summary = commandSummary(e.Data)
	}
	return v
}

// commandSummary decodes a gob-encoded store.Command (the same encoding the
// server writes) into a short human-readable string.
func commandSummary(data []byte) string {
	var cmd store.Command
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&cmd); err != nil {
		return "?"
	}
	switch cmd.Op {
	case store.OpPut:
		return fmt.Sprintf("PUT %s=%s", cmd.Key, cmd.Value)
	case store.OpDelete:
		return fmt.Sprintf("DEL %s", cmd.Key)
	case store.OpCAS:
		return fmt.Sprintf("CAS %s (%s→%s)", cmd.Key, cmd.ExpectValue, cmd.Value)
	default:
		return "?"
	}
}
