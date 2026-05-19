// Package client is a leader-aware, retrying client for a DistKV cluster.
//
// A Client is constructed from the host:port addresses of every cluster node.
// Each operation dials a node's KV gRPC service; if the contacted node is not
// the leader it answers with a leader_hint and the Client transparently
// retries against the hinted endpoint.  If an endpoint is unavailable the
// Client tries the next one.  Retries use bounded exponential backoff and are
// capped by a configurable attempt budget.
package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/NeilP211/distkv/api"
)

// Default retry tuning.  These are exported-as-defaults via Option setters.
const (
	defaultMaxAttempts = 5
	defaultBaseBackoff = 20 * time.Millisecond
	defaultMaxBackoff  = 500 * time.Millisecond
	// attemptTimeout bounds a single RPC attempt so a stuck call — a node
	// mid-election whose Propose blocks because an appended entry never
	// commits, or a connection in transient-failure backoff — eventually
	// fails and the retry loop moves on instead of burning the whole
	// operation deadline on one endpoint.  It is comfortably above the
	// latency of a healthy leader committing one entry.  A genuinely
	// unreachable node fails fast with codes.Unavailable (connection
	// refused) regardless of this bound.
	attemptTimeout = 2 * time.Second
)

// Option configures a Client.
type Option func(*Client)

// WithMaxAttempts sets the retry budget: the maximum number of RPC attempts a
// single operation makes before giving up.  Values < 1 are clamped to 1.
func WithMaxAttempts(n int) Option {
	return func(c *Client) {
		if n < 1 {
			n = 1
		}
		c.maxAttempts = n
	}
}

// WithBackoff sets the base and maximum backoff durations used between retry
// attempts.  Backoff doubles each attempt up to the maximum.
func WithBackoff(base, max time.Duration) Option {
	return func(c *Client) {
		if base > 0 {
			c.baseBackoff = base
		}
		if max >= base && max > 0 {
			c.maxBackoff = max
		}
	}
}

// Client is a leader-aware DistKV client.  All methods are safe for concurrent
// use.  Call Close to release the cached gRPC connections.
type Client struct {
	endpoints []string

	maxAttempts int
	baseBackoff time.Duration
	maxBackoff  time.Duration

	mu     sync.Mutex
	conns  map[string]*grpc.ClientConn // endpoint -> cached connection
	leader string                      // best guess at the current leader endpoint
	closed bool
}

// New constructs a Client over the given cluster endpoints (host:port).  At
// least one endpoint is required for any operation to succeed.
func New(endpoints []string, opts ...Option) *Client {
	eps := make([]string, len(endpoints))
	copy(eps, endpoints)
	c := &Client{
		endpoints:   eps,
		maxAttempts: defaultMaxAttempts,
		baseBackoff: defaultBaseBackoff,
		maxBackoff:  defaultMaxBackoff,
		conns:       make(map[string]*grpc.ClientConn),
	}
	if len(eps) > 0 {
		c.leader = eps[0]
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Close releases every cached gRPC connection.  It is idempotent.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	for ep, conn := range c.conns {
		_ = conn.Close()
		delete(c.conns, ep)
	}
	return nil
}

// StatusInfo is a snapshot of cluster status returned by Status.
type StatusInfo struct {
	// LeaderID is the node id of the current leader ("" if unknown).
	LeaderID string
	// Term is the current Raft election term.
	Term uint64
	// Role is the contacted node's role ("Leader"/"Follower"/"Candidate").
	Role string
	// CommitIndex is the highest committed log index on the contacted node.
	CommitIndex uint64
	// Members lists every cluster member's node id.
	Members []string
}

// conn returns a cached *grpc.ClientConn for endpoint, dialing one lazily.
func (c *Client) conn(endpoint string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("client: closed")
	}
	if cc, ok := c.conns[endpoint]; ok {
		return cc, nil
	}
	cc, err := grpc.NewClient(endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.conns[endpoint] = cc
	return cc, nil
}

// orderedEndpoints returns the endpoints to try, with the current leader guess
// first so the common case contacts the leader directly.
func (c *Client) orderedEndpoints() []string {
	c.mu.Lock()
	leader := c.leader
	c.mu.Unlock()

	out := make([]string, 0, len(c.endpoints))
	if leader != "" {
		out = append(out, leader)
	}
	for _, ep := range c.endpoints {
		if ep != leader {
			out = append(out, ep)
		}
	}
	return out
}

// setLeader records endpoint as the best guess at the current leader.
func (c *Client) setLeader(endpoint string) {
	if endpoint == "" {
		return
	}
	c.mu.Lock()
	c.leader = endpoint
	c.mu.Unlock()
}

// backoff sleeps for the exponential backoff of the given attempt (0-based),
// returning early if ctx is done.
func (c *Client) backoff(ctx context.Context, attempt int) error {
	d := c.baseBackoff << attempt
	if d > c.maxBackoff || d <= 0 {
		d = c.maxBackoff
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// call drives the leader-aware retry loop.  fn is invoked with a per-attempt
// context (bounded by attemptTimeout) and a KV client for a candidate
// endpoint; it returns (retry, leaderHint, err):
//   - retry=false, err=nil  -> success, call returns nil.
//   - retry=true            -> not-leader/unavailable, try another endpoint.
//   - retry=false, err!=nil -> a terminal application error, returned as-is.
//
// leaderHint, when non-empty, redirects the next attempt to that endpoint.
//
// Each attempt is bounded by attemptTimeout so a stuck call is abandoned and
// another endpoint tried; an Unavailable or DeadlineExceeded error advances
// to the next endpoint, while any other gRPC error is returned as-is.  CAS,
// the one non-idempotent operation, makes itself safe to retry by reading the
// key before each retried swap (see CAS).
func (c *Client) call(ctx context.Context, fn func(context.Context, api.KVClient) (retry bool, leaderHint string, err error)) error {
	if len(c.endpoints) == 0 {
		return errors.New("client: no endpoints configured")
	}

	var lastErr error
	endpoints := c.orderedEndpoints()
	epIdx := 0

	for attempt := 0; attempt < c.maxAttempts; attempt++ {
		if attempt > 0 {
			if err := c.backoff(ctx, attempt-1); err != nil {
				return err
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		endpoint := endpoints[epIdx%len(endpoints)]
		conn, err := c.conn(endpoint)
		if err != nil {
			lastErr = err
			epIdx++
			continue
		}

		// Cap each attempt with attemptTimeout so a stuck connection or a
		// blocked Propose is abandoned and another endpoint tried.
		actx, acancel := context.WithTimeout(ctx, attemptTimeout)
		retry, hint, err := fn(actx, api.NewKVClient(conn))
		acancel()
		if err != nil {
			// The parent ctx being done is terminal — the caller's deadline
			// has passed.  Otherwise codes.Unavailable/DeadlineExceeded mean
			// this endpoint is unhealthy; try the next one.  Any other gRPC
			// error is a genuine fault and is returned as-is.
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
			switch status.Code(err) {
			case codes.Unavailable, codes.DeadlineExceeded:
				lastErr = err
				epIdx++
				continue
			}
			return err
		}
		if !retry {
			c.setLeader(endpoint)
			return nil
		}

		// Not-leader response: redirect to the hint if it names a real
		// endpoint, else round-robin to the next endpoint.
		lastErr = fmt.Errorf("client: endpoint %s is not the leader", endpoint)
		if hint != "" && c.knownEndpoint(hint) {
			c.setLeader(hint)
			endpoints = c.orderedEndpoints()
			epIdx = 0
		} else {
			epIdx++
		}
	}
	if lastErr == nil {
		lastErr = errors.New("client: retry budget exhausted")
	}
	return fmt.Errorf("client: operation failed after %d attempts: %w", c.maxAttempts, lastErr)
}

// knownEndpoint reports whether ep is one of the configured endpoints.
func (c *Client) knownEndpoint(ep string) bool {
	for _, e := range c.endpoints {
		if e == ep {
			return true
		}
	}
	return false
}

// Put sets key to value.
func (c *Client) Put(ctx context.Context, key, value string) error {
	return c.call(ctx, func(actx context.Context, kv api.KVClient) (bool, string, error) {
		resp, err := kv.Put(actx, &api.PutReq{Key: key, Value: []byte(value)})
		if err != nil {
			return false, "", err
		}
		if !resp.GetSuccess() {
			return true, resp.GetLeaderHint(), nil
		}
		return false, "", nil
	})
}

// Delete removes key.
func (c *Client) Delete(ctx context.Context, key string) error {
	return c.call(ctx, func(actx context.Context, kv api.KVClient) (bool, string, error) {
		resp, err := kv.Delete(actx, &api.DeleteReq{Key: key})
		if err != nil {
			return false, "", err
		}
		if !resp.GetSuccess() {
			return true, resp.GetLeaderHint(), nil
		}
		return false, "", nil
	})
}

// Get retrieves the value for key, performing a linearizable read on the
// leader.  found is false if the key is absent.
//
// The KV Get RPC carries no leader_hint, so a non-leader and a genuine
// missing key both answer found=false.  To stay leader-aware, each attempt
// first calls Status on the contacted node: if that node is not the leader
// the attempt is retried (redirected by Status's leader id when it maps to a
// known endpoint), and only a leader's Get answer is accepted as definitive.
func (c *Client) Get(ctx context.Context, key string) (value string, found bool, err error) {
	cerr := c.call(ctx, func(actx context.Context, kv api.KVClient) (bool, string, error) {
		st, rpcErr := kv.Status(actx, &api.StatusReq{})
		if rpcErr != nil {
			return false, "", rpcErr
		}
		if st.GetRole() != "Leader" {
			// Not the leader: retry.  StatusResp carries the leader's node
			// id; pass it as a hint so call redirects when it happens to
			// match a configured endpoint, otherwise call round-robins.
			return true, st.GetLeaderId(), nil
		}
		resp, rpcErr := kv.Get(actx, &api.GetReq{Key: key})
		if rpcErr != nil {
			// codes.Unavailable (leadership lost mid-read) bubbles up as a
			// retryable error inside call.
			return false, "", rpcErr
		}
		value = string(resp.GetValue())
		found = resp.GetFound()
		return false, "", nil
	})
	if cerr != nil {
		return "", false, cerr
	}
	return value, found, nil
}

// CAS performs a compare-and-swap: it sets key to value only if the current
// value equals expect.  It returns whether the swap was applied.
//
// CAS is not naturally idempotent, so a retry after an ambiguous transport
// failure (the prior attempt may or may not have reached and been applied by
// the leader) must not blindly re-issue the swap.  Each attempt is therefore
// preceded — on every attempt after the first — by a leadership-confirmed
// read of the key:
//   - if the key already holds value, a previous attempt succeeded -> true.
//   - if the key holds expect, the swap has not happened -> issue the CAS.
//   - otherwise the value is something else -> genuine mismatch -> false.
//
// This makes CAS safe to retry across leader changes and dropped responses
// while still returning the correct application outcome.
func (c *Client) CAS(ctx context.Context, key, expect, value string) (bool, error) {
	swapped := false
	attempt := 0
	err := c.call(ctx, func(actx context.Context, kv api.KVClient) (bool, string, error) {
		// Every attempt confirms it is talking to the leader via Status,
		// the same leader-aware probe Get uses.
		st, rpcErr := kv.Status(actx, &api.StatusReq{})
		if rpcErr != nil {
			return false, "", rpcErr
		}
		if st.GetRole() != "Leader" {
			return true, st.GetLeaderId(), nil
		}

		// On a retry, read the current value first so a CAS that already
		// succeeded (but whose response was lost) is not mistaken for a
		// mismatch, and a stale re-issue is avoided.
		if attempt > 0 {
			cur, rerr := kv.Get(actx, &api.GetReq{Key: key})
			if rerr != nil {
				return false, "", rerr
			}
			switch {
			case cur.GetFound() && string(cur.GetValue()) == value:
				swapped = true
				return false, "", nil
			case !cur.GetFound() && expect == "" && value == "":
				// Degenerate no-op CAS already satisfied.
				swapped = true
				return false, "", nil
			case (cur.GetFound() && string(cur.GetValue()) != expect) ||
				(!cur.GetFound() && expect != ""):
				// Current value differs from expect: genuine mismatch.
				swapped = false
				return false, "", nil
			}
			// Current value equals expect: fall through and issue the CAS.
		}
		attempt++

		resp, rpcErr := kv.CAS(actx, &api.CASReq{
			Key:         key,
			ExpectValue: []byte(expect),
			NewValue:    []byte(value),
		})
		if rpcErr != nil {
			return false, "", rpcErr
		}
		if resp.GetSuccess() {
			swapped = true
			return false, "", nil
		}
		// success=false with a leader_hint is a not-leader redirect;
		// success=false with no hint is a genuine CAS mismatch (an
		// application outcome, not an error).
		if resp.GetLeaderHint() != "" {
			return true, resp.GetLeaderHint(), nil
		}
		return false, "", nil
	})
	if err != nil {
		return false, err
	}
	return swapped, nil
}

// Status returns cluster status as reported by a reachable node.
func (c *Client) Status(ctx context.Context) (*StatusInfo, error) {
	var info *StatusInfo
	err := c.call(ctx, func(actx context.Context, kv api.KVClient) (bool, string, error) {
		resp, rpcErr := kv.Status(actx, &api.StatusReq{})
		if rpcErr != nil {
			return false, "", rpcErr
		}
		info = &StatusInfo{
			LeaderID:    resp.GetLeaderId(),
			Term:        resp.GetTerm(),
			Role:        resp.GetRole(),
			CommitIndex: resp.GetCommitIndex(),
			Members:     resp.GetMemberIds(),
		}
		return false, "", nil
	})
	if err != nil {
		return nil, err
	}
	return info, nil
}
