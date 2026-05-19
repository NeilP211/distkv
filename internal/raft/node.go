package raft

import (
	"errors"
	"math/rand"
	"sort"
	"sync"
)

// Transport delivers a Raft message to another cluster member and returns the
// synchronous response.  It is defined here, rather than imported from
// internal/transport, to avoid an import cycle (the transport package depends
// on this one for the wire types).  Any value satisfying this interface —
// notably simnet's transport handle and the Phase 6 gRPC transport — can be
// supplied in Config.
type Transport interface {
	Send(to NodeID, msg Message) (Message, error)
}

// Role is the current Raft role of a Node.
type Role int

const (
	// Follower passively replicates the leader's log and grants votes.
	Follower Role = iota
	// Candidate is soliciting votes to become leader.
	Candidate
	// Leader replicates log entries and serves client proposals.
	Leader
)

// String returns the human-readable name of the role.
func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "UnknownRole"
	}
}

// Config carries everything needed to construct a Node.  This is the
// node-construction config; the cluster-membership configuration is a Phase 8
// concern and is unrelated.
type Config struct {
	// ID is this node's stable identifier.
	ID NodeID
	// Peers is every member of the cluster, including this node.
	Peers []NodeID
	// Storage persists the log, HardState, and snapshots.
	Storage Storage
	// Transport delivers messages to other cluster members.
	Transport Transport
	// ElectionTimeoutMin and ElectionTimeoutMax bound the randomized
	// election timeout, expressed in Tick units.
	ElectionTimeoutMin int
	ElectionTimeoutMax int
	// HeartbeatInterval is how many Ticks elapse between leader heartbeats.
	HeartbeatInterval int
}

// Node is a single Raft cluster member.  All state transitions happen under
// mu; the public API (Tick, Step, Propose, Ready, accessors) acquires it.
type Node struct {
	mu sync.Mutex

	id    NodeID
	peers []NodeID // sorted, includes self

	role        Role
	currentTerm uint64
	votedFor    NodeID
	leaderID    NodeID

	log         *raftLog
	commitIndex uint64
	lastApplied uint64

	// Leader-only per-peer replication progress.
	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64

	storage   Storage
	transport Transport

	// Election/heartbeat timing, in Tick units.
	electionElapsed    int
	heartbeatElapsed   int
	electionTimeout    int // randomized, re-rolled on each follower/candidate transition
	electionTimeoutMin int
	electionTimeoutMax int
	heartbeatInterval  int

	// votesGranted tracks votes received in the current candidate term.
	votesGranted map[NodeID]bool

	// pendingSnap holds a snapshot that was just installed via
	// MsgInstallSnapshot and has not yet been handed to the state machine.
	// hasPendingSnap distinguishes "no pending snapshot" from a zero-value
	// snapshot.  Both are guarded by mu and consumed by PendingSnapshot.
	pendingSnap    Snapshot
	hasPendingSnap bool

	rng *rand.Rand
}

// NewNode constructs a Node, loading any persisted HardState and building the
// raftLog over cfg.Storage.  The node starts as a Follower with a randomized
// election timeout.
func NewNode(cfg Config) (*Node, error) {
	if cfg.ElectionTimeoutMin <= 0 || cfg.ElectionTimeoutMax < cfg.ElectionTimeoutMin {
		return nil, errors.New("raft: invalid election timeout bounds")
	}
	if cfg.HeartbeatInterval <= 0 {
		return nil, errors.New("raft: invalid heartbeat interval")
	}
	hs, err := cfg.Storage.LoadHardState()
	if err != nil {
		return nil, err
	}
	rlog, err := newRaftLog(cfg.Storage)
	if err != nil {
		return nil, err
	}

	peers := make([]NodeID, len(cfg.Peers))
	copy(peers, cfg.Peers)
	sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })

	// If Storage already holds a snapshot at index N, everything it covers is
	// committed and applied by definition.  newRaftLog seeds commitIndex from
	// the snapshot; lastApplied must be seeded the same way, otherwise Ready()
	// would try to slice log entries at indices the snapshot has compacted
	// away (a known Phase 5 gap).
	snap, err := cfg.Storage.LoadSnapshot()
	if err != nil {
		return nil, err
	}

	n := &Node{
		id:                 cfg.ID,
		peers:              peers,
		role:               Follower,
		currentTerm:        hs.CurrentTerm,
		votedFor:           hs.VotedFor,
		log:                rlog,
		commitIndex:        rlog.commitIndex,
		lastApplied:        snap.Index,
		nextIndex:          make(map[NodeID]uint64),
		matchIndex:         make(map[NodeID]uint64),
		storage:            cfg.Storage,
		transport:          cfg.Transport,
		electionTimeoutMin: cfg.ElectionTimeoutMin,
		electionTimeoutMax: cfg.ElectionTimeoutMax,
		heartbeatInterval:  cfg.HeartbeatInterval,
		votesGranted:       make(map[NodeID]bool),
		rng:                rand.New(rand.NewSource(int64(hashID(cfg.ID)))), //nolint:gosec
	}
	n.resetElectionTimeout()
	return n, nil
}

// hashID derives a deterministic seed from a NodeID so each node's election
// timeout sequence differs, reducing split votes without nondeterminism.
func hashID(id NodeID) uint64 {
	var h uint64 = 1469598103934665603
	for i := 0; i < len(id); i++ {
		h ^= uint64(id[i])
		h *= 1099511628211
	}
	return h
}

// resetElectionTimeout re-rolls the randomized election timeout and clears the
// election-elapsed counter.  Caller must hold mu.
func (n *Node) resetElectionTimeout() {
	span := n.electionTimeoutMax - n.electionTimeoutMin
	if span > 0 {
		n.electionTimeout = n.electionTimeoutMin + n.rng.Intn(span+1)
	} else {
		n.electionTimeout = n.electionTimeoutMin
	}
	n.electionElapsed = 0
}

// persistHardState writes the current term and vote to stable storage.
// Caller must hold mu.
func (n *Node) persistHardState() {
	if err := n.storage.SaveHardState(HardState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
	}); err != nil {
		panic("raft: SaveHardState failed: " + err.Error())
	}
}

// becomeFollower transitions to Follower at the given term, recording leader
// as the known leader (which may be "").  Caller must hold mu.
func (n *Node) becomeFollower(term uint64, leader NodeID) {
	termChanged := term != n.currentTerm
	n.role = Follower
	n.leaderID = leader
	if termChanged {
		n.currentTerm = term
		n.votedFor = ""
	}
	n.votesGranted = make(map[NodeID]bool)
	n.resetElectionTimeout()
	n.heartbeatElapsed = 0
	if termChanged {
		n.persistHardState()
	}
}

// becomeCandidate transitions to Candidate: it increments the term, votes for
// itself, and resets the election timer.  Caller must hold mu.
//
// In a single-node cluster the self-vote alone constitutes a majority, so the
// node must promote itself to Leader within this same call path rather than
// waiting for a peer vote response that will never arrive.
func (n *Node) becomeCandidate() {
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = ""
	n.votesGranted = map[NodeID]bool{n.id: true}
	n.resetElectionTimeout()
	n.persistHardState()
	// The self-vote may already be a majority (cluster size 1).
	n.maybeBecomeLeader()
}

// maybeBecomeLeader promotes the node to Leader if it is still a Candidate and
// the votes granted so far constitute a majority of the cluster.  It is the
// single place the vote-majority check lives; both becomeCandidate (covering
// single-node clusters) and handleRequestVoteResp (covering multi-node
// clusters) call it.  It returns true iff the promotion happened.  Caller must
// hold mu.
func (n *Node) maybeBecomeLeader() bool {
	if n.role != Candidate {
		return false
	}
	if len(n.votesGranted) < n.quorum() {
		return false
	}
	n.becomeLeader()
	return true
}

// becomeLeader transitions to Leader: it initializes per-peer replication
// progress and appends a no-op entry so that entries from prior terms can be
// committed safely (§5.4.2).  Caller must hold mu.
func (n *Node) becomeLeader() {
	n.role = Leader
	n.leaderID = n.id
	n.heartbeatElapsed = 0

	// Append a no-op entry for the new term so prior-term entries can be
	// committed safely (§5.4.2).
	n.log.append([]LogEntry{{Term: n.currentTerm, Type: EntryNoop}})

	last := n.log.lastIndex()
	n.nextIndex = make(map[NodeID]uint64)
	n.matchIndex = make(map[NodeID]uint64)
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		// nextIndex starts one past the leader's last entry; the
		// conflict-backoff path walks it down for lagging followers.
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
	}
	n.matchIndex[n.id] = last
}

// quorum returns the number of nodes that constitute a majority.
func (n *Node) quorum() int {
	return len(n.peers)/2 + 1
}

// Role returns the node's current role.
func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// Term returns the node's current term.
func (n *Node) Term() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// Leader returns the node's currently known leader ("" if unknown).
func (n *Node) Leader() NodeID {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// CommitIndex returns the highest log index known to be committed.
func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// ID returns the node's stable identifier.
func (n *Node) ID() NodeID {
	return n.id
}
