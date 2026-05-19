// Package raft defines the wire types shared by the Raft algorithm
// (Phase 4) and all transport implementations (Phase 3+).  No Raft
// algorithm logic lives here.
package raft

// NodeID is the stable identifier of a cluster member.
type NodeID string

// EntryType distinguishes the kind of replicated log entry.
type EntryType int

const (
	// EntryNormal carries an application command.
	EntryNormal EntryType = iota
	// EntryConfChange carries a cluster-membership change.
	EntryConfChange
	// EntryNoop is written by a new leader to commit prior-term entries.
	EntryNoop
)

// LogEntry is a single record in the Raft replicated log.
type LogEntry struct {
	// Term is the election term in which the entry was created.
	Term uint64
	// Index is the one-based position of this entry in the log.
	Index uint64
	// Type classifies the entry payload.
	Type EntryType
	// Data is the opaque application payload.
	Data []byte
}

// MsgType identifies the kind of Raft RPC being carried by a Message.
type MsgType int

const (
	// MsgRequestVote is sent by a candidate to solicit a vote.
	MsgRequestVote MsgType = iota
	// MsgRequestVoteResp carries a vote grant or denial.
	MsgRequestVoteResp
	// MsgAppendEntries is sent by the leader to replicate log entries
	// and serve as a heartbeat.
	MsgAppendEntries
	// MsgAppendEntriesResp acknowledges (or rejects) an AppendEntries RPC.
	MsgAppendEntriesResp
	// MsgInstallSnapshot is sent by the leader when a follower is too
	// far behind to catch up via log entries alone.
	MsgInstallSnapshot
	// MsgInstallSnapshotResp acknowledges an InstallSnapshot RPC.
	MsgInstallSnapshotResp
)

// String returns a human-readable name for the MsgType, useful for
// logging and debugging.
func (t MsgType) String() string {
	switch t {
	case MsgRequestVote:
		return "MsgRequestVote"
	case MsgRequestVoteResp:
		return "MsgRequestVoteResp"
	case MsgAppendEntries:
		return "MsgAppendEntries"
	case MsgAppendEntriesResp:
		return "MsgAppendEntriesResp"
	case MsgInstallSnapshot:
		return "MsgInstallSnapshot"
	case MsgInstallSnapshotResp:
		return "MsgInstallSnapshotResp"
	default:
		return "MsgUnknown"
	}
}

// Snapshot is a point-in-time image of the state machine used when a
// follower is too far behind to be caught up via log entries alone.
type Snapshot struct {
	// Index is the log index of the last entry covered by this snapshot.
	Index uint64
	// Term is the election term of the last entry covered by this snapshot.
	Term uint64
	// Data is the opaque serialised state-machine image.
	Data []byte
	// Conf is the cluster-membership configuration in effect at the snapshot
	// point.  A node installing the snapshot restores its ClusterConfig from
	// this field; a restarted node seeds membership replay from it.
	Conf ClusterConfig
}

// Message is the unified Raft RPC envelope.  All inter-node communication
// in the Raft algorithm uses this type; the transport layer is responsible
// for serialising and deserialising it.
type Message struct {
	// Type identifies which Raft RPC this message carries.
	Type MsgType
	// From is the NodeID of the sender.
	From NodeID
	// To is the NodeID of the intended recipient.
	To NodeID
	// Term is the sender's current election term.
	Term uint64

	// RequestVote / RequestVoteResp fields.
	LastLogIndex uint64
	LastLogTerm  uint64
	VoteGranted  bool

	// AppendEntries / AppendEntriesResp fields.
	PrevLogIndex  uint64
	PrevLogTerm   uint64
	LeaderCommit  uint64
	Entries       []LogEntry
	Success       bool
	ConflictIndex uint64
	ConflictTerm  uint64

	// InstallSnapshot field.
	Snapshot *Snapshot

	// ReadID is piggybacked on AppendEntries for linearisable read-index
	// optimisation.
	ReadID uint64
}
