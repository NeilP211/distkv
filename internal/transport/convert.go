package transport

import (
	"fmt"

	"github.com/NeilP211/distkv/api"
	"github.com/NeilP211/distkv/internal/raft"
)

// ToProto converts a raft.Message to its protobuf wire representation.
func ToProto(m raft.Message) *api.Message {
	p := &api.Message{
		Type:          msgTypeToProto(m.Type),
		From:          string(m.From),
		To:            string(m.To),
		Term:          m.Term,
		LastLogIndex:  m.LastLogIndex,
		LastLogTerm:   m.LastLogTerm,
		VoteGranted:   m.VoteGranted,
		PrevLogIndex:  m.PrevLogIndex,
		PrevLogTerm:   m.PrevLogTerm,
		LeaderCommit:  m.LeaderCommit,
		Success:       m.Success,
		ConflictIndex: m.ConflictIndex,
		ConflictTerm:  m.ConflictTerm,
		ReadId:        m.ReadID,
	}

	// Convert log entries.
	if len(m.Entries) > 0 {
		p.Entries = make([]*api.LogEntry, len(m.Entries))
		for i, e := range m.Entries {
			p.Entries[i] = entryToProto(e)
		}
	}

	// Convert snapshot (nil-safe).
	if m.Snapshot != nil {
		p.Snapshot = snapshotToProto(m.Snapshot)
	}

	return p
}

// FromProto converts a protobuf Message back to a raft.Message.
func FromProto(p *api.Message) raft.Message {
	m := raft.Message{
		Type:          msgTypeFromProto(p.Type),
		From:          raft.NodeID(p.From),
		To:            raft.NodeID(p.To),
		Term:          p.Term,
		LastLogIndex:  p.LastLogIndex,
		LastLogTerm:   p.LastLogTerm,
		VoteGranted:   p.VoteGranted,
		PrevLogIndex:  p.PrevLogIndex,
		PrevLogTerm:   p.PrevLogTerm,
		LeaderCommit:  p.LeaderCommit,
		Success:       p.Success,
		ConflictIndex: p.ConflictIndex,
		ConflictTerm:  p.ConflictTerm,
		ReadID:        p.ReadId,
	}

	// Convert log entries.
	if len(p.Entries) > 0 {
		m.Entries = make([]raft.LogEntry, len(p.Entries))
		for i, e := range p.Entries {
			m.Entries[i] = entryFromProto(e)
		}
	}

	// Convert snapshot (nil-safe).
	if p.Snapshot != nil {
		m.Snapshot = snapshotFromProto(p.Snapshot)
	}

	return m
}

// ---- helpers ----------------------------------------------------------------

func entryToProto(e raft.LogEntry) *api.LogEntry {
	return &api.LogEntry{
		Term:  e.Term,
		Index: e.Index,
		Type:  entryTypeToProto(e.Type),
		Data:  e.Data,
	}
}

func entryFromProto(p *api.LogEntry) raft.LogEntry {
	return raft.LogEntry{
		Term:  p.Term,
		Index: p.Index,
		Type:  entryTypeFromProto(p.Type),
		Data:  p.Data,
	}
}

func snapshotToProto(s *raft.Snapshot) *api.Snapshot {
	if s == nil {
		return nil
	}
	return &api.Snapshot{
		Index:     s.Index,
		Term:      s.Term,
		Data:      s.Data,
		Voters:    nodeIDsToStrings(s.Conf.Voters),
		OldVoters: nodeIDsToStrings(s.Conf.OldVoters),
		Joint:     s.Conf.Joint,
	}
}

func snapshotFromProto(p *api.Snapshot) *raft.Snapshot {
	if p == nil {
		return nil
	}
	return &raft.Snapshot{
		Index: p.Index,
		Term:  p.Term,
		Data:  p.Data,
		Conf: raft.ClusterConfig{
			Voters:    stringsToNodeIDs(p.Voters),
			OldVoters: stringsToNodeIDs(p.OldVoters),
			Joint:     p.Joint,
		},
	}
}

func nodeIDsToStrings(ids []raft.NodeID) []string {
	if ids == nil {
		return nil
	}
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = string(id)
	}
	return out
}

func stringsToNodeIDs(ss []string) []raft.NodeID {
	if ss == nil {
		return nil
	}
	out := make([]raft.NodeID, len(ss))
	for i, s := range ss {
		out[i] = raft.NodeID(s)
	}
	return out
}

func msgTypeToProto(t raft.MsgType) api.MsgType {
	switch t {
	case raft.MsgRequestVote:
		return api.MsgType_MSG_REQUEST_VOTE
	case raft.MsgRequestVoteResp:
		return api.MsgType_MSG_REQUEST_VOTE_RESP
	case raft.MsgAppendEntries:
		return api.MsgType_MSG_APPEND_ENTRIES
	case raft.MsgAppendEntriesResp:
		return api.MsgType_MSG_APPEND_ENTRIES_RESP
	case raft.MsgInstallSnapshot:
		return api.MsgType_MSG_INSTALL_SNAPSHOT
	case raft.MsgInstallSnapshotResp:
		return api.MsgType_MSG_INSTALL_SNAPSHOT_RESP
	default:
		panic(fmt.Sprintf("transport: unknown MsgType %d", t))
	}
}

func msgTypeFromProto(t api.MsgType) raft.MsgType {
	switch t {
	case api.MsgType_MSG_REQUEST_VOTE:
		return raft.MsgRequestVote
	case api.MsgType_MSG_REQUEST_VOTE_RESP:
		return raft.MsgRequestVoteResp
	case api.MsgType_MSG_APPEND_ENTRIES:
		return raft.MsgAppendEntries
	case api.MsgType_MSG_APPEND_ENTRIES_RESP:
		return raft.MsgAppendEntriesResp
	case api.MsgType_MSG_INSTALL_SNAPSHOT:
		return raft.MsgInstallSnapshot
	case api.MsgType_MSG_INSTALL_SNAPSHOT_RESP:
		return raft.MsgInstallSnapshotResp
	default:
		panic(fmt.Sprintf("transport: unknown MsgType %d", t))
	}
}

func entryTypeToProto(t raft.EntryType) api.EntryType {
	switch t {
	case raft.EntryNormal:
		return api.EntryType_ENTRY_NORMAL
	case raft.EntryConfChange:
		return api.EntryType_ENTRY_CONF_CHANGE
	case raft.EntryNoop:
		return api.EntryType_ENTRY_NOOP
	default:
		panic(fmt.Sprintf("transport: unknown EntryType %d", t))
	}
}

func entryTypeFromProto(t api.EntryType) raft.EntryType {
	switch t {
	case api.EntryType_ENTRY_NORMAL:
		return raft.EntryNormal
	case api.EntryType_ENTRY_CONF_CHANGE:
		return raft.EntryConfChange
	case api.EntryType_ENTRY_NOOP:
		return raft.EntryNoop
	default:
		panic(fmt.Sprintf("transport: unknown EntryType %d", t))
	}
}
