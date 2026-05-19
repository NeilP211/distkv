package transport_test

import (
	"reflect"
	"testing"

	"github.com/NeilP211/distkv/internal/raft"
	"github.com/NeilP211/distkv/internal/transport"
)

// roundTrip converts m to proto and back, returning the result.
func roundTrip(m raft.Message) raft.Message {
	return transport.FromProto(transport.ToProto(m))
}

func TestConvertRoundTrip(t *testing.T) {
	t.Parallel()

	snap := &raft.Snapshot{Index: 99, Term: 3, Data: []byte("snap-data")}
	// A snapshot taken mid-membership-change carries a joint ClusterConfig;
	// the voters/old_voters/joint fields must survive the proto round-trip.
	snapWithConf := &raft.Snapshot{
		Index: 120, Term: 4, Data: []byte("joint-snap"),
		Conf: raft.ClusterConfig{
			Voters:    []raft.NodeID{"n1", "n2", "n3", "n4"},
			OldVoters: []raft.NodeID{"n1", "n2", "n3"},
			Joint:     true,
		},
	}
	entries := []raft.LogEntry{
		{Term: 2, Index: 10, Type: raft.EntryNormal, Data: []byte("cmd1")},
		{Term: 2, Index: 11, Type: raft.EntryConfChange, Data: []byte("cfg")},
		{Term: 3, Index: 12, Type: raft.EntryNoop, Data: nil},
	}

	cases := []struct {
		name string
		msg  raft.Message
	}{
		{
			name: "MsgRequestVote",
			msg: raft.Message{
				Type:         raft.MsgRequestVote,
				From:         "node1",
				To:           "node2",
				Term:         5,
				LastLogIndex: 42,
				LastLogTerm:  4,
			},
		},
		{
			name: "MsgRequestVoteResp_granted",
			msg: raft.Message{
				Type:        raft.MsgRequestVoteResp,
				From:        "node2",
				To:          "node1",
				Term:        5,
				VoteGranted: true,
			},
		},
		{
			name: "MsgRequestVoteResp_denied",
			msg: raft.Message{
				Type:        raft.MsgRequestVoteResp,
				From:        "node3",
				To:          "node1",
				Term:        5,
				VoteGranted: false,
			},
		},
		{
			name: "MsgAppendEntries_with_entries",
			msg: raft.Message{
				Type:         raft.MsgAppendEntries,
				From:         "leader",
				To:           "follower",
				Term:         7,
				PrevLogIndex: 9,
				PrevLogTerm:  6,
				LeaderCommit: 8,
				Entries:      entries,
				ReadID:       123,
			},
		},
		{
			name: "MsgAppendEntries_empty_entries",
			msg: raft.Message{
				Type:         raft.MsgAppendEntries,
				From:         "leader",
				To:           "follower",
				Term:         7,
				PrevLogIndex: 9,
				PrevLogTerm:  6,
				LeaderCommit: 8,
				Entries:      nil,
			},
		},
		{
			name: "MsgAppendEntriesResp_success",
			msg: raft.Message{
				Type:    raft.MsgAppendEntriesResp,
				From:    "follower",
				To:      "leader",
				Term:    7,
				Success: true,
			},
		},
		{
			name: "MsgAppendEntriesResp_conflict",
			msg: raft.Message{
				Type:          raft.MsgAppendEntriesResp,
				From:          "follower",
				To:            "leader",
				Term:          7,
				Success:       false,
				ConflictIndex: 5,
				ConflictTerm:  3,
			},
		},
		{
			name: "MsgInstallSnapshot",
			msg: raft.Message{
				Type:     raft.MsgInstallSnapshot,
				From:     "leader",
				To:       "follower",
				Term:     10,
				Snapshot: snap,
			},
		},
		{
			name: "MsgInstallSnapshot_joint_conf",
			msg: raft.Message{
				Type:     raft.MsgInstallSnapshot,
				From:     "leader",
				To:       "follower",
				Term:     11,
				Snapshot: snapWithConf,
			},
		},
		{
			name: "MsgInstallSnapshotResp",
			msg: raft.Message{
				Type:    raft.MsgInstallSnapshotResp,
				From:    "follower",
				To:      "leader",
				Term:    10,
				Success: true,
			},
		},
		{
			name: "nil_snapshot_empty_entries",
			msg: raft.Message{
				Type:     raft.MsgAppendEntries,
				From:     "a",
				To:       "b",
				Term:     1,
				Snapshot: nil,
				Entries:  nil,
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := roundTrip(tc.msg)
			if !reflect.DeepEqual(tc.msg, got) {
				t.Errorf("round-trip mismatch\n  want: %+v\n  got:  %+v", tc.msg, got)
			}
		})
	}
}
