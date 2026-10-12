// Package raft.
package raft

import (
	"errors"
	"log/slog"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type NodeState string

const (
	Follower  NodeState = "follower"
	Candidate NodeState = "candidate"
	Leader    NodeState = "leader"
)

const (
	// Heartbeat Interval (e.g., 50ms) << Min Election Timeout (e.g., 150ms)
	ElectionTimeout  = 150 * time.Millisecond
	HeartBeatTimeout = 50 * time.Millisecond
	Jitter           = 150
)

func RandomElectionTimeout() time.Duration {
	return ElectionTimeout + time.Duration(rand.Intn(Jitter))*time.Millisecond
}

type ExecCmdReply struct {
	LeaderID       *int
	PossibleLeader *int
	Success        bool
}

const SetCmd string = "set"

func IsValidCmd(cmd, val string) bool {
	if cmd != SetCmd {
		return false
	}
	before, after, found := strings.Cut(val, "=")
	if !found || before == "" || (before[0] >= '0' && before[0] <= '9') || after == "" {
		return false
	}
	return true
}

type LogEntry struct {
	Term    int
	Command string
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	FromID  int
	Term    int
	Success bool
}

type RequestVoteArgs struct {
	NodeID int
	Term   int
}
type VoteReply struct {
	FromID      int
	Term        int
	VoteGranted bool
}

type Node struct {
	NodeID        int
	LeaderID      *int
	State         NodeState
	Term          int
	VotedFor      *int
	Addr          string
	Log           []LogEntry
	Peers         map[int]string
	NextIndex     map[string]int
	MatchIndex    map[string]int
	ElectionTimer *time.Timer
	Mu            sync.RWMutex
}

func (n *Node) RequestVote(voteChan chan<- VoteReply) {
	for peerID, peerAddr := range n.Peers {
		if peerAddr == n.Addr {
			continue
		}
		slog.Info("Processing new rpc call", "peer", peerID, "address", peerAddr)

		go func(addr string) {
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				return
			}
			defer client.Close()

			var voteReply VoteReply
			candidate := RequestVoteArgs{NodeID: n.NodeID, Term: n.Term}
			if err := client.Call("Node.GiveVote", &candidate, &voteReply); err != nil {
				slog.Error("RPC voting call failed", "target", addr, "error", err)
				return
			}
			voteChan <- voteReply
		}(peerAddr)
	}
}

func (n *Node) GiveVote(candidate *RequestVoteArgs, reply *VoteReply) error {
	n.Mu.Lock()
	defer n.Mu.Unlock()
	reply.FromID = n.NodeID
	reply.Term = n.Term
	reply.VoteGranted = false

	// If candidate's term is smaller than ours, say NO.
	if candidate.Term < n.Term {
		return nil
	}
	// Update term and clear past votes
	// If candidate has a newer term, step down to FOLLOWER and clear our past vote so we can vote in this new term.
	if candidate.Term > n.Term {
		n.State = Follower
		n.Term = candidate.Term
		reply.Term = n.Term
		n.NextIndex = nil
		n.VotedFor = nil
		n.LeaderID = nil
	}
	// Grant vote if:
	// We haven't voted yet in this term (n.VotedFor == nil) or we already voted for this same candidate (n.VotedFor == args.CandidateID)
	if n.VotedFor == nil || *n.VotedFor == candidate.NodeID {
		n.VotedFor = &candidate.NodeID
		reply.VoteGranted = true
		n.ElectionTimer.Reset(RandomElectionTimeout())
	}
	return nil
}

func (n *Node) SendHeartBeat() {
	for peerID, peerAddr := range n.Peers {
		if peerAddr == n.Addr {
			continue
		}
		slog.Info("Sending heartbeat through RPC", "leader", n.NodeID, "follower", peerID)
		go func(addr string) {
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				return
			}
			defer client.Close()
			var appendReply AppendEntriesReply
			entriesArgs := AppendEntriesArgs{
				LeaderID: n.NodeID,
				Term:     n.Term,
			}
			if err := client.Call("Node.AppendEntry", &entriesArgs, &appendReply); err != nil {
				slog.Error("RPC heartbeat call failed", "target", peerID, "error", err)
				return
			}
		}(peerAddr)
	}
}

func (n *Node) AppendEntry(entry *AppendEntriesArgs, reply *AppendEntriesReply) error {
	n.Mu.Lock()
	defer n.Mu.Unlock()
	reply.FromID = n.NodeID
	reply.Term = n.Term
	reply.Success = false
	// Reply false if term < currentTerm (§5.1)
	if entry.Term < n.Term {
		return nil
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if entry.PrevLogIndex >= len(n.Log) || entry.PrevLogTerm != n.Log[entry.PrevLogIndex].Term {
		return nil
	}
	for idx, currentEntry := range entry.Entries {
		currentIdx := entry.PrevLogIndex + 1 + idx
		if currentIdx >= len(n.Log) {
			// Append any new entries not already in the log
			n.Log = append(n.Log, entry.Entries[idx:]...)
			break
		}
		// If an existing entry conflicts with a new one (same index but different terms),
		// delete the existing entry and all that follow it (§5.3)
		if existingEntry := n.Log[currentIdx]; existingEntry.Term != currentEntry.Term {
			n.Log = n.Log[:currentIdx]
			n.Log = append(n.Log, entry.Entries[idx:]...)
			break
		}
	}
	slog.Info("Current log", "entries", n.Log, "leader", entry.LeaderID, "follower", n.NodeID)
	n.State = Follower
	n.Term = entry.Term
	n.LeaderID = &entry.LeaderID
	n.ElectionTimer.Reset(RandomElectionTimeout())
	reply.Success = true
	return nil
}

func (n *Node) ExecuteCommand(cmd *string, reply *ExecCmdReply) error {
	n.Mu.Lock()
	defer n.Mu.Unlock()
	if n.LeaderID != nil && *n.LeaderID == n.NodeID {
		reply.LeaderID = &n.NodeID
		reply.Success = true
		n.Log = append(n.Log, LogEntry{Term: n.Term, Command: *cmd})
		for peerID, peerAddr := range n.Peers {
			if peerAddr == n.Addr {
				continue
			}
			slog.Info("Sending command to replicas through RPC", "leader", reply.LeaderID, "replica", peerAddr)
			go func(addr string) {
				client, err := rpc.Dial("tcp", addr)
				if err != nil {
					return
				}
				defer client.Close()
				var appendReply AppendEntriesReply
				nextIdx := n.NextIndex[addr]
				entriesArgs := AppendEntriesArgs{
					LeaderID:     *reply.LeaderID,
					Term:         n.Term,
					Entries:      n.Log[nextIdx:],
					PrevLogIndex: nextIdx - 1,
					PrevLogTerm:  n.Log[nextIdx-1].Term,
				}
				if err := client.Call("Node.AppendEntry", &entriesArgs, &appendReply); err != nil {
					slog.Error("RPC append entry call failed", "target", peerID, "error", err)
				}
			}(peerAddr)

		}
	} else {
		reply.PossibleLeader = n.LeaderID
		reply.Success = false
	}
	return nil
}

func InitIndexMap(peers map[int]string, nodeID, startIdx int) map[string]int {
	nextIndex := map[string]int{}
	for peerID, peerAddr := range peers {
		if peerID != nodeID {
			nextIndex[peerAddr] = startIdx
		}
	}
	return nextIndex
}

func MustLoadNode() *Node {
	nodeID, ok := os.LookupEnv("NODE_ID")
	if !ok {
		panic(errors.New("failed to load Node id"))
	}
	nodeIDInt, err := strconv.Atoi(nodeID)
	if err != nil {
		panic(errors.New("failed to cast env variable NODE_ID into int"))
	}
	addr, ok := os.LookupEnv("ADDRESS")
	if !ok {
		panic(errors.New("failed to load ADDRESS"))
	}
	peersMap, err := LoadPeers()
	if err != nil {
		panic(err)
	}
	return &Node{
		NodeID:        nodeIDInt,
		State:         Follower,
		Term:          0,
		Addr:          addr,
		Log:           []LogEntry{{}},
		Peers:         peersMap,
		ElectionTimer: time.NewTimer(RandomElectionTimeout()),
	}
}
