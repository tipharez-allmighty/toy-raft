// Package raft.
package raft

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
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
	NodeID       int
	Term         int
	LastLogIndex int
	LastLogTerm  int
}
type VoteReply struct {
	FromID      int
	Term        int
	VoteGranted bool
}

type NodeState string

const (
	Follower  NodeState = "follower"
	Candidate NodeState = "candidate"
	Leader    NodeState = "leader"
)

type StateMachine struct {
	State map[string]string
	Mu    sync.Mutex
}

func (sm *StateMachine) Apply(cmd string) error {
	sm.Mu.Lock()
	defer sm.Mu.Unlock()
	opKey, val, found := strings.Cut(cmd, "=")
	if !found {
		return fmt.Errorf("state machine failed to parse command: %v", cmd)
	}
	_, key, found := strings.Cut(opKey, " ")
	if !found {
		return fmt.Errorf("state machine faield to parse commands key value pair: %v", cmd)
	}
	sm.State[key] = val
	slog.Info("New command was applied to the state machine", "command", cmd, "current_state", sm.State)
	return nil
}

type Node struct {
	NodeID        int
	LeaderID      *int
	State         NodeState
	StateMachine  StateMachine
	Term          int
	VotedFor      *int
	Addr          string
	CommitIndex   int
	LastApplied   int
	Log           []LogEntry
	Peers         map[int]string
	NextIndex     map[string]int
	MatchIndex    map[string]int
	ElectionTimer *time.Timer
	Mu            sync.RWMutex
	ApplyCond     *sync.Cond
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
			n.Mu.RLock()
			lastLogIdx := len(n.Log) - 1
			lastLogTerm := n.Log[lastLogIdx].Term
			candidate := RequestVoteArgs{NodeID: n.NodeID, Term: n.Term, LastLogIndex: lastLogIdx, LastLogTerm: lastLogTerm}
			n.Mu.RUnlock()
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
	// if candidates log term is lower then nodes, or the terms are equal but index is lower then nodes,
	// say NO.
	lastLogIdx := len(n.Log) - 1
	lastLogTerm := n.Log[lastLogIdx].Term
	if candidate.LastLogTerm < lastLogTerm || (candidate.LastLogTerm == lastLogTerm && candidate.LastLogIndex < lastLogIdx) {
		return nil
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
		slog.Debug("Sending heartbeat through RPC", "leader", n.NodeID, "follower", peerID)
		go n.ReplicateLog(peerID, peerAddr)
	}
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
			slog.Info("Sending command to replicas through RPC", "leader", n.NodeID, "replica", peerAddr)
			go n.ReplicateLog(peerID, peerAddr)
		}
	} else {
		reply.PossibleLeader = n.LeaderID
		reply.Success = false
	}
	return nil
}

func (n *Node) ReplicateLog(peerID int, peerAddr string) {
	client, err := rpc.Dial("tcp", peerAddr)
	if err != nil {
		return
	}
	defer client.Close()
	n.Mu.RLock()
	if n.State != Leader || n.LeaderID == nil {
		n.Mu.RUnlock()
		return
	}
	var appendReply AppendEntriesReply
	nextIdx := n.NextIndex[peerAddr]
	entriesArgs := AppendEntriesArgs{
		LeaderID:     *n.LeaderID,
		Term:         n.Term,
		LeaderCommit: n.CommitIndex,
		Entries:      n.Log[nextIdx:],
		PrevLogIndex: nextIdx - 1,
		PrevLogTerm:  n.Log[nextIdx-1].Term,
	}
	n.Mu.RUnlock()
	if err := client.Call("Node.AppendEntry", &entriesArgs, &appendReply); err != nil {
		slog.Error("RPC append entry call failed", "target", peerID, "error", err)
		return
	}
	n.Mu.Lock()
	defer n.Mu.Unlock()
	if appendReply.Term > n.Term {
		n.Term = appendReply.Term
		n.State = Follower
		n.VotedFor = nil
		n.LeaderID = nil
		n.ElectionTimer.Reset(RandomElectionTimeout())
		return
	}
	if n.State != Leader || n.Term != entriesArgs.Term {
		return
	}
	if appendReply.Success {
		matchLogIdx := entriesArgs.PrevLogIndex + len(entriesArgs.Entries)
		n.MatchIndex[peerAddr] = matchLogIdx
		n.NextIndex[peerAddr] = matchLogIdx + 1
		// Qourum check for commit
		for i := len(n.Log) - 1; i > n.CommitIndex; i-- {
			// 1. If terms dont match keep looping towards begining of the log
			if n.Term != n.Log[i].Term {
				continue
			}
			// 2. Count how many followers including leader matched or exceeded current index
			count := 1
			for _, matchIdx := range n.MatchIndex {
				if matchIdx >= i {
					count++
				}
				// 3. If quorum achieved break
				if count > len(n.Peers)/2 {
					n.CommitIndex = i
					slog.Info("Current commit index found", "leader", n.NodeID, "term", n.Term, "commit_index", n.CommitIndex, "commited_log", n.Log[:n.CommitIndex+1])
					n.ApplyCond.Signal()
					break
				}
			}
		}
	} else {
		if n.NextIndex[peerAddr] > 1 {
			n.NextIndex[peerAddr]--
		}
	}
}

func (n *Node) ApplyLogWorker() {
	for {
		n.Mu.Lock()
		for n.CommitIndex <= n.LastApplied {
			n.ApplyCond.Wait()
		}
		n.LastApplied++
		entry := n.Log[n.LastApplied]
		n.Mu.Unlock()
		if err := n.StateMachine.Apply(entry.Command); err != nil {
			slog.Error("Worker received error while applaying entry command to state machine", "node", n.NodeID, "error", err)
			return
		}
		slog.Debug("Worker successfully aplied entry command to state machine", "node", n.NodeID)
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
	if entry.Term > n.Term {
		n.VotedFor = nil
	}
	n.State = Follower
	n.Term = entry.Term
	n.LeaderID = &entry.LeaderID
	n.ElectionTimer.Reset(RandomElectionTimeout())

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
	slog.Debug("Current log", "entries", n.Log, "leader", entry.LeaderID, "follower", n.NodeID)
	newCommitIdx := min(entry.LeaderCommit, entry.PrevLogIndex+len(entry.Entries))
	if newCommitIdx > n.CommitIndex {
		n.CommitIndex = newCommitIdx
		n.ApplyCond.Signal()
	}
	reply.Success = true
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
	node := Node{
		NodeID:        nodeIDInt,
		State:         Follower,
		StateMachine:  StateMachine{State: make(map[string]string)},
		Term:          0,
		Addr:          addr,
		Log:           []LogEntry{{}},
		Peers:         peersMap,
		ElectionTimer: time.NewTimer(RandomElectionTimeout()),
	}
	node.ApplyCond = sync.NewCond(&node.Mu)
	go node.ApplyLogWorker()
	return &node
}
