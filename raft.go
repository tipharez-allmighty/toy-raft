// Package raft.
package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"log/slog"
	"maps"
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
	Rounds           = 3
	// Snapshot
	SnapshotThreshold = 5
	ChunkSize         = 10 // bytes
)

func RandomElectionTimeout() time.Duration {
	return ElectionTimeout + time.Duration(rand.Intn(Jitter))*time.Millisecond
}

type ExecCmdReply struct {
	LeaderID           *int
	LeaderAddr         *string
	PossibleLeader     *int
	PossibleLeaderAddr *string
	LastLogIdx         *int
	Term               int
	Busy               bool
	Success            bool
}

type ExploreCmdReply struct {
	LeaderAddr         *string
	PossibleLeaderAddr *string
	Peers              map[int]string
	Success            bool
}

type GetNodeCmdReply struct {
	NodeID     int
	LeaderID   *int
	LeaderAddr *string
	Role       NodeRole
	Addr       string
	Peers      map[int]string
}

type BaseReply struct {
	Success bool
}

const (
	SetCmd     string = "set"
	AddCmd     string = "add"
	RemoveCmd  string = "remove"
	ExploreCmd string = "explore"
)

func IsValidCmd(cmd, val string) bool {
	switch cmd {
	case SetCmd:
		before, after, found := strings.Cut(val, "=")
		if !found || before == "" || (before[0] >= '0' && before[0] <= '9') || after == "" {
			return false
		}
		return true
	case AddCmd, RemoveCmd:
		node, addr, found := strings.Cut(val, ":")
		if !found || node == "" || addr == "" {
			return false
		}
		return true
	default:
		return false
	}
}

type LogCmd struct {
	Cmd    string
	Val    string
	NodeID *int
}
type LogEntry struct {
	Term         int
	Command      LogCmd
	VoterCluster map[int]string
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	LeaderAddr   string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	FromID       int
	Term         int
	ConflictIdx  *int
	ConflictTerm *int
	Success      bool
}

type RequestVoteArgs struct {
	NodeID       int
	Term         int
	LastLogIndex int
	LastLogTerm  int
}
type VoteReply struct {
	FromID      int
	VoteGranted bool
	Term        int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LeaderAddr        string
	LastIncludedIndex int
	LastIncludedTerm  int
	LastCluster       map[int]string
	Data              []byte
	Offset            int
	Done              bool
}
type InstallSnapshotReply struct {
	Term int
}
type ServerArgs struct {
	NodeID   int
	NodeAddr string
}

type ServerReply struct {
	LeaderID           *int
	LeaderAddr         *string
	PossibleLeader     *int
	PossibleLeaderAddr *string
	Success            bool
	Expired            bool
}
type AddServerArgs struct {
	ServerArgs
	Role NodeRole
}

type NodeRole string

const (
	Voter   NodeRole = "voter"
	Learner NodeRole = "learner"
)

func (nr NodeRole) IsValid() bool {
	switch nr {
	case Voter, Learner:
		return true
	default:
		return false
	}
}

type NodeState string

const (
	Follower     NodeState = "follower"
	PreCandidate NodeState = "precandidate"
	Candidate    NodeState = "candidate"
	Leader       NodeState = "leader"
)

type SnapshotBuffer struct {
	offset int
	buf    []byte
}
type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	LastCluster       map[int]string
	Data              []byte
	buffer            SnapshotBuffer
}
type StateMachine struct {
	State    map[string]string
	Snapshot Snapshot
}

type Node struct {
	NodeID            int
	LeaderID          *int
	LeaderAddr        *string
	Role              NodeRole
	State             NodeState
	StateMachine      StateMachine
	Term              int
	VotedFor          *int
	Addr              string
	CommitIndex       int
	LastApplied       int
	Log               []LogEntry
	LogStart          int
	Peers             map[int]string
	NextIndex         map[string]int
	MatchIndex        map[string]int
	SnapshotProgress  map[string]struct{}
	PreVotes          map[int]struct{}
	ElectionTimer     *time.Timer
	LastLeaderContact time.Time
	Mu                sync.RWMutex
	CommitCh          chan struct{}
}

func (n *Node) isLeader() bool {
	if (n.State == Leader) && (n.LeaderID != nil && n.LeaderAddr != nil) && (*n.LeaderID == n.NodeID && *n.LeaderAddr == n.Addr) {
		return true
	}
	return false
}

func (n *Node) entryAt(idx int) LogEntry {
	return n.Log[idx-n.LogStart]
}

func (n *Node) LastLogIdx() int {
	return n.LogStart + len(n.Log) - 1
}

func (n *Node) hasPendingChange() bool {
	for i := n.CommitIndex + 1; i <= n.LastLogIdx(); i++ {
		if n.entryAt(i).Command.Cmd == AddCmd || n.entryAt(i).Command.Cmd == RemoveCmd {
			return true
		}
	}
	return false
}

func (n *Node) peersFromLog() {
	cluster := maps.Clone(n.entryAt(n.LastLogIdx()).VoterCluster)
	delete(cluster, n.NodeID)
	n.Peers = cluster
}

// Optimizations so we dont check all indexes one by one if we can guess where next index should be (see AppendEntry method).
func (n *Node) nextIndexGuess(addr string, appendReply AppendEntriesReply) {
	if appendReply.ConflictIdx != nil && appendReply.ConflictTerm == nil {
		n.NextIndex[addr] = *appendReply.ConflictIdx
	} else if appendReply.ConflictIdx != nil && appendReply.ConflictTerm != nil {
		lastIdxTerm := -1
		for i := n.LastLogIdx(); i > n.LogStart; i-- {
			if n.entryAt(i).Term == *appendReply.ConflictTerm {
				lastIdxTerm = i
				break
			}
		}
		if lastIdxTerm != -1 {
			n.NextIndex[addr] = lastIdxTerm + 1
		} else {
			n.NextIndex[addr] = *appendReply.ConflictIdx
		}
	} else {
		if n.NextIndex[addr] > 1 {
			n.NextIndex[addr]--
		}
	}
}

func (n *Node) voterCluster() map[int]string {
	currentCluster := make(map[int]string, len(n.Peers)+1)
	maps.Copy(currentCluster, n.Peers)
	currentCluster[n.NodeID] = n.Addr
	return currentCluster
}

func (n *Node) RequestPreVote(preVoteChan chan<- VoteReply) {
	for peerID, peerAddr := range n.Peers {
		if peerAddr == n.Addr {
			continue
		}
		slog.Info("Processing pre vote rpc call", "peer", peerID, "address", peerAddr)

		go func(addr string) {
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				return
			}
			defer client.Close()

			var voteReply VoteReply
			n.Mu.RLock()
			lastLogIdx := n.LastLogIdx()
			lastLogTerm := n.entryAt(lastLogIdx).Term
			candidate := RequestVoteArgs{NodeID: n.NodeID, Term: n.Term + 1, LastLogIndex: lastLogIdx, LastLogTerm: lastLogTerm}
			n.Mu.RUnlock()
			if err := client.Call("Node.PreVote", &candidate, &voteReply); err != nil {
				slog.Error("RPC pre voting call failed", "target", addr, "error", err)
				return
			}
			preVoteChan <- voteReply
		}(peerAddr)
	}
}

func (n *Node) PreVote(candidate *RequestVoteArgs, reply *VoteReply) error {
	// Pre vote is almost the same as give vote but it doesnt change state
	n.Mu.Lock()
	defer n.Mu.Unlock()
	if n.Role != Voter {
		return nil
	}
	reply.FromID = n.NodeID
	reply.Term = candidate.Term
	reply.VoteGranted = false
	// A time check for last contact from the leader, to prevent paused node to raise terms and start election
	if n.State == Leader || time.Since(n.LastLeaderContact) < ElectionTimeout {
		return nil
	}
	// If candidate's term is smaller than ours, say NO.
	if candidate.Term < n.Term {
		return nil
	}
	// if candidates log term is lower then nodes, or the terms are equal but index is lower then nodes,
	// say NO.
	lastLogIdx := n.LastLogIdx()
	lastLogTerm := n.entryAt(lastLogIdx).Term
	if candidate.LastLogTerm < lastLogTerm || (candidate.LastLogTerm == lastLogTerm && candidate.LastLogIndex < lastLogIdx) {
		return nil
	}
	reply.VoteGranted = true
	return nil
}

func (n *Node) RequestVote(voteChan chan<- VoteReply) {
	for peerID, peerAddr := range n.Peers {
		if peerAddr == n.Addr {
			continue
		}
		slog.Info("Processing vote rpc call", "peer", peerID, "address", peerAddr)

		go func(addr string) {
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				return
			}
			defer client.Close()

			var voteReply VoteReply
			n.Mu.RLock()
			lastLogIdx := n.LastLogIdx()
			lastLogTerm := n.entryAt(lastLogIdx).Term
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
	// If node is listener it can not vote.
	if n.Role != Voter {
		return nil
	}
	reply.FromID = n.NodeID
	reply.Term = n.Term
	reply.VoteGranted = false
	// A time check for last contact from the leader, to prevent paused node to raise terms and start election
	if n.State == Leader || time.Since(n.LastLeaderContact) < ElectionTimeout {
		return nil
	}
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
	lastLogIdx := n.LastLogIdx()
	lastLogTerm := n.entryAt(lastLogIdx).Term
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

func (n *Node) ExploreCommand(cmd *string, reply *ExploreCmdReply) error {
	n.Mu.Lock()
	defer n.Mu.Unlock()
	if n.isLeader() {
		reply.LeaderAddr = &n.Addr
		reply.Peers = n.Peers
		reply.Success = true
	} else {
		reply.PossibleLeaderAddr = n.LeaderAddr
		reply.Success = false
	}
	return nil
}

func (n *Node) GetNode(cmd *string, reply *GetNodeCmdReply) error {
	n.Mu.RLock()
	defer n.Mu.RUnlock()
	reply.NodeID = n.NodeID
	reply.Addr = n.Addr
	reply.LeaderID = n.LeaderID
	reply.LeaderAddr = n.LeaderAddr
	reply.Peers = n.Peers
	reply.Role = n.Role
	return nil
}

func (n *Node) ExecuteCommand(cmd *LogCmd, reply *ExecCmdReply) error {
	n.Mu.Lock()
	defer n.Mu.Unlock()
	reply.Term = n.Term
	if n.isLeader() {
		if (cmd.Cmd == AddCmd || cmd.Cmd == RemoveCmd) && n.hasPendingChange() {
			slog.Warn("Membership change rejected, another change is not committed yet", "cmd", cmd.Cmd, "node", cmd.Val)
			reply.Busy = true
			reply.Success = false
			return nil
		}
		voterCluster := maps.Clone(n.entryAt(n.LastLogIdx()).VoterCluster)
		if cmd.Cmd == AddCmd && cmd.NodeID != nil {
			voterCluster[*cmd.NodeID] = cmd.Val
		} else if cmd.Cmd == RemoveCmd && cmd.NodeID != nil {
			delete(voterCluster, *cmd.NodeID)
		}
		reply.LeaderID = &n.NodeID
		reply.LeaderAddr = &n.Addr
		reply.Success = true
		n.Log = append(n.Log, LogEntry{Term: n.Term, Command: *cmd, VoterCluster: voterCluster})
		n.peersFromLog()
		lastLogIdx := n.LastLogIdx()
		reply.LastLogIdx = &lastLogIdx
		for peerID, peerAddr := range n.Peers {
			if peerAddr == n.Addr {
				continue
			}
			slog.Info("Sending command to replicas through RPC", "leader", n.NodeID, "replica", peerAddr)
			go n.ReplicateLog(peerID, peerAddr)
		}
	} else {
		reply.PossibleLeader = n.LeaderID
		reply.PossibleLeaderAddr = n.LeaderAddr
		reply.Success = false
	}
	return nil
}

func (n *Node) AddServer(entry *AddServerArgs, reply *ServerReply) error {
	n.Mu.Lock()
	reply.PossibleLeader = n.LeaderID
	reply.PossibleLeaderAddr = n.LeaderAddr
	reply.Success = false
	if !n.isLeader() || entry.Role != Learner {
		n.Mu.Unlock()
		return nil
	}
	lNodeAddr := entry.NodeAddr
	if _, ok := n.NextIndex[lNodeAddr]; !ok {
		n.NextIndex[lNodeAddr] = n.LastLogIdx() + 1
		n.MatchIndex[lNodeAddr] = 0
	}
	n.Mu.Unlock()
	timer := time.NewTimer(ElectionTimeout)
	defer timer.Stop()
	catchUpRounds := Rounds
	for {
		select {
		case <-timer.C:
			if catchUpRounds == 0 {
				reply.Expired = true
				return nil
			}
			catchUpRounds--
			timer.Reset(ElectionTimeout)
		default:
			var appendReply AppendEntriesReply
			n.Mu.Lock()
			nextIdx := n.NextIndex[lNodeAddr]
			lastInclIdx := n.StateMachine.Snapshot.LastIncludedIndex
			lastInclTerm := n.StateMachine.Snapshot.LastIncludedTerm
			if nextIdx <= lastInclIdx {
				_, ok := n.SnapshotProgress[lNodeAddr]
				n.Mu.Unlock()
				if !ok {
					go n.StreamSnapshot(lNodeAddr, lastInclIdx, lastInclTerm)
				}
				time.Sleep(HeartBeatTimeout)
				continue
			}
			entriesArgs := AppendEntriesArgs{
				LeaderID:     *n.LeaderID,
				LeaderAddr:   *n.LeaderAddr,
				Term:         n.Term,
				LeaderCommit: n.CommitIndex,
				Entries:      n.Log[nextIdx-n.LogStart:],
				PrevLogIndex: nextIdx - 1,
				PrevLogTerm:  n.entryAt(nextIdx - 1).Term,
			}
			n.Mu.Unlock()
			client, err := rpc.Dial("tcp", lNodeAddr)
			if err != nil {
				return nil
			}
			defer client.Close()
			if err := client.Call("Node.AppendEntry", &entriesArgs, &appendReply); err != nil {
				slog.Error("RPC append entry call failed", "target", lNodeAddr, "error", err)
				return nil
			}
			n.Mu.Lock()
			if appendReply.Success {
				matchLogIdx := entriesArgs.PrevLogIndex + len(entriesArgs.Entries)
				n.MatchIndex[lNodeAddr] = matchLogIdx
				n.NextIndex[lNodeAddr] = matchLogIdx + 1
				if matchLogIdx == n.LastLogIdx() {
					n.Mu.Unlock()
					var execReply ExecCmdReply
					cmd := LogCmd{Cmd: AddCmd, Val: lNodeAddr, NodeID: &entry.NodeID}
					if err := n.ExecuteCommand(&cmd, &execReply); err != nil {
						slog.Error("ADD command failed", "new_node", lNodeAddr, "error", err)
						return nil
					}
					if execReply.Success {
						n.Mu.RLock()
						for n.CommitIndex < *execReply.LastLogIdx {
							ch := n.CommitCh
							n.Mu.RUnlock()
							select {
							case <-ch:
								n.Mu.RLock()
							case <-timer.C:
								reply.Expired = true
								return nil
							}
						}
						if n.Term != execReply.Term {
							n.Mu.RUnlock()
							return nil
						}
						n.Mu.RUnlock()
						cmdPromote := "promoteVoter"
						if err := client.Call("Node.PromoteVoter", &cmdPromote, &BaseReply{}); err != nil {
							slog.Error("RPC promote to voter call failed", "target", lNodeAddr, "error", err)
						}
					} else {
						slog.Error("Add command failed to execute", "new_node", lNodeAddr)
					}
					n.Mu.RLock()
					reply.LeaderID = &n.NodeID
					reply.LeaderAddr = &n.Addr
					reply.Success = true
					n.Mu.RUnlock()
					return nil
				}
			} else {
				n.nextIndexGuess(lNodeAddr, appendReply)
			}
			n.Mu.Unlock()
		}
	}
}

func (n *Node) PromoteVoter(cmd *string, reply *BaseReply) error {
	n.Mu.Lock()
	defer n.Mu.Unlock()
	n.Role = Voter
	reply.Success = true
	return nil
}

func (n *Node) RemoveServer(entry *ServerArgs, reply *ServerReply) error {
	n.Mu.Lock()
	reply.PossibleLeader = n.LeaderID
	reply.PossibleLeaderAddr = n.LeaderAddr
	reply.Success = false
	if !n.isLeader() {
		n.Mu.Unlock()
		return nil
	}

	var execReply ExecCmdReply
	cmd := LogCmd{Cmd: RemoveCmd, Val: entry.NodeAddr, NodeID: &entry.NodeID}
	n.Mu.Unlock()
	if err := n.ExecuteCommand(&cmd, &execReply); err != nil {
		slog.Error("Remove command failed", "node_to_remove", entry.NodeAddr, "error", err)
		return nil
	}
	if execReply.Success {
		timer := time.NewTimer(ElectionTimeout)
		defer timer.Stop()
		n.Mu.RLock()
		for n.CommitIndex < *execReply.LastLogIdx {
			ch := n.CommitCh
			n.Mu.RUnlock()
			select {
			case <-ch:
				n.Mu.RLock()
			case <-timer.C:
				reply.Expired = true
				return nil
			}
		}
		n.Mu.RUnlock()
		n.Mu.Lock()
		if n.Term != execReply.Term {
			n.Mu.Unlock()
			return nil
		}
		reply.LeaderID = &n.NodeID
		reply.LeaderAddr = &n.Addr
		if n.NodeID == entry.NodeID {
			n.State = Follower
			n.Role = Learner
			reply.LeaderID = nil
			reply.LeaderAddr = nil
		}
		reply.Success = true
		n.Mu.Unlock()
	}
	return nil
}

func (n *Node) SaveSnapshot(idx, term int, cluster map[int]string) error {
	sm := &n.StateMachine
	sm.Snapshot.LastIncludedIndex = idx
	sm.Snapshot.LastIncludedTerm = term
	sm.Snapshot.LastCluster = cluster
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(sm.State); err != nil {
		return err
	}
	sm.Snapshot.Data = buf.Bytes()
	return nil
}

func (n *Node) StreamSnapshot(addr string, lastIdx, LastTerm int) {
	n.Mu.Lock()
	n.SnapshotProgress[addr] = struct{}{}
	defer func() {
		n.Mu.Lock()
		delete(n.SnapshotProgress, addr)
		n.Mu.Unlock()
	}()
	data := n.StateMachine.Snapshot.Data
	totalLen := len(data)
	curTerm := n.Term
	cluster := maps.Clone(n.StateMachine.Snapshot.LastCluster)
	n.Mu.Unlock()
	for offset := 0; offset < totalLen; offset += ChunkSize {
		end := offset + ChunkSize
		done := false
		if end >= totalLen {
			end = totalLen
			done = true
		}
		n.Mu.RLock()
		if n.State != Leader || n.Term != curTerm {
			n.Mu.RUnlock()
			return
		}
		chunk := append([]byte(nil), data[offset:end]...)
		entry := InstallSnapshotArgs{
			Term:              curTerm,
			LeaderID:          n.NodeID,
			LeaderAddr:        n.Addr,
			LastIncludedIndex: lastIdx,
			LastIncludedTerm:  LastTerm,
			LastCluster:       cluster,
			Data:              chunk,
			Offset:            offset,
			Done:              done,
		}
		n.Mu.RUnlock()
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			return
		}
		var reply InstallSnapshotReply
		if err := client.Call("Node.InstallSnapshot", &entry, &reply); err != nil {
			client.Close()
			slog.Error("RPC install snapshot failed", "target", addr, "error", err)
			return
		}
		client.Close()
		slog.Info("Successfully sent snapshot chunk", "target", addr, "offset", offset, "end", end, "total", totalLen)
		n.Mu.Lock()
		if reply.Term > n.Term {
			n.Term = reply.Term
			n.State = Follower
			n.VotedFor = nil
			n.LeaderID = nil
			n.ElectionTimer.Reset(RandomElectionTimeout())
			n.Mu.Unlock()
			return
		}
		if done && n.State == Leader && n.Term == curTerm {
			n.MatchIndex[addr] = lastIdx
			n.NextIndex[addr] = lastIdx + 1
		}
		n.Mu.Unlock()
	}
}

func (n *Node) InstallSnapshot(entry *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	n.Mu.Lock()
	defer n.Mu.Unlock()
	reply.Term = n.Term
	if n.Term > entry.Term {
		return nil
	}
	if entry.Term > n.Term {
		n.Term = entry.Term
		n.VotedFor = nil
	}
	n.State = Follower
	n.LeaderID = &entry.LeaderID
	n.LeaderAddr = &entry.LeaderAddr
	n.ElectionTimer.Reset(RandomElectionTimeout())
	n.LastLeaderContact = time.Now()
	if entry.LastIncludedIndex <= n.LogStart {
		return nil
	}
	smSnap := &n.StateMachine.Snapshot
	if entry.Offset == 0 {
		smSnap.buffer = SnapshotBuffer{}
	}
	if entry.Offset != smSnap.buffer.offset {
		return fmt.Errorf("unexpected chunk offset: got %d, expected %d", entry.Offset, smSnap.buffer.offset)
	}
	smSnap.buffer.buf = append(smSnap.buffer.buf, entry.Data...)
	smSnap.buffer.offset += len(entry.Data)

	slog.Info("Received snapshot chunk in memory", "offset", entry.Offset, "chunkLen", len(entry.Data), "done", entry.Done)
	if entry.Done {
		var state map[string]string
		buf := bytes.NewReader(smSnap.buffer.buf)
		dec := gob.NewDecoder(buf)
		if err := dec.Decode(&state); err != nil {
			return err
		}
		smSnap.Data = smSnap.buffer.buf
		smSnap.LastCluster = entry.LastCluster
		smSnap.LastIncludedIndex = entry.LastIncludedIndex
		smSnap.LastIncludedTerm = entry.LastIncludedTerm
		smSnap.buffer = SnapshotBuffer{}
		n.StateMachine.State = state // replacing state with snapshot
		if entry.LastIncludedIndex <= n.LastLogIdx() && n.entryAt(entry.LastIncludedIndex).Term == entry.LastIncludedTerm {
			n.Log = n.Log[entry.LastIncludedIndex-n.LogStart:]
		} else {
			n.Log = []LogEntry{{Term: entry.LastIncludedTerm, VoterCluster: entry.LastCluster}}
		}
		n.LogStart = entry.LastIncludedIndex
		n.CommitIndex = max(n.CommitIndex, entry.LastIncludedIndex)
		n.LastApplied = entry.LastIncludedIndex
		n.peersFromLog()
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
	if !n.isLeader() || n.LeaderID == nil {
		n.Mu.RUnlock()
		return
	}
	nextIdx := n.NextIndex[peerAddr]
	lastInclIdx := n.StateMachine.Snapshot.LastIncludedIndex
	lastInclTerm := n.StateMachine.Snapshot.LastIncludedTerm
	n.Mu.RUnlock()
	if nextIdx <= lastInclIdx {
		n.Mu.RLock()
		_, ok := n.SnapshotProgress[peerAddr]
		n.Mu.RUnlock()
		if !ok {
			go n.StreamSnapshot(peerAddr, lastInclIdx, lastInclTerm)
		}
		return
	}
	var appendReply AppendEntriesReply
	n.Mu.RLock()
	entriesArgs := AppendEntriesArgs{
		LeaderID:     *n.LeaderID,
		LeaderAddr:   *n.LeaderAddr,
		Term:         n.Term,
		LeaderCommit: n.CommitIndex,
		Entries:      n.Log[nextIdx-n.LogStart:],
		PrevLogIndex: nextIdx - 1,
		PrevLogTerm:  n.entryAt(nextIdx - 1).Term,
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
	if !n.isLeader() || n.Term != entriesArgs.Term {
		return
	}
	if appendReply.Success {
		matchLogIdx := entriesArgs.PrevLogIndex + len(entriesArgs.Entries)
		n.MatchIndex[peerAddr] = matchLogIdx
		n.NextIndex[peerAddr] = matchLogIdx + 1
		// Qourum check for commit
		for i := n.LastLogIdx(); i > n.CommitIndex; i-- {
			// 1. If terms dont match keep looping towards begining of the log
			if n.Term != n.entryAt(i).Term {
				continue
			}
			// 2. Count how many followers including leader matched or exceeded current index
			count := 1
			for _, addr := range n.Peers {
				if n.MatchIndex[addr] >= i {
					count++
				}
				// 3. If quorum achieved break
				if count > (len(n.Peers)+1)/2 {
					n.CommitIndex = i
					slog.Info("Current commit index found", "leader", n.NodeID, "term", n.Term, "commit_index", n.CommitIndex)
					close(n.CommitCh)
					n.CommitCh = make(chan struct{})
					break
				}
			}
		}
	} else {
		n.nextIndexGuess(peerAddr, appendReply)
	}
}

func (n *Node) ApplyState(cmd LogCmd) error {
	key, val, found := strings.Cut(cmd.Val, "=")
	if !found {
		return fmt.Errorf("state machine failed to parse command: %v", cmd)
	}
	n.StateMachine.State[key] = val
	slog.Info("New command was applied to the state machine", "command", cmd, "current_state", n.StateMachine.State)
	return nil
}

func (n *Node) ApplyLogWorker() {
	for {
		n.Mu.Lock()
		for n.CommitIndex <= n.LastApplied {
			ch := n.CommitCh
			n.Mu.Unlock()
			<-ch
			n.Mu.Lock()
		}
		n.LastApplied++
		lastEntryIdx := n.LastApplied
		entry := n.entryAt(lastEntryIdx)
		if entry.Command.Cmd != SetCmd {
			slog.Debug("Worker detected cluster change cmd, skipping applying command to state machine", "node", n.NodeID)
			n.Mu.Unlock()
			continue
		}
		if err := n.ApplyState(entry.Command); err != nil {
			slog.Error("Worker received error while applaying entry command to state machine", "node", n.NodeID, "error", err)
			n.Mu.Unlock()

			return
		}
		slog.Debug("Worker successfully aplied entry command to state machine", "node", n.NodeID)
		if len(n.Log) >= SnapshotThreshold {
			if err := n.SaveSnapshot(lastEntryIdx, entry.Term, entry.VoterCluster); err != nil {
				slog.Error("Error while saving a snahpost", "node", n.NodeID, "error", err)
				n.Mu.Unlock()
				return
			}
			n.Log = n.Log[lastEntryIdx-n.LogStart:]
			n.LogStart = lastEntryIdx
			slog.Info("Truncated log", "index", lastEntryIdx, "truncated_log", n.Log)
		}
		n.Mu.Unlock()

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
	n.LeaderAddr = &entry.LeaderAddr
	n.ElectionTimer.Reset(RandomElectionTimeout())
	n.LastLeaderContact = time.Now()

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	// Optimization to avoid trying every index if follower or learner (new added node that is most likely empty) log is empty
	if entry.PrevLogIndex > n.LastLogIdx() {
		lastIdx := n.LastLogIdx() + 1
		reply.ConflictIdx = &lastIdx
		reply.ConflictTerm = nil
		return nil
	}
	// Compaction: before reading, the follower checks whether that entry is one it already threw away
	if entry.PrevLogIndex < n.LogStart {
		nextIdx := n.LogStart + 1
		reply.ConflictIdx = &nextIdx
		reply.ConflictTerm = nil
		return nil
	}
	// Optimization to avoid trying all indexes if we know that log length is less or equal but terms are off
	if entry.PrevLogTerm != n.entryAt(entry.PrevLogIndex).Term {
		conflictIdx := entry.PrevLogIndex
		conflictTerm := n.entryAt(conflictIdx).Term
		for conflictIdx > n.LogStart && n.entryAt(conflictIdx-1).Term == conflictTerm {
			conflictIdx--
		}
		reply.ConflictIdx = &conflictIdx
		reply.ConflictTerm = &conflictTerm
		return nil
	}
	for idx, currentEntry := range entry.Entries {
		currentIdx := entry.PrevLogIndex + 1 + idx
		if currentIdx > n.LastLogIdx() {
			// Append any new entries not already in the log
			n.Log = append(n.Log, entry.Entries[idx:]...)
			break
		}
		// If an existing entry conflicts with a new one (same index but different terms),
		// delete the existing entry and all that follow it (§5.3)
		if existingEntry := n.entryAt(currentIdx); existingEntry.Term != currentEntry.Term {
			n.Log = n.Log[:currentIdx-n.LogStart]
			n.Log = append(n.Log, entry.Entries[idx:]...)
			break
		}
	}
	n.peersFromLog()
	slog.Debug("Current log", "entries", n.Log, "leader", entry.LeaderID, "follower", n.NodeID)
	newCommitIdx := min(entry.LeaderCommit, entry.PrevLogIndex+len(entry.Entries))
	if newCommitIdx > n.CommitIndex {
		n.CommitIndex = newCommitIdx
		close(n.CommitCh)
		n.CommitCh = make(chan struct{})
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
	nodeRole := Learner
	if roleEnv, ok := os.LookupEnv("ROLE"); ok {
		role := NodeRole(roleEnv)
		if valid := role.IsValid(); valid {
			nodeRole = role
		} else {
			panic(fmt.Errorf("wrong role %v, should be learner or voter", role))
		}
	}
	peersMap, err := LoadPeersMap()
	if err != nil {
		panic(err)
	}
	node := Node{
		NodeID:        nodeIDInt,
		Role:          nodeRole,
		State:         Follower,
		StateMachine:  StateMachine{State: make(map[string]string)},
		Term:          0,
		Addr:          addr,
		Peers:         peersMap,
		CommitCh:      make(chan struct{}),
		ElectionTimer: time.NewTimer(RandomElectionTimeout()),
	}
	node.Log = append(node.Log, LogEntry{VoterCluster: node.voterCluster()})
	go node.ApplyLogWorker()
	return &node
}
