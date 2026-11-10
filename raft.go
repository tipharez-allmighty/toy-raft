// Package raft.
package raft

import (
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

type PromoteVoterReply struct {
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
	case AddCmd:
		node, addr, found := strings.Cut(val, ":")
		if !found || node == "" || addr == "" {
			return false
		}
		return true
	case RemoveCmd:
		return false
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
	Term        int
	VoteGranted bool
}

type AddServerArgs struct {
	NodeID   int
	NodeAddr string
	Role     NodeRole
}

type AddServerReply struct {
	LeaderID           *int
	LeaderAddr         *string
	PossibleLeader     *int
	PossibleLeaderAddr *string
	Expired            bool
	Success            bool
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
	Follower  NodeState = "follower"
	Candidate NodeState = "candidate"
	Leader    NodeState = "leader"
)

type StateMachine struct {
	State map[string]string
	Mu    sync.Mutex
}

func (sm *StateMachine) Apply(cmd LogCmd) error {
	sm.Mu.Lock()
	defer sm.Mu.Unlock()
	key, val, found := strings.Cut(cmd.Val, "=")
	if !found {
		return fmt.Errorf("state machine failed to parse command: %v", cmd)
	}
	sm.State[key] = val
	slog.Info("New command was applied to the state machine", "command", cmd, "current_state", sm.State)
	return nil
}

type Node struct {
	NodeID        int
	LeaderID      *int
	LeaderAddr    *string
	Role          NodeRole
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
	CommitCh      chan struct{}
}

func (n *Node) isLeader() bool {
	if (n.State == Leader) && (n.LeaderID != nil && n.LeaderAddr != nil) && (*n.LeaderID == n.NodeID && *n.LeaderAddr == n.Addr) {
		return true
	}
	return false
}

func (n *Node) hasPendingChange() bool {
	for i := n.CommitIndex + 1; i < len(n.Log); i++ {
		if n.Log[i].Command.Cmd == AddCmd {
			return true
		}
	}
	return false
}

func (n *Node) voterCluster() map[int]string {
	currentCluster := make(map[int]string, len(n.Peers)+1)
	maps.Copy(currentCluster, n.Peers)
	currentCluster[n.NodeID] = n.Addr
	return currentCluster
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
	// If node is listener it can not vote.
	if n.Role != Voter {
		return nil
	}
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
	if n.isLeader() {
		if cmd.Cmd == AddCmd && n.hasPendingChange() {
			reply.Busy = true
			reply.Success = false
			return nil
		} else if cmd.Cmd == AddCmd && cmd.NodeID != nil {
			n.Peers[*cmd.NodeID] = cmd.Val
		}
		reply.LeaderID = &n.NodeID
		reply.LeaderAddr = &n.Addr
		reply.Success = true
		n.Log = append(n.Log, LogEntry{Term: n.Term, Command: *cmd, VoterCluster: n.voterCluster()})
		lastLogIdx := len(n.Log) - 1
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

func (n *Node) AddServer(entry *AddServerArgs, reply *AddServerReply) error {
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
		n.NextIndex[lNodeAddr] = len(n.Log)
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
			entriesArgs := AppendEntriesArgs{
				LeaderID:     *n.LeaderID,
				LeaderAddr:   *n.LeaderAddr,
				Term:         n.Term,
				LeaderCommit: n.CommitIndex,
				Entries:      n.Log[nextIdx:],
				PrevLogIndex: nextIdx - 1,
				PrevLogTerm:  n.Log[nextIdx-1].Term,
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
				if matchLogIdx == len(n.Log)-1 {
					n.Mu.Unlock()
					var execReply ExecCmdReply
					cmd := LogCmd{Cmd: AddCmd, Val: lNodeAddr, NodeID: &entry.NodeID}
					if err := n.ExecuteCommand(&cmd, &execReply); err != nil {
						slog.Error("ADD command failed", "new_node", lNodeAddr, "error", err)
					}
					if execReply.Success {
						for n.CommitIndex < *execReply.LastLogIdx {
							ch := n.CommitCh
							n.Mu.Unlock()
							select {
							case <-ch:
							case <-timer.C:
								reply.Expired = true
								return nil
							}
						}
						cmdPromote := "promoteVoter"
						if err := client.Call("Node.PromoteVoter", &cmdPromote, &PromoteVoterReply{}); err != nil {
							slog.Error("RPC promote to voter call failed", "target", lNodeAddr, "error", err)
						}
					} else {
						slog.Error("ADD command failed to execute", "new_node", lNodeAddr)
					}
					n.Mu.RLock()
					reply.LeaderID = &n.NodeID
					reply.LeaderAddr = &n.Addr
					reply.Success = true
					n.Mu.RUnlock()
					return nil
				}
				// Optimizations
			} else if appendReply.ConflictIdx != nil && appendReply.ConflictTerm == nil {
				n.NextIndex[lNodeAddr] = *appendReply.ConflictIdx
			} else if appendReply.ConflictIdx != nil && appendReply.ConflictTerm != nil {
				lastIdxTerm := -1
				for i := len(n.Log) - 1; i >= 1; i-- {
					if n.Log[i].Term == *appendReply.ConflictTerm {
						lastIdxTerm = i
						break
					}
				}
				if lastIdxTerm != -1 {
					n.NextIndex[lNodeAddr] = lastIdxTerm + 1
				} else {
					n.NextIndex[lNodeAddr] = *appendReply.ConflictIdx
				}
			} else {
				if n.NextIndex[lNodeAddr] > 1 {
					n.NextIndex[lNodeAddr]--
				}
			}
			n.Mu.Unlock()
		}
	}
}

func (n *Node) PromoteVoter(cmd *string, reply *PromoteVoterReply) error {
	n.Mu.Lock()
	defer n.Mu.Unlock()
	n.Role = Voter
	reply.Success = true
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
	var appendReply AppendEntriesReply
	nextIdx := n.NextIndex[peerAddr]
	entriesArgs := AppendEntriesArgs{
		LeaderID:     *n.LeaderID,
		LeaderAddr:   *n.LeaderAddr,
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
	if !n.isLeader() || n.Term != entriesArgs.Term {
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
					latestCluster := n.Log[i].VoterCluster
					delete(latestCluster, n.NodeID)
					n.Peers = latestCluster
					slog.Info("Current commit index found", "leader", n.NodeID, "term", n.Term, "commit_index", n.CommitIndex, "commited_log", n.Log[:n.CommitIndex+1])
					close(n.CommitCh)
					n.CommitCh = make(chan struct{})
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
			ch := n.CommitCh
			n.Mu.Unlock()
			<-ch
			n.Mu.Lock()
		}
		n.LastApplied++
		entry := n.Log[n.LastApplied]
		n.Mu.Unlock()
		if entry.Command.Cmd != SetCmd {
			slog.Debug("Worker detected cluster change cmd, skipping applying command to state machine", "node", n.NodeID)
			continue
		}
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
	n.LeaderAddr = &entry.LeaderAddr
	n.ElectionTimer.Reset(RandomElectionTimeout())

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	// Optimization to avoid trying every index if follower or learner (new added node that is most likely empty) log is empty
	if entry.PrevLogIndex >= len(n.Log) {
		lastIdx := len(n.Log)
		reply.ConflictIdx = &lastIdx
		reply.ConflictTerm = nil
		return nil
	}
	// Optimization to avoid trying all indexes if we know that log length is less or equal but terms or off
	if entry.PrevLogTerm != n.Log[entry.PrevLogIndex].Term {
		conflictIdx := entry.PrevLogIndex
		conflictTerm := n.Log[entry.PrevLogIndex].Term
		for conflictIdx > 0 && n.Log[conflictIdx-1].Term == conflictTerm {
			conflictIdx--
		}
		reply.ConflictIdx = &conflictIdx
		reply.ConflictTerm = &conflictTerm
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
		Log:           []LogEntry{{}},
		Peers:         peersMap,
		CommitCh:      make(chan struct{}),
		ElectionTimer: time.NewTimer(RandomElectionTimeout()),
	}
	go node.ApplyLogWorker()
	return &node
}
