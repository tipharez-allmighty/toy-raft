// Package raft.
package raft

import (
	"errors"
	"log/slog"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"
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
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []string
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
	CurrentLeader *int
	State         NodeState
	Term          int
	VotedFor      *int
	Addr          string
	Log           []string
	Peers         map[int]string
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

	// HOW WE DECIDE WHO TO VOTE FOR:
	// 1. REJECT OLD TERMS
	// If candidate's term is smaller than ours, say NO.
	if candidate.Term < n.Term {
		return nil
	}
	// 2. UPDATE TERM & CLEAR PAST VOTE
	// If candidate has a newer term, step down to FOLLOWER
	// and clear our past vote so we can vote in this new term.
	if candidate.Term > n.Term {
		n.State = Follower
		n.Term = candidate.Term
		reply.Term = n.Term
		n.VotedFor = nil
		n.CurrentLeader = nil
	}
	// 3. GRANT VOTE (ONLY ONE VOTE PER TERM)
	// Say YES if:
	// - We haven't voted yet in this term (n.VotedFor == nil)
	// - OR we already voted for this same candidate (n.VotedFor == args.CandidateID)
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
	n.Log = append(n.Log, entry.Entries...)
	reply.FromID = n.NodeID
	reply.Term = n.Term
	reply.Success = false
	if entry.Term < n.Term {
		return nil
	}
	n.State = Follower
	n.Term = entry.Term
	n.CurrentLeader = &entry.LeaderID
	n.ElectionTimer.Reset(RandomElectionTimeout())
	reply.Success = true
	return nil
}

func (n *Node) ExecuteCommand(cmd *string, reply *ExecCmdReply) error {
	n.Mu.Lock()
	defer n.Mu.Unlock()
	if n.CurrentLeader != nil && *n.CurrentLeader == n.NodeID {
		reply.LeaderID = &n.NodeID
		reply.Success = true
		n.Log = append(n.Log, *cmd)
	} else {
		reply.PossibleLeader = n.CurrentLeader
		reply.Success = false
	}
	return nil
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
		Log:           []string{},
		Peers:         peersMap,
		ElectionTimer: time.NewTimer(RandomElectionTimeout()),
	}
}
