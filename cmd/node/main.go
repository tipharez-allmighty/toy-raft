package main

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"time"

	env "toy-raft"
)

type NodeState string

const (
	Follower  NodeState = "follower"
	Candidate NodeState = "candidate"
	Leader    NodeState = "leader"
)

const (
	// Heartbeat Interval (e.g., 50ms) << Min Election Timeout (e.g., 150ms)
	ElectionTimeout = 150 * time.Millisecond
	HeartBeatTimeout = 50 * time.Millisecond
	Jitter      = 150
)

func RandomElectionTimeout() time.Duration {
	return ElectionTimeout + time.Duration(rand.Intn(Jitter))*time.Millisecond
}


type VoteReply struct {
	FromID      int
	Term        int
	VoteGranted bool
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
type Node struct {
	NodeID        int
	State         NodeState
	Term          int
	VotedFor      *int
	Addr          string
	Log           []string
	Peers         map[int]string
	ElectionTimer *time.Timer
	HeartbChan    chan AppendEntriesArgs
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
		n.VotedFor = nil
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
	n.Log = append(n.Log, entry.Entries...)
	reply.FromID = n.NodeID
	reply.Term = n.Term
	reply.Success = false
	if entry.Term < n.Term {
		return nil
	}
	reply.Success = true
	n.HeartbChan <- *entry
	return nil
}

func MustLoadNode() *Node {
	nodeID, ok := os.LookupEnv("NODE_ID")
	if !ok {
		panic(errors.New("faield to load Node id"))
	}
	nodeIDInt, err := strconv.Atoi(nodeID)
	if err != nil {
		panic(errors.New("failed to cast env variable NODE_ID into int"))
	}
	addr, ok := os.LookupEnv("ADDRESS")
	if !ok {
		panic(errors.New("faield to load ADDRESS"))
	}
	peersMap, err := env.LoadPeers()
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
		HeartbChan:    make(chan AppendEntriesArgs),
	}
}

func main() {
	node := MustLoadNode()
	fmt.Printf("%+v", node)
	rpc.Register(node)
	l, err := net.Listen("tcp", node.Addr)
	if err != nil {
		slog.Error("Failed to listen to the adress", "address", node.Addr, "error", err)
		os.Exit(1)
	}
	var votes int
	voteChan := make(chan VoteReply)
	defer l.Close()
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				slog.Warn("Failed listening to connection")
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()
	heartbTicker := time.NewTicker(HeartBeatTimeout)
	defer heartbTicker.Stop()
	for {
		select {
		case <-heartbTicker.C:
			if node.State == Leader {
				node.SendHeartBeat()
			}
		case heartbData := <-node.HeartbChan:
			if heartbData.Term >= node.Term {
				node.Term = heartbData.Term
				node.State = Follower
				node.ElectionTimer.Reset(RandomElectionTimeout())
			}
		case <-node.ElectionTimer.C:
			if node.State != Leader {
				node.State = Candidate
				node.Term++
				node.VotedFor = &node.NodeID
				votes = 1
				node.RequestVote(voteChan)
				node.ElectionTimer.Reset(RandomElectionTimeout())
			}
		case vote := <-voteChan:
			// 1. IF SOMEONE HAS HIGHER TERM STEP DOWN.
			if node.State == Candidate {
				if vote.Term > node.Term {
					node.Term = vote.Term
					node.State = Follower
					node.VotedFor = nil
					votes = 0
					node.ElectionTimer.Reset(RandomElectionTimeout())
					continue
				}
				// 2. IGNORE VOTES FROM PREVIOUS TERMS.
				if vote.Term < node.Term {
					continue
				}
				// 3. COUNT VOTES FROM CURRENT TERM.
				if vote.VoteGranted {
					votes++
					majority := len(node.Peers)/2 + 1
					if votes >= majority {
						node.State = Leader
						node.SendHeartBeat()
						heartbTicker.Reset(HeartBeatTimeout)
					}
				}
			}
		}
	}

