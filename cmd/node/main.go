package main

import (
	"fmt"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"time"

	raft "toy-raft"
)

func main() {
	node := raft.MustLoadNode()
	fmt.Printf("%+v", node)
	rpc.Register(node)
	l, err := net.Listen("tcp", node.Addr)
	if err != nil {
		slog.Error("Failed to listen to the adress", "address", node.Addr, "error", err)
		os.Exit(1)
	}
	var votes int
	voteChan := make(chan raft.VoteReply)
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
	heartbTicker := time.NewTicker(raft.HeartBeatTimeout)
	defer heartbTicker.Stop()
	for {
		select {
		case <-heartbTicker.C:
			node.Mu.RLock()
			isLeader := node.State == raft.Leader
			node.Mu.RUnlock()
			if isLeader {
				node.SendHeartBeat()
			}
		case <-node.ElectionTimer.C:
			node.Mu.Lock()
			if node.State != raft.Leader {
				node.State = raft.Candidate
				node.Term++
				node.VotedFor = &node.NodeID
				node.CurrentLeader = nil
				votes = 1
				node.RequestVote(voteChan)
				node.ElectionTimer.Reset(raft.RandomElectionTimeout())
			}
			node.Mu.Unlock()
		case vote := <-voteChan:
			// 1. IF SOMEONE HAS HIGHER TERM STEP DOWN.
			node.Mu.Lock()
			if node.State == raft.Candidate {
				if vote.Term > node.Term {
					node.Term = vote.Term
					node.State = raft.Follower
					node.VotedFor = nil
					votes = 0
					node.ElectionTimer.Reset(raft.RandomElectionTimeout())
					node.Mu.Unlock()
					continue
				}
				// 2. IGNORE VOTES FROM PREVIOUS TERMS.
				if vote.Term < node.Term {
					node.Mu.Unlock()
					continue
				}
				// 3. COUNT VOTES FROM CURRENT TERM.
				if vote.VoteGranted {
					votes++
					majority := len(node.Peers)/2 + 1
					if votes >= majority {
						node.State = raft.Leader
						node.CurrentLeader = &node.NodeID
						node.SendHeartBeat()
						heartbTicker.Reset(raft.HeartBeatTimeout)
						node.Mu.Unlock()
						slog.Info("Leader has been elected", "node", node.NodeID)
						continue
					}
				}
			}
			node.Mu.Unlock()
		}
	}
}
