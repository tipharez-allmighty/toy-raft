package main

import (
	"log/slog"
	"math/rand"
	"net/rpc"
	"os"

	raft "toy-raft"
)

func main() {
	peersMap, err := raft.LoadPeers()
	if err != nil {
		panic(err)
	}
	if len(os.Args) < 2 {
		slog.Error("usage: raft-cli <commands>")
		return
	}
	stack := make([]string, 0, len(peersMap))
	for _, peer := range peersMap {
		stack = append(stack, peer)
	}
	rand.Shuffle(len(stack), func(i, j int) {
		stack[i], stack[j] = stack[j], stack[i]
	})
	visited := make(map[string]bool, len(peersMap))
	for len(stack) > 0 {
		top := len(stack) - 1
		currentPeer := stack[top]
		stack = stack[:top]
		if visited[currentPeer] {
			continue
		}
		visited[currentPeer] = true
		slog.Info("Sending data", "data", os.Args[1:], "peer", currentPeer)
		client, err := rpc.Dial("tcp", currentPeer)
		if err != nil {
			slog.Error("Failed to dial peer", "peer", currentPeer, "error", err)
			os.Exit(1)
		}

		var reply raft.ExecCmdReply
		cmd := "SET X=10"
		if err := client.Call("Node.ExecuteCommand", &cmd, &reply); err != nil {
			slog.Error("RPC call failed", "target", currentPeer, "error", err)
			os.Exit(1)
		}
		client.Close()

		if reply.Success && reply.LeaderID != nil {
			slog.Info("Client successfully found a leader and got his command executed by leader", "leader", *reply.LeaderID)
			return
		} else if !reply.Success && reply.PossibleLeader != nil {
			possibleLeader, ok := peersMap[*reply.PossibleLeader]
			if !ok {
				slog.Error("Mismatch between peers map of client and server")
				os.Exit(1)
			}
			if !visited[possibleLeader] {
				stack = append(stack, possibleLeader)
				slog.Info("Leader is not found trying possible one", "possible_leader", *reply.PossibleLeader)
			} else {
				slog.Info("Possible leader already visited, trying next node", "possible_leader", *reply.PossibleLeader)
			}
		} else if !reply.Success && reply.PossibleLeader == nil {
			slog.Info("Leader is not found trying next node")
		} else {
			slog.Error("Invalid response from the node", "reply", reply)
			os.Exit(1)
		}
		slog.Info("Successfully sent RPC call to node", "peer", currentPeer)
	}
	slog.Info("Failed to find a leader")
}
