package main

import (
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"

	raft "toy-raft"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "explore" {
		ips, err := net.LookupHost("raft")
		if err != nil {
			slog.Error("Failed to dial host network to get nodes adresses", "error", err)
			return
		}
		if len(ips) == 0 {
			slog.Warn("DNS raft doesnt have nodes", "ips", ips)
			return
		}
		visited := make(map[string]bool, len(ips))
		for len(ips) > 0 {
			top := len(ips) - 1
			currentPeer := ips[top]
			ips = ips[:top]
			if visited[currentPeer] {
				continue
			}
			visited[currentPeer] = true
			nodeAddr := net.JoinHostPort(currentPeer, "50051")
			slog.Info("Trying possible leader", "peer", currentPeer)
			conn, err := net.Dial("tcp", nodeAddr)
			if err != nil {
				slog.Error("Failed to dial peer", "peer", currentPeer, "error", err)
				continue
			}
			conn.SetDeadline(time.Now().Add(2 * time.Second))
			client := rpc.NewClient(conn)

			var reply raft.ExploreCmdReply
			exploreCmd := "explore"
			if err := client.Call("Node.ExploreCommand", &exploreCmd, &reply); err != nil {
				slog.Error("RPC call failed", "target", currentPeer, "error", err)
				continue
			}
			client.Close()
			if reply.Success && reply.Leader != nil {
				var sb strings.Builder
				count := 0
				for key, val := range reply.Peers {
					sb.WriteString(strconv.Itoa(key))
					sb.WriteString("=")
					sb.WriteString(val)
					if count < len(reply.Peers)-1 {
						sb.WriteString(",")
					}
					count += 1
				}
				slog.Info("Client successfully discovered a cluster", "leader", *reply.Leader, "Peers", sb.String())
				return
			} else if !reply.Success && reply.PossibleLeader != nil {
				if !visited[*reply.PossibleLeader] {
					ips = append(ips, *reply.PossibleLeader)
					slog.Info("Leader is not found trying possible one", "possible_leader", *reply.PossibleLeader)
				} else {
					slog.Info("Possible leader already visited, trying next node", "possible_leader", *reply.PossibleLeader)
				}
			} else if !reply.Success && reply.PossibleLeader == nil {
				slog.Info("Possible leader is not found trying next node")
			} else {
				slog.Error("Invalid response from the node", "reply", reply)
				return
			}
		}
		return
	}
	peersMap, err := raft.LoadPeers()
	if err != nil {
		panic(err)
	}
	if len(os.Args) < 3 || !raft.IsValidCmd(os.Args[1], os.Args[2]) {
		slog.Error("usage: raft-cli set x=?", "error", err, "args", os.Args)
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
		conn, err := net.Dial("tcp", currentPeer)
		if err != nil {
			slog.Error("Failed to dial peer", "peer", currentPeer, "error", err)
			continue
		}
		conn.SetDeadline(time.Now().Add(2 * time.Second))
		client := rpc.NewClient(conn)

		var reply raft.ExecCmdReply
		cmd := strings.ToUpper(fmt.Sprintf("%v %v", os.Args[1], os.Args[2]))
		if err := client.Call("Node.ExecuteCommand", &cmd, &reply); err != nil {
			slog.Error("RPC call failed", "target", currentPeer, "error", err)
			continue
		}
		client.Close()

		if reply.Success && reply.LeaderID != nil {
			slog.Info("Client successfully found a leader and got his command executed by leader", "leader", *reply.LeaderID, "Command", cmd)
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
