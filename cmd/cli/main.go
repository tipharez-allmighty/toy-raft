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
	peersList := raft.MustLoadPeers()
	if len(os.Args) > 1 && os.Args[1] == raft.ExploreCmd {
		visited := make(map[string]bool, len(peersList))
		for len(peersList) > 0 {
			top := len(peersList) - 1
			currentPeer := peersList[top]
			peersList = peersList[:top]
			if visited[currentPeer] {
				continue
			}
			visited[currentPeer] = true
			slog.Info("Trying possible leader", "peer", currentPeer)
			conn, err := net.Dial("tcp", currentPeer)
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
					peersList = append(peersList, *reply.PossibleLeader)
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

	if len(os.Args) < 3 || !raft.IsValidCmd(os.Args[1], os.Args[2]) {
		slog.Error("usage: raft-cli set x=?", "args", os.Args)
		return
	}

	rand.Shuffle(len(peersList), func(i, j int) {
		peersList[i], peersList[j] = peersList[j], peersList[i]
	})
	visited := make(map[string]bool, len(peersList))
	for len(peersList) > 0 {
		top := len(peersList) - 1
		currentPeer := peersList[top]
		peersList = peersList[:top]
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
			if !visited[currentPeer] {
				peersList = append(peersList, currentPeer)
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
