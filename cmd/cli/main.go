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
			if reply.Success && reply.LeaderAddr != nil {
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
				slog.Info("Client successfully discovered a cluster", "leader", *reply.LeaderAddr, "Peers", sb.String())
				return
			} else if !reply.Success && reply.PossibleLeaderAddr != nil {
				if !visited[*reply.PossibleLeaderAddr] {
					peersList = append(peersList, *reply.PossibleLeaderAddr)
					slog.Info("Leader is not found trying possible one", "possible_leader", *reply.PossibleLeaderAddr)
				} else {
					slog.Info("Possible leader already visited, trying next node", "possible_leader", *reply.PossibleLeaderAddr)
				}
			} else if !reply.Success && reply.PossibleLeaderAddr == nil {
				slog.Info("Possible leader is not found trying next node")
			} else {
				slog.Error("Invalid response from the node", "reply", reply)
				return
			}
		}
		return
	}

	if len(os.Args) < 3 || !raft.IsValidCmd(os.Args[1], os.Args[2]) {
		slog.Error("usage: raft-cli set x=? or raft-cli add address:port", "args", os.Args)
		return
	}
	if os.Args[1] == raft.AddCmd {
		conn, err := net.Dial("tcp", os.Args[2])
		if err != nil {
			slog.Error("Failed to dial peer", "peer", os.Args[2], "error", err)
			return
		}
		conn.SetDeadline(time.Now().Add(2 * time.Second))
		client := rpc.NewClient(conn)
		var nodeReply raft.GetNodeCmdReply
		cmd := "getNode"
		if err := client.Call("Node.GetNode", &cmd, &nodeReply); err != nil {
			slog.Error("RPC call failed", "target", os.Args[2], "error", err)
			return
		}
		client.Close()
		peersList := make([]string, 0, len(nodeReply.Peers))
		for _, addr := range nodeReply.Peers {
			peersList = append(peersList, addr)
		}
		visited := make(map[string]bool, len(peersList))
		for len(peersList) > 0 {
			top := len(peersList) - 1
			currentPeer := peersList[top]
			peersList = peersList[:top]
			if visited[currentPeer] {
				continue
			}
			visited[currentPeer] = true
			slog.Info("Sending Add server command", "node", nodeReply.NodeID, "peer", currentPeer)
			conn, err := net.Dial("tcp", currentPeer)
			if err != nil {
				slog.Error("Failed to dial peer", "peer", currentPeer, "error", err)
				continue
			}
			conn.SetDeadline(time.Now().Add(2 * time.Second))
			client := rpc.NewClient(conn)

			var reply raft.AddServerReply
			cmd := raft.AddServerArgs{NodeID: nodeReply.NodeID, NodeAddr: nodeReply.Addr}
			if err := client.Call("Node.AddServer", &cmd, &reply); err != nil {
				slog.Error("RPC call failed", "target", currentPeer, "error", err)
				continue
			}
			client.Close()
		}

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

		if reply.Success && (reply.LeaderID != nil && reply.LeaderAddr != nil) {
			slog.Info("Client successfully found a leader and got his command got executed", "leader", *reply.LeaderAddr, "Command", cmd)
			return
		} else if !reply.Success && (reply.PossibleLeader != nil && reply.PossibleLeaderAddr != nil) {
			if !visited[*reply.PossibleLeaderAddr] {
				peersList = append(peersList, *reply.PossibleLeaderAddr)
				slog.Info("Leader is not found trying possible one", "possible_leader", *reply.PossibleLeaderAddr)
			} else {
				slog.Info("Possible leader already visited, trying next node", "possible_leader", *reply.PossibleLeaderAddr)
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
