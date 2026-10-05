package main

import (
	"log/slog"
	"math/rand"
	"net/rpc"
	"os"

	env "toy-raft"
)

func main() {
	peersMap, err := env.LoadPeers()
	if err != nil {
		panic(err)
	}
	if len(os.Args) < 2 {
		slog.Error("usage: raft-cli <commands>")
		return
	}
	slog.Info("Peers", "peers", peersMap)
	peerAddr := peersMap[rand.Intn(len(peersMap))+1]
	slog.Info("Sending data", "data", os.Args[1:], "peer", peerAddr)
	client, err := rpc.Dial("tcp", peerAddr)
	if err != nil {
		slog.Error("Failed to dial peer", "peer", peerAddr, "error", err)
		os.Exit(1)
	}
	defer client.Close()

	var reply bool
	msg := "Are you a leader"
	if err := client.Call("Node.AppendEntry", &msg, &reply); err != nil {
		slog.Error("RPC call failed", "target", peerAddr, "error", err)
		os.Exit(1)
	}
	slog.Info("Successfully sent RPC call to node", "peer", peerAddr)
}
