package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

type Node struct {
	NodeID int
	Addr   string
	Log    []string
	Peers  map[int]string
}

func (n *Node) AppendEntry(entry *string, reply *bool) error {
	n.Log = append(n.Log, *entry)
	*reply = true
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
	peers, ok := os.LookupEnv("PEERS")
	if !ok {
		panic(errors.New("failed to load env variabel PEERS"))
	}
	peersList := strings.Split(peers, ",")
	peersMap := make(map[int]string, len(peersList))
	for _, peer := range peersList {
		peerAddr := strings.Split(peer, "=")
		peerID, err := strconv.Atoi(peerAddr[0])
		if err != nil {
			panic(errors.New("failed to cast peer id into int"))
		}
		if peerAddr[1] == "" {
			panic(errors.New("peer address is not provided"))
		}
		peersMap[peerID] = peerAddr[1]
	}
	return &Node{nodeIDInt, addr, []string{}, peersMap}
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

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			slog.Info("Current log entries", "node", node.NodeID, "log", node.Log)
			for peerID, peerAddr := range node.Peers {
				if peerAddr == node.Addr {
					continue
				}
				slog.Info("Processing new rpc call", "peer", peerID, "address", peerAddr)

				go func(addr string) {
					client, err := rpc.Dial("tcp", addr)
					if err != nil {
						return
					}
					defer client.Close()

					var reply bool
					msg := ""
					if err := client.Call("Node.AppendEntry", &msg, &reply); err != nil {
						slog.Error("RPC call failed", "target", addr, "error", err)
					}
				}(peerAddr)
			}
		}
	}()
	select {}
}
