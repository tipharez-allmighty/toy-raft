package raft

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func LoadPeers() (map[int]string, error) {
	peers, ok := os.LookupEnv("PEERS")
	if !ok {
		return nil, fmt.Errorf("failed to load env variable PEERS")
	}
	peersList := strings.Split(peers, ",")
	peersMap := make(map[int]string, len(peersList))
	for _, peer := range peersList {
		peerAddr := strings.Split(peer, "=")
		peerID, err := strconv.Atoi(peerAddr[0])
		if err != nil {
			return nil, fmt.Errorf("failed to cast peer id into int")
		}
		if peerAddr[1] == "" {
			return nil, fmt.Errorf("peer address is not provided")
		}
		peersMap[peerID] = peerAddr[1]
	}
	return peersMap, nil
}
