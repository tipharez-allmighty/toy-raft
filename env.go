package raft

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

func LoadPeersMap() (map[int]string, error) {
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

func MustLoadPeers() []string {
	peers, ok := os.LookupEnv("PEERS")
	if ok {
		peersList := strings.Split(peers, ",")
		for _, peer := range peersList {
			peerAddr := strings.Split(peer, "=")
			if peerAddr[1] == "" {
				panic(fmt.Errorf("peer address is not provided for node %v", peerAddr[0]))
			}
			peersList = append(peersList, peerAddr[1])
		}
		return peersList
	}
	ips, err := net.LookupHost("raft")
	if err != nil {
		panic(fmt.Errorf("failed to dial host network to get nodes adresses, error: %w", err))
	}
	if len(ips) == 0 {
		panic(errors.New("DNS raft doesnt have nodes"))
	}
	for idx, ip := range ips {
		ips[idx] = net.JoinHostPort(ip, "50051")
	}
	return ips
}
