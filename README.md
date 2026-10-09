# toy-raft

A toy implementation of the Raft consensus algorithm in Go, based on the
[Raft paper](https://raft.github.io/raft.pdf).

## What's implemented

So far it covers **leader election** (section 5.2 of the paper):

- A 5-node cluster, each node in its own container, talking over Go `net/rpc`.
- Each node is a follower, candidate or leader, and tracks its current term and who it voted for.
- **Randomized election timeouts** (150–300ms): if a follower hears nothing from a leader, it becomes a candidate, increments its term, votes for itself and sends `RequestVote` to its peers.
- **Voting rules:** a node rejects candidates with an older term, steps down when it sees a newer term, and grants at most one vote per term.
- **Majority wins:** a candidate with votes from a majority of the cluster becomes leader.
- **Heartbeats:** the leader sends empty `AppendEntries` every 50ms to keep its leadership. Followers reset their election timer when a heartbeat arrives.

It also has **leader discovery** for the client:

- Each node remembers the current leader it learned from heartbeats.
- The CLI sends its command to a random node via the `ExecuteCommand` RPC. If that node is the leader, it accepts the command and appends it to its log.
- If the node is not the leader, it replies with the leader it knows about (if any). The client tries that node next, otherwise it moves on to another random node.
- Each node is tried at most once. If none of them is the leader, the client gives up.

Not implemented yet: log replication, commit/apply of entries, and persistence.

## Running

Start the cluster:

```sh
podman compose up --build
```

In another terminal, run the CLI client:

```sh
podman compose run --rm cli-client hello
```

### Testing re-election

You can take nodes down while the cluster is running and watch a new leader get elected. In another terminal:

```sh
podman pause node2      # freeze the node, like a network partition
podman unpause node2    # bring it back

podman stop node2       # or kill it completely
podman start node2
```

Find the current leader in the logs (`Leader has been elected`) and pause it. Within about 150–300ms the remaining nodes time out, start an election and pick a new leader. When the old leader comes back, it sees the newer term in the next heartbeat and steps down to follower.

The cluster needs a majority (3 of 5 nodes) to elect a leader. You can take down two nodes and still have a leader. With three nodes down, elections keep failing until a node comes back.

## Status

The CLI client is not finished yet. Planned features:

- sending the command given on the command line (the client currently always sends `SET X=10`)
- disconnecting nodes to test leader election
