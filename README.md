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

## Status

The CLI client is not finished yet. Planned features:

- sending commands to the cluster
- disconnecting nodes to test leader election
