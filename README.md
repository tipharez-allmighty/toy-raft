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
- **Log replication**: Log is replicated to all the followers after client sends execute command request to the leader.
- **Commit:** once an entry is stored on a majority of nodes, the leader advances its commit index and followers pick it up from the next `AppendEntries`.
- **State machine:** a key-value store (`map[string]string`). A background worker applies committed entries in order, so `set x=10` sets `x` to `10`.
- **Election restriction**: voter denies its vote if its own log is more up-to-date than that of the candidate.

It also has **leader discovery** for the client:

- Each node remembers the current leader it learned from heartbeats.
- The CLI sends its command to a random node via the `ExecuteCommand` RPC. If that node is the leader, it accepts the command and appends it to its log.
- If the node is not the leader, it replies with the leader it knows about (if any). The client tries that node next, otherwise it moves on to another random node.
- Each node is tried at most once. If none of them is the leader, the client gives up.

Not implemented yet: election restriction and persistence.

## Running

Start the cluster:

```sh
podman compose up --build
```

In another terminal, run the CLI client:

```sh
podman compose run --rm cli-client set x=10
```

### Testing re-election

You can take nodes down while the cluster is running and watch a new leader get elected. In another terminal:

```sh
podman pause node2      # freeze the node, but keeps it in memory
podman unpause node2    # bring it back

podman stop node2       # or kill it completely
podman start node2
```

Find the current leader in the logs (`Leader has been elected`) and pause it. Within about 150–300ms the remaining nodes time out, start an election and pick a new leader. When the old leader comes back, it sees the newer term in the next heartbeat and steps down to follower.

The cluster needs a majority (3 of 5 nodes) to elect a leader. You can take down two nodes and still have a leader. With three nodes down, elections keep failing until a node comes back.

## Status

Planned features:
- log compaction (section 7)
- cluster membership changes (section 6)
