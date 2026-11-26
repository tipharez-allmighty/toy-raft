# Manual test plan

All commands run from the repo root. Node logs go to stderr, so use `2>&1` when piping `podman logs` into `grep`.

Handy alias for the rest of this file:

```sh
alias raft-client='podman compose run --rm cli-client'
```

Compaction settings (`raft.go`): `SnapshotThreshold = 5`, `ChunkSize = 10` bytes.
The log starts with one config entry, so **every 4 `set` commands trigger a compaction**, and a snapshot of even a few keys is sent in several chunks.

---

## 0. Start a clean cluster

```sh
podman compose up --build
```

Nodes keep nothing on disk, so restarting the containers gives a fresh cluster.

If you ran this plan before, delete the old containers first:

```sh
podman rm -f node6 node7          # removes nodes added in step 6; compose doesn't know about them
                                  # because they were started with `podman run`
podman compose down               # removes node1..node5 (the ones compose started)
```

(In this order, so the network is free when `compose down` removes it.)

In a second terminal:

```sh
for n in 1 2 3 4 5; do echo "== node$n"; podman logs node$n 2>&1 | grep "Leader has been elected"; done
```

Expected: exactly one node logs `Leader has been elected`.

---

## 1. Leader discovery (`explore`)

```sh
raft-client explore
```

Expected: `Client successfully discovered a cluster` with the leader address and `Peers` = the other 4 nodes (the leader isn't in its own `Peers`).

---

## 2. Log replication + state machine

```sh
raft-client set a=1
raft-client set b=2
raft-client set c=3
```

Expected:
- CLI: `Client successfully found a leader and got his command got executed`.
- Every node: `New command was applied to the state machine` with `current_state` growing to `map[a:1 b:2 c:3]`.

```sh
for n in 1 2 3 4 5; do echo "== node$n"; podman logs node$n 2>&1 | grep "applied to the state machine" | tail -1; done
```

---

## 3. Compaction on every node

One more `set` pushes the log to 5 entries:

```sh
raft-client set d=4
```

Expected on **every** node:

```sh
for n in 1 2 3 4 5; do echo "== node$n"; podman logs node$n 2>&1 | grep "Truncated log" | tail -1; done
```

- `Truncated log` with `index=4` and `truncated_log` holding a single entry (the last applied one).

Keep going and check it repeats every 4 sets (index 8, 12, ...):

```sh
for i in 5 6 7 8; do raft-client set k$i=$i; done
podman logs node1 2>&1 | grep "Truncated log"
```

---

## 4. Lagging follower gets a snapshot (InstallSnapshot)

Pause a **follower**. Run `raft-client explore` first. The commands use node4; if node4 is the leader, use another node that isn't.

```sh
raft-client explore                      # make sure node4 is not the leader
podman pause node4
for i in 1 2 3 4 5 6 7 8 9; do raft-client set p$i=$i; done
podman unpause node4
```

Expected:
- node4: `Received snapshot chunk in memory` for each chunk, the last with `done=true`.
- node4 keeps working afterwards:

```sh
raft-client set after=snap
podman logs node4 2>&1 | grep "applied to the state machine" | tail -1
```

`current_state` on node4 should contain all `p1..p9` keys plus `after` (it got them from the snapshot, not from the log).

Check the chunks in the leader's logs. `<leader>` is the leader from the `explore` above (for example node1). `target=` stays node4, because node4 is the node that receives the chunks:

```sh
podman logs <leader> 2>&1 | grep "snapshot chunk target=node4"   # node4 = the follower you paused; if you paused another node, use its name
podman logs node4 2>&1 | grep "snapshot chunk"
```

Expected on the leader: several `Successfully sent snapshot chunk target=node4:50051 offset=... end=... total=...`, the last one with `end == total`.

### 4b. Same with a fully restarted node (empty memory)

```sh
podman stop node3
for i in 1 2 3 4 5; do raft-client set r$i=$i; done
podman start node3
raft-client set after=restart
podman logs node3 2>&1 | grep -E "snapshot chunk|applied to the state machine" | tail -5
```

Expected:
- After `podman start node3`: node3's log is empty and it has no snapshot.
- Leader: `Successfully sent snapshot chunk target=node3:50051 ...` (sent by heartbeats; the `set` is not needed for this).
- node3: `Received snapshot chunk in memory`, the last one with `done=true`. Its log is now just the placeholder entry at `LastIncludedIndex`.
- `set after=restart` reaches node3 through normal AppendEntries (the first append after the snapshot). Its log is the placeholder plus `after=restart`.
- node3's last `applied to the state machine` line: `current_state` has every key (`r1..r5` and all older keys from the snapshot) plus `after:restart`.

The placeholder is in `n.Log`, not in `current_state` (`n.StateMachine.State`). No log line or RPC prints `n.Log`, so you can't see the placeholder from outside.

---

## 5. Leader failover after compaction

```sh
raft-client explore                      # note the leader, e.g. node1
podman pause node1                       # pause the leader
raft-client explore                      # a new leader within ~300ms
raft-client set x=after_failover
podman unpause node1
raft-client set y=old_leader_back
```

Expected:
- A new `Leader has been elected` on another node.
- Writes keep working; the new leader keeps compacting (`Truncated log`).
- node1 steps down, catches up (log entries or a snapshot, depending on how far behind it is) and applies `x` and `y`.

---

## 6. Add a node after compaction (AddServer + snapshot)

Make sure the leader has already compacted (step 3), then start node6 as a learner:

```sh
podman run -d \
  --name node6 \
  --hostname node6 \
  --network raft-net \
  -e NODE_ID=6 \
  -e ADDRESS=node6:50051 \
  -e PEERS=1=node1:50051,2=node2:50051,3=node3:50051,4=node4:50051,5=node5:50051 \
  raft-node \
  ./raft-node

raft-client add node6:50051
```

Expected:
- CLI: `Client successfully changed cluster membership command=add`.
- Leader: `Successfully sent snapshot chunk target=node6:50051 ...` (the leader's log no longer starts at 1, so node6 can only catch up via the snapshot).
- node6: `Received snapshot chunk in memory ... done=true`.

```sh
raft-client explore                       # Peers now include 6=node6:50051
raft-client set new=node6
podman logs node6 2>&1 | grep "applied to the state machine" | tail -1
```

`current_state` on node6 should have every key written before it joined.

### 6b. Only one membership change at a time

Needs node6 (step 6) and node7 as members. Add node7:

```sh
podman run -d --name node7 --hostname node7 --network raft-net \
  -e NODE_ID=7 -e ADDRESS=node7:50051 \
  -e PEERS=1=node1:50051,2=node2:50051,3=node3:50051,4=node4:50051,5=node5:50051,6=node6:50051 \
  raft-node ./raft-node

raft-client add node7:50051
```

With all nodes up, a remove commits in a few milliseconds, so two removes sent "at the same time" almost never overlap. To force the overlap, pause every node except the leader, node6 and node7. Then the first remove can't commit, and the second one arrives while it is still pending.

The commands assume the leader is node4. Run `raft-client explore` first; if the leader is another node, pause node4 instead of that node.

```sh
raft-client explore                       # leader must be node4 (or swap names, see above)
podman pause node1 node2 node3 node5      # leader node4 + node6 + node7 = 3 of 7, no majority
raft-client remove node7:50051            # can't commit, CLI prints "Membership change process exipred"
raft-client remove node6:50051            # rejected, CLI only shows timeouts and "Possible leader already visited"
podman logs node4 2>&1 | grep "Membership change rejected"
podman unpause node1 node2 node3 node5
raft-client set after=6b                  # makes the pending remove commit if an election happened
raft-client explore                       # node7 gone from Peers, node6 still there
```

Expected:
- The `grep` prints `Membership change rejected, another change is not committed yet cmd=remove node=node6:50051`.
- After the unpause, the pending node7 remove commits, and node6 stays a member for step 7.
- The CLI never says "busy": `ServerReply` has no `Busy` field, so the rejection only shows in the leader's log.

---

## 7. Remove a node

```sh
raft-client remove node6:50051
raft-client explore                       # node6 gone from Peers
raft-client set after=remove
```

Expected: `Client successfully changed cluster membership command=remove`, writes keep committing.
Then stop the removed container so its election timer doesn't disturb the cluster:

```sh
podman rm -f node6 node7
```

---

## 8. Majority / no-majority

Pause only followers, never the leader. Run `raft-client explore` first. The commands assume the leader is node1; otherwise swap the names so the leader is never paused.

```sh
raft-client explore                      # leader must not be node3, node4 or node5
podman pause node4 node5
raft-client set two_down=ok              # still works (3 of 5)
podman pause node3
raft-client set three_down=fail          # CLI says success, but it is NOT committed (2 of 5)
podman logs node1 2>&1 | grep "applied to the state machine" | tail -1   # use the leader; no three_down here
podman unpause node3 node4 node5
sleep 2                                  # the unpaused nodes may hold an election first
raft-client set recovered=ok
for n in 1 2 3 4 5; do echo "== node$n"; podman logs node$n 2>&1 | grep "applied to the state machine" | tail -1; done
```

Expected:
- `two_down` is applied while 2 nodes are paused.
- `three_down` is not applied while 3 are paused. The CLI still says success, because `ExecuteCommand` replies before the commit.
- After the unpause, every node has the same `current_state` with `recovered:ok`. `three_down` is there too, unless an election dropped it (it was never committed, so both are correct).
## Cleanup

```sh
podman rm -f node6 node7
podman compose down
```
