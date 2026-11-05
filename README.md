# shard-drain

Empties one graph-node shard into another: every deployment is copied with
`graphman copy`, checked against its source, switched over, and the source
removed. Then it moves the chain caches, and, if the shard being emptied is
the primary, dumps the catalog that remains into a new database.

The use it was built for: a primary that holds a few hundred subgraphs, which
the operator wants to turn into a small catalog-only database without
reindexing anything.

It reads Postgres directly and never writes to graph-node's tables. Every
change is a graphman command, printed as it runs, and its effect is checked
in the database afterwards.

## The procedure

```sh
export GRAPH_NODE_CONFIG=/etc/graph-node/config.toml
sd="shard-drain --to data"            # --from defaults to primary

# 0. Add the new shard to the config, and point every deployment rule at it,
#    so nothing new lands in the shard being emptied. Restart graph-node.

# 1. See what will happen
$sd plan

# 2. Move, verify, switch, reclaim. Safe to stop and rerun: it resumes from
#    its journal (shard-drain.json) and from what the database says
$sd drain --reclaim --parallel 2 --max-gib 200

# 3. Move the chain block caches, restart graph-node, then drop the old ones
$sd chains --apply
$sd chains --remove-old               # after the restart, once ingesting

# 4. Check the shard is empty
$sd verify

# 5. Primary only: stop every graph-node, copy the catalog to the new server,
#    point [store.primary] at it, start graph-node, verify again
shard-drain relocate-primary --target postgresql://graph:...@small-db/graph --apply
shard-drain verify
```

The new shard can be a second database on the same Postgres server as the
old primary. Then no new machine is needed for the copy, only disk.

## What each deployment goes through

| phase | what happens |
|---|---|
| pending | waits for a slot (`--parallel`, `--max-gib`) |
| copying | `graphman copy create --offset N <src> <to> <node>`; waits for the copy, then for its head to reach the source's |
| caught up | compares the copy with the source at the lower of the two heads, table by table: row counts and an md5 of every row's content (`--verify checksum`, the default), counts only, or none |
| verified | `graphman copy activate`, then `graphman unassign` on the source |
| switched | with `--reclaim`: waits until the source's head has not moved for `--reclaim-delay-secs` (60), so graph-node has stopped writing to it, then `graphman unused record` and `unused remove -d <hash>`, after checking that the only unused rows for that hash are in the shard being emptied |

A mismatch stops that deployment before the switch, leaves queries on the
source, and the rest carry on. `shard-drain compare sgdA sgdB` runs the same
check by hand.

The copy keeps the source's assignment. A paused source gives a paused copy,
and an unassigned source that a subgraph name still serves gives an
unassigned copy (pass `--node` for its copy to be indexed somewhere).

A source that is still syncing indexes as fast as its copy, so the copy only
catches it at chain head. Unless `--no-pause-lagging` is given, the tool
pauses such a source once its copy has copied, so that the copy closes the
gap in a few hundred blocks.

## What `plan` will not move

- A deployment that has never indexed a block: graphman refuses to copy it.
  Remove it and redeploy once the rules point at the new shard.
- A head within `--offset` of its start, or an offset block that pruning has
  already removed. The message gives the largest offset that would work.
- Anything in a running copy.

Deployments graphman already counts as unused (unassigned, and not a current
or pending version of any name) are listed as reclaim-only, and removed only
with `--reclaim-unused`.

## Known limits

- `graphman copy` only runs while the copy is assigned and running. So a copy
  of a paused or unassigned source indexes past the source's head until the
  tool pauses or unassigns it again. The tool watches such copies every
  200 ms. On the test rig, whose handlers clear a hundred blocks in a tenth
  of a second, the copy still reached chain head, 97 blocks on. Pausing the
  copy earlier stops the copy itself.
- graph-node caches each shard's table layouts by deployment hash, and
  graphman removes deployments in its own process, so graph-node never hears
  of the removal. If a copy of a hash is started in a shard that hash left
  recently, graph-node starts it against the old, dropped schema. It fails
  with "Deployment details not found ... Record not found", and graph-node
  never retries a failed start. Worse, each attempt refreshes the stale
  entry, so restarting the copy does not help. The entry only goes once
  nothing has touched it for twice `GRAPH_QUERY_STATS_REFRESH_INTERVAL`
  (300 s), or when graph-node restarts. So the tool will not start a copy
  into a shard the hash left within that window plus a minute
  (`--layout-cache-secs` if you changed the interval). A copy that sits
  queued past `--stall-secs` (120) is paused for the same window, then
  resumed once.
- `graphman chain change-shard` leaves a running graph-node failing to ingest
  ("Record not found") until it restarts.
- Postgres connections are plain TCP; `sslmode=require` is refused. Use a
  local or tunnelled port.
- The tool panics if its stdout closes, for instance when piped into a filter
  that exits. Every step is safe to rerun, and the journal picks it up.

## Tested

On graph-node v0.45.0 with three Postgres 17 databases, against eleven
deployments chosen for their awkwardness. Moved: assigned, paused,
unassigned-but-served, failed at block 1500, a factory with templates, a
timeseries with an hourly aggregation, a source still syncing, and one pruned
to 100 blocks. Reclaimed: an orphan, and a superseded pending version.
Skipped with the right reason: one that never indexed.

Proofs of indexing and pinned GraphQL results at nine fixed blocks were taken
before the drain. All 114 values that existed in both were identical after
the drain, and again after the primary was moved to a new database with the
old one switched off. Names, current and pending versions, and version
switching to the synced pending version all survived. A SIGKILL mid-copy was
resumed from the journal. Tampering with a copy (a mutable value, an
immutable row, a deleted row, a POI digest) was caught each time by
`compare`.

Then every deployment was moved from the data shard into the new primary and
back again on the final build: 140 of 140 values identical after each leg.
`drain` adopted a hand-made copy with one tampered row and refused to switch,
leaving queries on the source. The stuck start above was reproduced on
purpose, both ways: a copy already stuck was paused for the cache window,
resumed, and completed; and a hash sent straight back to the shard it had
just left waited out the window, then started first time.

Not tested: deployments larger than about 40 MiB, so nothing about copy time
or disk at scale; more than one index node; chain reorgs (the chain was
anvil); graph-node releases other than v0.45.0.
