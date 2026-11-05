use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;

use crate::catalog::{self, Assignment, Detail, Site};
use crate::graphman::Graphman;
use crate::plan::{self, Action};
use crate::verify::{self, Depth};

pub struct Ctx {
    pub primary: Client,
    pub from: Client,
    pub to: Option<Client>,
    pub from_shard: String,
    pub to_shard: Option<String>,
    pub graphman: Graphman,
}

impl Ctx {
    fn to(&self) -> &Client {
        self.to.as_ref().expect("a target shard")
    }
    fn to_shard(&self) -> &str {
        self.to_shard.as_deref().expect("a target shard")
    }
}

pub struct Surveyed {
    pub site: Site,
    pub detail: Option<Detail>,
    pub bytes: i64,
    pub action: Action,
}

pub async fn survey(ctx: &Ctx, offset: i32, node: Option<&str>) -> Result<Vec<Surveyed>> {
    let copies = catalog::active_copies(&ctx.primary).await?;
    let mut out = Vec::new();
    for site in catalog::sites_in_shard(&ctx.primary, &ctx.from_shard).await? {
        // a row without metadata in its shard is reported, not fatal
        let detail = catalog::detail(&ctx.from, site.id).await.ok();
        let bytes = catalog::schema_bytes(&ctx.from, &site.namespace).await?;
        let current_or_pending = catalog::is_current_or_pending(&ctx.primary, &site.hash).await?;
        let others = catalog::sites_for_hash(&ctx.primary, &site.hash).await?;
        let active_elsewhere = others.iter().any(|o| o.id != site.id && o.active);
        let in_copy = copies
            .iter()
            .any(|(s, d, _)| *s == site.id || *d == site.id);
        let mut copy_into_target = false;
        if let Some(to) = ctx.to_shard.as_deref() {
            for (_, d, _) in copies.iter().filter(|(s, _, _)| *s == site.id) {
                if let Some(dst) = catalog::site_by_id(&ctx.primary, *d).await? {
                    copy_into_target |= dst.shard == to && dst.hash == site.hash;
                }
            }
        }
        let action = plan::classify(&plan::Input {
            site: &site,
            detail: detail.as_ref(),
            current_or_pending,
            in_copy,
            copy_into_target,
            active_elsewhere,
            offset,
            node_override: node,
        });
        out.push(Surveyed {
            site,
            detail,
            bytes,
            action,
        });
    }
    Ok(out)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Phase {
    Pending,
    Copying,
    CaughtUp,
    Verified,
    Switched,
    Reclaimed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Move {
    pub hash: String,
    pub src_id: i32,
    pub src_ns: String,
    pub bytes: i64,
    pub original: Assignment,
    pub node: String,
    pub src_failed: bool,
    pub dst_id: Option<i32>,
    pub dst_ns: Option<String>,
    pub paused_src_for_catchup: bool,
    pub verified_block: Option<i32>,
    pub phase: Phase,
    pub note: Option<String>,
    #[serde(default)]
    pub queued_since: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    pub cache_pause_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    pub resumed_after_cache: bool,
    #[serde(default)]
    pub switched_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    pub src_head_at_switch: Option<i32>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Journal {
    pub from: String,
    pub to: String,
    pub moves: BTreeMap<i32, Move>,
    #[serde(skip)]
    path: PathBuf,
}

impl Journal {
    pub fn open(path: &Path, from: &str, to: &str) -> Result<Self> {
        let mut j = if path.exists() {
            let text = std::fs::read_to_string(path)?;
            let j: Journal = serde_json::from_str(&text)
                .with_context(|| format!("reading journal {}", path.display()))?;
            if j.from != from || j.to != to {
                bail!(
                    "journal {} is for {} -> {}, not {from} -> {to}",
                    path.display(),
                    j.from,
                    j.to
                );
            }
            j
        } else {
            Journal {
                from: from.into(),
                to: to.into(),
                ..Default::default()
            }
        };
        j.path = path.to_path_buf();
        Ok(j)
    }

    pub fn save(&self) -> Result<()> {
        let tmp = self.path.with_extension("tmp");
        std::fs::write(&tmp, serde_json::to_string_pretty(self)?)?;
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

pub struct Opts {
    pub offset: i32,
    pub parallel: usize,
    pub max_bytes: Option<i64>,
    pub depth: Depth,
    pub reclaim: bool,
    pub reclaim_unused: bool,
    pub pause_lagging: bool,
    pub poll: Duration,
    /// How long a copy may sit queued before it is treated as stuck
    pub stall: Duration,
    /// graph-node's GRAPH_QUERY_STATS_REFRESH_INTERVAL
    pub layout_cache: Duration,
    /// How long a switched source must sit still before it is removed
    pub reclaim_delay: Duration,
}

enum Step {
    /// Did something; look at it again straight away
    Advanced,
    Waiting(String),
    Done,
}

pub async fn drain(
    ctx: &Ctx,
    journal: &mut Journal,
    opts: &Opts,
    node: Option<&str>,
    only: &[String],
) -> Result<bool> {
    let mut unused_failed = false;
    let mut skipped = false;
    // Add anything movable that the journal does not know yet. A source the
    // journal has seen keeps its recorded original assignment.
    for s in survey(ctx, opts.offset, node).await? {
        if !only.is_empty() && !only.contains(&s.site.hash) && !only.contains(&s.site.namespace) {
            continue;
        }
        if journal.moves.contains_key(&s.site.id) {
            continue;
        }
        match s.action {
            Action::Move { node } => {
                let src_failed = s.detail.as_ref().is_some_and(|d| d.failed);
                journal.moves.insert(
                    s.site.id,
                    Move {
                        hash: s.site.hash.clone(),
                        src_id: s.site.id,
                        src_ns: s.site.namespace.clone(),
                        bytes: s.bytes,
                        original: s.site.assignment.clone(),
                        node,
                        src_failed,
                        dst_id: None,
                        dst_ns: None,
                        paused_src_for_catchup: false,
                        verified_block: None,
                        phase: Phase::Pending,
                        note: None,
                        queued_since: None,
                        cache_pause_at: None,
                        resumed_after_cache: false,
                        switched_at: None,
                        src_head_at_switch: None,
                    },
                );
            }
            Action::Reclaim if opts.reclaim_unused => {
                println!(
                    "{} {}: already unused, reclaiming",
                    s.site.namespace, s.site.hash
                );
                if let Err(e) = reclaim(ctx, &s.site.hash, &[s.site.id]).await {
                    println!("{} {}: not reclaimed, {e:#}", s.site.namespace, s.site.hash);
                    unused_failed = true;
                }
            }
            Action::Reclaim => println!(
                "{} {}: already unused; --reclaim-unused removes it",
                s.site.namespace, s.site.hash
            ),
            Action::Blocked(why) => {
                skipped = true;
                println!("{} {}: skipped, {why}", s.site.namespace, s.site.hash)
            }
        }
    }
    journal.save()?;

    let mut last: BTreeMap<i32, String> = BTreeMap::new();
    loop {
        let mut advanced = false;
        let mut open = 0;
        let ids: Vec<i32> = journal.moves.keys().copied().collect();
        for id in ids {
            let phase = journal.moves[&id].phase;
            if matches!(phase, Phase::Reclaimed | Phase::Failed) {
                continue;
            }
            if phase == Phase::Switched && !opts.reclaim {
                continue;
            }
            open += 1;
            let in_flight = journal
                .moves
                .values()
                .filter(|m| matches!(m.phase, Phase::Copying | Phase::CaughtUp | Phase::Verified))
                .collect::<Vec<_>>();
            let busy = in_flight.len();
            let busy_bytes: i64 = in_flight.iter().map(|m| m.bytes).sum();
            let mut m = journal.moves[&id].clone();
            let step = match advance(ctx, &mut m, opts, busy, busy_bytes).await {
                Ok(step) => step,
                Err(e) => {
                    fail(ctx, &mut m, format!("{e:#}")).await;
                    Step::Advanced
                }
            };
            journal.moves.insert(id, m.clone());
            journal.save()?;
            match step {
                Step::Advanced => advanced = true,
                Step::Done => {}
                Step::Waiting(why) => {
                    if last.get(&id) != Some(&why) {
                        println!("{} {}: {why}", m.src_ns, m.hash);
                        last.insert(id, why);
                    }
                }
            }
        }
        if open == 0 {
            break;
        }
        if !advanced {
            // graphman copy only runs while the copy is assigned, so a copy
            // of a paused or unassigned source indexes on until it is paused
            // again; look often to keep that overrun short
            let watching = journal.moves.values().any(|m| {
                m.phase == Phase::Copying && !matches!(m.original, Assignment::Assigned(_))
            });
            let wait = if watching {
                opts.poll.min(Duration::from_millis(200))
            } else {
                opts.poll
            };
            tokio::time::sleep(wait).await;
        }
    }

    let failed: Vec<&Move> = journal
        .moves
        .values()
        .filter(|m| m.phase == Phase::Failed)
        .collect();
    for m in &failed {
        println!(
            "FAILED {} {}: {}",
            m.src_ns,
            m.hash,
            m.note.as_deref().unwrap_or("")
        );
    }
    Ok(failed.is_empty() && !unused_failed && !skipped)
}

async fn fail(ctx: &Ctx, m: &mut Move, why: String) {
    println!("{} {}: failed, {why}", m.src_ns, m.hash);
    // queries are still on the source, so put it back as it was
    if m.paused_src_for_catchup {
        let _ = ctx.graphman.run(&["resume", &m.src_ns]).await;
        m.paused_src_for_catchup = false;
    }
    m.phase = Phase::Failed;
    m.note = Some(why);
}

async fn find_dst(ctx: &Ctx, hash: &str) -> Result<Option<Site>> {
    Ok(catalog::sites_for_hash(&ctx.primary, hash)
        .await?
        .into_iter()
        .filter(|s| s.shard == ctx.to_shard())
        .max_by_key(|s| s.id))
}

async fn advance(
    ctx: &Ctx,
    m: &mut Move,
    opts: &Opts,
    busy: usize,
    busy_bytes: i64,
) -> Result<Step> {
    match m.phase {
        Phase::Pending => {
            if let Some(dst) = find_dst(ctx, &m.hash).await? {
                println!(
                    "{} {}: found existing copy {} in {}, resuming",
                    m.src_ns, m.hash, dst.namespace, dst.shard
                );
                m.dst_id = Some(dst.id);
                m.dst_ns = Some(dst.namespace);
                m.phase = Phase::Copying;
                return Ok(Step::Advanced);
            }
            if busy >= opts.parallel {
                return Ok(Step::Waiting(format!("queued, {busy} copies running")));
            }
            if let Some(max) = opts.max_bytes
                && busy > 0
                && busy_bytes + m.bytes > max
            {
                return Ok(Step::Waiting(format!(
                    "queued, {} in flight and this one is {}",
                    human(busy_bytes),
                    human(m.bytes)
                )));
            }
            // graph-node caches table layouts by hash and keeps a stale one
            // alive for as long as something asks for it; graphman removes
            // deployments out of process, so graph-node never evicts it.
            // A copy started into a shard this hash recently left would
            // start against the dropped schema and fail for good.
            let window = cache_window(opts).as_secs_f64();
            if let Some(left) =
                catalog::removal_wait(&ctx.primary, &m.hash, ctx.to_shard(), window).await?
            {
                // absolute time, so the line prints once rather than every poll
                let at = chrono::Utc::now() + chrono::Duration::seconds(left);
                return Ok(Step::Waiting(format!(
                    "a copy of this hash left {} recently; waiting until {} UTC for \
                     graph-node's layout cache to forget it",
                    ctx.to_shard(),
                    at.format("%H:%M:%S")
                )));
            }
            println!(
                "{} {}: copying to {} on {} ({})",
                m.src_ns,
                m.hash,
                ctx.to_shard(),
                m.node,
                human(m.bytes)
            );
            let out = ctx
                .graphman
                .run(&[
                    "copy",
                    "create",
                    "--offset",
                    &opts.offset.to_string(),
                    &m.src_ns,
                    ctx.to_shard(),
                    &m.node,
                ])
                .await?;
            match find_dst(ctx, &m.hash).await? {
                Some(dst) => {
                    m.dst_id = Some(dst.id);
                    m.dst_ns = Some(dst.namespace);
                    m.phase = Phase::Copying;
                    Ok(Step::Advanced)
                }
                None => bail!("graphman copy create made no copy: {}", out.output.trim()),
            }
        }
        Phase::Copying => {
            let dst_id = m.dst_id.expect("copying has a destination");
            let Some(state) = catalog::copy_state(ctx.to(), dst_id).await? else {
                // graph-node never retries a start that failed. The cause
                // seen in testing is the stale layout above; the cure is to
                // leave the copy alone long enough for the cache to sweep it
                let now = chrono::Utc::now();
                let since = *m.queued_since.get_or_insert(now);
                let waited = (now - since).to_std().unwrap_or_default();
                let dst_ns = m.dst_ns.clone().expect("copying has a destination");
                if let Some(at) = m.cache_pause_at {
                    let paused = (now - at).to_std().unwrap_or_default();
                    if paused < cache_window(opts) {
                        let resume =
                            at + chrono::Duration::from_std(cache_window(opts)).unwrap_or_default();
                        return Ok(Step::Waiting(format!(
                            "copy {dst_ns} paused so graph-node's layout cache can forget it, \
                             resuming at {} UTC",
                            resume.format("%H:%M:%S")
                        )));
                    }
                    println!("{} {}: resuming the copy {dst_ns}", m.src_ns, m.hash);
                    ctx.graphman.run(&["resume", &dst_ns]).await?;
                    m.cache_pause_at = None;
                    m.resumed_after_cache = true;
                    m.queued_since = Some(chrono::Utc::now());
                    return Ok(Step::Advanced);
                }
                if waited > opts.stall {
                    if m.resumed_after_cache {
                        bail!(
                            "graph-node has not started the copy {dst_ns}; look for \
                             SubgraphStartFailure for it in the graph-node log, or restart \
                             graph-node, which clears its caches"
                        );
                    }
                    println!(
                        "{} {}: copy {dst_ns} has not started after {}s; pausing it for {}s so \
                         graph-node drops any stale layout, then resuming",
                        m.src_ns,
                        m.hash,
                        waited.as_secs(),
                        cache_window(opts).as_secs()
                    );
                    ctx.graphman.run(&["pause", &dst_ns]).await?;
                    m.cache_pause_at = Some(chrono::Utc::now());
                    return Ok(Step::Advanced);
                }
                return Ok(Step::Waiting(
                    "copy queued, waiting for the node to start it".into(),
                ));
            };
            if state.cancelled {
                bail!("the copy was cancelled");
            }
            if !state.finished {
                return Ok(Step::Waiting(format!(
                    "copying up to block {}",
                    state.target_block
                )));
            }
            let src = catalog::detail(&ctx.from, m.src_id).await?;
            let dst = catalog::detail(ctx.to(), dst_id).await?;
            let (sh, dh) = (src.head.unwrap_or(0), dst.head.unwrap_or(0));
            if dst.failed && !m.src_failed {
                bail!(
                    "the copy failed where the source has not: {}",
                    dst.fatal_error.unwrap_or_default()
                );
            }
            if dh >= sh {
                // A copy of a paused or unassigned source would otherwise
                // index on towards chain head while it is being verified
                restore_assignment(ctx, m).await?;
                if !matches!(m.original, Assignment::Assigned(_)) {
                    let now = catalog::detail(ctx.to(), dst_id).await?.head.unwrap_or(0);
                    println!(
                        "{} {}: copy {} at {now}, the source stopped at {sh}",
                        m.src_ns,
                        m.hash,
                        if matches!(m.original, Assignment::Paused(_)) {
                            "paused"
                        } else {
                            "unassigned"
                        }
                    );
                }
                m.phase = Phase::CaughtUp;
                return Ok(Step::Advanced);
            }
            if opts.pause_lagging
                && !src.synced
                && !m.paused_src_for_catchup
                && matches!(m.original, Assignment::Assigned(_))
            {
                // A source that is still syncing runs as fast as the copy,
                // so the copy only catches it at chain head. Pausing it lets
                // the copy close the gap in a few hundred blocks.
                println!(
                    "{} {}: source is still syncing, pausing it so the copy can catch up",
                    m.src_ns, m.hash
                );
                ctx.graphman.run(&["pause", &m.src_ns]).await?;
                m.paused_src_for_catchup = true;
                return Ok(Step::Advanced);
            }
            Ok(Step::Waiting(format!(
                "copied, catching up: copy at {dh}, source at {sh}"
            )))
        }
        Phase::CaughtUp => {
            let dst_id = m.dst_id.expect("caught up has a destination");
            let dst_ns = m.dst_ns.clone().expect("caught up has a destination");
            let src = catalog::detail(&ctx.from, m.src_id).await?;
            let dst = catalog::detail(ctx.to(), dst_id).await?;
            let block = src.head.unwrap_or(0).min(dst.head.unwrap_or(0));
            let mut diffs = verify::compare_metadata(&ctx.from, m.src_id, ctx.to(), dst_id).await?;
            let (data, tables, rows) =
                verify::compare(&ctx.from, &m.src_ns, ctx.to(), &dst_ns, block, opts.depth).await?;
            diffs.extend(data);
            if !diffs.is_empty() {
                bail!(
                    "copy does not match the source at block {block}: {}",
                    diffs.join("; ")
                );
            }
            println!(
                "{} {}: verified at block {block}, {tables} tables, {rows} rows ({:?})",
                m.src_ns, m.hash, opts.depth
            );
            m.verified_block = Some(block);
            m.phase = Phase::Verified;
            Ok(Step::Advanced)
        }
        Phase::Verified => {
            let dst_id = m.dst_id.expect("verified has a destination");
            let dst_ns = m.dst_ns.clone().expect("verified has a destination");
            let dst = catalog::site_by_id(&ctx.primary, dst_id)
                .await?
                .context("the copy vanished")?;
            if !dst.active {
                ctx.graphman
                    .run(&["copy", "activate", &m.hash, ctx.to_shard()])
                    .await?;
                let dst = catalog::site_by_id(&ctx.primary, dst_id)
                    .await?
                    .context("the copy vanished")?;
                if !dst.active {
                    bail!("graphman copy activate did not activate {dst_ns}");
                }
            }
            let dst = restore_assignment(ctx, m).await?;
            let src = catalog::site_by_id(&ctx.primary, m.src_id)
                .await?
                .context("the source vanished")?;
            if src.assignment != Assignment::Unassigned {
                ctx.graphman.run(&["unassign", &m.src_ns]).await?;
                let src = catalog::site_by_id(&ctx.primary, m.src_id)
                    .await?
                    .context("the source vanished")?;
                if src.assignment != Assignment::Unassigned {
                    bail!("graphman unassign did not unassign {}", m.src_ns);
                }
            }
            m.paused_src_for_catchup = false;
            println!(
                "{} {}: switched to {dst_ns} in {}, {}",
                m.src_ns,
                m.hash,
                ctx.to_shard(),
                dst.assignment
            );
            m.phase = Phase::Switched;
            Ok(Step::Advanced)
        }
        Phase::Switched => {
            // dropping the schema while graph-node still flushes the
            // source's last writes poisons that writer
            let head = catalog::detail(&ctx.from, m.src_id).await?.head;
            let now = chrono::Utc::now();
            if m.switched_at.is_none() || m.src_head_at_switch != head {
                m.switched_at = Some(now);
                m.src_head_at_switch = head;
                return Ok(Step::Waiting(format!(
                    "waiting {}s for graph-node to stop the source before removing it",
                    opts.reclaim_delay.as_secs()
                )));
            }
            let still = (now - m.switched_at.unwrap()).to_std().unwrap_or_default();
            if still < opts.reclaim_delay {
                return Ok(Step::Waiting(format!(
                    "waiting {}s for graph-node to stop the source before removing it",
                    opts.reclaim_delay.as_secs()
                )));
            }
            reclaim(ctx, &m.hash, &[m.src_id]).await?;
            m.phase = Phase::Reclaimed;
            Ok(Step::Advanced)
        }
        Phase::Reclaimed | Phase::Failed => Ok(Step::Done),
    }
}

/// How long graph-node can keep an unused layout: its sweep drops entries
/// older than twice the TTL
fn cache_window(opts: &Opts) -> Duration {
    opts.layout_cache * 2 + Duration::from_secs(60)
}

/// Give the copy the source's original assignment: paused stays paused,
/// unassigned stays unassigned.
async fn restore_assignment(ctx: &Ctx, m: &Move) -> Result<Site> {
    let dst_id = m.dst_id.expect("restoring needs a destination");
    let dst_ns = m.dst_ns.as_deref().expect("restoring needs a destination");
    let dst = catalog::site_by_id(&ctx.primary, dst_id)
        .await?
        .context("the copy vanished")?;
    match (&m.original, &dst.assignment) {
        (Assignment::Paused(_), Assignment::Assigned(_)) => {
            ctx.graphman.run(&["pause", dst_ns]).await?;
        }
        (Assignment::Unassigned, a) if *a != Assignment::Unassigned => {
            ctx.graphman.run(&["unassign", dst_ns]).await?;
        }
        _ => {}
    }
    let dst = catalog::site_by_id(&ctx.primary, dst_id)
        .await?
        .context("the copy vanished")?;
    let matches = matches!(
        (&m.original, &dst.assignment),
        (Assignment::Assigned(_), Assignment::Assigned(_))
            | (Assignment::Paused(_), Assignment::Paused(_))
            | (Assignment::Unassigned, Assignment::Unassigned)
    );
    if !matches {
        bail!(
            "the copy is {} but the source was {}",
            dst.assignment,
            m.original
        );
    }
    Ok(dst)
}

/// Remove the unused rows for `hash` in the drained shard, and only those.
/// `must` are ids that have to be among them.
pub async fn reclaim(ctx: &Ctx, hash: &str, must: &[i32]) -> Result<()> {
    ctx.graphman.run(&["unused", "record"]).await?;
    let records: Vec<(i32, String)> = catalog::unused_records(&ctx.primary, hash)
        .await?
        .into_iter()
        .filter(|(_, _, removed)| !removed)
        .map(|(id, shard, _)| (id, shard))
        .collect();
    for id in must {
        if !records.iter().any(|(r, _)| r == id) {
            bail!("graphman does not consider id {id} unused, so it was not removed");
        }
    }
    if let Some((id, shard)) = records.iter().find(|(_, s)| *s != ctx.from_shard) {
        bail!(
            "unused id {id} of the same hash is in shard {shard}; `unused remove -d` would take it \
             too, so this is left for you"
        );
    }
    ctx.graphman.run(&["unused", "remove", "-d", hash]).await?;
    for (id, _) in &records {
        if let Some(site) = catalog::site_by_id(&ctx.primary, *id).await? {
            bail!(
                "{} is still in deployment_schemas after unused remove",
                site.namespace
            );
        }
    }
    for (id, _) in &records {
        let ns = format!("sgd{id}");
        if catalog::schema_exists(&ctx.from, &ns).await? {
            bail!(
                "schema {ns} is still in the {} database after unused remove",
                ctx.from_shard
            );
        }
    }
    println!(
        "{hash}: reclaimed {} from {}",
        records
            .iter()
            .map(|(i, _)| format!("sgd{i}"))
            .collect::<Vec<_>>()
            .join(", "),
        ctx.from_shard
    );
    Ok(())
}

pub fn human(bytes: i64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = bytes as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{bytes} B")
    } else {
        format!("{v:.1} {}", UNITS[u])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn human_sizes() {
        assert_eq!(human(512), "512 B");
        assert_eq!(human(1536), "1.5 KiB");
        assert_eq!(human(3 * 1024 * 1024 * 1024), "3.0 GiB");
    }

    #[test]
    fn journal_round_trips_and_refuses_another_pair() {
        let dir = std::env::temp_dir().join(format!("sd-journal-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("j.json");
        let _ = std::fs::remove_file(&path);
        let mut j = Journal::open(&path, "primary", "data").unwrap();
        j.moves.insert(
            3,
            Move {
                hash: "QmA".into(),
                src_id: 3,
                src_ns: "sgd3".into(),
                bytes: 10,
                original: Assignment::Paused("idx0".into()),
                node: "idx0".into(),
                src_failed: false,
                dst_id: Some(9),
                dst_ns: Some("sgd9".into()),
                paused_src_for_catchup: false,
                verified_block: None,
                phase: Phase::Copying,
                note: None,
                queued_since: None,
                cache_pause_at: None,
                resumed_after_cache: false,
                switched_at: None,
                src_head_at_switch: None,
            },
        );
        j.save().unwrap();
        let j2 = Journal::open(&path, "primary", "data").unwrap();
        assert_eq!(j2.moves[&3].original, Assignment::Paused("idx0".into()));
        assert_eq!(j2.moves[&3].phase, Phase::Copying);
        assert!(Journal::open(&path, "primary", "other").is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
