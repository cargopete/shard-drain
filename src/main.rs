mod catalog;
mod config;
mod drain;
mod finish;
mod graphman;
mod plan;
mod verify;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Result, bail};
use clap::{Parser, Subcommand};

use crate::config::GnConfig;
use crate::drain::{Ctx, human};
use crate::plan::Action;

/// Move every deployment out of one graph-node shard into another with
/// graphman copy, verify each copy before switching to it, and reclaim the
/// source. Reads Postgres directly; every write goes through graphman.
#[derive(Parser)]
#[command(version)]
struct Cli {
    /// graph-node config file
    #[arg(long, short, env = "GRAPH_NODE_CONFIG")]
    config: PathBuf,
    /// Shard to empty
    #[arg(long, default_value = "primary")]
    from: String,
    /// Shard to move into
    #[arg(long)]
    to: Option<String>,
    /// How to run graphman, if not `graphman --config <config>`
    #[arg(long)]
    graphman: Option<String>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Show what would be moved, what would be skipped and why
    Plan {
        #[arg(long, default_value_t = 200)]
        offset: i32,
        /// Node to index the copies (default: the source's node)
        #[arg(long)]
        node: Option<String>,
    },
    /// Copy, verify, switch and (with --reclaim) remove the sources
    Drain {
        /// Blocks behind the source head that the copy starts from
        #[arg(long, default_value_t = 200)]
        offset: i32,
        #[arg(long)]
        node: Option<String>,
        /// Copies running at once
        #[arg(long, default_value_t = 1)]
        parallel: usize,
        /// Stop starting new copies once the sources in flight add up to this
        #[arg(long)]
        max_gib: Option<f64>,
        /// How hard to check each copy before switching to it
        #[arg(long, value_enum, default_value = "checksum")]
        verify: verify::Depth,
        /// Remove each source with graphman unused once the copy serves
        #[arg(long)]
        reclaim: bool,
        /// Also remove deployments in the shard that graphman already
        /// considers unused (unassigned and not a current or pending version)
        #[arg(long)]
        reclaim_unused: bool,
        /// Don't pause a still-syncing source to let its copy catch up
        #[arg(long)]
        no_pause_lagging: bool,
        #[arg(long, default_value = "shard-drain.json")]
        journal: PathBuf,
        #[arg(long, default_value_t = 5)]
        poll_secs: u64,
        /// Treat a copy that has sat queued this long as stuck
        #[arg(long, default_value_t = 120)]
        stall_secs: u64,
        /// graph-node's GRAPH_QUERY_STATS_REFRESH_INTERVAL, if changed from 300
        #[arg(long, default_value_t = 300)]
        layout_cache_secs: u64,
        /// Remove a switched source only after its head has not moved for this long
        #[arg(long, default_value_t = 60)]
        reclaim_delay_secs: u64,
        /// Only these deployments (hash or sgdN)
        only: Vec<String>,
    },
    /// Compare two copies of a deployment as of one block, the check drain
    /// runs before switching
    Compare {
        /// Schema of one copy (sgdN)
        a: String,
        /// Schema of the other
        b: String,
        /// Defaults to the lower of the two heads
        #[arg(long)]
        block: Option<i32>,
        #[arg(long, value_enum, default_value = "checksum")]
        verify: verify::Depth,
    },
    /// Show the journal
    Status {
        #[arg(long, default_value = "shard-drain.json")]
        journal: PathBuf,
    },
    /// Move the chain block caches out of the drained shard
    Chains {
        #[arg(long)]
        apply: bool,
        /// Remove the `<chain>-old` caches left behind by a move
        #[arg(long, conflicts_with = "apply")]
        remove_old: bool,
    },
    /// Check the drained shard is empty and the foreign servers match the config
    Verify,
    /// Dump an empty primary into a new database, and check it arrived whole
    RelocatePrimary {
        /// Connection string of the new, empty database
        #[arg(long)]
        target: String,
        #[arg(long, default_value = "pg_dump")]
        pg_dump: String,
        #[arg(long, default_value = "pg_restore")]
        pg_restore: String,
        #[arg(long)]
        apply: bool,
    },
}

#[tokio::main]
async fn main() {
    match run().await {
        Ok(true) => {}
        Ok(false) => std::process::exit(1),
        Err(e) => {
            eprintln!("error: {e:#}");
            std::process::exit(2);
        }
    }
}

async fn run() -> Result<bool> {
    let cli = Cli::parse();
    let cfg = GnConfig::load(&cli.config)?;
    if let Some(to) = &cli.to
        && to == &cli.from
    {
        bail!("--from and --to are both {to}");
    }
    let ctx = Ctx {
        primary: catalog::connect(cfg.url(config::PRIMARY)?).await?,
        from: catalog::connect(cfg.url(&cli.from)?).await?,
        to: match &cli.to {
            Some(t) => Some(catalog::connect(cfg.url(t)?).await?),
            None => None,
        },
        from_shard: cli.from.clone(),
        to_shard: cli.to.clone(),
        graphman: graphman::Graphman::new(cli.graphman.as_deref(), &cli.config)?,
    };
    let need_to = || -> Result<()> {
        if ctx.to_shard.is_none() {
            bail!("--to is required for this command");
        }
        Ok(())
    };

    match cli.cmd {
        Cmd::Plan { offset, node } => plan(&cfg, &ctx, offset, node.as_deref()).await,
        Cmd::Drain {
            offset,
            node,
            parallel,
            max_gib,
            verify,
            reclaim,
            reclaim_unused,
            no_pause_lagging,
            journal,
            poll_secs,
            stall_secs,
            layout_cache_secs,
            reclaim_delay_secs,
            only,
        } => {
            need_to()?;
            let mut j =
                drain::Journal::open(&journal, &cli.from, ctx.to_shard.as_deref().unwrap())?;
            let opts = drain::Opts {
                offset,
                parallel: parallel.max(1),
                max_bytes: max_gib.map(|g| (g * 1024.0 * 1024.0 * 1024.0) as i64),
                depth: verify,
                reclaim,
                reclaim_unused,
                pause_lagging: !no_pause_lagging,
                poll: Duration::from_secs(poll_secs.max(1)),
                stall: Duration::from_secs(stall_secs),
                layout_cache: Duration::from_secs(layout_cache_secs),
                reclaim_delay: Duration::from_secs(reclaim_delay_secs),
            };
            warn_rules(&cfg, &cli.from);
            drain::drain(&ctx, &mut j, &opts, node.as_deref(), &only).await
        }
        Cmd::Compare {
            a,
            b,
            block,
            verify,
        } => compare(&cfg, &ctx, &a, &b, block, verify).await,
        Cmd::Status { journal } => {
            need_to()?;
            let j = drain::Journal::open(&journal, &cli.from, ctx.to_shard.as_deref().unwrap())?;
            for m in j.moves.values() {
                println!(
                    "{:>8} -> {:<8} {:<10?} {} {} {}",
                    m.src_ns,
                    m.dst_ns.as_deref().unwrap_or("-"),
                    m.phase,
                    m.hash,
                    m.original,
                    m.note.as_deref().unwrap_or("")
                );
            }
            Ok(true)
        }
        Cmd::Chains { apply, remove_old } => {
            need_to()?;
            finish::chains(&ctx, apply, remove_old).await
        }
        Cmd::Verify => finish::verify(&cfg, &ctx).await,
        Cmd::RelocatePrimary {
            target,
            pg_dump,
            pg_restore,
            apply,
        } => finish::relocate_primary(&cfg, &ctx, &target, &pg_dump, &pg_restore, apply).await,
    }
}

fn warn_rules(cfg: &GnConfig, from: &str) {
    for r in cfg.rules_placing_in(from) {
        println!(
            "warning: deployment rule {} ({}) can still place new deployments in {from}",
            r.index + 1,
            if r.default {
                "the default rule"
            } else {
                "a match rule"
            }
        );
    }
}

async fn plan(cfg: &GnConfig, ctx: &Ctx, offset: i32, node: Option<&str>) -> Result<bool> {
    warn_rules(cfg, &ctx.from_shard);
    let all = drain::survey(ctx, offset, node).await?;
    let (mut moves, mut reclaims, mut blocked, mut bytes) = (0, 0, 0, 0i64);
    println!(
        "{:<8} {:<46} {:<36} {:>7} {:>10}  action",
        "schema", "deployment", "state", "head", "size"
    );
    for s in &all {
        let state = match &s.detail {
            Some(d) if d.failed => format!("failed, {}", s.site.assignment),
            Some(d) if !d.synced => format!("syncing, {}", s.site.assignment),
            Some(_) => s.site.assignment.to_string(),
            None => "no metadata".into(),
        };
        let head = s
            .detail
            .as_ref()
            .and_then(|d| d.head)
            .map_or("-".into(), |h| h.to_string());
        let action = match &s.action {
            Action::Move { node } => {
                moves += 1;
                bytes += s.bytes;
                format!("move, copy indexed by {node}")
            }
            Action::Reclaim => {
                reclaims += 1;
                "reclaim only (already unused)".into()
            }
            Action::Blocked(why) => {
                blocked += 1;
                format!("SKIP: {why}")
            }
        };
        println!(
            "{:<8} {:<46} {:<36} {:>7} {:>10}  {action}{}",
            s.site.namespace,
            s.site.hash,
            state,
            head,
            human(s.bytes),
            if s.site.active { "" } else { " [inactive]" }
        );
    }
    let chains: Vec<String> = catalog::chains(&ctx.primary)
        .await?
        .into_iter()
        .filter(|c| c.shard == ctx.from_shard)
        .map(|c| format!("{} ({})", c.name, c.namespace))
        .collect();
    println!(
        "\n{moves} to move ({}), {reclaims} to reclaim, {blocked} skipped",
        human(bytes)
    );
    if !chains.is_empty() {
        println!("chain caches in {}: {}", ctx.from_shard, chains.join(", "));
    }
    Ok(blocked == 0)
}

async fn compare(
    cfg: &GnConfig,
    ctx: &Ctx,
    a: &str,
    b: &str,
    block: Option<i32>,
    depth: verify::Depth,
) -> Result<bool> {
    let find = async |ns: &str| -> Result<(catalog::Site, tokio_postgres::Client)> {
        let id: i32 = ns
            .strip_prefix("sgd")
            .and_then(|n| n.parse().ok())
            .ok_or_else(|| anyhow::anyhow!("`{ns}` is not an sgdN schema name"))?;
        let site = catalog::site_by_id(&ctx.primary, id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("{ns} is not in deployment_schemas"))?;
        let db = catalog::connect(cfg.url(&site.shard)?).await?;
        Ok((site, db))
    };
    let (sa, da) = find(a).await?;
    let (sb, db) = find(b).await?;
    if sa.hash != sb.hash {
        bail!("{a} is {} and {b} is {}", sa.hash, sb.hash);
    }
    let block = match block {
        Some(b) => b,
        None => {
            let ha = catalog::detail(&da, sa.id).await?.head.unwrap_or(0);
            let hb = catalog::detail(&db, sb.id).await?.head.unwrap_or(0);
            ha.min(hb)
        }
    };
    let mut diffs = verify::compare_metadata(&da, sa.id, &db, sb.id).await?;
    let (data, tables, rows) = verify::compare(&da, a, &db, b, block, depth).await?;
    diffs.extend(data);
    for d in &diffs {
        println!("  x {d}");
    }
    if diffs.is_empty() {
        println!("{a} and {b} match at block {block}: {tables} tables, {rows} rows ({depth:?})");
    }
    Ok(diffs.is_empty())
}
