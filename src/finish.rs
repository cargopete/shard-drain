//! After the deployments: chain caches, the final check, and moving a
//! drained primary to another server.

use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail};
use tokio_postgres::Client;

use crate::catalog;
use crate::config::{GnConfig, PRIMARY};
use crate::drain::Ctx;

pub async fn chains(ctx: &Ctx, apply: bool, remove_old: bool) -> Result<bool> {
    let to = ctx
        .to_shard
        .as_deref()
        .context("--to is needed to move chains")?;
    let chains = catalog::chains(&ctx.primary).await?;
    let names: Vec<&str> = chains.iter().map(|c| c.name.as_str()).collect();
    let mut ok = true;

    if remove_old {
        for c in chains
            .iter()
            .filter(|c| c.shard == ctx.from_shard && c.name.ends_with("-old"))
        {
            println!(
                "{}: removing the old cache {} from {}",
                c.name, c.namespace, c.shard
            );
            ctx.graphman.run(&["chain", "remove", &c.name]).await?;
            let still = catalog::chains(&ctx.primary)
                .await?
                .into_iter()
                .any(|x| x.name == c.name);
            if still || catalog::schema_exists(&ctx.from, &c.namespace).await? {
                println!("{}: still there after graphman chain remove", c.name);
                ok = false;
            }
        }
        return Ok(ok);
    }

    let left = catalog::sites_in_shard(&ctx.primary, &ctx.from_shard).await?;
    let copies = catalog::active_copies(&ctx.primary).await?;
    let movable: Vec<_> = chains
        .iter()
        .filter(|c| c.shard == ctx.from_shard && !c.name.ends_with("-old"))
        .collect();
    if movable.is_empty() {
        println!("no chain caches in {}", ctx.from_shard);
        return Ok(true);
    }
    for c in &movable {
        println!("{}: cache {} in {}", c.name, c.namespace, c.shard);
    }
    // copy create looks up its base block in the chain cache, so a cache
    // moved under a running copy would make it fail
    if !copies.is_empty() {
        println!(
            "refusing: {} copies are still in active_copies",
            copies.len()
        );
        return Ok(false);
    }
    if !left.is_empty() {
        println!(
            "refusing: {} deployments are still in {}; drain them first",
            left.len(),
            ctx.from_shard
        );
        return Ok(false);
    }
    for c in &movable {
        let old = format!("{}-old", c.name);
        if names.contains(&old.as_str()) {
            println!(
                "{}: {old} already exists; remove it first with `graphman chain remove {old}`",
                c.name
            );
            ok = false;
        }
    }
    if !ok || !apply {
        if ok {
            println!("dry run; pass --apply to move them");
        }
        return Ok(ok);
    }
    for c in &movable {
        ctx.graphman
            .run(&["chain", "change-shard", &c.name, to])
            .await?;
        let now = catalog::chains(&ctx.primary).await?;
        let new = now.iter().find(|x| x.name == c.name);
        let old = now.iter().find(|x| x.name == format!("{}-old", c.name));
        match (new, old) {
            (Some(n), Some(o)) if n.shard == to && o.shard == ctx.from_shard => {
                println!(
                    "{}: now in {to} as {}, the old cache is {}-old",
                    c.name, n.namespace, c.name
                )
            }
            _ => {
                println!("{}: graphman chain change-shard did not take", c.name);
                ok = false;
            }
        }
    }
    println!(
        "restart graph-node so it opens the new caches, then `chains --remove-old` once it is \
         ingesting into them"
    );
    Ok(ok)
}

/// Everything that should be true once a shard is empty. Prints each
/// problem; returns whether there were none.
pub async fn verify(cfg: &GnConfig, ctx: &Ctx) -> Result<bool> {
    let mut ok = true;
    let mut problem = |msg: String| {
        println!("  x {msg}");
        ok = false;
    };

    println!("{}:", ctx.from_shard);
    for s in catalog::sites_in_shard(&ctx.primary, &ctx.from_shard).await? {
        problem(format!(
            "{} {} is still here ({}, active={})",
            s.namespace, s.hash, s.assignment, s.active
        ));
    }
    for c in catalog::chains(&ctx.primary)
        .await?
        .iter()
        .filter(|c| c.shard == ctx.from_shard)
    {
        problem(format!(
            "chain {} keeps its cache {} here",
            c.name, c.namespace
        ));
    }
    for ns in catalog::data_schemas(&ctx.from).await? {
        problem(format!("schema {ns} is still in the database"));
    }
    for (src, dst, _) in catalog::active_copies(&ctx.primary).await? {
        problem(format!(
            "copy sgd{src} -> sgd{dst} is still in active_copies"
        ));
    }
    if ok {
        println!("  empty");
    }

    println!("foreign servers:");
    let mut fdw_ok = true;
    for (shard, url) in &cfg.shards {
        let db = catalog::connect(url).await?;
        for (srv, host, port, dbname) in catalog::foreign_servers(&db).await? {
            let Some(target) = srv.strip_prefix("shard_") else {
                continue;
            };
            let Some(turl) = cfg.shards.get(target) else {
                println!("  ! {shard} still has {srv} for a shard that is not in the config");
                continue;
            };
            let t: tokio_postgres::Config = turl.parse()?;
            let thost = match t.get_hosts().first() {
                Some(tokio_postgres::config::Host::Tcp(h)) => h.clone(),
                _ => String::new(),
            };
            let tport = t.get_ports().first().copied().unwrap_or(5432).to_string();
            let tdb = t.get_dbname().unwrap_or_default().to_string();
            if host != thost || port != tport || dbname != tdb {
                println!(
                    "  x {shard}: {srv} points at {host}:{port}/{dbname}, the config says {thost}:{tport}/{tdb} \
                     (graph-node fixes this when it starts)"
                );
                fdw_ok = false;
            }
        }
    }
    if fdw_ok {
        println!("  all match the config");
    }
    Ok(ok && fdw_ok)
}

async fn table_counts(db: &Client) -> Result<Vec<(String, i64)>> {
    let rows = db
        .query(
            "select format('%I.%I', n.nspname, c.relname)
               from pg_class c join pg_namespace n on n.oid = c.relnamespace
              where c.relkind in ('r', 'p')
                and n.nspname not in ('pg_catalog', 'information_schema')
                and n.nspname not like 'pg_toast%'
              order by 1",
            &[],
        )
        .await?;
    let mut out = Vec::new();
    for r in rows {
        let t: String = r.get(0);
        let n: i64 = db
            .query_one(&format!("select count(*) from {t}"), &[])
            .await?
            .get(0);
        out.push((t, n));
    }
    Ok(out)
}

async fn sequences(db: &Client) -> Result<Vec<(String, Option<i64>)>> {
    let rows = db
        .query(
            "select format('%I.%I', schemaname, sequencename), last_value
               from pg_sequences order by 1",
            &[],
        )
        .await?;
    Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
}

pub async fn relocate_primary(
    cfg: &GnConfig,
    ctx: &Ctx,
    target_url: &str,
    pg_dump: &str,
    pg_restore: &str,
    apply: bool,
) -> Result<bool> {
    if ctx.from_shard != PRIMARY {
        bail!("relocate-primary only moves the primary; use --from primary");
    }
    let src_url = cfg.url(PRIMARY)?;
    let target = catalog::connect(target_url).await?;
    let mut ok = true;

    let left = catalog::sites_in_shard(&ctx.primary, PRIMARY).await?;
    if !left.is_empty() {
        println!(
            "refusing: {} deployments are still in the primary; drain them first",
            left.len()
        );
        ok = false;
    }
    let chains: Vec<_> = catalog::chains(&ctx.primary)
        .await?
        .into_iter()
        .filter(|c| c.shard == PRIMARY)
        .collect();
    if !chains.is_empty() {
        println!(
            "refusing: chain caches still in the primary: {}",
            chains
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        ok = false;
    }
    let schemas = catalog::data_schemas(&ctx.primary).await?;
    if !schemas.is_empty() {
        println!(
            "refusing: data schemas still in the primary database: {}",
            schemas.join(", ")
        );
        ok = false;
    }
    let sessions = catalog::other_sessions(&ctx.primary).await?;
    if !sessions.is_empty() {
        println!("refusing: stop every graph-node first; the primary has other sessions:");
        for s in &sessions {
            println!("  {s}");
        }
        ok = false;
    }
    let target_tables = target
        .query_one(
            "select count(*) from pg_class c join pg_namespace n on n.oid = c.relnamespace
              where c.relkind in ('r', 'p', 'v', 'f')
                and n.nspname not in ('pg_catalog', 'information_schema')",
            &[],
        )
        .await?
        .get::<_, i64>(0);
    if target_tables > 0 {
        println!("refusing: the target database is not empty ({target_tables} relations)");
        ok = false;
    }
    if !ok {
        return Ok(false);
    }
    let before = table_counts(&ctx.primary).await?;
    let rows: i64 = before.iter().map(|(_, n)| n).sum();
    println!("primary holds {} tables, {rows} rows", before.len());
    if !apply {
        println!("dry run; pass --apply to dump and restore it");
        return Ok(true);
    }

    println!(
        "  $ {pg_dump} --format=custom | {pg_restore} --single-transaction --exit-on-error --no-owner"
    );
    let mut dump = Command::new(pg_dump)
        .args(["--format=custom", "--dbname", src_url])
        .stdout(Stdio::piped())
        .spawn()
        .with_context(|| format!("starting {pg_dump}"))?;
    let restore = Command::new(pg_restore)
        .args([
            "--single-transaction",
            "--exit-on-error",
            "--no-owner",
            "--dbname",
            target_url,
        ])
        .stdin(dump.stdout.take().expect("piped"))
        .status()
        .with_context(|| format!("starting {pg_restore}"))?;
    let dumped = dump.wait()?;
    if !dumped.success() || !restore.success() {
        bail!("pg_dump exited with {dumped}, pg_restore with {restore}; the target is unchanged");
    }

    let after = table_counts(&target).await?;
    for (t, n) in &before {
        match after.iter().find(|(a, _)| a == t) {
            Some((_, m)) if m == n => {}
            Some((_, m)) => {
                println!("  x {t}: {n} rows in the primary, {m} in the target");
                ok = false;
            }
            None => {
                println!("  x {t} is missing from the target");
                ok = false;
            }
        }
    }
    let (s1, s2) = (sequences(&ctx.primary).await?, sequences(&target).await?);
    for s in &s1 {
        if !s2.contains(s) {
            println!(
                "  x sequence {} is {:?} in the primary but not in the target",
                s.0, s.1
            );
            ok = false;
        }
    }
    if ok {
        println!(
            "target matches: {} tables, {rows} rows, {} sequences. Point [store.primary] at it, \
             start graph-node, then run `verify` to check the foreign servers moved with it.",
            before.len(),
            s1.len()
        );
    }
    Ok(ok)
}
