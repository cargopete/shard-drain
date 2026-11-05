//! Read-only queries against graph-node's catalog and shards. Every write
//! goes through graphman.

use anyhow::{Context, Result, anyhow};
use tokio_postgres::{Client, NoTls};

pub async fn connect(url: &str) -> Result<Client> {
    let mut cfg: tokio_postgres::Config = url
        .parse()
        .with_context(|| "parsing a connection string from the config")?;
    cfg.application_name("shard-drain");
    if cfg.get_ssl_mode() == tokio_postgres::config::SslMode::Require {
        anyhow::bail!(
            "sslmode=require is not supported yet; connect over a local or tunnelled port"
        );
    }
    let host = format!("{:?}", cfg.get_hosts());
    let (client, conn) = cfg
        .connect(NoTls)
        .await
        .with_context(|| format!("connecting to {host}"))?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("postgres connection error: {e}");
        }
    });
    Ok(client)
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Assignment {
    Assigned(String),
    Paused(String),
    Unassigned,
}

impl Assignment {
    pub fn node(&self) -> Option<&str> {
        match self {
            Assignment::Assigned(n) | Assignment::Paused(n) => Some(n),
            Assignment::Unassigned => None,
        }
    }
}

impl std::fmt::Display for Assignment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Assignment::Assigned(n) => write!(f, "assigned to {n}"),
            Assignment::Paused(n) => write!(f, "paused on {n}"),
            Assignment::Unassigned => write!(f, "unassigned"),
        }
    }
}

/// A row of `deployment_schemas`, with its assignment.
#[derive(Debug, Clone)]
pub struct Site {
    pub id: i32,
    pub hash: String,
    pub namespace: String,
    pub shard: String,
    pub active: bool,
    pub assignment: Assignment,
}

const SITE_SELECT: &str = "
select ds.id, ds.subgraph, ds.name, ds.shard, ds.active,
       a.node_id, a.paused_at is not null as paused
  from deployment_schemas ds
  left join subgraphs.subgraph_deployment_assignment a on a.id = ds.id";

fn site_from_row(r: &tokio_postgres::Row) -> Site {
    let node: Option<String> = r.get(5);
    let paused: Option<bool> = r.get(6);
    let assignment = match (node, paused) {
        (Some(n), Some(true)) => Assignment::Paused(n),
        (Some(n), _) => Assignment::Assigned(n),
        (None, _) => Assignment::Unassigned,
    };
    Site {
        id: r.get(0),
        hash: r.get(1),
        namespace: r.get(2),
        shard: r.get(3),
        active: r.get(4),
        assignment,
    }
}

pub async fn sites_in_shard(primary: &Client, shard: &str) -> Result<Vec<Site>> {
    let q = format!("{SITE_SELECT} where ds.shard = $1 order by ds.id");
    let rows = primary.query(&q, &[&shard]).await?;
    Ok(rows.iter().map(site_from_row).collect())
}

pub async fn sites_for_hash(primary: &Client, hash: &str) -> Result<Vec<Site>> {
    let q = format!("{SITE_SELECT} where ds.subgraph = $1 order by ds.id");
    let rows = primary.query(&q, &[&hash]).await?;
    Ok(rows.iter().map(site_from_row).collect())
}

pub async fn site_by_id(primary: &Client, id: i32) -> Result<Option<Site>> {
    let q = format!("{SITE_SELECT} where ds.id = $1");
    Ok(primary
        .query_opt(&q, &[&id])
        .await?
        .as_ref()
        .map(site_from_row))
}

/// Whether any subgraph name has this deployment as its current or pending
/// version. graph-node keeps such a deployment even when it is unassigned.
pub async fn is_current_or_pending(primary: &Client, hash: &str) -> Result<bool> {
    let row = primary
        .query_one(
            "select exists(
               select 1 from subgraphs.subgraph_version v
                 join subgraphs.subgraph s
                   on v.id = s.current_version or v.id = s.pending_version
                where v.deployment = $1)",
            &[&hash],
        )
        .await?;
    Ok(row.get(0))
}

/// Ids involved in a copy that has not been cleaned up yet.
pub async fn active_copies(primary: &Client) -> Result<Vec<(i32, i32, bool)>> {
    let rows = primary
        .query(
            "select src, dst, cancelled_at is not null from active_copies order by dst",
            &[],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect())
}

#[derive(Debug, Clone)]
pub struct Detail {
    pub head: Option<i32>,
    pub failed: bool,
    pub fatal_error: Option<String>,
    pub synced: bool,
    pub earliest_block: i32,
}

/// Per-deployment state, read from the deployment's own shard.
pub async fn detail(shard: &Client, id: i32) -> Result<Detail> {
    let r = shard
        .query_opt(
            "select h.block_number, d.failed, e.message,
                    d.synced_at is not null, d.earliest_block_number
               from subgraphs.deployment d
               join subgraphs.head h on h.id = d.id
               left join subgraphs.subgraph_error e on e.id = d.fatal_error
              where d.id = $1",
            &[&id],
        )
        .await?
        .ok_or_else(|| anyhow!("no subgraphs.deployment row for id {id}"))?;
    Ok(Detail {
        head: r.get(0),
        failed: r.get(1),
        fatal_error: r.get(2),
        synced: r.get(3),
        earliest_block: r.get(4),
    })
}

/// Bytes on disk for a schema, tables plus their indexes and toast.
pub async fn schema_bytes(shard: &Client, namespace: &str) -> Result<i64> {
    let r = shard
        .query_one(
            "select coalesce(sum(pg_total_relation_size(c.oid)), 0)::int8
               from pg_class c join pg_namespace n on n.oid = c.relnamespace
              where n.nspname = $1 and c.relkind in ('r', 'm', 'p')",
            &[&namespace],
        )
        .await?;
    Ok(r.get(0))
}

pub async fn schema_exists(shard: &Client, namespace: &str) -> Result<bool> {
    let r = shard
        .query_one(
            "select exists(select 1 from pg_namespace where nspname = $1)",
            &[&namespace],
        )
        .await?;
    Ok(r.get(0))
}

#[derive(Debug)]
pub struct CopyState {
    pub finished: bool,
    pub cancelled: bool,
    pub target_block: i32,
}

/// The copy's progress, from `subgraphs.copy_state` in the destination
/// shard. `None` while the copy is queued and has not started.
pub async fn copy_state(dst_shard: &Client, dst: i32) -> Result<Option<CopyState>> {
    let r = dst_shard
        .query_opt(
            "select finished_at is not null, cancelled_at is not null, target_block_number
               from subgraphs.copy_state where dst = $1",
            &[&dst],
        )
        .await?;
    Ok(r.map(|r| CopyState {
        finished: r.get(0),
        cancelled: r.get(1),
        target_block: r.get(2),
    }))
}

#[derive(Debug, Clone)]
pub struct Chain {
    pub name: String,
    pub shard: String,
    pub namespace: String,
}

pub async fn chains(primary: &Client) -> Result<Vec<Chain>> {
    let rows = primary
        .query(
            "select name, shard, namespace from chains order by name",
            &[],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|r| Chain {
            name: r.get(0),
            shard: r.get(1),
            namespace: r.get(2),
        })
        .collect())
}

/// `(id, shard, removed)` for every unused record of this hash.
pub async fn unused_records(primary: &Client, hash: &str) -> Result<Vec<(i32, String, bool)>> {
    let rows = primary
        .query(
            "select id, shard, removed_at is not null from unused_deployments
              where deployment = $1 order by id",
            &[&hash],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect())
}

/// Schemas in a database that hold deployment or chain data.
pub async fn data_schemas(db: &Client) -> Result<Vec<String>> {
    let rows = db
        .query(
            "select nspname from pg_namespace
              where nspname ~ '^(sgd|chain)[0-9]+$' order by nspname",
            &[],
        )
        .await?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

/// Sessions on this database other than our own.
pub async fn other_sessions(db: &Client) -> Result<Vec<String>> {
    let rows = db
        .query(
            "select pid::text || ' ' || coalesce(nullif(application_name, ''), '-')
                    || ' from ' || coalesce(client_addr::text, 'local')
               from pg_stat_activity
              where datname = current_database() and pid <> pg_backend_pid()
                and backend_type = 'client backend'
                and application_name <> 'shard-drain'
              order by pid",
            &[],
        )
        .await?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

/// `(server, host, port, dbname)` for every foreign server.
pub async fn foreign_servers(db: &Client) -> Result<Vec<(String, String, String, String)>> {
    let rows = db
        .query(
            "select srvname,
                    coalesce((select split_part(o, '=', 2) from unnest(srvoptions) o where o like 'host=%'), ''),
                    coalesce((select split_part(o, '=', 2) from unnest(srvoptions) o where o like 'port=%'), ''),
                    coalesce((select split_part(o, '=', 2) from unnest(srvoptions) o where o like 'dbname=%'), '')
               from pg_foreign_server order by srvname",
            &[],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect())
}

/// Seconds left until `window` has passed since a copy of `hash` was last
/// removed from `shard`; `None` when that is already the case.
pub async fn removal_wait(
    primary: &Client,
    hash: &str,
    shard: &str,
    window: f64,
) -> Result<Option<i64>> {
    let r = primary
        .query_one(
            "select ceil(extract(epoch from (max(removed_at) + make_interval(secs => $3) - now())))::int8
               from unused_deployments where deployment = $1 and shard = $2",
            &[&hash, &shard, &window],
        )
        .await?;
    Ok(r.get::<_, Option<i64>>(0).filter(|s| *s > 0))
}
