//! Comparing a copy with its source as of one block.

use anyhow::Result;
use tokio_postgres::Client;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, serde::Serialize, serde::Deserialize,
)]
pub enum Depth {
    /// Skip the comparison
    None,
    /// Row counts per table
    Counts,
    /// Row counts and a checksum of every row's content
    Checksum,
}

fn quote(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

async fn tables(db: &Client, ns: &str) -> Result<Vec<String>> {
    let rows = db
        .query(
            "select table_name::text from information_schema.tables
              where table_schema = $1 and table_type = 'BASE TABLE' order by 1",
            &[&ns],
        )
        .await?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

async fn columns(db: &Client, ns: &str, table: &str) -> Result<Vec<(String, String)>> {
    let rows = db
        .query(
            "select column_name::text, data_type::text from information_schema.columns
              where table_schema = $1 and table_name = $2 order by ordinal_position",
            &[&ns, &table],
        )
        .await?;
    Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
}

/// The query for one table's rows visible at `block`. `vid` and the upper
/// end of `block_range` are left out: they legitimately differ once the copy
/// indexes on its own.
pub fn table_query(ns: &str, table: &str, cols: &[String], block: i32, depth: Depth) -> String {
    let mutable = cols.iter().any(|c| c == "block_range");
    let immutable = cols.iter().any(|c| c == "block$");
    let mut exprs: Vec<String> = cols
        .iter()
        .filter(|c| *c != "vid" && *c != "block_range")
        .map(|c| quote(c))
        .collect();
    if mutable {
        exprs.push("lower(block_range)".into());
    }
    let filter = if mutable {
        format!(" where block_range @> {block}")
    } else if immutable {
        format!(" where \"block$\" <= {block}")
    } else {
        String::new()
    };
    let from = format!("{}.{}{}", quote(ns), quote(table), filter);
    match depth {
        Depth::Checksum => format!(
            "select count(*), coalesce(md5(string_agg(h, '' order by h)), '') \
               from (select md5(row({})::text) as h from {from}) s",
            exprs.join(", ")
        ),
        _ => format!("select count(*), ''::text from {from}"),
    }
}

/// Compare the deployment data of `src_ns` and `dst_ns` as of `block`.
/// Returns one line per difference; empty means they match.
pub async fn compare(
    src: &Client,
    src_ns: &str,
    dst: &Client,
    dst_ns: &str,
    block: i32,
    depth: Depth,
) -> Result<(Vec<String>, usize, i64)> {
    let mut diffs = Vec::new();
    let mut rows_checked = 0i64;
    if depth == Depth::None {
        return Ok((diffs, 0, 0));
    }

    let st = tables(src, src_ns).await?;
    let dt = tables(dst, dst_ns).await?;
    for t in st.iter().filter(|t| !dt.contains(t)) {
        diffs.push(format!("table {t} is missing from the copy"));
    }
    for t in dt.iter().filter(|t| !st.contains(t)) {
        diffs.push(format!("table {t} exists only in the copy"));
    }

    let mut compared = 0;
    for t in st.iter().filter(|t| dt.contains(t)) {
        let sc = columns(src, src_ns, t).await?;
        let dc = columns(dst, dst_ns, t).await?;
        if sc != dc {
            diffs.push(format!("table {t} has different columns"));
            continue;
        }
        let names: Vec<String> = sc.into_iter().map(|(n, _)| n).collect();
        let sq = table_query(src_ns, t, &names, block, depth);
        let dq = table_query(dst_ns, t, &names, block, depth);
        let s = src.query_one(&sq, &[]).await?;
        let d = dst.query_one(&dq, &[]).await?;
        let (s_n, s_h): (i64, String) = (s.get(0), s.get(1));
        let (d_n, d_h): (i64, String) = (d.get(0), d.get(1));
        rows_checked += s_n;
        compared += 1;
        if s_n != d_n {
            diffs.push(format!(
                "{t}: {s_n} rows in the source, {d_n} in the copy at block {block}"
            ));
        } else if s_h != d_h {
            diffs.push(format!(
                "{t}: same {s_n} rows, different content at block {block}"
            ));
        }
    }
    Ok((diffs, compared, rows_checked))
}

/// Compare the per-deployment metadata that should survive a copy.
pub async fn compare_metadata(
    src: &Client,
    src_id: i32,
    dst: &Client,
    dst_id: i32,
) -> Result<Vec<String>> {
    const MANIFEST: &str = "select md5(row(spec_version, description, repository, features, schema,
                                   use_bytea_prefix, start_block_number, start_block_hash, raw_yaml,
                                   entities_with_causality_region, history_blocks)::text)
                              from subgraphs.subgraph_manifest where id = $1";
    const DEPLOYMENT: &str =
        "select row(d.failed, d.health::text, e.message, e.block_hash, e.deterministic)::text
                                from subgraphs.deployment d
                                left join subgraphs.subgraph_error e on e.id = d.fatal_error
                               where d.id = $1";
    let mut diffs = Vec::new();
    let s: String = src.query_one(MANIFEST, &[&src_id]).await?.get(0);
    let d: String = dst.query_one(MANIFEST, &[&dst_id]).await?.get(0);
    if s != d {
        diffs.push("subgraph_manifest differs".into());
    }
    let s: String = src.query_one(DEPLOYMENT, &[&src_id]).await?.get(0);
    let d: String = dst.query_one(DEPLOYMENT, &[&dst_id]).await?.get(0);
    if s != d {
        diffs.push(format!(
            "health or fatal error differs: source {s}, copy {d}"
        ));
    }
    Ok(diffs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols(c: &[&str]) -> Vec<String> {
        c.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn mutable_table_filters_on_range_and_drops_vid() {
        let q = table_query(
            "sgd3",
            "counter",
            &cols(&["vid", "block_range", "id", "value"]),
            900,
            Depth::Checksum,
        );
        assert!(q.contains("where block_range @> 900"), "{q}");
        assert!(
            q.contains("row(\"id\", \"value\", lower(block_range))"),
            "{q}"
        );
        assert!(!q.contains("\"vid\""), "{q}");
    }

    #[test]
    fn immutable_table_filters_on_creation_block() {
        let q = table_query(
            "sgd3",
            "tick",
            &cols(&["vid", "block$", "id"]),
            900,
            Depth::Counts,
        );
        assert_eq!(
            q,
            "select count(*), ''::text from \"sgd3\".\"tick\" where \"block$\" <= 900"
        );
    }

    #[test]
    fn other_tables_are_compared_whole() {
        let q = table_query("sgd3", "weird", &cols(&["id"]), 900, Depth::Counts);
        assert!(!q.contains("where"), "{q}");
    }

    #[test]
    fn identifiers_are_quoted() {
        assert_eq!(quote("poi2$"), "\"poi2$\"");
        assert_eq!(quote("a\"b"), "\"a\"\"b\"");
    }
}
