use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};

pub const PRIMARY: &str = "primary";

/// The parts of a graph-node config file this tool needs: where each shard
/// lives, and where deployment rules send new deployments.
#[derive(Debug)]
pub struct GnConfig {
    pub shards: BTreeMap<String, String>,
    pub rules: Vec<Rule>,
}

#[derive(Debug, PartialEq)]
pub struct Rule {
    pub index: usize,
    pub default: bool,
    pub shards: Vec<String>,
}

impl GnConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading graph-node config {}", path.display()))?;
        Self::parse(&text).with_context(|| format!("parsing {}", path.display()))
    }

    pub fn parse(text: &str) -> Result<Self> {
        let doc: toml::Table = text.parse()?;
        let store = doc
            .get("store")
            .and_then(|v| v.as_table())
            .ok_or_else(|| anyhow!("no [store] section"))?;

        let mut shards = BTreeMap::new();
        for (name, shard) in store {
            let conn = shard
                .get("connection")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("[store.{name}] has no connection"))?;
            // graph-node runs the connection through shellexpand::env too
            let conn = shellexpand::env(conn)
                .with_context(|| format!("expanding [store.{name}] connection"))?;
            shards.insert(name.clone(), conn.into_owned());
        }
        if !shards.contains_key(PRIMARY) {
            bail!("no [store.primary] section");
        }

        let mut rules = Vec::new();
        if let Some(list) = doc
            .get("deployment")
            .and_then(|d| d.get("rule"))
            .and_then(|r| r.as_array())
        {
            for (index, rule) in list.iter().enumerate() {
                let default = match rule.get("match") {
                    None => true,
                    Some(m) => m.as_table().is_none_or(|t| t.is_empty()),
                };
                // graph-node accepts `shard` or `shards`, a string or a list,
                // and defaults to the primary
                let shards = match rule.get("shards").or_else(|| rule.get("shard")) {
                    None => vec![PRIMARY.to_string()],
                    Some(toml::Value::String(s)) => vec![s.clone()],
                    Some(toml::Value::Array(a)) => a
                        .iter()
                        .map(|v| {
                            v.as_str()
                                .map(str::to_string)
                                .ok_or_else(|| anyhow!("rule {index}: shard names must be strings"))
                        })
                        .collect::<Result<_>>()?,
                    Some(_) => bail!("rule {index}: shards must be a string or a list"),
                };
                rules.push(Rule {
                    index,
                    default,
                    shards,
                });
            }
        }

        Ok(GnConfig { shards, rules })
    }

    pub fn url(&self, shard: &str) -> Result<&str> {
        self.shards
            .get(shard)
            .map(String::as_str)
            .ok_or_else(|| anyhow!("shard `{shard}` is not in the config"))
    }

    /// Rules that can still place a new deployment in `shard`.
    pub fn rules_placing_in(&self, shard: &str) -> Vec<&Rule> {
        self.rules
            .iter()
            .filter(|r| r.shards.iter().any(|s| s == shard))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CFG: &str = r#"
[store.primary]
connection = "postgresql://graph:${SD_TEST_PW}@db1:5432/graph"
pool_size = 10

[store.data]
connection = "postgresql://graph:pw@db2:5432/graph"
pool_size = 10

[chains]
ingestor = "index_node_0"

[deployment]
[[deployment.rule]]
match = { name = "pinned/.*" }
shard = "data"
indexers = [ "index_node_0" ]

[[deployment.rule]]
match = { network = "mainnet" }
shards = [ "primary", "data" ]
indexers = [ "index_node_0" ]

[[deployment.rule]]
indexers = [ "index_node_0" ]
"#;

    #[test]
    fn parses_shards_and_expands_env() {
        // SAFETY: tests in this module are the only readers of this variable
        unsafe { std::env::set_var("SD_TEST_PW", "s3cret") };
        let cfg = GnConfig::parse(CFG).unwrap();
        assert_eq!(
            cfg.url("primary").unwrap(),
            "postgresql://graph:s3cret@db1:5432/graph"
        );
        assert_eq!(
            cfg.url("data").unwrap(),
            "postgresql://graph:pw@db2:5432/graph"
        );
        assert!(cfg.url("nope").is_err());
    }

    #[test]
    fn rules_default_to_primary() {
        unsafe { std::env::set_var("SD_TEST_PW", "s3cret") };
        let cfg = GnConfig::parse(CFG).unwrap();
        assert_eq!(
            cfg.rules,
            vec![
                Rule {
                    index: 0,
                    default: false,
                    shards: vec!["data".into()]
                },
                Rule {
                    index: 1,
                    default: false,
                    shards: vec!["primary".into(), "data".into()]
                },
                Rule {
                    index: 2,
                    default: true,
                    shards: vec!["primary".into()]
                },
            ]
        );
        let placing: Vec<usize> = cfg
            .rules_placing_in("primary")
            .iter()
            .map(|r| r.index)
            .collect();
        assert_eq!(placing, vec![1, 2]);
    }

    #[test]
    fn missing_primary_is_an_error() {
        let err =
            GnConfig::parse("[store.data]\nconnection = \"postgresql://a@b/c\"\n").unwrap_err();
        assert!(err.to_string().contains("primary"));
    }

    #[test]
    fn unset_env_var_is_an_error() {
        let err = GnConfig::parse(
            "[store.primary]\nconnection = \"postgresql://a:${SD_TEST_UNSET_VAR}@b/c\"\n",
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("SD_TEST_UNSET_VAR"));
    }
}
