use std::path::Path;
use std::process::Stdio;

use anyhow::{Context, Result, bail};

/// Runs graphman. Callers check the effect in the database afterwards rather
/// than trusting the exit status: graphman has been seen to segfault after
/// doing its work.
pub struct Graphman {
    argv: Vec<String>,
}

pub struct Outcome {
    pub output: String,
}

impl Graphman {
    pub fn new(command: Option<&str>, config: &Path) -> Result<Self> {
        let argv = match command {
            Some(c) => shell_words::split(c).context("splitting --graphman")?,
            None => vec![
                "graphman".to_string(),
                "--config".to_string(),
                config.display().to_string(),
            ],
        };
        if argv.is_empty() {
            bail!("--graphman is empty");
        }
        Ok(Graphman { argv })
    }

    pub async fn run(&self, args: &[&str]) -> Result<Outcome> {
        let shown = args.join(" ");
        println!("  $ graphman {shown}");
        let out = tokio::process::Command::new(&self.argv[0])
            .args(&self.argv[1..])
            .args(args)
            // a prompt reads EOF and aborts instead of hanging
            .stdin(Stdio::null())
            .output()
            .await
            .with_context(|| format!("running {}", self.argv[0]))?;
        let mut output = String::from_utf8_lossy(&out.stdout).into_owned();
        output.push_str(&String::from_utf8_lossy(&out.stderr));
        for line in output.lines().filter(|l| !l.trim().is_empty()) {
            println!("    {line}");
        }
        if !out.status.success() {
            println!("    (graphman exited with {})", out.status);
        }
        Ok(Outcome { output })
    }
}
