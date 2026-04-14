//! Minimal parser for Nix .drv files (ATerm format).
//!
//! Extracts only what the submitter needs: input derivations (DAG edges),
//! outputs (paths), system, and name. Does NOT parse builder, args, or
//! full env — those are irrelevant for the coordinator.
//!
//! The ATerm format is:
//!   Derive(OUTPUTS, INPUT_DRVS, INPUT_SRCS, SYSTEM, BUILDER, ARGS, ENV)
//!
//! Each field is a list of tuples or a string, all comma-separated.
//!
//! Internally the hand-rolled recursive-descent parser uses `anyhow`
//! for its error chain (convenient for `.context(...)` at each parsing
//! step). The public `parse()` boundary converts any internal error
//! into a typed [`DrvParseError`] so callers can classify failures
//! (eg UTF-8 vs. syntax vs. truncation) instead of grepping strings.

use anyhow::{bail, Context, Result};
use thiserror::Error;

/// Error returned from [`parse`]. Carries a classification tag plus
/// the context chain that led to the failure so production logs can
/// attribute a bad .drv to the exact phase that rejected it.
#[derive(Debug, Error)]
pub enum DrvParseError {
    #[error("drv is not valid UTF-8")]
    NotUtf8,
    #[error("drv does not start with Derive(")]
    MissingHeader,
    #[error("drv does not end with )")]
    MissingFooter,
    #[error("drv syntax: {context}")]
    BadSyntax {
        /// Human-readable explanation of the parse failure, including
        /// the context chain from the internal anyhow error.
        context: String,
    },
}

impl DrvParseError {
    fn from_internal(err: anyhow::Error) -> Self {
        // Preserve the full context chain as a readable blob; no
        // downstream path cares about the individual anyhow layers.
        Self::BadSyntax {
            context: err
                .chain()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(" → "),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct DrvOutput {
    pub name: String,
    pub path: String,
    pub hash_algo: String,
    pub hash: String,
}

#[derive(Debug, Clone)]
pub struct ParsedDrv {
    pub outputs: Vec<DrvOutput>,
    /// Direct build dependencies: drv store path → output names consumed.
    pub input_drvs: Vec<(String, Vec<String>)>,
    /// Build platform (e.g., "x86_64-linux").
    pub system: String,
    /// Package name (extracted from env vars).
    pub name: String,
}

impl ParsedDrv {
    /// A fixed-output derivation has exactly one output with a non-empty hash.
    pub fn is_fod(&self) -> bool {
        self.outputs.len() == 1 && !self.outputs[0].hash.is_empty()
    }
}

/// Parse a .drv file from raw bytes. Public boundary — returns a
/// typed error. Internal parsing uses anyhow for context chaining;
/// the chain is preserved in the typed error's `context` field.
pub fn parse(bytes: &[u8]) -> std::result::Result<ParsedDrv, DrvParseError> {
    let s = std::str::from_utf8(bytes).map_err(|_| DrvParseError::NotUtf8)?;
    let s = s.trim();
    let s = s
        .strip_prefix("Derive(")
        .ok_or(DrvParseError::MissingHeader)?;
    let s = s.strip_suffix(')').ok_or(DrvParseError::MissingFooter)?;

    parse_body(s).map_err(DrvParseError::from_internal)
}

/// Internal body-parser using anyhow for convenient context chaining.
fn parse_body(s: &str) -> Result<ParsedDrv> {
    let mut parser = Parser::new(s);

    // Field 1: outputs
    let outputs = parser.parse_outputs().context("parsing outputs")?;
    parser.expect_comma()?;

    // Field 2: inputDrvs
    let input_drvs = parser.parse_input_drvs().context("parsing inputDrvs")?;
    parser.expect_comma()?;

    // Field 3: inputSrcs (skip)
    parser.skip_list().context("skipping inputSrcs")?;
    parser.expect_comma()?;

    // Field 4: system
    let system = parser.parse_string().context("parsing system")?;
    parser.expect_comma()?;

    // Field 5: builder (skip)
    parser.parse_string().context("parsing builder")?;
    parser.expect_comma()?;

    // Field 6: args (skip)
    parser.skip_list().context("skipping args")?;
    parser.expect_comma()?;

    // Field 7: env (extract "name")
    let env = parser.parse_env().context("parsing env")?;
    let name = env
        .iter()
        .find(|(k, _)| k == "name")
        .map(|(_, v)| v.clone())
        .unwrap_or_else(|| "unknown".to_string());

    Ok(ParsedDrv {
        outputs,
        input_drvs,
        system,
        name,
    })
}

struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn remaining(&self) -> &'a str {
        &self.input[self.pos..]
    }

    fn advance(&mut self, n: usize) {
        self.pos += n;
    }

    fn skip_ws(&mut self) {
        while self.pos < self.input.len() && self.input.as_bytes()[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn expect_char(&mut self, c: char) -> Result<()> {
        self.skip_ws();
        if self.remaining().starts_with(c) {
            self.advance(c.len_utf8());
            Ok(())
        } else {
            bail!(
                "expected '{}' at position {}, got {:?}",
                c,
                self.pos,
                &self.remaining()[..self.remaining().len().min(20)]
            );
        }
    }

    fn expect_comma(&mut self) -> Result<()> {
        self.expect_char(',')
    }

    fn parse_string(&mut self) -> Result<String> {
        self.skip_ws();
        self.expect_char('"')?;
        // Accumulate bytes, not chars. `ch as char` would interpret
        // the byte as Latin-1 and corrupt UTF-8 literals in the .drv
        // (FOD URLs and meta strings routinely contain non-ASCII).
        let mut bytes: Vec<u8> = Vec::new();
        loop {
            if self.pos >= self.input.len() {
                bail!("unterminated string");
            }
            let ch = self.input.as_bytes()[self.pos];
            match ch {
                b'"' => {
                    self.advance(1);
                    return Ok(String::from_utf8_lossy(&bytes).into_owned());
                }
                b'\\' => {
                    self.advance(1);
                    if self.pos >= self.input.len() {
                        bail!("unterminated escape");
                    }
                    let esc = self.input.as_bytes()[self.pos];
                    match esc {
                        b'n' => bytes.push(b'\n'),
                        b'r' => bytes.push(b'\r'),
                        b't' => bytes.push(b'\t'),
                        b'\\' => bytes.push(b'\\'),
                        b'"' => bytes.push(b'"'),
                        other => {
                            bytes.push(b'\\');
                            bytes.push(other);
                        }
                    }
                    self.advance(1);
                }
                _ => {
                    bytes.push(ch);
                    self.advance(1);
                }
            }
        }
    }

    /// Parse outputs: [("out","/nix/store/...-hello","",""), ...]
    fn parse_outputs(&mut self) -> Result<Vec<DrvOutput>> {
        self.expect_char('[')?;
        let mut outputs = Vec::new();
        self.skip_ws();
        if self.remaining().starts_with(']') {
            self.advance(1);
            return Ok(outputs);
        }
        loop {
            self.expect_char('(')?;
            let name = self.parse_string()?;
            self.expect_comma()?;
            let path = self.parse_string()?;
            self.expect_comma()?;
            let hash_algo = self.parse_string()?;
            self.expect_comma()?;
            let hash = self.parse_string()?;
            self.expect_char(')')?;
            outputs.push(DrvOutput {
                name,
                path,
                hash_algo,
                hash,
            });
            self.skip_ws();
            if self.remaining().starts_with(']') {
                self.advance(1);
                break;
            }
            self.expect_comma()?;
        }
        Ok(outputs)
    }

    /// Parse inputDrvs: [("/nix/store/...-bash.drv",["out"]), ...]
    fn parse_input_drvs(&mut self) -> Result<Vec<(String, Vec<String>)>> {
        self.expect_char('[')?;
        let mut inputs = Vec::new();
        self.skip_ws();
        if self.remaining().starts_with(']') {
            self.advance(1);
            return Ok(inputs);
        }
        loop {
            self.expect_char('(')?;
            let drv_path = self.parse_string()?;
            self.expect_comma()?;
            let output_names = self.parse_string_list()?;
            self.expect_char(')')?;
            inputs.push((drv_path, output_names));
            self.skip_ws();
            if self.remaining().starts_with(']') {
                self.advance(1);
                break;
            }
            self.expect_comma()?;
        }
        Ok(inputs)
    }

    fn parse_string_list(&mut self) -> Result<Vec<String>> {
        self.expect_char('[')?;
        let mut items = Vec::new();
        self.skip_ws();
        if self.remaining().starts_with(']') {
            self.advance(1);
            return Ok(items);
        }
        loop {
            items.push(self.parse_string()?);
            self.skip_ws();
            if self.remaining().starts_with(']') {
                self.advance(1);
                break;
            }
            self.expect_comma()?;
        }
        Ok(items)
    }

    /// Parse env pairs: [("name","hello"),("system","x86_64-linux"), ...]
    fn parse_env(&mut self) -> Result<Vec<(String, String)>> {
        self.expect_char('[')?;
        let mut env = Vec::new();
        self.skip_ws();
        if self.remaining().starts_with(']') {
            self.advance(1);
            return Ok(env);
        }
        loop {
            self.expect_char('(')?;
            let key = self.parse_string()?;
            self.expect_comma()?;
            let val = self.parse_string()?;
            self.expect_char(')')?;
            env.push((key, val));
            self.skip_ws();
            if self.remaining().starts_with(']') {
                self.advance(1);
                break;
            }
            self.expect_comma()?;
        }
        Ok(env)
    }

    /// Skip a list without parsing its contents (for fields we don't need).
    fn skip_list(&mut self) -> Result<()> {
        self.expect_char('[')?;
        let mut depth = 1;
        let mut in_string = false;
        let mut escape = false;
        while self.pos < self.input.len() && depth > 0 {
            let ch = self.input.as_bytes()[self.pos];
            if escape {
                escape = false;
            } else if in_string {
                match ch {
                    b'\\' => escape = true,
                    b'"' => in_string = false,
                    _ => {}
                }
            } else {
                match ch {
                    b'"' => in_string = true,
                    b'[' => depth += 1,
                    b']' => depth -= 1,
                    _ => {}
                }
            }
            self.advance(1);
        }
        if depth != 0 {
            bail!("unterminated list");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_drv() {
        let drv = r#"Derive([("out","/nix/store/aaaa-hello","","")],[("/nix/store/bbbb-bash.drv",["out"]),("/nix/store/cccc-stdenv.drv",["out"])],["/nix/store/dddd-builder.sh"],"x86_64-linux","/nix/store/bbbb-bash/bin/bash",["-e","/nix/store/dddd-builder.sh"],[("name","hello"),("out","/nix/store/aaaa-hello"),("system","x86_64-linux")])"#;
        let parsed = parse(drv.as_bytes()).unwrap();
        assert_eq!(parsed.name, "hello");
        assert_eq!(parsed.system, "x86_64-linux");
        assert_eq!(parsed.outputs.len(), 1);
        assert_eq!(parsed.outputs[0].name, "out");
        assert_eq!(parsed.outputs[0].path, "/nix/store/aaaa-hello");
        assert!(!parsed.is_fod());
        assert_eq!(parsed.input_drvs.len(), 2);
        assert_eq!(parsed.input_drvs[0].0, "/nix/store/bbbb-bash.drv");
        assert_eq!(parsed.input_drvs[0].1, vec!["out"]);
    }

    #[test]
    fn parse_fod() {
        let drv = r#"Derive([("out","/nix/store/aaaa-src","sha256","abc123")],[],["/nix/store/dddd-builder.sh"],"x86_64-linux","/bin/sh",[],[("name","src"),("system","x86_64-linux")])"#;
        let parsed = parse(drv.as_bytes()).unwrap();
        assert_eq!(parsed.name, "src");
        assert!(parsed.input_drvs.is_empty());
        assert!(parsed.is_fod(), "drv with hash in output should be FOD");
    }

    #[test]
    fn parse_escaped_strings() {
        let drv = r#"Derive([("out","/nix/store/aaaa-test","","")],[],["/nix/store/builder"],"x86_64-linux","/bin/sh",[],[("name","test\"quoted"),("system","x86_64-linux")])"#;
        let parsed = parse(drv.as_bytes()).unwrap();
        assert_eq!(parsed.name, "test\"quoted");
    }

    /// Parse EVERY .drv file in the local Nix store. Catches edge cases
    /// in real-world derivations that synthetic tests miss.
    /// Run manually: `cargo test -p nix-ci parse_all_store -- --ignored`
    #[test]
    #[ignore]
    fn parse_all_store_drvs() {
        let store = std::path::Path::new("/nix/store");
        if !store.exists() {
            eprintln!("skipping: /nix/store not available");
            return;
        }

        let mut total = 0;
        let mut ok = 0;
        let mut fods = 0;
        let mut errors = Vec::new();

        for entry in std::fs::read_dir(store).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.ends_with(".drv") {
                continue;
            }
            total += 1;

            let bytes = std::fs::read(entry.path()).unwrap();
            match parse(&bytes) {
                Ok(parsed) => {
                    ok += 1;
                    if parsed.is_fod() {
                        fods += 1;
                    }
                    // Basic sanity: system and name should be non-empty.
                    assert!(!parsed.system.is_empty(), "{name}: empty system");
                    assert!(!parsed.name.is_empty(), "{name}: empty name");
                    assert!(!parsed.outputs.is_empty(), "{name}: no outputs");
                }
                Err(e) => {
                    errors.push(format!("{name}: {e}"));
                }
            }
        }

        eprintln!("  parsed {ok}/{total} .drv files ({fods} FODs)");
        if !errors.is_empty() {
            for e in &errors[..errors.len().min(10)] {
                eprintln!("  ERROR: {e}");
            }
            panic!("{} of {total} .drv files failed to parse", errors.len());
        }
    }
}
