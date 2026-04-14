//! Correctness tests for the ATerm parser and closure walker.
//! Pure in-memory tests against hand-built .drv byte strings; no real
//! Nix store required.

#![cfg(test)]

use std::collections::{HashMap, HashSet};

use nix_ci_core::runner::drv_parser::{parse, ParsedDrv};
use nix_ci_core::runner::drv_walk::walk_filtered;

/// Minimal well-formed non-FOD: one "out" output with empty hash.
fn simple_non_fod(name: &str, system: &str) -> String {
    // Derive(outputs, input_drvs, input_srcs, system, builder, args, env)
    format!(
        r#"Derive([("out","/nix/store/out-{name}","","")],[],[],"{system}","/bin/sh",[],[("name","{name}"),("system","{system}")])"#
    )
}

fn simple_fod(name: &str) -> String {
    format!(
        r#"Derive([("out","/nix/store/out-{name}","sha256","deadbeef")],[],[],"x86_64-linux","/bin/sh",[],[("name","{name}")])"#
    )
}

fn with_input_drvs(name: &str, deps: &[(&str, &[&str])]) -> String {
    let input_drvs = deps
        .iter()
        .map(|(path, outs)| {
            let out_list: Vec<String> = outs.iter().map(|o| format!(r#""{o}""#)).collect();
            format!(r#"("{path}",[{}])"#, out_list.join(","))
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        r#"Derive([("out","/nix/store/out-{name}","","")],[{}],[],"x86_64-linux","/bin/sh",[],[("name","{name}")])"#,
        input_drvs
    )
}

// ─── Parser ──────────────────────────────────────────────────────────

#[test]
fn parse_simple_non_fod() {
    let drv = simple_non_fod("hello-2.12", "x86_64-linux");
    let parsed = parse(drv.as_bytes()).expect("parses");
    assert_eq!(parsed.outputs.len(), 1);
    assert_eq!(parsed.outputs[0].name, "out");
    assert_eq!(parsed.outputs[0].path, "/nix/store/out-hello-2.12");
    assert_eq!(parsed.outputs[0].hash_algo, "");
    assert_eq!(parsed.outputs[0].hash, "");
    assert_eq!(parsed.system, "x86_64-linux");
    assert_eq!(parsed.name, "hello-2.12");
    assert!(parsed.input_drvs.is_empty());
    assert!(!parsed.is_fod());
}

#[test]
fn parse_fod_detected() {
    let drv = simple_fod("source-1.0");
    let parsed = parse(drv.as_bytes()).expect("parses");
    assert!(parsed.is_fod(), "fod must be detected");
    assert_eq!(parsed.outputs.len(), 1);
    assert_eq!(parsed.outputs[0].hash_algo, "sha256");
}

#[test]
fn parse_input_drvs_with_outputs() {
    let drv = with_input_drvs(
        "dependent-1",
        &[
            ("/nix/store/hash1-dep1.drv", &["out"]),
            ("/nix/store/hash2-dep2.drv", &["out", "dev"]),
        ],
    );
    let parsed = parse(drv.as_bytes()).expect("parses");
    assert_eq!(parsed.input_drvs.len(), 2);
    assert_eq!(parsed.input_drvs[0].0, "/nix/store/hash1-dep1.drv");
    assert_eq!(parsed.input_drvs[0].1, vec!["out"]);
    assert_eq!(parsed.input_drvs[1].0, "/nix/store/hash2-dep2.drv");
    assert_eq!(parsed.input_drvs[1].1, vec!["out", "dev"]);
}

#[test]
fn parse_handles_escape_sequences() {
    // A builder string with escapes.
    let drv = r#"Derive([("out","/nix/store/out-x","","")],[],[],"x86_64-linux","/bin/\"sh\"",["-c","echo \"hi\"\n"],[("name","x")])"#;
    let parsed = parse(drv.as_bytes()).expect("parses escaped strings");
    assert_eq!(parsed.name, "x");
}

#[test]
fn parse_preserves_utf8_in_env() {
    // Package name with a non-ASCII character.
    let drv = r#"Derive([("out","/nix/store/out-x","","")],[],[],"x86_64-linux","/bin/sh",[],[("name","café-1.0")])"#;
    let parsed = parse(drv.as_bytes()).expect("parses utf-8 env");
    assert_eq!(parsed.name, "café-1.0");
}

#[test]
fn parse_rejects_bad_prefix() {
    use nix_ci_core::runner::drv_parser::DrvParseError;
    let err = parse(b"notadrv(...)").unwrap_err();
    assert!(matches!(err, DrvParseError::MissingHeader));
}

#[test]
fn parse_rejects_missing_footer() {
    use nix_ci_core::runner::drv_parser::DrvParseError;
    let err = parse(b"Derive(").unwrap_err();
    // Missing closing paren → MissingFooter (strip_suffix fails).
    assert!(matches!(err, DrvParseError::MissingFooter));
}

#[test]
fn parse_rejects_truncated_body() {
    use nix_ci_core::runner::drv_parser::DrvParseError;
    // Header + footer OK but body truncated after `[`.
    let err = parse(b"Derive([)").unwrap_err();
    assert!(
        matches!(err, DrvParseError::BadSyntax { .. }),
        "expected BadSyntax, got {err:?}"
    );
}

#[test]
fn parse_rejects_invalid_utf8() {
    use nix_ci_core::runner::drv_parser::DrvParseError;
    let bad: &[u8] = &[0xFF, 0xFE, 0xFD];
    let err = parse(bad).unwrap_err();
    assert!(matches!(err, DrvParseError::NotUtf8));
}

#[test]
fn parse_rejects_unterminated_string() {
    use nix_ci_core::runner::drv_parser::DrvParseError;
    let err = parse(br#"Derive([("out","/nix/store/out","#).unwrap_err();
    // The unterminated string is detected mid-body, so we get
    // MissingFooter (the outer strip_suffix fails before body
    // parsing even starts).
    assert!(
        matches!(
            err,
            DrvParseError::MissingFooter | DrvParseError::BadSyntax { .. }
        ),
        "expected MissingFooter or BadSyntax, got {err:?}"
    );
}

#[test]
fn bad_syntax_preserves_context_chain() {
    use nix_ci_core::runner::drv_parser::DrvParseError;
    // An outer Derive() but with garbage inside — BadSyntax should
    // carry a readable context describing where in the body parsing
    // failed.
    let err = parse(b"Derive(garbage)").unwrap_err();
    if let DrvParseError::BadSyntax { context } = err {
        assert!(
            context.contains("parsing outputs") || context.contains("expected"),
            "context should describe the parse phase: {context}"
        );
    } else {
        panic!("expected BadSyntax");
    }
}

// ─── Closure walker ──────────────────────────────────────────────────

fn mk_parsed(name: &str, system: &str, inputs: Vec<(String, Vec<String>)>) -> ParsedDrv {
    use nix_ci_core::runner::drv_parser::DrvOutput;
    ParsedDrv {
        outputs: vec![DrvOutput {
            name: "out".into(),
            path: format!("/nix/store/out-{name}"),
            hash_algo: String::new(),
            hash: String::new(),
        }],
        input_drvs: inputs,
        system: system.into(),
        name: name.into(),
    }
}

fn mk_fod(name: &str) -> ParsedDrv {
    use nix_ci_core::runner::drv_parser::DrvOutput;
    ParsedDrv {
        outputs: vec![DrvOutput {
            name: "out".into(),
            path: format!("/nix/store/out-{name}"),
            hash_algo: "sha256".into(),
            hash: "deadbeef".into(),
        }],
        input_drvs: vec![],
        system: "x86_64-linux".into(),
        name: name.into(),
    }
}

/// Populate the parsed cache directly and assert `walk_filtered` only
/// reads from it (no filesystem access for drvs we've pre-cached).
#[test]
fn walker_respects_needed_set() {
    // DAG: root → {a, b}; a → c; b cached (not in needed_set).
    let mut cache: HashMap<String, ParsedDrv> = HashMap::new();
    cache.insert(
        "/nix/store/root.drv".into(),
        mk_parsed(
            "root",
            "x86_64-linux",
            vec![
                ("/nix/store/a.drv".into(), vec!["out".into()]),
                ("/nix/store/b.drv".into(), vec!["out".into()]),
            ],
        ),
    );
    cache.insert(
        "/nix/store/a.drv".into(),
        mk_parsed(
            "a",
            "x86_64-linux",
            vec![("/nix/store/c.drv".into(), vec!["out".into()])],
        ),
    );
    cache.insert(
        "/nix/store/c.drv".into(),
        mk_parsed("c", "x86_64-linux", vec![]),
    );
    cache.insert(
        "/nix/store/b.drv".into(),
        mk_parsed("b", "x86_64-linux", vec![]),
    );

    // needed_set omits b → it should not be emitted.
    let mut needed: HashSet<String> = HashSet::new();
    needed.insert("/nix/store/root.drv".into());
    needed.insert("/nix/store/a.drv".into());
    needed.insert("/nix/store/c.drv".into());

    let walked = walk_filtered(&["/nix/store/root.drv".into()], &needed, &mut cache).expect("walk");

    let paths: HashSet<_> = walked.iter().map(|w| w.drv_path.as_str()).collect();
    assert!(paths.contains("/nix/store/root.drv"));
    assert!(paths.contains("/nix/store/a.drv"));
    assert!(paths.contains("/nix/store/c.drv"));
    assert!(
        !paths.contains("/nix/store/b.drv"),
        "cached dep leaked into output"
    );
    // root is a root.
    let root_walked = walked
        .iter()
        .find(|w| w.drv_path == "/nix/store/root.drv")
        .unwrap();
    assert!(root_walked.is_root);
    // root's input_drvs should be filtered: only a is in needed_set, b is not.
    assert_eq!(root_walked.input_drvs, vec!["/nix/store/a.drv"]);
}

#[test]
fn walker_skips_fods() {
    let mut cache = HashMap::new();
    cache.insert(
        "/nix/store/root.drv".into(),
        mk_parsed(
            "root",
            "x86_64-linux",
            vec![("/nix/store/src-fetch.drv".into(), vec!["out".into()])],
        ),
    );
    cache.insert("/nix/store/src-fetch.drv".into(), mk_fod("src-fetch"));

    let mut needed = HashSet::new();
    needed.insert("/nix/store/root.drv".into());
    needed.insert("/nix/store/src-fetch.drv".into());

    let walked = walk_filtered(&["/nix/store/root.drv".into()], &needed, &mut cache).expect("walk");
    let paths: HashSet<_> = walked.iter().map(|w| w.drv_path.as_str()).collect();
    assert!(paths.contains("/nix/store/root.drv"));
    assert!(
        !paths.contains("/nix/store/src-fetch.drv"),
        "FOD must be skipped"
    );
    let root = walked
        .iter()
        .find(|w| w.drv_path == "/nix/store/root.drv")
        .unwrap();
    assert!(
        root.input_drvs.is_empty(),
        "FOD dep must not appear as an edge"
    );
}

#[test]
fn walker_handles_diamond() {
    // root → a → base, root → b → base
    let mut cache = HashMap::new();
    cache.insert(
        "/nix/store/root.drv".into(),
        mk_parsed(
            "root",
            "x86_64-linux",
            vec![
                ("/nix/store/a.drv".into(), vec!["out".into()]),
                ("/nix/store/b.drv".into(), vec!["out".into()]),
            ],
        ),
    );
    cache.insert(
        "/nix/store/a.drv".into(),
        mk_parsed(
            "a",
            "x86_64-linux",
            vec![("/nix/store/base.drv".into(), vec!["out".into()])],
        ),
    );
    cache.insert(
        "/nix/store/b.drv".into(),
        mk_parsed(
            "b",
            "x86_64-linux",
            vec![("/nix/store/base.drv".into(), vec!["out".into()])],
        ),
    );
    cache.insert(
        "/nix/store/base.drv".into(),
        mk_parsed("base", "x86_64-linux", vec![]),
    );

    let needed: HashSet<String> = vec![
        "/nix/store/root.drv",
        "/nix/store/a.drv",
        "/nix/store/b.drv",
        "/nix/store/base.drv",
    ]
    .into_iter()
    .map(String::from)
    .collect();

    let walked = walk_filtered(&["/nix/store/root.drv".into()], &needed, &mut cache).expect("walk");

    assert_eq!(walked.len(), 4);
    // `base` appears once, not twice, even though two parents reference it.
    let base_count = walked
        .iter()
        .filter(|w| w.drv_path == "/nix/store/base.drv")
        .count();
    assert_eq!(base_count, 1);
}

#[test]
fn walker_cache_avoids_reparsing() {
    // Run the walker twice with the same cache; the second run must
    // not attempt any additional parsing (we'd see failed reads for
    // nonexistent filesystem paths otherwise).
    let mut cache = HashMap::new();
    cache.insert(
        "/nix/store/root.drv".into(),
        mk_parsed("root", "x86_64-linux", vec![]),
    );
    let needed: HashSet<String> = std::iter::once("/nix/store/root.drv".to_string()).collect();

    let walked1 =
        walk_filtered(&["/nix/store/root.drv".into()], &needed, &mut cache).expect("first walk");
    let cache_size_after_first = cache.len();

    let walked2 =
        walk_filtered(&["/nix/store/root.drv".into()], &needed, &mut cache).expect("second walk");
    assert_eq!(
        cache.len(),
        cache_size_after_first,
        "second walk allocated new entries"
    );
    assert_eq!(walked1.len(), walked2.len());
}
