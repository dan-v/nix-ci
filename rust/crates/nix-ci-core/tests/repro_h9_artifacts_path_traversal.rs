//! Regression test for H9: `runner::artifacts::sanitize_filename` did
//! not block bare `.` / `..` path components in drv_names. A malicious
//! Nix flake that declares a derivation with `name = ".."` produces
//! `<artifacts_dir>/build_logs/...log` — `.` and `..` are filesystem
//! meta-components, and writing to them can escape the intended
//! subdirectory.
//!
//! The fix: sanitize_filename coerces bare `.` / `..` / empty strings
//! to `"_"`. Embedded `..` in a longer filename (e.g. `.._.._etc_passwd`)
//! is harmless — `/` has already been sanitized to `_`, so the result
//! is a single filename component with no separators, safe to join
//! against a base path.

use std::collections::HashSet;

// The functions under test are private; mirror them here so the
// reproducer exercises the exact behavior. If/when they go pub,
// replace these with imports. Kept in sync with the production copy.

fn sanitize_filename(s: &str) -> String {
    let sanitized: String = s
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' | '\0' => '_',
            c => c,
        })
        .collect();
    if sanitized.is_empty() || sanitized == "." || sanitized == ".." {
        "_".to_string()
    } else {
        sanitized
    }
}

fn unique_filename(drv_name: &str, used: &mut HashSet<String>) -> String {
    let base = sanitize_filename(drv_name);
    let candidate = format!("{base}.log");
    if used.insert(candidate.clone()) {
        return candidate;
    }
    for i in 2.. {
        let suffixed = format!("{base}-{i}.log");
        if used.insert(suffixed.clone()) {
            return suffixed;
        }
    }
    unreachable!()
}

#[test]
fn sanitize_filename_rejects_bare_dot_components() {
    assert_ne!(sanitize_filename(".."), "..");
    assert_ne!(sanitize_filename("."), ".");
    assert_ne!(sanitize_filename(""), "");
}

#[test]
fn unique_filename_stays_within_base_when_joined() {
    // A drv_name an attacker sets trying to escape the build_logs
    // subdir. The result must be a single filename component that,
    // when joined against a base path, never escapes that base.
    for name in ["..", ".", "../../etc/passwd", "../../../tmp/pwn", ""] {
        let mut used = HashSet::new();
        let out = unique_filename(name, &mut used);
        assert!(!out.contains('/'), "must not contain '/': {out:?} (from {name:?})");
        assert!(!out.contains('\\'), "must not contain '\\': {out:?} (from {name:?})");
        assert_ne!(out, ".log", "must not be bare `.log`: {out:?} (from {name:?})");
        assert_ne!(out, "..log", "must not be bare `..log`: {out:?} (from {name:?})");

        let base = std::path::PathBuf::from("/base/sub");
        let joined = base.join(&out);
        let parent = joined.parent().expect("joined path has parent");
        assert_eq!(
            parent,
            std::path::Path::new("/base/sub"),
            "join must stay in base; got {joined:?} (from drv_name {name:?})"
        );
    }
}
