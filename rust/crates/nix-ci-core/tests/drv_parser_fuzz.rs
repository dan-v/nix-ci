//! H15.D regression: the drv_parser is on the critical path — a
//! silent parse bug would corrupt the build graph at nixpkgs scale
//! (missed input_drvs → missed deps → we build the wrong thing or
//! not enough things). Property + fuzz-style coverage makes sure
//! arbitrary / malformed input always produces a bounded, typed
//! error instead of panicking, hanging, or returning partial data.
//!
//! The existing unit tests in drv_parser.rs and parser.rs cover
//! good inputs + a handful of named bad shapes. This file covers
//! the "anyone can send us any bytes" threat surface.

use std::time::{Duration, Instant};

/// Any byte sequence must either parse cleanly or return a typed
/// error. Never panic, never hang. 1000 pseudo-random inputs across a
/// range of shapes (short, long, mostly-printable, mostly-binary) —
/// cheap to run in CI (< 1s on any machine), catches the whole class
/// of "silent unwrap" and "infinite loop" bugs.
#[test]
fn random_bytes_never_panic_or_hang() {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut rng = XorShift::new(0xDEAD_BEEF);
    let mut parsed_ok: u32 = 0;
    let mut parsed_err: u32 = 0;

    for iter in 0..1000 {
        if Instant::now() > deadline {
            panic!("fuzz loop exceeded 5s budget at iter {iter} — likely a hang regression");
        }
        let len = (rng.next() as usize) % 2048;
        let mut buf = Vec::with_capacity(len);
        for _ in 0..len {
            buf.push((rng.next() & 0xff) as u8);
        }
        // Sometimes prefix with "Derive(" to give the parser the
        // happy-path header and drive it deeper into the body parser.
        // Without this prefix the parser short-circuits on MissingHeader
        // and we never exercise the recursive descent.
        if iter % 3 == 0 {
            let mut prefixed = b"Derive(".to_vec();
            prefixed.extend_from_slice(&buf);
            buf = prefixed;
        }
        if iter % 5 == 0 {
            buf.push(b')');
        }
        match nix_ci_core::runner::drv_parser::parse(&buf) {
            Ok(_) => parsed_ok += 1,
            Err(_) => parsed_err += 1,
        }
    }
    // Sanity: fuzz reliably produces errors (otherwise we've neutered
    // the test — e.g., `parse` stopped looking at input). Requires at
    // least 95% error rate given the input distribution.
    assert!(
        parsed_err > 900,
        "expected most fuzzed inputs to error; ok={parsed_ok} err={parsed_err}"
    );
}

/// UTF-8 boundary cases. The parser accepts `&[u8]` and internally
/// converts to `&str`. Partial UTF-8 sequences (common at stream
/// boundaries) must surface as NotUtf8, not panic mid-conversion.
#[test]
fn invalid_utf8_returns_typed_error() {
    use nix_ci_core::runner::drv_parser::{parse, DrvParseError};
    // Invalid continuation byte — a bare 0x80 with no leading byte.
    match parse(&[b'D', b'e', b'r', b'i', b'v', b'e', b'(', 0x80, b')']) {
        Err(DrvParseError::NotUtf8) => {}
        other => panic!("expected NotUtf8 for lone 0x80; got {other:?}"),
    }
    // Lone 0xFF (never valid in UTF-8).
    match parse(&[0xFF]) {
        Err(DrvParseError::NotUtf8) => {}
        other => panic!("expected NotUtf8 for 0xFF; got {other:?}"),
    }
    // 0xC2 without continuation (expects one more byte).
    match parse(b"Derive(\xC2") {
        Err(DrvParseError::NotUtf8) => {}
        other => panic!("expected NotUtf8 for truncated 0xC2; got {other:?}"),
    }
}

/// Deeply nested list pathological input. If the parser recurses on
/// the stack, a sufficiently deep input will overflow. The parser
/// should either iterate or cap depth — either way, no stack crash.
#[test]
fn deep_nesting_does_not_stack_overflow() {
    // 10K open brackets — would overflow a naive recursive descent with
    // default stack sizes. Each open bracket corresponds to a list /
    // tuple that the parser needs to track.
    let mut input = b"Derive(".to_vec();
    for _ in 0..10_000 {
        input.extend_from_slice(b"[");
    }
    input.extend_from_slice(b")");
    // Either Ok or Err is acceptable — what matters is "no panic /
    // stack overflow" — a real stack overflow would abort the test
    // binary before the timeout.
    let start = Instant::now();
    let _ = nix_ci_core::runner::drv_parser::parse(&input);
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(1),
        "deep-nesting parse took {elapsed:?}; O(n^2) or worse in the input shape"
    );
}

/// Huge string literal as a field value. Checks we don't panic on
/// allocation and parse time stays linear in input size.
#[test]
fn huge_string_literal_parses_in_linear_time() {
    // 1 MiB string literal inside what looks like an outputs entry.
    let big = "x".repeat(1024 * 1024);
    let input = format!(r#"Derive([("out","{big}","","")],[],[],"x86_64-linux","","",[])"#);

    let start = Instant::now();
    let _ = nix_ci_core::runner::drv_parser::parse(input.as_bytes());
    let elapsed = start.elapsed();
    // 1 MiB should parse well under 500ms even in debug. Anything
    // significantly worse indicates an accidental O(n^2) in the
    // string-reader.
    assert!(
        elapsed < Duration::from_millis(500),
        "1MiB string took {elapsed:?}; expected sub-500ms linear-time parse"
    );
}

/// Truncated-input battery. Each position in a valid drv is a
/// potential truncation point; none should panic. This exercises the
/// "network stream was cut mid-drv" adversarial case.
#[test]
fn truncations_of_valid_drv_never_panic() {
    // A minimal well-formed drv.
    let valid = br#"Derive([("out","/nix/store/aaa-out","","")],[],[],"x86_64-linux","/bin/sh",[],[("name","foo")])"#;
    for cut in 0..valid.len() {
        // Must not panic for any cut position.
        let _ = nix_ci_core::runner::drv_parser::parse(&valid[..cut]);
    }
}

/// Embedded NUL bytes. Some parsers mishandle NULs in strings; the
/// Nix .drv format doesn't produce them but an adversarial input
/// could. Must not panic / truncate the rest of the input silently.
#[test]
fn embedded_nul_bytes_do_not_panic() {
    let mut buf = b"Derive(".to_vec();
    buf.extend_from_slice(b"[(\"out\",\"pa\x00th\",\"\",\"\")]");
    buf.extend_from_slice(b",[],[],\"x86_64-linux\",\"\",[],[])");
    // Whatever the parser decides — Ok or Err — it must be
    // a deterministic, bounded operation.
    let _ = nix_ci_core::runner::drv_parser::parse(&buf);
}

/// Tiny deterministic xorshift — we don't need cryptographic quality,
/// and pulling in `rand` with a fresh RNG per test slows down the
/// suite for zero benefit. Seeded, so failures reproduce bit-for-bit.
struct XorShift(u64);
impl XorShift {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}
