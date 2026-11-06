//! The example Claude Code hook in README.md, run the way Claude Code runs a hook: the script
//! the README shows (taken out of it here, not copied), `/bin/sh`, the tool call as JSON on
//! stdin, exit code 2 meaning "refused". The README documents a script people paste into
//! their own setup, so what it says is the contract: if the script is wrong the README is.
//!
//! Needs `jq`, as the hook does. Without it (or without README.md, which the nix source does
//! not include) the test says so and passes.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

/// The script of the one `sh` block in README.md that starts with `#!/bin/sh`.
fn hook_in_readme() -> Option<String> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("README.md");
    let readme = std::fs::read_to_string(&path).ok()?;
    let mut hooks = Vec::new();
    let mut block: Option<Vec<&str>> = None;
    for line in readme.lines() {
        match (&mut block, line) {
            (None, "```sh") => block = Some(Vec::new()),
            (Some(lines), "```") => {
                if lines.first() == Some(&"#!/bin/sh") {
                    hooks.push(lines.join("\n") + "\n");
                }
                block = None;
            }
            (Some(lines), _) => lines.push(line),
            (None, _) => {}
        }
    }
    assert_eq!(hooks.len(), 1, "README.md has one hook script");
    hooks.pop()
}

/// What Claude Code sends a `PreToolUse` hook for a Bash call, made by `jq` so that quoting
/// is nobody's guess.
fn tool_call(command: &str) -> String {
    let made = Command::new("jq")
        .args(["-n", "--arg", "c", command])
        .arg(r#"{hook_event_name: "PreToolUse", tool_name: "Bash", tool_input: {command: $c}}"#)
        .output()
        .expect("jq runs");
    assert!(made.status.success());
    String::from_utf8(made.stdout).unwrap()
}

/// The hook's exit code and what it said on stderr, for `stdin`.
fn run(script: &std::path::Path, stdin: &str) -> (Option<i32>, String) {
    let mut child = Command::new("/bin/sh")
        .arg(script)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stderr).into(),
    )
}

const REFUSED: Option<i32> = Some(2);
const PASSED: Option<i32> = Some(0);

#[test]
fn the_example_hook_refuses_heavy_commands_that_skip_the_queue_and_nothing_else() {
    if Command::new("jq").arg("--version").output().is_err() {
        println!("skipped: jq is not installed, and the hook needs it");
        return;
    }
    let Some(hook) = hook_in_readme() else {
        println!("skipped: README.md is not part of this source tree");
        return;
    };
    let dir = std::env::temp_dir().join(format!("membrake-test-{}-hook", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let script = dir.join("membrake-queue.sh");
    std::fs::write(&script, &hook).unwrap();

    let syntax = Command::new("/bin/sh").arg("-n").arg(&script).status();
    assert!(syntax.unwrap().success(), "sh -n: the hook parses");

    let cases: &[(&str, Option<i32>)] = &[
        // The cases of the review that found the hook wanting.
        ("tsc -p .", REFUSED),
        ("pnpm test", REFUSED),
        ("membrake exec -- tsc -p .", PASSED),
        ("membrake exec -- tsc -p . && vitest run", REFUSED),
        ("CI=1 pnpm test", REFUSED),
        ("pnpm exec vitest run", REFUSED),
        ("echo 'example; tsc -p .'", PASSED),
        // Quoted text, and commands that only mention a tool.
        ("echo 'tsc -p .'", PASSED),
        ("echo \"it's\" && tsc -p .", REFUSED),
        ("git commit -m \"fix\ntsc crash\"", PASSED),
        ("git status", PASSED),
        ("rg tsc src", PASSED),
        ("rg 'cargo test' docs", PASSED),
        ("cat tsconfig.json", PASSED),
        // Each listed tool.
        ("tsgo -p .", REFUSED),
        ("tsgolint --tsconfig tsconfig.json", REFUSED),
        ("vitest run", REFUSED),
        ("jest --ci", REFUSED),
        ("xcodebuild -scheme App build", REFUSED),
        ("cargo build --release", REFUSED),
        ("cargo test", REFUSED),
        ("cargo clippy --all-targets", REFUSED),
        ("swift build", REFUSED),
        ("swift test", REFUSED),
        ("bun test", REFUSED),
        ("pnpm test", REFUSED),
        ("npm test", REFUSED),
        ("yarn test", REFUSED),
        ("bun run test", REFUSED),
        ("pnpm run check", REFUSED),
        ("npm run typecheck", REFUSED),
        ("yarn run verify", REFUSED),
        ("bun run build", REFUSED),
        ("npm run build:web", REFUSED),
        // Through a package runner.
        ("npx tsc --noEmit", REFUSED),
        ("npx -y vitest run", REFUSED),
        ("bunx tsgo -p .", REFUSED),
        ("pnpm exec jest", REFUSED),
        ("pnpm dlx tsc", REFUSED),
        ("yarn dlx tsgolint", REFUSED),
        ("npm exec -- vitest run", REFUSED),
        ("./node_modules/.bin/tsc -p .", REFUSED),
        // Tools that are not heavy, or not on the list.
        ("cargo fmt --check", PASSED),
        ("cargo check", PASSED),
        ("swift package resolve", PASSED),
        ("bun install", PASSED),
        ("bun run lint", PASSED),
        ("npm run dev", PASSED),
        ("npx prettier --check .", PASSED),
        ("pnpm exec prettier .", PASSED),
        ("ls -la", PASSED),
        // The queue, and what follows it in a command.
        (
            "membrake exec --label \"web types\" -- tsc -p apps/web",
            PASSED,
        ),
        ("membrake exec -- bun run test | tail -5", PASSED),
        ("cpu-lock run --shared --label x -- cargo test", PASSED),
        ("CI=1 membrake exec -- pnpm test", PASSED),
        ("membrake exec -- tsc 2>&1 | tail && vitest run", REFUSED),
        ("cpu-lock run -- cargo test; cargo build", REFUSED),
        // Segments: every separator, extra whitespace, assignments, line ends.
        ("cd app && tsc -p .", REFUSED),
        ("cd app || tsc", REFUSED),
        ("echo hi; vitest", REFUSED),
        ("ls | tsc", REFUSED),
        ("tsc &", REFUSED),
        ("(cd app && tsc)", REFUSED),
        ("x=$(tsc -p .)", REFUSED),
        ("cd app\ntsc -p .", REFUSED),
        ("membrake exec -- tsc -p . &&\n  vitest run", REFUSED),
        ("  CI=1    NODE_ENV=test   pnpm    test  ", REFUSED),
        ("FOO=\"a b\" cargo   test", REFUSED),
        // What the README says it does not catch.
        ("bash -c 'tsc -p .'", PASSED),
        ("time tsc -p .", PASSED),
    ];
    let mut wrong = Vec::new();
    for &(command, expected) in cases {
        let (code, said) = run(&script, &tool_call(command));
        if code != expected {
            wrong.push(format!("{command:?}: exit {code:?}, expected {expected:?}"));
        }
        if code == REFUSED && !said.contains("membrake exec") {
            wrong.push(format!(
                "{command:?}: the refusal does not say what to do: {said:?}"
            ));
        }
    }
    // Not a Bash call, or nothing to look at.
    for stdin in [
        "{}",
        r#"{"tool_name":"Read","tool_input":{"file_path":"/x"}}"#,
        "",
    ] {
        let (code, _) = run(&script, stdin);
        if code != PASSED {
            wrong.push(format!("{stdin:?}: exit {code:?}, expected {PASSED:?}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "the hook in README.md:\n{}",
        wrong.join("\n")
    );
    let _ = std::fs::remove_dir_all(&dir);
}
