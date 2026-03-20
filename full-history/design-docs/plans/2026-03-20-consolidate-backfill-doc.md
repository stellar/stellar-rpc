# Consolidate Backfill Design Doc — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Consolidate all backfill-related content from 6+ separate design docs into a single self-contained `03-backfill-workflow.md`. Then delete the standalone docs from the design PR branch and push.

**Architecture:** The current `03-backfill-workflow.md` already has the DAG/task structure. We fold in: meta store keys (from 02), directory structure (from 09), configuration (from 10), and crash recovery details (from 07). The result is one doc a reviewer can read top-to-bottom to understand the entire backfill pipeline. No cross-references needed for essential information.

**Tech Stack:** Markdown. Two git branches: `karthik/stellar-history-code-v1` (source of truth) and `karthik/stellar-history-design` (PR branch, curated subset).

---

## What the consolidated doc must contain

These sections, in this order:

1. **Overview** — what backfill does, three invariants, design principles
2. **Geometry** — chunk/index math, ID formulas, concrete examples
3. **Meta Store Keys** — the 3 backfill keys, when set, when deleted, WAL invariant, durability guarantees
4. **Directory Structure** — backfill-only file tree, LFS paths, raw txhash paths, RecSplit paths
5. **Configuration** — full TOML reference for backfill mode only (service, meta_store, immutable_stores, backfill, bsb, captive_core, logging), example configs, validation rules
6. **Tasks and Dependencies** — task graph, dependency diagram, cadence table
7. **Task Details** — process_chunk (with LFS-first), build_txhash_index (with RecSplit phases), cleanup_txhash. Minimal pseudocode — describe what each task does in prose, not line-by-line code.
8. **Execution Model** — DAG scheduler, tasks as black boxes, worker pool, task-level vs internal concurrency, parallelism flow example
9. **Crash Recovery** — startup triage flowchart, reconciliation, concurrent access prevention, illustrative crash scenarios, "what is never safe" table
10. **getStatus API** — response model during backfill
11. **Error Handling** — error table
12. **Future: getEvents** — placeholder

## What to cut / not include

- No streaming content (streaming checkpoint, active stores, streaming crash scenarios)
- No `[streaming]` or `[streaming.captive_core]` or `[active_stores]` config sections
- No `[rocksdb]` block_cache tuning (backfill doesn't use it meaningfully)
- No Related Documents section (this doc IS the reference)
- No go code snippets for path formulas — describe in prose or show the path pattern
- Pseudocode should be brief — describe the task's behavior, don't write a Python program. The current process_chunk pseudocode is ~35 lines; reduce to ~15 lines max.
- Remove the "Design Principles" section (it duplicates the Overview)
- Remove "Desired End State" section (folded into Overview)

## Source material mapping

| Consolidated section | Primary source | What to pull |
|---|---|---|
| Overview | Current 03 lines 1-19 | Three invariants, no-RocksDB principle |
| Geometry | 02-meta-store §ID Formulas | ID formulas table, quick reference table |
| Meta Store Keys | 02-meta-store §Key Schema, §Chunk Keys, §Index Key, §Durability | Key table, durability table, WAL invariant, "flags never deleted" |
| Directory Structure | 09-directory-structure (backfill parts only) | Backfill file tree, LFS path convention, raw txhash path convention, RecSplit path convention |
| Configuration | 10-configuration (backfill parts only) | [service], [meta_store], [immutable_stores], [backfill], [backfill.bsb], [backfill.captive_core], [logging], validation rules, examples 1 & 2 |
| Tasks and Dependencies | Current 03 §Tasks and Dependencies | Keep as-is (already clean) |
| Task Details | Current 03 §Task Details | Trim pseudocode, strengthen prose descriptions |
| Execution Model | Current 03 §Execution Model | Keep as-is (already rewritten) |
| Crash Recovery | 07-crash-recovery (backfill parts) + current 03 §Crash Recovery | Startup triage, reconciliation, concurrent access, crash scenarios, "never safe" table |
| getStatus API | Current 03 §getStatus API Response | Keep as-is |
| Error Handling | Current 03 §Error Handling | Keep as-is |
| Future: getEvents | Current 03 §getEvents placeholder | Keep brief |

---

## Task 1: Write the consolidated `03-backfill-workflow.md`

**Files:**
- Rewrite: `full-history/design-docs/03-backfill-workflow.md`
- Read (source): `full-history/design-docs/02-meta-store-design.md`, `07-crash-recovery.md`, `09-directory-structure.md`, `10-configuration.md`
- Read (code for accuracy): `full-history/all-code/backfill-workflow/config.go`, `tasks.go`, `pipeline.go`, `paths.go`, `dag.go`, `resume.go`, `meta_store.go`

- [ ] **Step 1: Read all source docs and code files listed above**

Verify: meta store key patterns in code match doc. Config struct fields match TOML table. Path functions match directory structure. Task types match DAG construction.

- [ ] **Step 2: Write the consolidated doc**

Rewrite `03-backfill-workflow.md` with all sections from the outline above. Rules:
- Each section is self-contained — no "see doc X for details"
- Meta store keys: include the key table, durability guarantees, WAL invariant, "flags never deleted" rule. Do NOT include streaming keys.
- Directory structure: show the backfill-only file tree (no active/ directory, no streaming stores). Include path conventions with examples.
- Configuration: include full TOML reference tables for backfill-relevant sections only. Include 2 example configs (BSB and CaptiveCore). Include validation rules.
- Crash recovery: include startup triage flowchart, reconciliation pass, concurrent access prevention (flock), crash scenario table, "never safe" table. Do NOT include streaming crash scenarios.
- Pseudocode for tasks: keep it brief. process_chunk: ~10-15 lines showing the key decision points (source selection, write, fsync sequence, flag write). build_txhash_index: describe the 4 phases in prose, no pseudocode. cleanup_txhash: 3-4 lines.
- Cross-check every meta store key pattern against `meta_store.go`
- Cross-check every config field against `config.go`
- Cross-check every path pattern against `paths.go`

- [ ] **Step 3: Verify the doc against code**

Run these checks:
1. Every meta store key mentioned in the doc exists in `meta_store.go`
2. Every config field mentioned in the doc exists in `config.go` (struct fields + TOML tags)
3. Every directory path mentioned in the doc matches `paths.go` path functions
4. Task names match `tasks.go` task type constants and ID functions
5. DAG construction description matches `pipeline.go:buildDAG()`
6. Crash recovery triage matches `resume.go:ResumeIndex()`

- [ ] **Step 4: Commit on code branch**

```bash
git add full-history/design-docs/03-backfill-workflow.md
git commit -m "docs: consolidate backfill design doc — self-contained with meta store, config, directory layout, crash recovery"
```

---

## Task 2: Review loop (3 iterations minimum)

- [ ] **Step 1: First review pass — structural completeness**

Re-read the doc top to bottom. For each section, verify:
- Does it contain ALL the information a reviewer needs without opening another doc?
- Is there any forward/backward reference that should be inlined?
- Is there any streaming content that leaked in?

Fix any gaps found.

- [ ] **Step 2: Second review pass — code accuracy**

For each claim in the doc, verify against the actual code:
- Meta store key format strings: compare doc patterns with `meta_store.go` key functions
- Config defaults: compare doc defaults with `config.go` Validate() defaults
- Path patterns: compare doc paths with `paths.go` functions
- Task behavior: compare doc descriptions with `tasks.go` Execute() methods
- Resume logic: compare doc triage flowchart with `resume.go` ResumeIndex()

Fix any discrepancies — code is the source of truth.

- [ ] **Step 3: Third review pass — concision and readability**

Read as a reviewer seeing this for the first time:
- Cut any section that repeats information already stated elsewhere in the same doc
- Cut verbose pseudocode — if a 5-line description conveys the same info as 20 lines of pseudocode, use the description
- Cut "Related Documents" section (this doc is self-contained)
- Ensure section ordering makes sense: a reader should never encounter a concept before it's defined
- Tables over prose where structure helps

- [ ] **Step 4: Commit fixes**

```bash
git add full-history/design-docs/03-backfill-workflow.md
git commit -m "docs: backfill doc review — accuracy fixes and concision pass"
```

---

## Task 3: Update design PR branch and push

- [ ] **Step 1: Switch to design branch**

```bash
git checkout karthik/stellar-history-design
```

- [ ] **Step 2: Copy the consolidated doc and README from code branch**

```bash
git checkout karthik/stellar-history-code-v1 -- full-history/design-docs/03-backfill-workflow.md
git checkout karthik/stellar-history-code-v1 -- full-history/design-docs/README.md
```

- [ ] **Step 3: Delete standalone docs that were subsumed**

```bash
git rm full-history/design-docs/02-meta-store-design.md
git rm full-history/design-docs/07-crash-recovery.md
git rm full-history/design-docs/09-directory-structure.md
git rm full-history/design-docs/10-configuration.md
git rm full-history/design-docs/11-checkpointing-and-transitions.md
git rm full-history/design-docs/12-metrics-and-sizing.md
git rm full-history/design-docs/13-recommended-operator-approach.md
git rm full-history/design-docs/14-open-questions.md
git rm full-history/design-docs/15-query-performance.md
git rm full-history/design-docs/16-backfill-run-metrics.md
git rm full-history/design-docs/04-streaming-and-transition.md
git rm full-history/design-docs/08-query-routing.md
git rm full-history/design-docs/FAQ.md
```

- [ ] **Step 4: Update README.md for backfill-only doc set**

Edit README.md to reference only the docs that remain: README.md, 01-architecture-overview.md, 03-backfill-workflow.md. Remove all other doc references and streaming-specific content from the README. Keep the glossary and "What Changed from v1" sections but trim to backfill-relevant content only.

- [ ] **Step 5: Commit and push**

```bash
git add -A full-history/design-docs/
git commit -m "docs: ship consolidated backfill doc, remove standalone docs for separate streaming PR"
git push origin karthik/stellar-history-design
```

- [ ] **Step 6: Switch back to code branch**

```bash
git checkout karthik/stellar-history-code-v1
```
