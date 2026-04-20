---
name: semantic-review
description: >-
  Review semantic materialization quality in CI Failure Atlas by querying the
  local failure-pattern and review-signals APIs, identifying overmerged,
  undermerged, and low-quality failure patterns, and proposing concrete engine
  improvements. Use when the user asks to review failure patterns, check
  materialization quality, audit semantic output, or run a review pass.
---

# Semantic Materialization Review

Review the currently materialized failure patterns from the local app, identify
semantic quality problems, and propose concrete improvements to the semantic
workflow. Work read-only unless the user explicitly asks for implementation.

## Review Categories

1. **Overmerged** — distinct failures merged into one pattern (mixed root causes, generic wrapper hiding multiple issues)
2. **Undermerged** — multiple patterns clearly describe the same issue, differing only by noise (resource names, timestamps, IDs)
3. **Low-quality** — stable pattern text that isn't useful (wrapper instead of nested cause, too generic, missing key detail)

## Workflow

### Step 1 — Determine the target week

- Default to the current UTC week unless the user specifies one.
- Compute Sunday-starting UTC week boundaries.
- The user may ask for a specific week date or "go back N weeks."

### Step 2 — Fetch failure patterns

```
Base URL: http://127.0.0.1:8082
GET /api/failure-patterns/window?start_date=<sunday>&end_date=<saturday>
```

If the response has zero rows across all environments, step back one week at a
time (up to 8 attempts) until a non-empty week is found.

### Step 3 — Fetch review signals

```
GET /api/review/signals/week?week=<semantic-week>
```

Treat signals as a **prioritization input**, not ground truth. Start with
high-severity signals, then work down.

Key signal reasons and what they mean:

| Reason | What it detects |
|--------|-----------------|
| `likely_undermerged` | Near-duplicate canonicals across clusters (≥80% token overlap) |
| `high_sample_variance` | ≥3 distinct Azure ERROR CODEs within one cluster |
| `ambiguous_provider_merge` | Multiple Azure providers merged under one generic canonical |
| `low_confidence_evidence` | Assignment-level confidence was low |
| `placeholder_dominated_canonical` | >50% of tokens are placeholders |
| `short_uninformative_canonical` | <15 chars and generically named |
| `single_occurrence` | Only one supporting sample |
| `new_this_week` | Pattern didn't exist in the previous week |

Signals include a `severity` field (`high` / `medium` / `low`).

### Step 4 — Inspect suspicious patterns

For each suspicious pattern, fetch its detail view including:
`failure_pattern_id`, `failure_pattern`, `runs_affected`, `occurrences`,
`failed_at`, `contributing_tests`, `full_error_samples`, `affected_runs`.

### Step 5 — Corroborate when needed

Use [Search OpenShift CI](https://search.dptools.openshift.org/) to validate
suspected merge boundaries with real log-level evidence.

## Review Heuristics

- Patterns whose samples describe multiple different failures → likely overmerged.
- Generic text + diverse raw samples → likely overmerged.
- Nearly identical text differing only by noise → likely undermerged.
- Phrases that omit the most useful nested error detail → low quality.
- Wrapper phrases, provider-only phrases, placeholder-dominated phrases → suspicious.
- Require evidence from ≥2-3 representative samples before confirming a finding.
- Not every review signal is a confirmed defect — validate before reporting.

## Codebase Focus Areas

When inspecting the engine to understand why a problem occurs:

- `pkg/semantic/engine/phase1` — evidence extraction, canonicalization, classification
- `pkg/semantic/engine/phase2` — cross-test merge identity
- `pkg/semantic/workflow` — orchestration
- `pkg/semantic/query` — data loading
- `pkg/frontend/readmodel` — review signal computation, API models

## Finding Template

For each finding, produce:

- **Category**: overmerged / undermerged / low-quality
- **Severity**: high / medium / low
- **Environment(s)**
- **Failure pattern ID(s)**
- **Current failure pattern text**
- **Why it looks wrong**
- **Evidence**: relevant API fields, representative `full_error_samples`, CI-search observations
- **Likely pipeline layer**: phase1 extraction, phase2 merge, readmodel, review-signal heuristics
- **Recommended improvement**
- **Suggested regression coverage**

## Output Format

1. Findings ordered by severity (high first).
2. Short **Improvement plan** grouping recommendations by pipeline layer.
3. Short **Validation plan** describing how to verify after rematerialization.

## Constraints

- Tie every recommendation to a specific observed pattern or class of patterns.
- Distinguish confirmed problems from weaker suspicions.
- Ignore deprecated Phase3/manual-linking state.
- Do not implement changes unless the user explicitly asks.
- If the local API is unavailable, say so clearly and stop.
