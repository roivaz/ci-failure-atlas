# Semantic Phase1 Parity Rules

This document freezes the phase1 behavior we preserve from the Python reference scripts:

- `/home/rvazquez/projects/ARO-HCP/zz-ignore/tiger-team/metrics-cli/cmd/metrics-cli/assets/normalize_failures.py`
- `/home/rvazquez/projects/ARO-HCP/zz-ignore/tiger-team/metrics-cli/cmd/metrics-cli/assets/classify_phase1_clusters.py`

## Preserved Invariants

- Evidence extraction precedence:
  - Assertion-context extraction first (`to be true/false`, `to match`, etc.).
  - Known literal failure patterns next (`ERROR CODE`, `failed to run ARM step`, `failed to gather logs`, and related tokens).
  - `caused by:` fallback and final error-line fallback last.
- Provider anchor logic:
  - Prefer `/providers/Microsoft.*` path anchors.
  - Fall back to plain `Microsoft.*` text anchors.
  - Ignore wrapper providers (`Microsoft.Resources`, `Microsoft.RedHatOpenShift`, `Microsoft.Azure.ARO`, `Microsoft.Azure.ARO.HCP*`).
- Canonical phrase cleaning:
  - Remove pointer noise, Go file line references, and wrapper prefixes (`unexpected error`, `msg`, `err`, `caused by`).
  - Replace unstable tokens (URLs, subscription IDs, UUID-like values, long hex values).
  - Preserve deterministic truncation for long phrases.
- Generic/wrapper behavior:
  - Generic Azure codes are recognized (`DeploymentFailed`, `InternalServerError`, `Conflict`, `BadRequest`, `MultipleErrorsOccurred`).
  - Wrapper-only canonical phrases are unwrapped with `caused by` where possible.
- Phase1 key derivation:
  - `phase1_key = collapse(lower(canonical_evidence_phrase))`.
  - Placeholder tokens (`<uuid>`, `<hex>`, `<url>`) are stripped before keying.
  - Generic phrases are provider-scoped using `|provider:<anchor-or-none>`.
- Classification behavior:
  - Group scope remains `lane|job_name|test_name`.
  - Local cluster key is deterministic (`k-` + SHA256 prefix of normalized phase1 key).
  - Ambiguous provider merges and weak canonical evidence produce low confidence + normalized reason tags.

## Compile/Output Invariants

- One assignment per input row, keyed by deterministic `row_id`.
- Test cluster IDs are deterministic hashes of test scope + member signatures.
- Search query provenance is preserved by selecting literal substring candidates from reference rows.
- Review queue items are deterministic and reason-driven (`phase`, `reason`, `review_item_id` sorted).

## Intentional Deviations

- `workflow phase1` persists intermediate artifacts (`phase1_workset`, `phase1_normalized`, `phase1_assignments`) in `data/semantic/` for reproducibility and debugging.
- Compile-stage search phrase recovery uses the same precedence model, but remains fail-open: if no explicit candidate is provable, it derives a literal fallback from row excerpts and marks `search_query_source_not_found`.

## Behavioral Guardrails

- Fixture-driven parity tests compare Go outputs against Python-generated references for:
  - phase1 key/provider/generic derivation,
  - local cluster assignment and confidence reasons,
  - support distributions, high-overlap membership, and top review reasons.
