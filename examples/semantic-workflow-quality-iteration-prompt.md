# Prompt: Semantic Workflow Quality Iteration

Use this prompt in a new chat.

---

You are working in repository `ci-failure-analyser`.

## Variables (edit these only)

```bash
START_DATE=<input start date YYYY-MM-DD>
END_DATE=<input end date YYYY-MM-DD>

BASE_SEMANTIC_SUBDIR="${START_DATE}"
BASE_REPORTS_SUBDIR="${START_DATE}"

WORK_SEMANTIC_SUBDIR="${START_DATE}-work"
WORK_REPORTS_SUBDIR="${WORK_SEMANTIC_SUBDIR}"
```

## Objective

Iterate on the **semantic workflow quality** (not report cosmetics) to reduce low-quality semantic signatures in:

- `data/reports/${BASE_REPORTS_SUBDIR}/semantic-quality-flagged.ndjson`
- `data/reports/${BASE_REPORTS_SUBDIR}/semantic-quality.html`

Primary KPI:

- Reduce total flagged signatures.
- Reduce high-severity flagged signatures (**score >= 9**) while preserving useful detection of true low-information signatures.

## Current signal quality context

- Score `>= 9` rows are considered correctly flagged and should remain mostly true positives.
- `Interrupted by User` rows are currently treated as likely **false positives** for semantic-quality defects (expected cancellation artifacts with poor inner context).

Do not optimize the report by hiding issues. Improve semantic extraction/normalization quality in the workflow.

## Architecture invariants (must preserve)

1. Semantic input is **on-demand in-memory join** (`runs` + `raw_failures`) via:
   - `pkg/semantic/input/enriched_failures.go`
2. `workflow phase1` consumes only enriched input.
   - No fallback inference from run URLs/timestamps/raw text metadata.
   - Missing required metadata remains fail-closed (explicit data quality failure).
3. Phase1 workset builder stays a projection from enriched rows.
4. Keep facts/semantic layer separation.
5. Keep lane classification centralized in `testrules`.
6. Prefer explicit data gaps over silent backfill logic.

## Read first

- `pkg/semantic/input/enriched_failures.go`
- `pkg/workflow/phase1/options.go`
- `pkg/semantic/engine/phase1/workset_builder.go`
- `pkg/semantic/engine/phase1/normalize.go`
- `pkg/semantic/engine/phase1/compile.go`
- `pkg/report/testsummary/testsummary.go`
- `pkg/report/testsummary/quality_html.go`
- `pkg/testrules/rules.go`
- `data/reports/${BASE_REPORTS_SUBDIR}/semantic-quality-flagged.ndjson`

## What to do

1. Analyze flagged signatures by:
   - issue code
   - canonical evidence phrase
   - environment/lane
   - frequency/support
2. Identify root causes in semantic normalization/extraction that produce poor canonical phrases (examples: structural fragments, placeholder-only snippets, generic punctuation artifacts).
3. Implement targeted improvements in semantic workflow logic (phase1 normalization/extraction/compile path) to improve canonical phrase quality.
4. Rebuild semantic outputs for the same weekly snapshot and regenerate quality report/export.
5. Compare before vs after and report:
   - total flagged count
   - count of score `>= 9`
   - top issue-code deltas
   - top remaining problematic signatures
6. Add/adjust unit tests for the new behavior.

## Commands (verification loop)

Use these exact commands unless you have a strong reason to change scope:

```bash
go test ./...
```

```bash
go run cmd/main.go workflow build \
  --source.envs dev,int,stg,prod \
  --storage.ndjson.semantic-subdir "${WORK_SEMANTIC_SUBDIR}" \
  --workflow.window.start "${START_DATE}" \
  --workflow.window.end "${END_DATE}"
```

```bash
go run cmd/main.go report test-summary \
  --storage.ndjson.semantic-subdir "${WORK_SEMANTIC_SUBDIR}" \
  --format html \
  --output "data/reports/${WORK_REPORTS_SUBDIR}/semantic-quality.html" \
  --quality-export "data/reports/${WORK_REPORTS_SUBDIR}/semantic-quality-flagged.ndjson" \
  --top 0 \
  --recent 4 \
  --min-runs 10
```

```bash
wc -l "data/reports/${BASE_REPORTS_SUBDIR}/semantic-quality-flagged.ndjson" \
      "data/reports/${WORK_REPORTS_SUBDIR}/semantic-quality-flagged.ndjson"
```

NOTE: keep `BASE_*` unchanged and iterate in `WORK_*` so you can always compare baseline vs current output without overwriting the baseline report.


## Expected output from this task

- Code changes improving semantic output quality.
- Tests for new normalization/extraction behavior.
- Before/after quality metrics summary with concrete numbers.
- Brief note on residual false positives and next iteration candidates.

---

Important: prioritize semantic quality improvements over broad refactors.
