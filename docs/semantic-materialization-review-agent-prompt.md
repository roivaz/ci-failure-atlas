# Semantic Materialization Review Agent Prompt

Use the prompt below when you want an agent to review the current semantic materialization quality and propose targeted improvements without immediately editing code.

## Ready-To-Use Prompt

```text
You are reviewing the semantic materialization quality in CI Failure Atlas. Work read-only unless I explicitly ask for implementation later.

Goal:
Review the currently materialized failure patterns for the current week from the local app running on localhost, identify semantic quality problems, and propose concrete improvements to the semantic workflow.

Primary review categories:
1. Overmerged patterns
   - distinct underlying failures have been merged into one failure pattern
   - examples: mixed root causes, mixed nested error details, generic wrapper text hiding multiple real issues
2. Undermerged patterns
   - multiple failure patterns clearly describe the same issue and should probably be merged into a single cluster
   - examples: patterns that differ only by noisy tokens, resource names, timestamps, IDs, or other low-signal variation
3. Low-quality patterns
   - the failure pattern text is stable but not useful enough
   - examples: wrapper error chosen over the real nested cause, phrase too generic to search well, phrase missing the most relevant detail present in the raw error

Required workflow:
1. Query the local failure-pattern API on localhost.
   - Base URL: http://127.0.0.1:8082
   - Endpoint: /api/failure-patterns/window
2. Determine the current semantic week to review.
   - Start from the current UTC week.
   - Convert it to Sunday-starting UTC week boundaries.
   - Query the full week window using start_date=<sunday> and end_date=<saturday>.
   - If the response has zero rows across all environments, step back one week at a time until you find the latest non-empty materialized week. Stop after 8 weeks and report if nothing is available.
3. Fetch the full-week failure-pattern payload for all environments first.
4. Fetch the semantic review signals for the same semantic week.
   - Endpoint: /api/review/signals/week?week=<semantic-week>
   - Treat this as a prioritization input, not as ground truth.
   - Use it to identify clusters that the semantic pipeline itself already considers suspicious.
5. Then inspect suspicious patterns in more detail, including:
   - failure_pattern_id
   - failure_pattern
   - runs_affected
   - occurrences
   - failed_at
   - contributing_tests
   - full_error_samples
   - affected_runs
   - any matching review signals and their reasons
6. Use the failure-pattern API output as the primary evidence source.
7. Use review signals as a secondary accelerator, especially reasons such as:
   - low_confidence_evidence
   - ambiguous_provider_merge
   - other non-informational semantic review reasons
8. When you need extra validation, corroborate suspicious patterns against concrete CI evidence using Search OpenShift CI: https://search.dptools.openshift.org/

Review heuristics:
- Look for patterns whose samples obviously describe multiple different failures.
- Look for clusters with generic text but very diverse raw samples.
- Look for multiple patterns with nearly identical text/search meaning that differ only by noise.
- Look for phrases that omit the most useful nested error detail.
- Use the review-signals endpoint to prioritize where to look first, but do not assume every emitted signal is a confirmed defect.
- Be especially suspicious of wrapper phrases, provider-only phrases, or phrases dominated by resource-instance noise.
- Prefer evidence from at least 2-3 representative samples before calling something overmerged or undermerged.

If you inspect the codebase, focus especially on:
- pkg/semantic/engine/phase1
- pkg/semantic/engine/phase2
- pkg/semantic/workflow
- pkg/semantic/query
- pkg/frontend/readmodel

For each finding, produce:
- Category: overmerged / undermerged / low-quality
- Severity: high / medium / low
- Environment(s)
- Failure pattern ID(s)
- Current failure pattern text
- Why it looks wrong
- Evidence:
  - relevant API fields
  - representative full_error_samples
  - if used, supporting CI-search observations
- Likely pipeline layer:
  - phase1 extraction
  - phase2 merge identity
  - readmodel/window composition
  - review-signal heuristics
- Recommended improvement
- Suggested regression coverage to add

Output format:
1. Findings first, ordered by severity.
2. Then a short “Improvement plan” section grouping recommendations by pipeline layer.
3. Then a short “Validation plan” section describing how to verify the changes after rematerialization.

Important constraints:
- Do not propose vague ideas only. Tie every recommendation to a specific observed pattern or class of patterns.
- Distinguish between confirmed problems and weaker suspicions.
- Ignore deprecated Phase3/manual-linking state; the runtime no longer applies it.
- Do not implement changes yet unless asked.
- If the local API is unavailable, say so clearly and stop rather than inventing evidence.
```

## Notes

- The prompt intentionally makes the local API the primary evidence source, because the goal is to review the actual currently materialized output, not just the code.
- It also encourages targeted corroboration in [Search OpenShift CI](https://search.dptools.openshift.org/) when a suspected merge boundary needs real log-level confirmation.
- If you want the agent to move directly from review to implementation, append a final instruction such as: `After the review, implement the highest-confidence fixes and add focused regression tests.`
