SHELL := /usr/bin/env bash

CFA ?= go run cmd/main.go
SOURCE_ENVS ?= dev,int,stg,prod

# Latest Sunday at or before today.
START_DATE ?= $(shell dow=$$(date +%u); date -d "$$(date +%F) -$$((dow % 7)) days" +%F)
END_DATE ?= $(shell date -d "$(START_DATE) +7 days" +%F)
SUBDIR ?= $(START_DATE)

REPORTS_DIR ?= data/reports/$(SUBDIR)

.PHONY: help report-context semantic-workflow weekly-report test-summary-html summary-html reports

help:
	@echo "Report targets:"
	@echo "  make semantic-workflow"
	@echo "  make weekly-report"
	@echo "  make test-summary-html"
	@echo "  make summary-html"
	@echo "  make reports"
	@echo ""
	@echo "Variables (override as needed):"
	@echo "  START_DATE=$(START_DATE)"
	@echo "  END_DATE=$(END_DATE)"
	@echo "  SUBDIR=$(SUBDIR)"
	@echo "  SOURCE_ENVS=$(SOURCE_ENVS)"
	@echo ""
	@echo "Example:"
	@echo "  make reports START_DATE=2026-03-01"

report-context:
	@echo "START_DATE=$(START_DATE)"
	@echo "END_DATE=$(END_DATE)"
	@echo "SUBDIR=$(SUBDIR)"
	@echo "SOURCE_ENVS=$(SOURCE_ENVS)"

semantic-workflow: report-context
	$(CFA) workflow build \
		--source.envs "$(SOURCE_ENVS)" \
		--storage.ndjson.semantic-subdir "$(SUBDIR)" \
		--workflow.window.start "$(START_DATE)" \
		--workflow.window.end "$(END_DATE)"

weekly-report: report-context
	@mkdir -p "$(REPORTS_DIR)"
	$(CFA) report weekly \
		--start-date "$(START_DATE)" \
		--storage.ndjson.semantic-subdir "$(SUBDIR)" \
		--reports.subdir "$(SUBDIR)" \
		--output "$(REPORTS_DIR)/weekly-metrics.html"

test-summary-html: report-context
	@mkdir -p "$(REPORTS_DIR)"
	$(CFA) report test-summary \
		--storage.ndjson.semantic-subdir "$(SUBDIR)" \
		--reports.subdir "$(SUBDIR)" \
		--workflow.window.start "$(START_DATE)" \
		--workflow.window.end "$(END_DATE)" \
		--format html \
		--output "$(REPORTS_DIR)/semantic-quality.html" \
		--quality-export "$(REPORTS_DIR)/semantic-quality-flagged.ndjson" \
		--source.envs "$(SOURCE_ENVS)" \
		--top 0 \
		--recent 4 \
		--min-runs 10

summary-html: report-context
	@mkdir -p "$(REPORTS_DIR)"
	$(CFA) report summary \
		--storage.ndjson.semantic-subdir "$(SUBDIR)" \
		--reports.subdir "$(SUBDIR)" \
		--workflow.window.start "$(START_DATE)" \
		--workflow.window.end "$(END_DATE)" \
		--format html \
		--output "$(REPORTS_DIR)/global-signature-triage.html" \
		--source.envs "$(SOURCE_ENVS)" \
		--top 25 \
		--min-percent 1

reports: weekly-report test-summary-html summary-html
