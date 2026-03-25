SHELL := /usr/bin/env bash

CFA ?= go run cmd/main.go
SOURCE_ENVS ?= dev,int,stg,prod
GO ?= go
GO_PACKAGES ?= ./...
RUN_ARGS ?=
BINARY ?= bin/cfa
COVER_PROFILE ?= .work/coverage.out

# Latest Sunday at or before today.
START_DATE ?= $(shell dow=$$(date +%u); date -d "$$(date +%F) -$$((dow % 7)) days" +%F)
SITE_ROOT ?= site
HISTORY_WEEKS ?= 4
AZ_STORAGE_ACCOUNT ?= cihealthreports
AZ_STORAGE_AUTH_MODE ?= login
AZ_RESOURCE_GROUP ?= ci-health-reports
AZ_DEPLOYMENT_NAME ?= ci-failure-reports-static-web-$(shell date -u +%Y%m%d%H%M%S)
AZ_LOCATION ?= westus3
AZ_STORAGE_SKU ?= Standard_LRS
AZ_STATIC_WEBSITE_ENABLED ?= true
AZ_STATIC_INDEX_DOCUMENT ?= index.html
AZ_STATIC_ERROR_DOCUMENT ?= 404.html

.PHONY: help fmt fmt-check vet test test-race test-cover build run tidy check clean report-context site-build site-build-from-existing site-push deploy-static-website-storage

help:
	@echo "Go targets:"
	@echo "  make fmt"
	@echo "  make fmt-check"
	@echo "  make vet"
	@echo "  make test"
	@echo "  make test-race"
	@echo "  make test-cover"
	@echo "  make build"
	@echo "  make run RUN_ARGS='report weekly --start-date 2026-03-08'"
	@echo "  make tidy"
	@echo "  make check"
	@echo "  make clean"
	@echo ""
	@echo "Report targets:"
	@echo "  make site-build"
	@echo "  make site-build-from-existing"
	@echo "  make site-push"
	@echo "  make deploy-static-website-storage"
	@echo ""
	@echo "Variables (override as needed):"
	@echo "  GO=$(GO)"
	@echo "  GO_PACKAGES=$(GO_PACKAGES)"
	@echo "  RUN_ARGS=$(RUN_ARGS)"
	@echo "  BINARY=$(BINARY)"
	@echo "  COVER_PROFILE=$(COVER_PROFILE)"
	@echo "  START_DATE=$(START_DATE)"
	@echo "  SITE_ROOT=$(SITE_ROOT)"
	@echo "  HISTORY_WEEKS=$(HISTORY_WEEKS)"
	@echo "  SOURCE_ENVS=$(SOURCE_ENVS)"
	@echo "  AZ_STORAGE_ACCOUNT=$(AZ_STORAGE_ACCOUNT)"
	@echo "  AZ_STORAGE_AUTH_MODE=$(AZ_STORAGE_AUTH_MODE)"
	@echo "  AZ_RESOURCE_GROUP=$(AZ_RESOURCE_GROUP)"
	@echo "  AZ_DEPLOYMENT_NAME=$(AZ_DEPLOYMENT_NAME)"
	@echo "  AZ_LOCATION=$(AZ_LOCATION)"
	@echo "  AZ_STORAGE_SKU=$(AZ_STORAGE_SKU)"
	@echo "  AZ_STATIC_WEBSITE_ENABLED=$(AZ_STATIC_WEBSITE_ENABLED)"
	@echo "  AZ_STATIC_INDEX_DOCUMENT=$(AZ_STATIC_INDEX_DOCUMENT)"
	@echo "  AZ_STATIC_ERROR_DOCUMENT=$(AZ_STATIC_ERROR_DOCUMENT)"
	@echo ""
	@echo "Example:"
	@echo "  make site-build START_DATE=2026-03-08 HISTORY_WEEKS=4"
	@echo "  make run RUN_ARGS='report review --storage.semantic-subdir 2026-03-08'"
	@echo "  make deploy-static-website-storage AZ_RESOURCE_GROUP=my-rg AZ_STORAGE_ACCOUNT=myreportstorage"

fmt:
	$(GO) fmt $(GO_PACKAGES)

fmt-check:
	@set -euo pipefail; \
	files="$$(git ls-files '*.go')"; \
	if [[ -z "$$files" ]]; then \
		echo "No Go files found."; \
		exit 0; \
	fi; \
	unformatted="$$(gofmt -l $$files)"; \
	if [[ -n "$$unformatted" ]]; then \
		echo "The following files are not gofmt-formatted:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi

vet:
	$(GO) vet $(GO_PACKAGES)

test:
	$(GO) test $(GO_PACKAGES)

test-race:
	$(GO) test -race $(GO_PACKAGES)

test-cover:
	@mkdir -p "$$(dirname "$(COVER_PROFILE)")"
	$(GO) test -coverprofile="$(COVER_PROFILE)" $(GO_PACKAGES)
	@echo "coverage profile written to $(COVER_PROFILE)"

build:
	@mkdir -p "$$(dirname "$(BINARY)")"
	$(GO) build -o "$(BINARY)" ./cmd/main.go

run:
	$(GO) run cmd/main.go $(RUN_ARGS)

CONTROLLER_RUN_ENVS ?= dev,int,stg,prod
CONTROLLER_HISTORY_WEEKS ?= $(HISTORY_WEEKS)
run-controllers: RUN_ARGS=run --source.envs $(CONTROLLER_RUN_ENVS) --history.weeks $(CONTROLLER_HISTORY_WEEKS)
run-controllers: run

tidy:
	$(GO) mod tidy

check: fmt-check vet test

clean:
	@rm -rf "$(BINARY)" "$(COVER_PROFILE)"

report-context:
	@echo "START_DATE=$(START_DATE)"
	@echo "SOURCE_ENVS=$(SOURCE_ENVS)"

site-build: report-context
	$(CFA) report site build \
		--site.root "$(SITE_ROOT)" \
		--source.envs "$(SOURCE_ENVS)" \
		--history.weeks "$(HISTORY_WEEKS)" \
		--start-date "$(START_DATE)"

site-build-from-existing:
	$(CFA) report site build \
		--site.root "$(SITE_ROOT)" \
		--from-existing

site-build-only-latest:
	@make site-build \
		START_DATE="$(START_DATE)" \
		HISTORY_WEEKS="$(HISTORY_WEEKS)" \
		SOURCE_ENVS="$(SOURCE_ENVS)" \
		SITE_ROOT="$(SITE_ROOT)"

site-push:
	@if [[ -z "$(AZ_STORAGE_ACCOUNT)" ]]; then \
		echo "AZ_STORAGE_ACCOUNT is required (example: make site-push AZ_STORAGE_ACCOUNT=myreportstorage)"; \
		exit 1; \
	fi
	$(CFA) report site push \
		--site.root "$(SITE_ROOT)" \
		--site.storage-account "$(AZ_STORAGE_ACCOUNT)" \
		--site.auth-mode "$(AZ_STORAGE_AUTH_MODE)" \
		--site.container '$$web'

deploy-static-website-storage:
	@if [[ -z "$(AZ_RESOURCE_GROUP)" ]]; then \
		echo "AZ_RESOURCE_GROUP is required (example: make $@ AZ_RESOURCE_GROUP=my-rg AZ_STORAGE_ACCOUNT=myreportstorage)"; \
		exit 1; \
	fi
	@if [[ -z "$(AZ_STORAGE_ACCOUNT)" ]]; then \
		echo "AZ_STORAGE_ACCOUNT is required (example: make $@ AZ_RESOURCE_GROUP=my-rg AZ_STORAGE_ACCOUNT=myreportstorage)"; \
		exit 1; \
	fi
	@params=( \
		"storageAccountName=$(AZ_STORAGE_ACCOUNT)" \
		"skuName=$(AZ_STORAGE_SKU)" \
		"enableStaticWebsite=$(AZ_STATIC_WEBSITE_ENABLED)" \
		"indexDocument=$(AZ_STATIC_INDEX_DOCUMENT)" \
		"errorDocument404Path=$(AZ_STATIC_ERROR_DOCUMENT)" \
	); \
	if [[ -n "$(AZ_LOCATION)" ]]; then \
		params+=("location=$(AZ_LOCATION)"); \
	fi; \
	echo "Ensuring resource group $(AZ_RESOURCE_GROUP) has tag persist=true..."; \
	az group update \
		--name "$(AZ_RESOURCE_GROUP)" \
		--set tags.persist=true >/dev/null; \
	az deployment group create \
		--resource-group "$(AZ_RESOURCE_GROUP)" \
		--name "$(AZ_DEPLOYMENT_NAME)" \
		--template-file "infra/azure/report-static-website-storage.bicep" \
		--parameters "$${params[@]}"
