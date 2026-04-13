SHELL := /usr/bin/env bash

CFA ?= go run cmd/main.go
GO ?= go
GO_PACKAGES ?= ./...
RUN_ARGS ?=
BINARY ?= bin/cfa
COVER_PROFILE ?= .work/coverage.out
DOCKER ?= docker
DOCKERFILE ?= Dockerfile
IMAGE_REPOSITORY ?= quay.io/roivaz/cfa
IMAGE_TAG ?= latest
IMAGE ?= $(IMAGE_REPOSITORY):$(IMAGE_TAG)
IMAGE_SOURCES := $(shell find cmd pkg -type f) go.mod go.sum

APP_LISTEN ?= 127.0.0.1:8082
APP_WEEK ?=
APP_ARGS ?=

SITE_ROOT ?= site
HISTORY_WEEKS ?= 4
EXPORT_SITE_ARGS ?=

SOURCE_ENVS ?= dev,int,stg,prod
SEMANTIC_WEEK ?=
SEMANTIC_WEEKS ?= 4
SEMANTIC_ARGS ?=
CONTROLLER_ENVS ?= $(SOURCE_ENVS)
CONTROLLER_HISTORY_WEEKS ?= $(HISTORY_WEEKS)
CONTROLLER_ARGS ?=
RUN_ONCE_ARGS ?=
SYNC_ONCE_ARGS ?=

LEGACY_DATA_DIR ?= data
MIGRATE_ARGS ?=

AZ_STORAGE_ACCOUNT ?= cihealthreports
AZ_STORAGE_AUTH_MODE ?= login
AZ_UPLOAD_ARGS ?=
AZ_RESOURCE_GROUP ?= ci-health-reports
AZ_DEPLOYMENT_NAME ?= ci-failure-reports-static-web-$(shell date -u +%Y%m%d%H%M%S)
AZ_LOCATION ?= westus3
AZ_STORAGE_SKU ?= Standard_LRS
AZ_STATIC_WEBSITE_ENABLED ?= true
AZ_STATIC_INDEX_DOCUMENT ?= index.html
AZ_STATIC_ERROR_DOCUMENT ?= 404.html

.PHONY: help fmt fmt-check vet test test-race test-cover build image build-and-push show-image run tidy check clean clean-site distclean semantic-materialize semantic-backfill app export-site site-upload deploy-static-website-storage run-controllers run-once sync-once migrate-legacy-data

help:
	@echo "Go targets:"
	@echo "  make fmt"
	@echo "  make fmt-check"
	@echo "  make vet"
	@echo "  make test"
	@echo "  make test-race"
	@echo "  make test-cover"
	@echo "  make build"
	@echo "  make image IMAGE_TAG=latest"
	@echo "  make build-and-push IMAGE_TAG=latest"
	@echo "  make show-image"
	@echo "  make run RUN_ARGS='app --app.listen 127.0.0.1:8082'"
	@echo "  make tidy"
	@echo "  make check"
	@echo "  make clean"
	@echo "  make clean-site"
	@echo "  make distclean"
	@echo ""
	@echo "Semantic targets:"
	@echo "  make semantic-materialize"
	@echo "  make semantic-backfill"
	@echo ""
	@echo "App targets:"
	@echo "  make app"
	@echo "  make export-site"
	@echo "  make site-upload"
	@echo "  make deploy-static-website-storage"
	@echo ""
	@echo "Controller and migration targets:"
	@echo "  make run-controllers"
	@echo "  make run-once RUN_ONCE_ARGS='--controllers.name <name> --controllers.key <key>'"
	@echo "  make sync-once SYNC_ONCE_ARGS='--controllers.name <name>'"
	@echo "  make migrate-legacy-data LEGACY_DATA_DIR=data"
	@echo ""
	@echo "Variables (override as needed):"
	@echo "  GO=$(GO)"
	@echo "  GO_PACKAGES=$(GO_PACKAGES)"
	@echo "  RUN_ARGS=$(RUN_ARGS)"
	@echo "  BINARY=$(BINARY)"
	@echo "  COVER_PROFILE=$(COVER_PROFILE)"
	@echo "  DOCKER=$(DOCKER)"
	@echo "  DOCKERFILE=$(DOCKERFILE)"
	@echo "  IMAGE_REPOSITORY=$(IMAGE_REPOSITORY)"
	@echo "  IMAGE_TAG=$(IMAGE_TAG)"
	@echo "  IMAGE=$(IMAGE)"
	@echo "  APP_LISTEN=$(APP_LISTEN)"
	@echo "  APP_WEEK=$(APP_WEEK)"
	@echo "  SITE_ROOT=$(SITE_ROOT)"
	@echo "  HISTORY_WEEKS=$(HISTORY_WEEKS)"
	@echo "  SEMANTIC_WEEK=$(SEMANTIC_WEEK)"
	@echo "  SEMANTIC_WEEKS=$(SEMANTIC_WEEKS)"
	@echo "  CONTROLLER_ENVS=$(CONTROLLER_ENVS)"
	@echo "  CONTROLLER_HISTORY_WEEKS=$(CONTROLLER_HISTORY_WEEKS)"
	@echo "  LEGACY_DATA_DIR=$(LEGACY_DATA_DIR)"
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
	@echo "  make semantic-materialize SEMANTIC_WEEK=2026-03-29"
	@echo "  make semantic-backfill SEMANTIC_WEEKS=8"
	@echo "  make app APP_WEEK=2026-03-08"
	@echo "  make export-site SITE_ROOT=site HISTORY_WEEKS=4"
	@echo "  make site-upload AZ_STORAGE_ACCOUNT=myreportstorage SITE_ROOT=site"
	@echo "  make run-controllers CONTROLLER_ENVS=dev,int,stg,prod"
	@echo "  make migrate-legacy-data LEGACY_DATA_DIR=data"
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
	$(GO) build -o "$(BINARY)" ./cmd

image: $(DOCKERFILE) $(IMAGE_SOURCES)
	$(DOCKER) build . --file "$(DOCKERFILE)" --tag "$(IMAGE)"

build-and-push: image
	$(DOCKER) push "$(IMAGE)"

show-image:
	@echo "Image: $(IMAGE)"
	@echo "Repository: $(IMAGE_REPOSITORY)"
	@echo "Tag: $(IMAGE_TAG)"

run:
	$(CFA) $(RUN_ARGS)

semantic-materialize:
	$(CFA) semantic materialize \
		$(if $(strip $(SEMANTIC_WEEK)),--week "$(SEMANTIC_WEEK)",) $(SEMANTIC_ARGS)

semantic-backfill:
	@set -euo pipefail; \
		weeks="$(SEMANTIC_WEEKS)"; \
		if ! [[ "$$weeks" =~ ^[0-9]+$$ ]] || [[ "$$weeks" -le 0 ]]; then \
			echo "SEMANTIC_WEEKS must be a positive integer (got: $$weeks)"; \
			exit 1; \
		fi; \
		base_week="$$(date -u -d "$$(date -u +%F) -$$(date -u +%w) days" +%F)"; \
		for ((offset = weeks - 1; offset >= 0; offset--)); do \
			week="$$(date -u -d "$$base_week - $$((7 * offset)) days" +%F)"; \
			echo "materializing $$week"; \
			$(CFA) semantic materialize --week "$$week" $(SEMANTIC_ARGS); \
		done

app:
	$(CFA) app \
		--app.listen "$(APP_LISTEN)" \
		--history.weeks "$(HISTORY_WEEKS)" $(if $(strip $(APP_WEEK)),--week "$(APP_WEEK)",) $(APP_ARGS)

site-export:
	$(CFA) app export-site \
		--site.root "$(SITE_ROOT)" \
		--history.weeks "$(HISTORY_WEEKS)" $(EXPORT_SITE_ARGS)

site-upload: site-export
	@if [[ -z "$(AZ_STORAGE_ACCOUNT)" ]]; then \
		echo "AZ_STORAGE_ACCOUNT is required (example: make site-upload AZ_STORAGE_ACCOUNT=myreportstorage)"; \
		exit 1; \
	fi
	@if [[ ! -d "$(SITE_ROOT)" ]]; then \
		echo "SITE_ROOT \"$(SITE_ROOT)\" does not exist; run 'make export-site' first"; \
		exit 1; \
	fi
	az storage blob upload-batch \
		--destination '$$web' \
		--source "$(SITE_ROOT)" \
		--account-name "$(AZ_STORAGE_ACCOUNT)" \
		--auth-mode "$(AZ_STORAGE_AUTH_MODE)" \
		--overwrite $(AZ_UPLOAD_ARGS)

run-controllers:
	$(CFA) run \
		--source.envs "$(CONTROLLER_ENVS)" \
		--history.weeks "$(CONTROLLER_HISTORY_WEEKS)" $(CONTROLLER_ARGS)

run-once:
	$(CFA) run-once $(RUN_ONCE_ARGS)

sync-once:
	$(CFA) sync-once $(SYNC_ONCE_ARGS)

migrate-legacy-data:
	$(CFA) migrate import-legacy-data \
		--legacy.data-dir "$(LEGACY_DATA_DIR)" $(MIGRATE_ARGS)

tidy:
	$(GO) mod tidy

check: fmt-check vet test

clean:
	@rm -rf "$(BINARY)" "$(COVER_PROFILE)"

clean-site:
	@rm -rf "$(SITE_ROOT)"

distclean: clean clean-site

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
