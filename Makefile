# Trading_project — common dev commands (MVP scaffold)
# Run from the repo root: make <target>

COMPOSE := docker compose -f code/01_platform/01_docker/docker-compose.yml
# R-143: default to ONLINE maven (a fresh checkout has an empty ~/.m2 and -o
# fails obscurely). Set MVN_FLAGS=-o when the local cache is warm.
MVN := mvn $(MVN_FLAGS)

.PHONY: help env ddl up down logs build clean cep-check test test-ingestion gate static-check docs-audit pin-check

help:
	@echo "Targets:"
	@echo "  env    copy code/01_platform/01_docker/.env.example to code/01_platform/01_docker/.env, then add secrets"
	@echo "  ddl    validate + emit schema manifest; apply gated on pinned versions + evidence"
	@echo "  up     docker compose up -d, full stack"
	@echo "  down   docker compose down"
	@echo "  logs   tail compose logs"
	@echo "  build  build all service images"
	@echo "  clean  stop stack + remove volumes"
	@echo "  cep-check   fail if Flink CEP is referenced (project policy)"
	@echo "  test        run unit tests (common + ingestion)"
	@echo "  test-ingestion  run only the ingestion module tests"
	@echo "  gate        run the full Monday verification gate (static + compose + go + java + schema/perf)"
	@echo "  static-check  bash -n + shellcheck every repo shell script"
	@echo "  docs-audit  doc-vs-code truth gate (foundation L388): manifest, ownership matrix,"
	@echo "              schema-state diagram, compat vocabulary, stale phrases, test counts, version pins"
	@echo "  pin-check   pin discipline (foundation L548/553/554): matrix shape, corpus integrity,"
	@echo "              external-SNAPSHOT ban, platform version pins"

env:
	@if [ ! -f code/01_platform/01_docker/.env ]; then \
		cp code/01_platform/01_docker/.env.example code/01_platform/01_docker/.env; \
	fi
	@echo "Created code/01_platform/01_docker/.env — edit it with real secrets."

ddl:
	@python3 code/01_platform/04_scripts/ddl_apply.py \
		$(if $(APPLY),--apply-verified,) $(if $(EVIDENCE),--matrix-evidence $(EVIDENCE),)
	@echo "(DDL application is blocked until reconciliation blocker exit criteria are met.)"

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

build:
	cd code && $(MVN) -q package -pl 02_services/01_ingestion -am -DskipTests

# Fail the build if Apache Flink CEP (Complex Event Processing) is referenced.
# Project rule: no CEP dependency in the MVP order path.
cep-check:
	@bash code/01_platform/04_scripts/cep_guard.sh .

# Run all unit tests (common + ingestion modules).
test:
	cd code && $(MVN) -q test -pl common,02_services/01_ingestion

# Run only the ingestion module tests.
test-ingestion:
	cd code && $(MVN) -q test -pl 02_services/01_ingestion -am

# Phase 8: every guard fires on every run — the full Monday gate.
gate:
	bash code/01_platform/04_scripts/run-monday-gates.sh

# Phase 8 G4: static script hygiene without needing the full gate.
static-check:
	@set -e; fail=0; for s in $$(find code -name '*.sh' -not -path '*/target/*' -not -path '*/third_party/*' | sort); do \
		bash -n "$$s" || fail=1; \
		if command -v shellcheck >/dev/null 2>&1; then \
			shellcheck -S warning "$$s" || fail=1; \
		fi; \
	done; \
	echo "static-check: $$fail failures"; [ "$$fail" -eq 0 ]

# Foundation L388: docs must not silently contradict code. Runs the machine-
# verifiable invariant set established by the 2026-08-13 ground-truth audit.
docs-audit:
	@python3 code/01_platform/04_scripts/docs_audit.py

# Foundation L548/553/554: pin discipline — matrix shape, corpus integrity,
# external-SNAPSHOT ban, platform version pins. CI SHALL run this.
pin-check:
	@bash code/01_platform/04_scripts/pin-check.sh

clean:
	$(COMPOSE) down -v
