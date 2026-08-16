# Trading_project — common dev commands (MVP scaffold)
# Run from the repo root: make <target>

COMPOSE := docker compose -f code/01_platform/01_docker/docker-compose.yml
# R-143: default to ONLINE maven (a fresh checkout has an empty ~/.m2 and -o
# fails obscurely). Set MVN_FLAGS=-o when the local cache is warm.
MVN := mvn $(MVN_FLAGS)

.PHONY: help env ddl up down logs build clean cep-check test test-ingestion test-audit-r2 gate gate-order static-check docs-audit stale-tables pin-check ddl-apply-smoke ddl-image evidence-ownership-check

help:
	@echo "Targets:"
	@echo "  env    copy code/01_platform/01_docker/.env.example to code/01_platform/01_docker/.env, then add secrets"
	@echo "  ddl    validate + emit schema manifest; APPLY=1 EVIDENCE=<file> runs the full 9-step"
	@echo "         application contract (empty catalog, apply, parity, smoke, evidence record; PASS"
	@echo "         only when every table smoke passes — composite-PK limitations need"
	@echo "         DDL_APPLY_ACK_LIMITATIONS=<tables>); FLUSS_BOOTSTRAP=<host:port> picks the"
	@echo "         cluster; DDL_APPLY_TABLE_PREFIX=<p> for dev-scratch verification only"
	@echo "  ddl-apply-smoke  live regression smoke for the exit-code contract (0/6/1 + sentinels):"
	@echo "         runs the orchestrator 3x against scratch-prefixed catalogs (PASS / refused /"
	@echo "         acknowledged-auto) + a containerized bad-ownership drill (pre-seeded 644 record"
	@echo "         -> apply exit 1 + EVIDENCE OWNERSHIP CHECK FAILED) when docker + the ddl-apply"
	@echo "         image are available; env-gated on FLUSS_BOOTSTRAP, wired into make gate"
	@echo "  ddl-image  build the ddl-apply contract image (one-shot container on trading-net;"
	@echo "         run: docker compose -f code/01_platform/01_docker/docker-compose.yml run"
	@echo "         --rm ddl-apply {validate|apply|smoke|self-test}) — FLUSS_BOOTSTRAP resolves"
	@echo "         via compose DNS, no host /etc/hosts aliases"
	@echo "  evidence-ownership-check  non-root ownership contract gate: evidence root setgid 2775,"
	@echo "         every container-written record group-writable with the engine GID, none root-owned;"
	@echo "         wired into docs-audit C15 + the Monday gate DDL step"
	@echo "  up     docker compose up -d, full stack"
	@echo "  down   docker compose down"
	@echo "  logs   tail compose logs"
	@echo "  build  build all service images"
	@echo "  clean  stop stack + remove volumes"
	@echo "  cep-check   fail if Flink CEP is referenced (project policy)"
	@echo "  test        run unit tests (common + ingestion)"
	@echo "  test-ingestion  run only the ingestion module tests"
	@echo "  test-audit-r2   run audit_r2.py unit tests (stdlib unittest, no R2 access needed)"
	@echo "  gate        run the full Monday verification gate (static + compose + go + java + schema/perf)"
	@echo "  gate-order  mandatory implementation order gate (01-foundation.md): 7 tasks in sequence,"
	@echo "              stops if any upstream task's acceptance checks are red or missing"
	@echo "  static-check  bash -n + shellcheck every repo shell script"
	@echo "  docs-audit  doc-vs-code truth gate (foundation L388): manifest, ownership matrix,"
	@echo "              schema-state diagram, compat vocabulary, stale phrases, test counts, version pins"
	@echo "  stale-tables  doc truth scan (dossiers + upstream layers: decisions,"
	@echo "              requirements, architecture, contracts, deployment minus change-records):"
	@echo "              fail when a line reads feature_candles_15s as LOG,"
	@echo "              Signal_Candidates as KV, or feature_candles_15s_current as live"
	@echo "              without a historical/superseded annotation (2026-08-13 re-scope),"
	@echo "              a stale phase-status claim, or a drifted count (21 tables vs 24,"
	@echo "              151 acceptance IDs vs 152, common/ingestion/compute test counts"
	@echo "              and docs-audit C6 line N/N/N citations vs the truth 340/234/325)"
	@echo "              (forming-bar postponed, ranking/reservation postponed,"
	@echo "              Trade_Decisions active) without a status marker"
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
	@echo "(Plain 'make ddl' only validates; run 'make ddl APPLY=1 EVIDENCE=<file>' to execute the contract.)"

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

# audit_r2.py unit tests (stdlib unittest — SigV4 golden vector, config
# parsing, provisioning/validation against an in-memory fake client).
test-audit-r2:
	python3 -m unittest discover -s code/01_platform/04_scripts/tests -v

# Live regression smoke for the DDL apply exit-code contract (0 full PASS / 6
# acknowledged PASS_WITH_LIMITATION / 1 refused) + the machine-readable
# sentinels. Env-gated: SKIPPED when FLUSS_BOOTSTRAP is unset; wired into the
# Monday verification gate (run-monday-gates.sh) after the Java full gate.
ddl-apply-smoke:
	@python3 code/01_platform/04_scripts/ddl_apply_smoke.py

# Run the SCH-23 EOD controller CLI on the host (status/run/extend/reconcile/
# reset) against the live Fluss cluster. Env: FLUSS_BOOTSTRAP + EOD_* (see
# eod_controller.py). Example: make eod-controller ARGS="status"
eod-controller:
	@python3 code/01_platform/04_scripts/eod_controller.py $(ARGS)

# Build the DDL apply contract image (code/01_platform/01_docker/ddl-apply/):
# multi-stage — maven builder compiles the engine, temurin-jre + python3
# runtime carries the classes, the 5 pinned jars, the orchestrator/smoke, the
# DDL corpus + manifest, and the matrix evidence. One-shot run inside the
# compose network: docker compose run --rm ddl-apply {validate|apply|smoke|self-test}.
ddl-image:
	@docker compose -f code/01_platform/01_docker/docker-compose.yml build ddl-apply

# Non-root ownership contract gate (evidence_ownership_check.py): every
# apply.json the ddl-apply container wrote (owner == DDL_APPLY_UID) must be
# group-writable (setgid 2775 evidence root + umask 002 -> 664) and no record
# may be root-owned. Host-side records are out of scope. Also docs-audit C15
# and the Monday gate DDL step.
evidence-ownership-check:
	@python3 code/01_platform/04_scripts/evidence_ownership_check.py

# Phase 8: every guard fires on every run — the full Monday gate.
gate:
	bash code/01_platform/04_scripts/run-monday-gates.sh

# 01-foundation.md "Mandatory implementation order": enforce the 7-task
# sequence — refuse to proceed past a task whose acceptance checks are red
# or missing. Tasks run in order; the first failing task blocks all downstream.
gate-order:
	@python3 code/01_platform/04_scripts/implementation_gate.py

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
# Freebuff-worktree bootstrap for docs-audit: C6 (test counts) reads
# module target/surefire-reports and C14 resolves evidence artifacts under
# logs/ — both gitignored, so a fresh worktree must symlink logs/ to the main
# project folder (repo convention; see CLAUDE.md symlinks) and copy the
# surefire reports from the main project's target/ dirs before this gate.
docs-audit:
	@python3 code/01_platform/04_scripts/docs_audit.py

# Doc truth scan (2026-08-13 re-scope + phase status): fail when any line in the
# implementation dossiers or the authoritative upstream layers (decisions,
# requirements, architecture, contracts) reads feature_candles_15s as a LOG,
# Signal_Candidates as a KV table, or feature_candles_15s_current as live — or
# claims forming-bar postponed / ranking-reservation postponed / Trade_Decisions
# active — unless the claim is annotated historical/superseded (or carries a
# current-status marker). Exit 1 on un-annotated hits.
stale-tables:
	@python3 code/01_platform/04_scripts/stale_table_kind_scan.py --upstream

# Foundation L548/553/554: pin discipline — matrix shape, corpus integrity,
# external-SNAPSHOT ban, platform version pins. CI SHALL run this.
pin-check:
	@bash code/01_platform/04_scripts/pin-check.sh

clean:
	$(COMPOSE) down -v
