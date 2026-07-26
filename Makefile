# Trading_project — common dev commands (MVP scaffold)
# Run from the repo root: make <target>

COMPOSE := docker compose -f code/01_platform/01_docker/docker-compose.yml

.PHONY: help env ddl up down logs build clean

help:
	@echo "Targets:"
	@echo "  env    copy code/01_platform/01_docker/.env.example to code/01_platform/01_docker/.env, then add secrets"
	@echo "  ddl    apply sql/ddl/*.sql to Fluss - requires Fluss up"
	@echo "  up     docker compose up -d, full stack"
	@echo "  down   docker compose down"
	@echo "  logs   tail compose logs"
	@echo "  build  build all service images"
	@echo "  clean  stop stack + remove volumes"

env:
	@if [ ! -f code/01_platform/01_docker/.env ]; then cp code/01_platform/01_docker/.env.example code/01_platform/01_docker/.env; fi
	@echo "Created code/01_platform/01_docker/.env — edit it with real secrets."

ddl:
	@echo "Apply SQL DDL to Fluss - run against the Flink SQL client or Fluss CLI:"
	@for f in code/01_platform/02_sql/ddl/01_catalog.sql \
	          code/01_platform/02_sql/ddl/02_raw_table_1.sql \
	          code/01_platform/02_sql/ddl/03_feature_candles_15s.sql \
	          code/01_platform/02_sql/ddl/04_fills_table.sql \
	          code/01_platform/02_sql/ddl/05_trade_management_table.sql \
	          code/01_platform/02_sql/ddl/06_gaps.sql \
	          code/01_platform/02_sql/ddl/07_signal_candidates.sql \
	          code/01_platform/02_sql/ddl/08_ranking_results.sql \
	          code/01_platform/02_sql/ddl/09_trade_decisions.sql \
	          code/01_platform/02_sql/ddl/11_instruments.sql; do \
		echo "  -> $$f"; \
	done
	@echo "Tip: flink-sql-client -Dflink.sql-client.execution.result-mode=table \
		-e code/01_platform/02_sql/ddl/01_catalog.sql -e code/01_platform/02_sql/ddl/02_raw_table_1.sql ..."

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

build:
	$(COMPOSE) build

clean:
	$(COMPOSE) down -v
