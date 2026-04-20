.PHONY: help install deps seed build test notion-sync refresh web-dev web-build clean

help:
	@echo "Partner reporting — common commands"
	@echo
	@echo "  make install      Install Python + dbt dependencies"
	@echo "  make deps         dbt package install (dbt deps)"
	@echo "  make seed         Load seeds into Neon"
	@echo "  make build        Run the full dbt pipeline (seed + run + test) against Neon"
	@echo "  make test         Run dbt tests only"
	@echo "  make notion-sync  Push selected mart rows into Notion (per web-app config)"
	@echo "  make refresh      build + notion-sync (what the scheduler calls)"
	@echo "  make web-dev      Run the Next.js dashboard locally"
	@echo "  make web-build    Build the Next.js dashboard for production"

install:
	pip install -r scripts/requirements.txt
	dbt deps

deps:
	dbt deps

seed:
	dbt seed

build:
	dbt build

test:
	dbt test

notion-sync:
	python scripts/notion_sync.py

refresh: build notion-sync

web-dev:
	cd web && npm run dev

web-build:
	cd web && npm run build

clean:
	rm -rf target dbt_packages logs web/.next web/node_modules
