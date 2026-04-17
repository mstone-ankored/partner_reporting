.PHONY: help install deps seed build test sync bootstrap refresh clean

help:
	@echo "Partner reporting — common commands"
	@echo
	@echo "  make install    Install Python + dbt dependencies"
	@echo "  make deps       dbt package install (dbt deps)"
	@echo "  make seed       Load ref_partners + partner_total_customers into the warehouse"
	@echo "  make build      Run the full dbt pipeline (seed + run + test)"
	@echo "  make test       Run dbt tests only"
	@echo "  make bootstrap  One-time: create Zoho Analytics views from warehouse schema"
	@echo "  make sync       Push mart tables to Zoho Analytics"
	@echo "  make refresh    build + sync (what the scheduler calls)"

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

bootstrap:
	python scripts/bootstrap_zoho_workspace.py

sync:
	python scripts/zoho_sync.py

refresh: build sync

clean:
	rm -rf target dbt_packages logs
