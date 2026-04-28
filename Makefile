.DEFAULT_GOAL := help

PYTHON ?= python3
VENV ?= .venv
PIP := $(VENV)/bin/python -m pip
CLI := $(VENV)/bin/lovelive-benchmark

KEYWORD ?= LoveLive! Series Asia Tour 2024
SETLIST_KEYWORD ?= LoveLive
DELAY ?= 1
EVENT_PAGES ?= 1
EVENT_OUTPUT ?= data/raw/eventernote_main_groups_events.jsonl
LLFANS_CANDIDATES ?= 5
LIVEFANS_PAGES ?= 1
LIVEFANS_EVENT_QUERIES ?= 3
THRESHOLD ?= 55
TOP_N ?= 2

.PHONY: help venv install clean-data cli-help \
	crawl-eventernote crawl-main-group-eventernote crawl-llfans crawl-livefans crawl-setlists crawl-setlists-with-setlistfm \
	match export sample full compile

help:
	@echo "Targets:"
	@echo "  make install                    Create .venv and install the CLI"
	@echo "  make cli-help                   Show CLI help"
	@echo "  make sample                     Run sample crawl -> match -> export"
	@echo "  make full                       Run full crawl -> match -> export"
	@echo "  make crawl-eventernote          Crawl Eventernote only"
	@echo "  make crawl-main-group-eventernote  Crawl stored main group actor URLs"
	@echo "  make crawl-llfans               Crawl LL-Fans from Eventernote events"
	@echo "  make crawl-livefans             Crawl legacy LiveFans from Eventernote events"
	@echo "  make crawl-setlists             Crawl LL-Fans setlists"
	@echo "  make crawl-setlists-with-setlistfm  Crawl LL-Fans plus setlist.fm"
	@echo "  make match                      Generate JSONL matches"
	@echo "  make export                     Export Markdown summary"
	@echo "  make clean-data                 Remove generated data outputs"
	@echo ""
	@echo "Variables:"
	@echo "  KEYWORD='$(KEYWORD)'"
	@echo "  SETLIST_KEYWORD='$(SETLIST_KEYWORD)'"
	@echo "  EVENT_OUTPUT=$(EVENT_OUTPUT)"
	@echo "  EVENT_PAGES=$(EVENT_PAGES) LLFANS_CANDIDATES=$(LLFANS_CANDIDATES)"
	@echo "  LIVEFANS_PAGES=$(LIVEFANS_PAGES) LIVEFANS_EVENT_QUERIES=$(LIVEFANS_EVENT_QUERIES)"
	@echo "  DELAY=$(DELAY) THRESHOLD=$(THRESHOLD) TOP_N=$(TOP_N)"

venv:
	$(PYTHON) -m venv $(VENV)

install: venv
	$(PIP) install --upgrade pip
	$(PIP) install -e .

compile:
	$(PYTHON) -m compileall src

cli-help:
	$(CLI) --help

crawl-eventernote:
	$(CLI) crawl-eventernote \
		--keyword "$(KEYWORD)" \
		--max-pages $(EVENT_PAGES) \
		--delay $(DELAY) \
		--overwrite

crawl-main-group-eventernote:
	$(CLI) crawl-eventernote \
		--output "$(EVENT_OUTPUT)" \
		--main-group-actors \
		--max-pages $(EVENT_PAGES) \
		--delay $(DELAY) \
		--continue-on-error \
		--overwrite

crawl-llfans:
	$(CLI) crawl-setlists \
		--keyword "$(SETLIST_KEYWORD)" \
		--llfans-candidates $(LLFANS_CANDIDATES) \
		--delay $(DELAY) \
		--overwrite

crawl-livefans:
	$(CLI) crawl-setlists \
		--keyword "$(SETLIST_KEYWORD)" \
		--skip-llfans \
		--no-skip-livefans \
		--livefans-max-pages $(LIVEFANS_PAGES) \
		--livefans-event-queries $(LIVEFANS_EVENT_QUERIES) \
		--delay $(DELAY) \
		--overwrite

crawl-setlists: crawl-llfans

crawl-setlists-with-setlistfm:
	$(CLI) crawl-setlists \
		--keyword "$(SETLIST_KEYWORD)" \
		--llfans-candidates $(LLFANS_CANDIDATES) \
		--no-skip-setlistfm \
		--livefans-max-pages $(LIVEFANS_PAGES) \
		--livefans-event-queries $(LIVEFANS_EVENT_QUERIES) \
		--setlistfm-pages-per-query 1 \
		--delay $(DELAY) \
		--overwrite

match:
	$(CLI) match --threshold $(THRESHOLD) --top-n $(TOP_N)

export:
	$(CLI) export-markdown

sample: crawl-eventernote crawl-llfans match export

full:
	$(MAKE) crawl-main-group-eventernote EVENT_PAGES=0
	$(MAKE) crawl-llfans
	$(MAKE) match THRESHOLD=65 TOP_N=3
	$(MAKE) export

clean-data:
	rm -rf data/raw data/processed data/exports data/cache
