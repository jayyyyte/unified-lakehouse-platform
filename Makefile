# Thin delegation to mosaic.py, which is the canonical entry point.
# `make` is not available on every platform this project must run on (notably
# Windows), so all real logic lives in the Python task runner.
PY ?= python

.PHONY: up up-gov up-all down clean status logs connectors check-abstraction sql psql mysql

up:                 ; $(PY) mosaic.py up
up-gov:             ; $(PY) mosaic.py up --profile gov
up-all:             ; $(PY) mosaic.py up --profile gov --profile orch --profile console
down:               ; $(PY) mosaic.py down
clean:              ; $(PY) mosaic.py clean
status:             ; $(PY) mosaic.py status
logs:               ; $(PY) mosaic.py logs
connectors:         ; $(PY) mosaic.py connectors
check-abstraction:  ; $(PY) mosaic.py check-abstraction
sql:                ; $(PY) mosaic.py sql
psql:               ; $(PY) mosaic.py psql
mysql:              ; $(PY) mosaic.py mysql
