"""Continuous OLTP load generator and the stock-out injection lever.

See tools/loadgen/run.py for the CLI and docs/PROBLEM.md (rung L0) for why this
exists: without live writes there is no freshness asymmetry to measure, and
without the stock-out lever the L0 failure cannot be reproduced on demand.
"""
