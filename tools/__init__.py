"""Repo-local helper scripts package.

This marker keeps imports such as ``tools.query_unified_collection`` resolving to
pert-gym's local tools directory even when an embedding runtime also has a
separate top-level ``tools`` package on ``sys.path``.
"""
