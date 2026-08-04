#!/usr/bin/env python3
"""Build-time import smoke check for parser-os-worker images.

Fails the Docker build if critical modules are missing — historically caused by
a Bang/parser-os fork copied into the SowSmith/ slot overwriting app.*.

Do not use ``from pkg import name`` (including ``from __future__``) at line
start. ACR Dockerfile dependency scanning can treat those as FROM instructions
when smoke scripts are referenced by RUN (see failed runs chck/chcj).
"""
import importlib

obe = importlib.import_module("app.core.orbitbrief_envelope")
proj = importlib.import_module("parser_os_service.server.projector")
sow = importlib.import_module("sowsmith")

assert callable(getattr(obe, "build_orbitbrief_envelope"))
assert callable(getattr(proj, "to_scope_process_v1"))
assert callable(getattr(sow, "build_sow_markdown"))
print("critical_imports_ok")
