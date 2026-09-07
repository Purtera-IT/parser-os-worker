#!/usr/bin/env python3
"""Build-time import smoke check for parser-os-worker images.

Fails the Docker build if critical modules are missing — historically caused by
a Bang/parser-os fork copied into the SowSmith/ slot overwriting app.*.

It now also imports EVERY app.* module this worker depends on, because a
missing one is otherwise invisible until production. On 2026-08-04 an image
built against a parser-os ref lacking `app.core.manifest_artifact_dedup` shipped
clean and then failed every single compile at _do_compile with
ModuleNotFoundError. The import is inside a function, so nothing at build or
start time touched it. Importing them here turns "every compile dies" into "the
build fails", which is the difference between a five-minute rebuild and a dev
outage.

Keep this list in sync with the `from app.` imports in
src/parser_os_worker/*.py:
    grep -ohE "from app\\.[a-zA-Z0-9_.]+ import" src/parser_os_worker/*.py \\
      | sed 's/.*from //; s/ import$//' | sort -u

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

# Every app.* module the worker imports, including the ones imported lazily
# inside functions — those are exactly the ones that reach production unnoticed.
WORKER_DEPS = (
    "app.core.compiler",
    "app.core.manifest_artifact_dedup",
    "app.core.orbitbrief_envelope",
    "app.core.span_extractor",
    "app.core.type_head",
)

missing = []
for name in WORKER_DEPS:
    try:
        importlib.import_module(name)
    except Exception as exc:  # noqa: BLE001 - report every failure, not the first
        missing.append(f"{name}: {type(exc).__name__}: {exc}")

if missing:
    raise SystemExit(
        "parser-os is missing modules this worker imports — the bundled "
        "parser_os_ref is incompatible with this worker:\n  "
        + "\n  ".join(missing)
    )

# Importing a module is not the same as it having its data.
#
# `app/core/geo_reference.py` imports fine with no gazetteer and answers
# "unknown" to every lookup by design — degrading open is right, but it means a
# data file dropped from the wheel is INVISIBLE at runtime. That has now
# happened three times in this package (the OCR wordlist, the lifecycle labels,
# the postal gazetteer): setuptools ships no non-Python file it is not told
# about, and the symptom is always a feature that quietly stops working rather
# than a build that fails.
#
# This runs inside the built image, which is the only place the difference
# between "in the repo" and "in the wheel" is observable.
from app.core.geo_reference import available as _geo_available  # noqa: E402

if not _geo_available():
    raise SystemExit(
        "parser-os installed without its US postal gazetteer — every ZIP and "
        "city lookup will abstain, and sites will ship with a ZIP and no "
        "state. Check [tool.setuptools.package-data] in parser-os/pyproject.toml."
    )

print(f"critical_imports_ok ({len(WORKER_DEPS)} worker deps verified, gazetteer present)")
