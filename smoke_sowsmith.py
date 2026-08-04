#!/usr/bin/env python3
"""Build-time SowSmith identity check.

Rejects Bang/parser-os forks (name=purtera-evidence-mvp + top-level app/) that
overwrite parser-os and cause ModuleNotFoundError: app.core.orbitbrief_envelope.

Avoid ``from ... import`` at line start — ACR Dockerfile dependency scanning
can mis-parse those tokens (failed runs chck/chcj).
"""
import pathlib
import sys
import tomllib

root = pathlib.Path("SowSmith")
toml_path = root / "pyproject.toml"
if not toml_path.is_file():
    sys.exit("SowSmith/pyproject.toml missing")

with toml_path.open("rb") as fh:
    data = tomllib.load(fh)
name = (data.get("project") or {}).get("name")
if name != "sowsmith":
    sys.exit(
        "SowSmith must be package sowsmith, got %r "
        "(Bang fork overwrites parser-os)" % (name,)
    )
if not ((root / "src" / "sowsmith").is_dir() or (root / "sowsmith").is_dir()):
    sys.exit("SowSmith src/sowsmith layout missing")
print("SowSmith package ok:", name)
