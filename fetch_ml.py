"""Fetch ML artifacts (heads, kNN stores, TrainingLog) from the ml-artifacts blob
container to a local dir on worker startup, so the deflectors + stores fire live.

STRICT BY DEFAULT (2026-11-05). This script used to end every failure path with
``sys.exit(0)`` and the entrypoint ran it as ``python /fetch_ml.py || echo
"fetch_ml skipped"``, so a worker that fetched nothing booted anyway and served
compiles with every head inert. That is the silent zero the repo's doctrine
forbids: a deal then gets an envelope with no ``c3_brief`` at all and nothing
anywhere says the heads never ran.

Now: any failure, an empty fetch, or a configured artifact that is missing after
extraction exits non-zero and the container does not start. Set
``ML_ARTIFACTS_OPTIONAL=1`` to get the old behaviour back — deliberately, by
name, for a runtime that really is meant to serve without heads.

A receipt is written to ``${ML_ARTIFACT_DIR}/_fetch_ml_receipt.json`` either way,
so the compile can report what actually landed (app.learning.fetch_ml.artifact_self_check).
Read-only warm base; live-learning write-back is a separate step.
"""
import json
import os
import sys
import time

DST = os.environ.get("ML_ARTIFACT_DIR", "/tmp/ml")
RECEIPT = os.path.join(DST, "_fetch_ml_receipt.json")
OPTIONAL = os.environ.get("ML_ARTIFACTS_OPTIONAL", "").strip().lower() in {"1", "true", "yes", "on"}

#: Paths under DST that the runtime CREATES rather than fetches, so their absence
#: after a fetch is normal and must not fail the boot.
_RUNTIME_CREATED = ("cache", "_shadow_history", "_feedback", "_rule_decisions")


def _die(msg: str, receipt: dict) -> None:
    """Record what happened, then stop the boot unless opted out."""
    receipt["ok"] = False
    receipt["error"] = msg
    _write_receipt(receipt)
    if OPTIONAL:
        print(f"fetch_ml: {msg} — continuing, ML_ARTIFACTS_OPTIONAL=1 (heads will be inert)", file=sys.stderr)
        sys.exit(0)
    print(
        f"fetch_ml: FATAL {msg}. Refusing to start: a worker with no ML artifacts "
        f"compiles deals with every head inert and says nothing. "
        f"Set ML_ARTIFACTS_OPTIONAL=1 only if this runtime is meant to serve without heads.",
        file=sys.stderr,
    )
    sys.exit(1)


def _write_receipt(receipt: dict) -> None:
    try:
        os.makedirs(DST, exist_ok=True)
        receipt.setdefault("checked_at", time.time())
        with open(RECEIPT, "w") as fh:
            json.dump(receipt, fh, sort_keys=True)
    except OSError as ex:          # a receipt we cannot write must not mask the real error
        print(f"fetch_ml: could not write receipt ({ex})", file=sys.stderr)


def _required_paths() -> dict:
    """Every artifact path the worker is CONFIGURED to load out of DST.

    Derived from the environment rather than hardcoded, so a head added to the
    container config is covered the day it is added instead of the day someone
    remembers to update a list here. ``_head_registry`` is excluded because this
    script deliberately skips it (see FETCH_ML_INCLUDE_REGISTRY below), and
    runtime-created caches/DBs are excluded because they are written, not fetched.
    """
    out = {}
    base = os.path.realpath(DST)
    for key, val in os.environ.items():
        if not key.startswith("SOWSMITH_") or not val.strip():
            continue
        # Keyed on what the VALUE is, not what the NAME looks like. Matching
        # `_DIR`/`_DB`/`_PATH` suffixes let SOWSMITH_ADMISSION_REGISTRY
        # (=/tmp/ml/_admission_heads) straight through the gate -- and that is
        # the one artifact here with a live call path and no C3 replacement
        # (`col:admission` is `no_labels` and replaces "parser: span_admission
        # (lessons)"). A name pattern decided what got checked; now the path does.
        p = val.strip()
        if os.path.realpath(p) == base or not os.path.realpath(p).startswith(base + os.sep):
            continue                                   # not under the ML dir (e.g. /tmp/_feedback.db)
        low = os.path.basename(p).lower() + "|" + p.lower()
        if any(m in low for m in _RUNTIME_CREATED) or "_head_registry" in low:
            continue
        out[key] = p
    return out


receipt: dict = {"dst": DST, "optional": OPTIONAL}

conn = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")
if not conn:
    _die("no AZURE_STORAGE_CONNECTION_STRING; nothing could be fetched", receipt)

try:
    from azure.storage.blob import ContainerClient

    cc = ContainerClient.from_connection_string(conn, "ml-artifacts")
    os.makedirs(DST, exist_ok=True)
    # ml-artifacts is a shared dumping ground (training DBs, YOLO dataset images,
    # zip packages — GBs / 730+ blobs). The worker only needs the head tarballs +
    # the small pkl/json registries. Downloading everything pushed cold-start boot
    # past the queue's message-visibility timeout -> the compile message gets
    # consumed by a still-booting Job and lost (a queue zombie). Skip the junk so
    # boot stays ~1 min.
    # `_head_registry/` is append-only: every nightly run adds a candidate per
    # relation and nothing is ever pruned. It reached 14k files / ~5GB, and since
    # ml-artifacts as a whole is now ~9GB while these containers have 4-8Gi of
    # ephemeral disk, fetching it filled /tmp and every compile died with
    # `OSError: [Errno 28] Insufficient space under /tmp`.
    #
    # The compile path does not read the registry — serving is via the side-dir
    # head artifacts, and there are no promoted champions to load. Only the
    # nightly retrain needs it, to have an incumbent to beat and somewhere to
    # write the new candidate, so that job opts in with
    # FETCH_ML_INCLUDE_REGISTRY=1.
    include_registry = os.environ.get(
        "FETCH_ML_INCLUDE_REGISTRY", ""
    ).strip().lower() in {"1", "true", "yes", "on"}

    def _skip(name: str) -> bool:
        low = name.lower()
        if low.startswith("dataset/") or low.endswith((".png", ".jpg", ".jpeg", ".zip")):
            return True
        # Skip training dumps / backups / runpod scratch — but ALWAYS fetch the
        # live TrainingLog. Without `_training_deepseek.db` the nightly can't
        # see silver_audit / veto rows, and mid-day seed uploads never land.
        if low == "_training_deepseek.db":
            return False
        if "_training_" in low or "_runpod_" in low or "_backup_" in low:
            return True
        if not include_registry and low.startswith("_head_registry/"):
            return True
        return False

    n = skipped = 0
    #: Top-level names the blob store can actually supply, so a configured path
    #: with no source anywhere is told apart from one the fetch failed to land.
    seen_roots: set[str] = set()
    for b in cc.list_blobs():
        seen_roots.add(str(b.name).split("/", 1)[0])
        if _skip(b.name):
            skipped += 1
            continue
        p = os.path.join(DST, b.name)
        os.makedirs(os.path.dirname(p) or DST, exist_ok=True)
        with open(p, "wb") as f:
            f.write(cc.download_blob(b.name).readall())
        n += 1
    print(f"fetch_ml: downloaded {n} artifact files ({skipped} junk skipped) -> {DST}")
    receipt["downloaded"] = n
    receipt["skipped"] = skipped
    if n == 0:
        # Listing worked and produced nothing we wanted: the container is empty,
        # renamed, or the credential sees a different account. Booting here is
        # exactly the quiet-no-heads case.
        _die(f"downloaded 0 artifact files from ml-artifacts ({skipped} skipped)", receipt)

    # Unpack the fine-tuned model tarballs to the dirs the runtime loaders expect:
    #   gate_rubric_best.tgz (best/)           -> {DST}/_gate_rubric/best
    #   span_heads_gpu.tgz (runs/span_*/best)  -> {DST}/_span_gpu/runs/span_*/best
    # Set on the worker: SOWSMITH_RUBRIC_GATE_DIR={DST}/_gate_rubric/best ,
    #   SOWSMITH_SPAN_GPU_DIR={DST}/_span_gpu/runs .
    # An extract that fails is NOT shrugged off any more: the head it carries is
    # one the config says to load, so the missing-artifact check below fails the boot.
    import tarfile

    extract_errors: list[str] = []

    def _drop_archive(path: str, label: str) -> None:
        """Delete a tarball once it is safely extracted.

        The archives total ~2.8GB (span_heads_gpu.tgz alone is 1.14GB) and keeping
        them alongside their extracted contents doubled the footprint to ~5.7GB on
        containers with 4-8Gi of ephemeral disk. That is what left /tmp with
        ~480MB free and killed every compile with `OSError: [Errno 28]`. The
        tarball has no use after extraction — fetch_ml re-downloads it next start.
        """
        try:
            freed = os.path.getsize(path)
            os.remove(path)
            print(f"fetch_ml: freed {freed / 1e6:.0f}MB ({label} archive)")
        except OSError as ex:
            print(f"fetch_ml: could not drop {label} ({ex})", file=sys.stderr)

    for name, sub in (("gate_rubric_best.tgz", "_gate_rubric"),
                      ("span_heads_gpu.tgz", "_span_gpu")):
        tp = os.path.join(DST, name)
        if not os.path.exists(tp):
            continue
        outd = os.path.join(DST, sub)
        os.makedirs(outd, exist_ok=True)
        try:
            with tarfile.open(tp) as tf:
                try:
                    tf.extractall(outd, filter="data")   # py>=3.12 safe extraction
                except TypeError:
                    tf.extractall(outd)
            print(f"fetch_ml: extracted {name} -> {outd}")
            seen_roots.add(sub.lstrip("/"))
            _drop_archive(tp, name)
        except Exception as ex:
            extract_errors.append(f"{name}: {type(ex).__name__}: {ex}")
            print(f"fetch_ml: extract {name} failed ({ex})", file=sys.stderr)
    # Clean-rubric heads (2026-06): contrastive gate + facet kNN stores and the
    # GPU type head. These tarballs have the marker dir as their ROOT
    # (_contrastive_type/, _contrastive_facet/, _type_head_gpu/) so extract to DST.
    # Loaders read: SOWSMITH_CONTRASTIVE_TYPE_DIR={DST}/_contrastive_type ,
    #   SOWSMITH_CONTRASTIVE_FACET_DIR={DST}/_contrastive_facet ,
    #   SOWSMITH_TYPE_HEAD_GPU_DIR={DST}/_type_head_gpu/best .
    # contrastive_router.tgz root = _contrastive_router/ (store.npz + knn_meta.json
    # + best/ bge-base encoder). Loader: SOWSMITH_SERVICE_ROUTER_DIR={DST}/_contrastive_router
    for name in ("contrastive_type.tgz", "contrastive_facet.tgz", "type_head_gpu.tgz",
                 "contrastive_router.tgz"):
        tp = os.path.join(DST, name)
        if not os.path.exists(tp):
            continue
        try:
            with tarfile.open(tp) as tf:
                try:
                    tf.extractall(DST, filter="data")   # py>=3.12 safe extraction
                except TypeError:
                    tf.extractall(DST)
            print(f"fetch_ml: extracted {name} -> {DST}")
            for _root in os.listdir(DST):
                seen_roots.add(_root)
            _drop_archive(tp, name)
        except Exception as ex:
            extract_errors.append(f"{name}: {type(ex).__name__}: {ex}")
            print(f"fetch_ml: extract {name} failed ({ex})", file=sys.stderr)
    receipt["extract_errors"] = extract_errors

    # The boot gate: every artifact this container is CONFIGURED to load must now
    # be on disk. This is the check whose absence let a quoteless envelope look
    # exactly like a normal one.
    required = _required_paths()
    absent = {k: p for k, p in required.items() if not os.path.exists(p)}

    def _root_of(path: str) -> str:
        rel = os.path.relpath(os.path.realpath(path), os.path.realpath(DST))
        return rel.split(os.sep, 1)[0]

    # A path the blob store has no source for was never going to arrive: that is
    # DEAD CONFIG, not a fetch that broke, and failing the boot on it would take
    # the worker down for something no fetch could ever fix. Live example:
    # SOWSMITH_CALIBRATOR_PATH=/tmp/ml/_calibrator/calibrator.joblib is set on
    # dev AND prod, while the only calibrator ever in ml-artifacts is
    # `_quarantine/calibrator_leaked_20260804.joblib`, quarantined 2026-08-04 --
    # and nothing reads it either (the worker calls compile_project() without
    # calibrator_path). Say so loudly, every boot, and carry on.
    no_source = sorted(k for k, p in absent.items() if _root_of(p) not in seen_roots)
    missing = sorted(k for k in absent if k not in no_source)
    receipt["required"] = required
    receipt["missing"] = missing
    receipt["no_source"] = no_source
    receipt["present"] = sorted(k for k in required if k not in absent)
    for k in no_source:
        print(f"fetch_ml: DEAD CONFIG {k}={required[k]} — ml-artifacts has no source for it; "
              f"nothing can load this. Unset it or publish the artifact.", file=sys.stderr)
    if missing:
        detail = ", ".join(f"{k}={required[k]}" for k in missing)
        _die(f"{len(missing)} configured artifact(s) the store HAS but the fetch did not land: {detail}", receipt)
    if extract_errors:
        _die("extract failed: " + "; ".join(extract_errors), receipt)

    receipt["ok"] = True
    _write_receipt(receipt)
    print(f"fetch_ml: ok — {n} files, {len(required)} configured artifact(s) present")
except SystemExit:
    raise                                   # _die already reported and chose the code
except Exception as e:
    _die(f"{type(e).__name__}: {e}", receipt)
