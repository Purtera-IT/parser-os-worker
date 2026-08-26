"""Fetch ML deflector artifacts (type-head registry, feedback store, admission
heads) from the ml-artifacts blob container to a local dir on worker startup, so
the #70/#71 deflectors + kNN store fire live. Non-fatal: any failure just leaves
the dir empty and the deflectors abstain (byte-identical to the LLM-only path).
Read-only warm base; live-learning write-back is a separate step."""
import os, sys
DST = os.environ.get("ML_ARTIFACT_DIR", "/tmp/ml")
conn = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")
if not conn:
    print("fetch_ml: no storage connection string; skipping", file=sys.stderr); sys.exit(0)
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
    for b in cc.list_blobs():
        if _skip(b.name):
            skipped += 1
            continue
        p = os.path.join(DST, b.name)
        os.makedirs(os.path.dirname(p) or DST, exist_ok=True)
        with open(p, "wb") as f:
            f.write(cc.download_blob(b.name).readall())
        n += 1
    print(f"fetch_ml: downloaded {n} artifact files ({skipped} junk skipped) -> {DST}")
    # Unpack the fine-tuned model tarballs to the dirs the runtime loaders expect:
    #   gate_rubric_best.tgz (best/)           -> {DST}/_gate_rubric/best
    #   span_heads_gpu.tgz (runs/span_*/best)  -> {DST}/_span_gpu/runs/span_*/best
    # Set on the worker: SOWSMITH_RUBRIC_GATE_DIR={DST}/_gate_rubric/best ,
    #   SOWSMITH_SPAN_GPU_DIR={DST}/_span_gpu/runs . Non-fatal.
    import tarfile

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
            _drop_archive(tp, name)
        except Exception as ex:
            print(f"fetch_ml: extract {name} failed ({ex})", file=sys.stderr)
    # Clean-rubric heads (2026-06): contrastive gate + facet kNN stores and the
    # GPU type head. These tarballs have the marker dir as their ROOT
    # (_contrastive_type/, _contrastive_facet/, _type_head_gpu/) so extract to DST.
    # Loaders read: SOWSMITH_CONTRASTIVE_TYPE_DIR={DST}/_contrastive_type ,
    #   SOWSMITH_CONTRASTIVE_FACET_DIR={DST}/_contrastive_facet ,
    #   SOWSMITH_TYPE_HEAD_GPU_DIR={DST}/_type_head_gpu/best . Non-fatal.
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
            _drop_archive(tp, name)
        except Exception as ex:
            print(f"fetch_ml: extract {name} failed ({ex})", file=sys.stderr)
except Exception as e:
    print(f"fetch_ml: skipped ({type(e).__name__}: {e})", file=sys.stderr)
    sys.exit(0)
