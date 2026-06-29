"""Persist the grown training log + retrained deflector heads back to the
ml-artifacts blob after a compile, so new teacher rows + PM corrections
accumulate ACROSS runs and the deflectors learn live. Fully non-fatal."""
import os, sys
DST = os.environ.get("ML_ARTIFACT_DIR", "/tmp/ml")
conn = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")


def main():
    if not conn:
        return
    try:
        from azure.storage.blob import ContainerClient
        cc = ContainerClient.from_connection_string(conn, "ml-artifacts")
        n = 0
        # the training log (the substrate that grows with corrections)
        log_p = os.path.join(DST, "_training_deepseek.db")
        if os.path.isfile(log_p):
            with open(log_p, "rb") as f:
                cc.upload_blob("_training_deepseek.db", f, overwrite=True); n += 1
        # the retrained head registries
        # _head_registry persists the eval-gated champions so promote/rollback
        # is STATEFUL across nightly runs (the gate has an incumbent to beat);
        # _calibrator carries the trained confidence model. fetch_ml re-hydrates
        # both on the next worker start. Walk one level deeper so champion
        # subdirs (per-relation versions) round-trip too.
        for d in ("_type_head", "_span_heads", "_head_registry", "_calibrator"):
            base = os.path.join(DST, d)
            if not os.path.isdir(base):
                continue
            for root, _dirs, files in os.walk(base):
                for fn in files:
                    fp = os.path.join(root, fn)
                    rel = os.path.relpath(fp, DST).replace(os.sep, "/")
                    with open(fp, "rb") as f:
                        cc.upload_blob(rel, f, overwrite=True); n += 1
        print(f"write_back_ml: persisted {n} artifacts to blob")
    except Exception as e:
        print(f"write_back_ml: skipped ({type(e).__name__}: {e})", file=sys.stderr)


if __name__ == "__main__":
    main()
