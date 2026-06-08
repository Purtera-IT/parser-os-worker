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
        for d in ("_type_head", "_span_heads"):
            base = os.path.join(DST, d)
            if os.path.isdir(base):
                for fn in os.listdir(base):
                    fp = os.path.join(base, fn)
                    if os.path.isfile(fp):
                        with open(fp, "rb") as f:
                            cc.upload_blob(f"{d}/{fn}", f, overwrite=True); n += 1
        print(f"write_back_ml: persisted {n} artifacts to blob")
    except Exception as e:
        print(f"write_back_ml: skipped ({type(e).__name__}: {e})", file=sys.stderr)


if __name__ == "__main__":
    main()
