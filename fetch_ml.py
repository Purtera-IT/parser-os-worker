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
    n = 0
    for b in cc.list_blobs():
        p = os.path.join(DST, b.name)
        os.makedirs(os.path.dirname(p) or DST, exist_ok=True)
        with open(p, "wb") as f:
            f.write(cc.download_blob(b.name).readall())
        n += 1
    print(f"fetch_ml: downloaded {n} artifact files -> {DST}")
except Exception as e:
    print(f"fetch_ml: skipped ({type(e).__name__}: {e})", file=sys.stderr)
    sys.exit(0)
