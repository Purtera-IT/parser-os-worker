"""Persist the grown training log + retrained deflector heads back to the
ml-artifacts blob after a compile, so new teacher rows + PM corrections
accumulate ACROSS runs and the deflectors learn live. Fully non-fatal.

Uploads are INCREMENTAL. `_head_registry` is append-only and its entries are
content-addressed (`h_<ts>_<hash>.json/.npz`), so a name already in blob cannot
have different content. Re-uploading them all cost ~14k PUTs / ~5GB per night,
which never finished inside the caller's timeout — so the write-back was killed
every night and every promotion was silently thrown away. Skipping what is
already there turns that into a handful of uploads.
"""
import os, sys
DST = os.environ.get("ML_ARTIFACT_DIR", "/tmp/ml")
conn = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")

# Promotion state first: if anything does cut this run short, the champion the
# eval gate just promoted is already durable.
DIRS = ("_head_registry", "_calibrator", "_type_head", "_span_heads")


def _remote_sizes(cc, prefix):
    """name -> size for what is already in blob. One paged LIST beats thousands
    of blind PUTs."""
    try:
        return {b.name: (b.size or 0) for b in cc.list_blobs(name_starts_with=prefix)}
    except Exception as e:
        # Unknown remote state: upload everything rather than skip a file that
        # might genuinely be missing.
        print(f"write_back_ml: list({prefix}) failed, uploading all ({e})", file=sys.stderr)
        return {}


def main():
    if not conn:
        return
    try:
        from azure.storage.blob import ContainerClient
        cc = ContainerClient.from_connection_string(conn, "ml-artifacts")
        sent = skipped = 0

        deleted = 0
        for d in DIRS:
            base = os.path.join(DST, d)
            if not os.path.isdir(base):
                continue
            remote = _remote_sizes(cc, d + "/")
            local: set[str] = set()
            for root, _dirs, files in os.walk(base):
                for fn in files:
                    fp = os.path.join(root, fn)
                    rel = os.path.relpath(fp, DST).replace(os.sep, "/")
                    local.add(rel)
                    try:
                        # Same name + same size = same artifact, for names that
                        # carry a content hash.
                        if remote.get(rel) == os.path.getsize(fp):
                            skipped += 1
                            continue
                        with open(fp, "rb") as f:
                            cc.upload_blob(rel, f, overwrite=True)
                        sent += 1
                    except Exception as e:
                        # One bad artifact must not strand the rest.
                        print(f"write_back_ml: {rel} failed ({e})", file=sys.stderr)

            # Mirror deletions for the registry ONLY. `head_registry.prune` bounds
            # local history each night, but blob is the durable copy — without
            # this the prune reclaims nothing and the container keeps growing
            # (it reached ~5GB and filled the workers' /tmp).
            #
            # Guarded hard: only when this run actually has local registry files.
            # If fetch_ml was skipped or failed, `local` would be empty and a
            # blind mirror would wipe the entire registry from blob.
            if d == "_head_registry" and local:
                for name in remote:
                    if name in local:
                        continue
                    try:
                        cc.delete_blob(name)
                        deleted += 1
                    except Exception as e:
                        print(f"write_back_ml: delete {name} failed ({e})", file=sys.stderr)

        # The training log grows every run, so it always uploads. Last, because
        # it is the one artifact never safe to skip on a size match.
        log_p = os.path.join(DST, "_training_deepseek.db")
        if os.path.isfile(log_p):
            with open(log_p, "rb") as f:
                cc.upload_blob("_training_deepseek.db", f, overwrite=True)
            sent += 1

        print(
            f"write_back_ml: persisted {sent} artifacts to blob "
            f"({skipped} already current, {deleted} pruned)"
        )
    except Exception as e:
        print(f"write_back_ml: skipped ({type(e).__name__}: {e})", file=sys.stderr)


if __name__ == "__main__":
    main()
