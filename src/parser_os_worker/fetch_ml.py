"""Fetch + unpack the ML head artifacts (gate / facet / type) to ML_ARTIFACT_DIR.

Runs once at container startup (entrypoint.sh) BEFORE the compile worker. Pulls
the tarballs the parser-os heads load from blob ``ml-artifacts`` and unpacks each
to the dir the parser-os loaders read (``SOWSMITH_*_DIR``).

BEST-EFFORT: any failure leaves the artifact dir partial/empty and the parser-os
heads simply abstain (no torch/model -> no-op) — the compile then runs LLM-only,
byte-identical to today. :func:`main` always returns 0 so it never fails the
entrypoint.

Auth mirrors ``main.py``: ``AZURE_STORAGE_CONNECTION_STRING`` if set, else
``DefaultAzureCredential`` (the job's managed identity).

NOTE (perf): Container Apps Jobs run one message per replica, so this downloads
~900 MB on every compile. Acceptable for dev; a follow-up can bake the artifacts
into the image at build time to drop the per-job fetch.
"""
from __future__ import annotations

import logging
import os
import tarfile
import tempfile
from pathlib import Path

log = logging.getLogger("parser-os-worker.fetch_ml")

ACCOUNT_NAME = os.environ.get("AZURE_STORAGE_ACCOUNT", "purpulsedevstg01")
ML_CONTAINER = os.environ.get("ML_ARTIFACTS_CONTAINER", "ml-artifacts")
ML_DIR = Path(os.environ.get("ML_ARTIFACT_DIR", "/tmp/ml"))

# blob tarball -> marker dir (the tarball's root IS the marker dir, so unpacking
# at ML_DIR yields e.g. ML_DIR/_contrastive_type/{best,store.npz}).
ARTIFACTS = {
    "contrastive_type.tgz": "_contrastive_type",   # gate (keep deflector)
    "contrastive_facet.tgz": "_contrastive_facet",  # facet (7 dashboard sections)
    "type_head_gpu.tgz": "_type_head_gpu",          # type head #70
}


def _blob_service():
    cs = (os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
          or os.environ.get("ORBITBRIEF_ARTIFACTS_CONNECTION_STRING"))
    from azure.storage.blob import BlobServiceClient
    if cs:
        return BlobServiceClient.from_connection_string(cs)
    from azure.identity import DefaultAzureCredential
    return BlobServiceClient(
        account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
        credential=DefaultAzureCredential(),
    )


def fetch() -> int:
    """Download + unpack each head tarball. Returns the count now ready."""
    ML_DIR.mkdir(parents=True, exist_ok=True)
    try:
        container = _blob_service().get_container_client(ML_CONTAINER)
    except Exception as exc:
        log.warning("fetch_ml: cannot reach blob %s: %s", ML_CONTAINER, exc)
        return 0
    ready = 0
    for blob_name, marker in ARTIFACTS.items():
        dest = ML_DIR / marker
        if dest.is_dir():                       # already unpacked (idempotent)
            ready += 1
            continue
        tmp: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".tgz", delete=False) as fh:
                tmp = Path(fh.name)
                fh.write(container.download_blob(blob_name).readall())
            with tarfile.open(tmp, "r:gz") as tar:
                tar.extractall(ML_DIR)          # tar root == marker dir
            if dest.is_dir():
                ready += 1
                log.info("fetch_ml: ready %s -> %s", blob_name, dest)
            else:
                log.warning("fetch_ml: %s unpacked but %s missing", blob_name, dest)
        except Exception as exc:
            log.warning("fetch_ml: %s failed: %s", blob_name, exc)
        finally:
            if tmp is not None:
                tmp.unlink(missing_ok=True)
    return ready


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    n = fetch()
    log.info("fetch_ml: %d/%d head artifacts ready in %s", n, len(ARTIFACTS), ML_DIR)
    return 0   # never fail the entrypoint


if __name__ == "__main__":
    raise SystemExit(main())
