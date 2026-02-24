"""
Zapper Direct — Azure Large File Uploader
Production-ready CLI utility for uploading files (or entire directory trees) to Azure Blob Storage.

Usage:
    python -m zapper_direct <path> [container_name] [--blob-prefix PREFIX] [--dry-run]

    <path> can be a single file or a directory.
    When a directory is given, all files under it (recursively) are uploaded,
    preserving the relative directory structure as the blob path.

Features:
    - Accepts a file or an entire directory tree as input
    - Block blob chunked upload (up to ~4.77 TB per blob)
    - Resumable: progress saved to disk per file, re-run to continue
    - Concurrent chunk uploads (configurable thread count)
    - Exponential-backoff retry on transient network failures
    - Blob metadata stamped on commit
"""

import os
import sys
import math
import base64
import binascii
import json
import hashlib
import logging
import signal
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from azure.storage.blob import BlobServiceClient, BlobBlock, ContentSettings
from azure.core.exceptions import (
    ResourceExistsError,
    ServiceRequestError,
    ServiceResponseError,
    AzureError,
)
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


def _build_logger(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "uploader_process.log"

    logger = logging.getLogger("zapper_direct")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))

    logger.addHandler(console)
    logger.addHandler(fh)
    return logger


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_DEFAULTS = {
    "CHUNK_SIZE_MB": 64,
    "CONCURRENCY": 8,

    "MAX_RETRIES": 5,
    "RETRY_BASE_DELAY": 2,
}


class Config:
    def __init__(self) -> None:
        load_dotenv()

        self.conn_str: str = os.environ["AZURE_CONN_STR"]
        self.container_name: str = os.getenv("CONTAINER_NAME", "")
        self.chunk_size: int = (
            int(os.getenv("CHUNK_SIZE_MB", _DEFAULTS["CHUNK_SIZE_MB"])) * 1024 * 1024
        )
        self.concurrency: int = int(
            os.getenv("CONCURRENCY", _DEFAULTS["CONCURRENCY"])
        )
        self.log_path: Optional[str] = os.getenv("LOG_PATH")
        self.max_retries: int = int(
            os.getenv("MAX_RETRIES", _DEFAULTS["MAX_RETRIES"])
        )
        self.retry_base_delay: int = int(
            os.getenv("RETRY_BASE_DELAY", _DEFAULTS["RETRY_BASE_DELAY"])
        )

        # Validate the connection string and account key before anything else
        self._validate_connection_string()

        # Validate chunk size (Azure max block size is 4000 MiB)
        max_chunk = 4000 * 1024 * 1024
        if self.chunk_size > max_chunk:
            raise ValueError(
                f"CHUNK_SIZE_MB exceeds Azure maximum (4000 MB). Got {self.chunk_size // (1024*1024)} MB."
            )
        if self.chunk_size < 1024 * 1024:
            raise ValueError("CHUNK_SIZE_MB must be at least 1 MB.")

    def _validate_connection_string(self) -> None:
        """Parse the connection string and validate each component before connecting."""
        cs = self.conn_str.strip()

        # Must have the key=value; structure
        parts = {}
        for segment in cs.split(";"):
            segment = segment.strip()
            if not segment:
                continue
            if "=" not in segment:
                raise ValueError(
                    f"Malformed AZURE_CONN_STR: segment '{segment}' has no '=' separator.\n"
                    "Copy a fresh connection string from Azure Portal → Storage account → Access keys."
                )
            key, _, value = segment.partition("=")
            parts[key.strip()] = value.strip()

        # Required keys
        for required in ("AccountName", "AccountKey", "DefaultEndpointsProtocol"):
            if required not in parts:
                raise ValueError(
                    f"AZURE_CONN_STR is missing the '{required}' field.\n"
                    "Copy a fresh connection string from Azure Portal → Storage account → Access keys."
                )

        # AccountName must not be empty or placeholder
        account_name = parts["AccountName"]
        if not account_name or account_name in ("your_account", "your_account_name", ""):
            raise ValueError(
                "AZURE_CONN_STR has a placeholder AccountName. "
                "Replace it with your real Azure Storage account name."
            )

        # AccountKey must be valid, non-truncated base64.
        # Re-join from raw string because the key ends with '=' padding which the partition above strips.
        raw_key = cs.split("AccountKey=", 1)[-1].split(";")[0].strip()

        if not raw_key or raw_key in ("your_account_key", "your_key"):
            raise ValueError(
                "AZURE_CONN_STR has a placeholder AccountKey. "
                "Copy a fresh connection string from Azure Portal → Storage account → Access keys."
            )

        # Azure storage account keys are 64-byte values, base64-encoded → 88 chars with padding
        # Minimum sanity: must be at least 40 chars and valid base64
        if len(raw_key) < 40:
            raise ValueError(
                f"AZURE_CONN_STR AccountKey looks too short ({len(raw_key)} chars). "
                "It was likely truncated. Copy a fresh connection string from the Azure Portal."
            )

        # Check padding: base64 length must be a multiple of 4
        padding_needed = len(raw_key) % 4
        if padding_needed != 0:
            padded = raw_key + "=" * (4 - padding_needed)
        else:
            padded = raw_key

        try:
            decoded = base64.b64decode(padded, validate=True)
        except (binascii.Error, ValueError):
            raise ValueError(
                "AZURE_CONN_STR AccountKey is not valid base64 — it is corrupted or truncated.\n"
                "Fix: Go to Azure Portal → Storage account → Access keys → copy the full Connection string."
            )

        # Azure storage keys decode to exactly 64 bytes
        if len(decoded) != 64:
            raise ValueError(
                f"AZURE_CONN_STR AccountKey decoded to {len(decoded)} bytes (expected 64). "
                "The key appears truncated.\n"
                "Fix: Go to Azure Portal → Storage account → Access keys → copy the full Connection string."
            )

        # Protocol must be https (warn only)
        if parts.get("DefaultEndpointsProtocol", "").lower() != "https":
            raise ValueError(
                "AZURE_CONN_STR uses a non-HTTPS protocol. Set DefaultEndpointsProtocol=https."
            )


# ---------------------------------------------------------------------------
# Progress tracking
# ---------------------------------------------------------------------------

class ProgressLog:
    """Persists upload progress to a JSON file for resumability.

    The log is never deleted on success — it is stamped with completed=true instead.
    This lets re-runs skip files that have already been fully committed to Azure,
    even after a machine shutdown or crash.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self.data: dict = {
            "schema_version": 2,
            "file_path": "",
            "file_size": 0,
            "file_md5": None,
            "container": "",
            "blob_name": "",
            "chunk_size": 0,
            "total_chunks": 0,
            "uploaded_chunks": [],
            "completed": False,       # True once commit_block_list succeeds
            "completed_at": None,
            "started_at": None,
            "updated_at": None,
        }

    @property
    def is_completed(self) -> bool:
        return bool(self.data.get("completed", False))

    def load(self) -> bool:
        """Load existing progress. Returns True if valid data found."""
        if not self.path.exists():
            return False
        try:
            with self.path.open("r", encoding="utf-8") as fh:
                loaded = json.load(fh)
            self.data.update(loaded)
            return True
        except (json.JSONDecodeError, KeyError):
            return False

    def save(self) -> None:
        self.data["updated_at"] = datetime.now(timezone.utc).isoformat()
        tmp = self.path.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(self.data, fh, indent=2)
        tmp.replace(self.path)  # atomic rename

    def mark_completed(self) -> None:
        """Stamp the log as fully done. Kept on disk so re-runs skip this file."""
        self.data["completed"] = True
        self.data["completed_at"] = datetime.now(timezone.utc).isoformat()
        self.save()

    def mark_started(
        self,
        file_path: str,
        file_size: int,
        container: str,
        blob_name: str,
        chunk_size: int,
        total_chunks: int,
    ) -> None:
        self.data.update(
            {
                "file_path": str(file_path),
                "file_size": file_size,
                "container": container,
                "blob_name": blob_name,
                "chunk_size": chunk_size,
                "total_chunks": total_chunks,
                "started_at": self.data["started_at"]
                or datetime.now(timezone.utc).isoformat(),
            }
        )
        self.save()

    @property
    def uploaded_set(self) -> set:
        return set(self.data.get("uploaded_chunks", []))

    def mark_chunk(self, index: int, all_uploaded: set) -> None:
        self.data["uploaded_chunks"] = sorted(all_uploaded)
        self.save()


# ---------------------------------------------------------------------------
# Uploader core
# ---------------------------------------------------------------------------

class ChunkUploader:
    """Uploads a single file to Azure using the Block Blob pattern."""

    def __init__(
        self,
        cfg: Config,
        logger: logging.Logger,
        file_path: Path,
        container_name: str,
        blob_name: str,
        progress_log: ProgressLog,
    ) -> None:
        self.cfg = cfg
        self.logger = logger
        self.file_path = file_path
        self.container_name = container_name
        self.blob_name = blob_name
        self.progress_log = progress_log

        self._abort = False
        signal.signal(signal.SIGINT, self._handle_interrupt)
        signal.signal(signal.SIGTERM, self._handle_interrupt)

    def _handle_interrupt(self, signum, frame):  # type: ignore[override]
        self.logger.warning(
            "Interrupt received. Saving progress and exiting gracefully..."
        )
        self._abort = True

    # ------------------------------------------------------------------
    # Azure clients
    # ------------------------------------------------------------------

    def _get_clients(self):
        try:
            svc = BlobServiceClient.from_connection_string(
                self.cfg.conn_str,
                connection_timeout=30,
                read_timeout=120,
            )
        except Exception as exc:
            self.logger.error(f"Cannot connect to Azure: {exc}")
            sys.exit(1)

        container_client = svc.get_container_client(self.container_name)
        try:
            container_client.create_container()
            self.logger.info(f"Created container '{self.container_name}'.")
        except ResourceExistsError:
            self.logger.debug(f"Container '{self.container_name}' already exists.")

        blob_client = container_client.get_blob_client(self.blob_name)
        return blob_client

    # ------------------------------------------------------------------
    # Chunk helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _block_id(index: int) -> str:
        return base64.b64encode(index.to_bytes(8, byteorder="big")).decode("ascii")

    def _read_chunk(self, index: int, file_size: int) -> bytes:
        start = index * self.cfg.chunk_size
        end = min(start + self.cfg.chunk_size, file_size)
        with self.file_path.open("rb") as fh:
            fh.seek(start)
            return fh.read(end - start)

    def _upload_chunk_with_retry(
        self, blob_client, index: int, file_size: int
    ) -> int:
        """Upload one chunk with exponential-backoff retry. Returns chunk index on success."""
        data = self._read_chunk(index, file_size)
        block_id = self._block_id(index)

        last_exc: Optional[Exception] = None
        for attempt in range(1, self.cfg.max_retries + 2):  # +2 so attempt 1 = first try
            if self._abort:
                raise RuntimeError("Upload aborted by user.")
            try:
                blob_client.stage_block(
                    block_id=block_id,
                    data=data,
                    length=len(data),
                )
                return index
            except (ServiceRequestError, ServiceResponseError, AzureError) as exc:
                last_exc = exc
                if attempt > self.cfg.max_retries:
                    break
                delay = self.cfg.retry_base_delay ** attempt
                self.logger.warning(
                    f"Chunk {index}: transient error (attempt {attempt}/{self.cfg.max_retries}), "
                    f"retrying in {delay}s — {exc}"
                )
                time.sleep(delay)
            except Exception as exc:
                self.logger.error(f"Chunk {index}: non-retryable error — {exc}")
                raise

        self.logger.error(
            f"Chunk {index}: failed after {self.cfg.max_retries} retries — {last_exc}"
        )
        raise last_exc  # type: ignore[misc]

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------

    def run(self) -> bool:
        """Execute the upload. Returns True on success."""
        file_size = self.file_path.stat().st_size
        total_chunks = math.ceil(file_size / self.cfg.chunk_size) if file_size > 0 else 1

        self.logger.info(
            f"File : {self.file_path}  ({file_size:,} bytes / "
            f"{file_size / (1024**3):.3f} GiB)"
        )
        self.logger.info(
            f"Chunk: {self.cfg.chunk_size // (1024*1024)} MB  |  "
            f"Chunks: {total_chunks}  |  "
            f"Threads: {self.cfg.concurrency}"
        )
        self.logger.info(
            f"Target: {self.container_name}/{self.blob_name}"
        )

        # Initialise / load progress
        loaded = self.progress_log.load()
        if loaded:
            prev = self.progress_log.data

            # Check if this file was already fully committed on a previous run
            if self.progress_log.is_completed and (
                os.path.abspath(prev.get("file_path", "")) == str(self.file_path.resolve())
                and prev.get("container") == self.container_name
                and prev.get("blob_name") == self.blob_name
                and prev.get("file_size") == file_size
            ):
                self.logger.info(
                    f"Already completed on {prev.get('completed_at', 'a previous run')} — skipping."
                )
                return True  # treat as success, nothing to do

            if (
                os.path.abspath(prev.get("file_path", "")) == str(self.file_path.resolve())
                and prev.get("container") == self.container_name
                and prev.get("blob_name") == self.blob_name
                and prev.get("file_size") == file_size
                and prev.get("chunk_size") == self.cfg.chunk_size
            ):
                self.logger.info(
                    f"Resuming: {len(self.progress_log.uploaded_set)}/{total_chunks} chunks already done."
                )
            else:
                self.logger.warning(
                    "Progress log mismatch — starting fresh (file or config changed)."
                )
                self.progress_log.data["uploaded_chunks"] = []
                self.progress_log.data["completed"] = False

        self.progress_log.mark_started(
            str(self.file_path.resolve()),
            file_size,
            self.container_name,
            self.blob_name,
            self.cfg.chunk_size,
            total_chunks,
        )

        blob_client = self._get_clients()
        uploaded: set = self.progress_log.uploaded_set
        to_upload = [i for i in range(total_chunks) if i not in uploaded]

        if not to_upload:
            self.logger.info("All chunks already uploaded — skipping to commit.")
        else:
            self.logger.info(
                f"Uploading {len(to_upload)} chunk(s) with {self.cfg.concurrency} thread(s)..."
            )
            t0 = time.monotonic()
            bytes_uploaded = 0

            with ThreadPoolExecutor(max_workers=self.cfg.concurrency) as pool:
                futures = {
                    pool.submit(self._upload_chunk_with_retry, blob_client, idx, file_size): idx
                    for idx in to_upload
                }
                for future in as_completed(futures):
                    orig_idx = futures[future]
                    if self._abort:
                        self.logger.warning("Aborting remaining tasks.")
                        for f in futures:
                            f.cancel()
                        return False
                    try:
                        done_idx = future.result()
                    except Exception as exc:
                        self.logger.error(
                            f"Fatal: chunk {orig_idx} could not be uploaded — {exc}"
                        )
                        for f in futures:
                            f.cancel()
                        return False

                    uploaded.add(done_idx)
                    self.progress_log.mark_chunk(done_idx, uploaded)

                    # Progress logging
                    chunk_start = done_idx * self.cfg.chunk_size
                    chunk_end = min(chunk_start + self.cfg.chunk_size, file_size)
                    bytes_uploaded += chunk_end - chunk_start
                    elapsed = max(time.monotonic() - t0, 0.001)
                    speed_mb = (bytes_uploaded / elapsed) / (1024 * 1024)
                    pct = len(uploaded) / total_chunks * 100
                    eta_s = (
                        (file_size - bytes_uploaded) / (bytes_uploaded / elapsed)
                        if bytes_uploaded
                        else 0
                    )
                    self.logger.info(
                        f"[{pct:5.1f}%] chunk {done_idx + 1}/{total_chunks}  "
                        f"speed={speed_mb:.1f} MB/s  eta={_fmt_seconds(eta_s)}"
                    )

        # Sanity check
        if len(uploaded) < total_chunks:
            missing = total_chunks - len(uploaded)
            self.logger.error(
                f"Incomplete: {missing} chunk(s) missing. Re-run to resume."
            )
            return False

        # Commit
        self.logger.info("All chunks staged — committing block list ...")
        block_list = [BlobBlock(self._block_id(i)) for i in range(total_chunks)]
        metadata = {
            "uploaded_by": "zapper_direct",
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
            "original_filename": self.file_path.name,
            "file_size_bytes": str(file_size),
        }
        try:
            blob_client.commit_block_list(
                block_list,
                metadata=metadata,
                content_settings=ContentSettings(
                    content_type=_guess_content_type(self.file_path)
                ),
            )
        except Exception as exc:
            self.logger.error(
                f"Commit failed: {exc}. Re-run to retry without re-uploading chunks."
            )
            return False

        self.logger.info(
            f"Committed '{self.blob_name}' to container '{self.container_name}'."
        )

        # Stamp the log as completed so re-runs skip this file (do NOT delete it)
        try:
            self.progress_log.mark_completed()
        except Exception:
            pass

        self.logger.info("Upload complete.")
        return True



# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_seconds(s: float) -> str:
    if s <= 0:
        return "--:--"
    s = int(s)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h:
        return f"{h}h{m:02d}m{sec:02d}s"
    if m:
        return f"{m}m{sec:02d}s"
    return f"{sec}s"


def _guess_content_type(path: Path) -> str:
    suffix = path.suffix.lower()
    return {
        ".csv": "text/csv",
        ".json": "application/json",
        ".parquet": "application/octet-stream",
        ".zip": "application/zip",
        ".gz": "application/gzip",
        ".tar": "application/x-tar",
        ".txt": "text/plain",
        ".tsv": "text/tab-separated-values",
    }.get(suffix, "application/octet-stream")


def _parse_args():
    import argparse

    parser = argparse.ArgumentParser(
        prog="zapper-direct",
        description=(
            "Upload a file or an entire directory tree to Azure Blob Storage "
            "with resumable, chunked, parallel transfers."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  # Upload a single file\n"
            "  python -m zapper_direct report.csv\n\n"
            "  # Upload all files in a directory (recursively)\n"
            '  python -m zapper_direct "C:\\Data\\exports" my-container\n\n'
            "  # Upload directory with a virtual folder prefix in Azure\n"
            '  python -m zapper_direct /data/exports my-container --blob-prefix 2024/q1\n\n'
            "  # Dry run — validate config without uploading\n"
            "  python -m zapper_direct /data/exports --dry-run\n"
        ),
    )
    parser.add_argument(
        "path",
        help="Path to a file or directory to upload. Directories are walked recursively.",
    )
    parser.add_argument(
        "container_name",
        nargs="?",
        default=None,
        help="Target Azure container name. Overrides CONTAINER_NAME in .env.",
    )
    parser.add_argument(
        "--blob-prefix",
        default="",
        metavar="PREFIX",
        help=(
            "Optional prefix (virtual folder) prepended to every blob name. "
            'E.g. --blob-prefix "2024/q1" stores files under that path in Azure.'
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate config and list files that would be uploaded, without uploading.",
    )
    args = parser.parse_args()
    return args.path, args.container_name, args.blob_prefix.strip("/"), args.dry_run


# ---------------------------------------------------------------------------
# Directory helpers
# ---------------------------------------------------------------------------

def _collect_files(root: Path) -> list[Path]:
    """Return all files under root, sorted for deterministic order."""
    return sorted(f for f in root.rglob("*") if f.is_file())


def _make_blob_name(file: Path, root: Path, prefix: str) -> str:
    """
    Compute the blob name for a file relative to root, with optional prefix.

    Example:
        root   = /data/exports
        file   = /data/exports/subdir/report.csv
        prefix = 2024/q1
        result = 2024/q1/subdir/report.csv
    """
    relative = file.relative_to(root)
    # Use forward slashes for Azure blob paths (even on Windows)
    blob = relative.as_posix()
    if prefix:
        blob = f"{prefix}/{blob}"
    return blob


def _resolve_progress_path(blob_name: str, base_dir: Path, log_path_override: Optional[str]) -> Path:
    """Return the path for a file's progress JSON log."""
    safe = blob_name.replace("/", "_").replace("\\", "_")
    if log_path_override:
        log_dir = Path(log_path_override)
        if log_dir.is_dir() or not log_dir.suffix:
            log_dir.mkdir(parents=True, exist_ok=True)
            return log_dir / f"{safe}.upload.json"
        return log_dir
    return base_dir / "logs" / f"{safe}.upload.json"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    path_arg, container_arg, blob_prefix, dry_run = _parse_args()

    # Config — validate everything (including connection string) before touching Azure
    try:
        cfg = Config()
    except KeyError:
        print(
            "ERROR: AZURE_CONN_STR not set. Copy .env.template to .env and fill in your credentials.",
            file=sys.stderr,
        )
        sys.exit(1)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    base_dir = Path.cwd()
    logger = _build_logger(base_dir / "logs")

    logger.info("=" * 60)
    logger.info("  Zapper Direct — Azure Large File Uploader")
    logger.info("=" * 60)

    # Resolve container
    container_name = container_arg or cfg.container_name
    if not container_name:
        logger.error(
            "No container name provided. Pass as argument or set CONTAINER_NAME in .env."
        )
        sys.exit(1)

    # Resolve input path
    input_path = Path(path_arg).expanduser().resolve()
    if not input_path.exists():
        logger.error(f"Path not found: {input_path}")
        sys.exit(1)

    # Build the list of (file_path, blob_name) pairs
    if input_path.is_file():
        # Single-file mode — blob name is the filename (or prefix/filename if prefix given)
        blob_name = f"{blob_prefix}/{input_path.name}" if blob_prefix else input_path.name
        files: list[tuple[Path, str]] = [(input_path, blob_name)]
    elif input_path.is_dir():
        all_files = _collect_files(input_path)
        if not all_files:
            logger.error(f"Directory is empty (no files found): {input_path}")
            sys.exit(1)
        files = [(f, _make_blob_name(f, input_path, blob_prefix)) for f in all_files]
    else:
        logger.error(f"Path is neither a file nor a directory: {input_path}")
        sys.exit(1)

    total_files = len(files)
    total_bytes = sum(f.stat().st_size for f, _ in files)

    logger.info(f"Mode      : {'single file' if input_path.is_file() else 'directory'}")
    logger.info(f"Source    : {input_path}")
    logger.info(f"Container : {container_name}")
    if blob_prefix:
        logger.info(f"Prefix    : {blob_prefix}/")
    logger.info(f"Files     : {total_files:,}  ({total_bytes / (1024**3):.3f} GiB total)")
    logger.info(f"Chunk     : {cfg.chunk_size // (1024 * 1024)} MB  |  Threads: {cfg.concurrency}")

    # Dry run — just list files and exit
    if dry_run:
        logger.info("[DRY RUN] Files that would be uploaded:")
        for i, (fp, bn) in enumerate(files, 1):
            logger.info(f"  [{i:>{len(str(total_files))}}] {fp.stat().st_size:>14,} bytes  →  {bn}")
        logger.info("[DRY RUN] No files were uploaded.")
        sys.exit(0)

    # Upload files one by one (each file already uses parallel chunk threads)
    succeeded: list[str] = []
    failed: list[tuple[str, str]] = []  # (blob_name, reason)

    for file_num, (file_path, blob_name) in enumerate(files, 1):
        logger.info("")
        logger.info(
            f"[{file_num}/{total_files}] {file_path.name}  →  {blob_name}"
        )

        progress_path = _resolve_progress_path(blob_name, base_dir, cfg.log_path)
        progress_log = ProgressLog(progress_path)

        uploader = ChunkUploader(
            cfg=cfg,
            logger=logger,
            file_path=file_path,
            container_name=container_name,
            blob_name=blob_name,
            progress_log=progress_log,
        )

        try:
            success = uploader.run()
        except Exception as exc:
            success = False
            reason = str(exc)
        else:
            reason = "upload incomplete — re-run to resume"

        if success:
            succeeded.append(blob_name)
        else:
            failed.append((blob_name, reason if not success else ""))
            logger.warning(f"File skipped/failed: {file_path.name} — continuing with remaining files.")

    # Final summary
    logger.info("")
    logger.info("=" * 60)
    logger.info(f"  Summary: {len(succeeded)}/{total_files} files uploaded successfully")
    if failed:
        logger.warning(f"  {len(failed)} file(s) did not complete:")
        for bn, reason in failed:
            logger.warning(f"    - {bn}" + (f"  ({reason})" if reason else ""))
        logger.warning("  Re-run the same command to resume failed files.")
    logger.info("=" * 60)

    if failed:
        sys.exit(2)
    else:
        print(f"\nAll {total_files} file(s) uploaded successfully.")
        sys.exit(0)


if __name__ == "__main__":
    main()
