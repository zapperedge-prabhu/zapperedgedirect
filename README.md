# Zapper Direct

A production-grade Python command-line utility for uploading very large files (up to ~100 TB) to Azure Blob Storage using the Block Blob pattern.

- Chunked, parallel uploads with configurable block size (1–4000 MB)
- Fully resumable — re-run after any interruption to pick up exactly where it left off
- Exponential-backoff retry on transient network failures
- Blob metadata and content-type stamped on commit
- Atomic progress-log writes (no corruption on crash)
- Graceful SIGINT / SIGTERM handling (saves progress before exit)
- Dry-run mode to validate config without touching Azure

---

## Requirements

- Python 3.8 or later
- An Azure Storage Account and its connection string

---

## Installation

### Option A — Run directly (no install)

```bash
# Clone or download this repository, then:
pip install azure-storage-blob python-dotenv

python -m zapper_direct path/to/bigfile.dat
```

### Option B — Install as a package

```bash
pip install .          # from the project directory
# or
pip install -e .       # editable install for development
```

This registers the `zapper-direct` command on your PATH:

```bash
zapper-direct path/to/bigfile.dat my-container
```

---

## Configuration

Copy `.env.template` to `.env` in the same directory you run the tool from, then fill in your values:

```bash
cp .env.template .env
```

| Variable          | Required | Default     | Description |
|-------------------|----------|-------------|-------------|
| `AZURE_CONN_STR`  | Yes      | —           | Azure Storage connection string (from Azure Portal → Access keys) |
| `CONTAINER_NAME`  | No*      | —           | Target container; can also be passed as a CLI argument |
| `CHUNK_SIZE_MB`   | No       | `64`        | Block size in MB. Larger = fewer API calls, more RAM per thread (max 4000) |
| `CONCURRENCY`     | No       | `8`         | Number of parallel upload threads |
| `MAX_RETRIES`     | No       | `5`         | Maximum retry attempts per chunk on transient errors |
| `RETRY_BASE_DELAY`| No       | `2`         | Base seconds for exponential backoff: delay = base ^ attempt |
| `LOG_PATH`        | No       | `./logs/`   | Directory (or file path) for progress log files |

\* Required unless provided on the command line.

### Tuning tips

| Scenario | Recommended settings |
|---|---|
| Fast datacenter link (1 Gbps+) | `CHUNK_SIZE_MB=256`, `CONCURRENCY=16` |
| Good office connection (100–500 Mbps) | `CHUNK_SIZE_MB=64`, `CONCURRENCY=8` |
| Home / unreliable network | `CHUNK_SIZE_MB=16`, `CONCURRENCY=4`, `MAX_RETRIES=8` |

---

## Usage

```
python -m zapper_direct <path> [container_name] [--blob-prefix PREFIX] [--dry-run]

Arguments:
  path                 Path to a file or directory. Directories are walked recursively.
  container_name       Target Azure container (optional if set in .env)

Options:
  --blob-prefix PREFIX Virtual folder prefix prepended to every blob name in Azure
  --dry-run            List files that would be uploaded without uploading anything
  -h, --help           Show this help message and exit
```

### Examples

```bash
# Upload a single file (container from .env)
python -m zapper_direct report.csv

# Upload an entire directory recursively
python -m zapper_direct "C:\Data\exports" my-container

# Upload directory under a virtual folder prefix in Azure
python -m zapper_direct /data/exports raw-data --blob-prefix 2024/q1

# Preview what would be uploaded without touching Azure
python -m zapper_direct /data/exports --dry-run
```

A directory upload preserves the full subdirectory structure as the blob path:

```
Local:  /data/exports/invoices/jan.csv
Azure:  invoices/jan.csv          (or 2024/q1/invoices/jan.csv with --blob-prefix 2024/q1)
```

### Resuming an interrupted upload

Simply re-run the exact same command. Each file has its own progress log in `./logs/`. Already-staged chunks are skipped, and already-completed files are skipped entirely.

```bash
# First run — interrupted partway through
python -m zapper_direct /data/exports my-container

# Second run — picks up exactly where it left off, file by file
python -m zapper_direct /data/exports my-container
# [2/15] report.csv  →  report.csv
# [INFO] Resuming: 37/64 chunks already done.
```

If some files fail, only those files need to be re-run. Completed files are skipped automatically.

---

## How It Works

```
Input path (file or directory)
    │
    ├─ If directory: walk recursively → sorted list of files
    │
    └─ For each file (sequentially):
           │
           ├─ Split into N blocks (CHUNK_SIZE_MB each)
           │
           ├─ ThreadPoolExecutor (CONCURRENCY threads)
           │   ├── stage_block(block_0) ─┐
           │   ├── stage_block(block_1)  │ parallel, with retry
           │   ├── stage_block(block_2)  │
           │   └── ...                  ─┘
           │
           │   Progress saved atomically after each block
           │   (skip file entirely on re-run if already completed)
           │
           └─ commit_block_list()  →  Blob finalised in Azure

Final summary: N/N files uploaded  (failed files listed for re-run)
```

Azure stores each staged block until `commit_block_list` is called. Blocks persist on Azure between runs, so resuming an interrupted upload does **not** re-upload already-staged blocks.

---

## Progress Logs

A JSON progress file is written to `./logs/<blob_name>.upload.json` (or `LOG_PATH` if configured). It is deleted automatically on successful completion.

Example:
```json
{
  "schema_version": 2,
  "file_path": "/data/export.parquet",
  "file_size": 107374182400,
  "container": "raw-data",
  "blob_name": "2024/q1/export.parquet",
  "chunk_size": 67108864,
  "total_chunks": 1600,
  "uploaded_chunks": [0, 1, 2, ...],
  "started_at": "2024-02-01T09:00:00+00:00",
  "updated_at": "2024-02-01T09:47:23+00:00"
}
```

If a progress log is corrupted or mismatched (different file size or container), the tool detects this and starts fresh automatically.

---

## File Structure

```
zapper_direct/
├── __init__.py
├── __main__.py        # enables: python -m zapper_direct
└── uploader.py        # all upload logic

.env.template          # copy to .env and fill in your credentials
requirements.txt       # pip dependencies
pyproject.toml         # package metadata (for pip install .)
README.md
```

---

## Troubleshooting

**"AZURE_CONN_STR not set"**
Copy `.env.template` to `.env` and fill in your Azure connection string.

**Upload stuck / slow**
Lower `CONCURRENCY` or `CHUNK_SIZE_MB`. Check your Azure Storage account's ingress limits (Standard = up to 60 Gbps, Premium = higher).

**"Commit failed" after all chunks uploaded**
The block list is still staged on Azure. Re-run the same command — the tool will skip re-uploading and retry the commit immediately.

**Want to restart from scratch**
Delete the progress log file in `./logs/` and re-run.

**Very large files (> 4.77 TB in a single blob)**
Azure limits a single block blob to ~4.77 TB (50,000 blocks × 4000 MB each). For files larger than this, split the file first or use `--blob-name` with a naming convention and upload in parts.
