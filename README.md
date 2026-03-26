# Parquet Schema Transformer

A desktop GUI tool for modifying the column schemas of Parquet files stored in Azure Blob Storage — without rewriting your data pipeline. Connect to a container, inspect the schema of any folder, select a transform per column, and apply the changes to every file in the folder in one click.

Built to solve a specific Databricks Autoloader compatibility problem: Parquet files written with `fixed_size_binary[16]` IDs and `timestamp[ns]` timestamps cannot be read natively by Spark. This tool converts them to `string` (UUID format) and `timestamp[ms]` respectively, while keeping every other column untouched.

---

## Features

- **Visual schema editor** — loads the Parquet schema and displays every column with its current Arrow type
- **Per-column transform dropdowns** — choose a transformation independently for each column; leave others unchanged
- **Auto-suggest** — automatically pre-selects the right transform based on the detected column type
- **Dry run mode** — simulates the full transformation and logs what would change, without uploading anything
- **Folder-level batch processing** — applies the same transform to every `.parquet` file in the specified folder prefix
- **In-place or new prefix** — overwrite source files or write to a different prefix
- **Cancellable** — stop processing after the current file finishes
- **Extensible** — add a new transform by writing one decorated Python function; it appears in all dropdowns automatically

---

## Prerequisites

- Python 3.10 or later
- An Azure Storage account with a connection string
- The Parquet files must all share the same schema within the target folder (standard for partitioned datasets)

---

## Installation

```bash
git clone <repo-url>
cd change_parquet_schema
pip install -r requirements.txt
```

**Dependencies** (`requirements.txt`):

| Package | Purpose |
|---|---|
| `pyarrow >= 14.0` | Reading, transforming, and writing Parquet files |
| `azure-storage-blob >= 12.19` | Connecting to Azure Blob Storage |
| `PyQt6 >= 6.5` | Desktop GUI |

---

## Running the Tool

```bash
python main.py
```

---

## How to Use

### Step 1 — Enter connection details

Fill in the **Azure Connection** section at the top:

- **Connection String** — your Azure Storage connection string (input is masked by default; click the 👁 button to reveal it)
- **Container** — the blob container name (e.g. `my-container`)
- **Folder Prefix** — the folder path inside the container (e.g. `raw/events/`). All `.parquet` files directly or recursively under this prefix will be processed.

Click **Load Schema**.

### Step 2 — Review the schema

The **Schema** section shows every column in the Parquet files:

| Column | Current Type | Transform |
|---|---|---|
| Id | fixed_size_binary[16] | → String (UUID-Format) ▼ |
| TsCreate | timestamp[ns] | → timestamp[ms] (Spark) ▼ |
| Name | string | — no change — ▼ |

The number of files found in the folder is shown in the section header.

Transforms are **auto-suggested** based on the detected type. You can override any dropdown or leave columns as `— no change —` to skip them.

### Step 3 — Choose the output destination

Under **Output**, select one of:

- **In-place** — overwrite the source files (the original data is replaced)
- **New prefix** — write transformed files to a different prefix, e.g. `transformed/events/`. The relative path beneath the prefix is preserved.

### Step 4 — Dry run (recommended first)

Click **Dry Run** to simulate the transformation. The log shows which files would be processed and which transforms would be applied — no files are uploaded.

### Step 5 — Apply

Click **Apply to All Files**. Progress is shown in the status bar (`2 / 3 files`). Each file is logged individually. If a file fails, the error is shown in red and processing continues with the next file.

Click **Cancel** to stop after the current file finishes.

---

## Built-in Transforms

| Transform | Source Type | Target Type | Parquet Annotation | Spark Type | When to use |
|---|---|---|---|---|---|
| `→ String (UUID-Format)` | `fixed_size_binary[16]` | `string` | `BYTE_ARRAY (UTF8)` | `StringType` | Binary UUIDs that Spark should read as strings |
| `→ timestamp[ms] (Spark)` | `timestamp[ns]` | `timestamp[ms]` | `TIMESTAMP(isAdjustedToUTC=False, MILLIS)` | Local timestamp | Timestamps with no timezone — Spark reads as local time |
| `→ timestamp[ms, UTC] (Spark)` | `timestamp[ns]` | `timestamp[ms, UTC]` | `TIMESTAMP(isAdjustedToUTC=True, MILLIS)` | `TimestampType` | Timestamps that represent UTC instants |

Sub-millisecond precision (nanoseconds and microseconds) is **truncated**, not rounded, when converting to `ms`.

---

## Adding Custom Transforms

To add a new column transform, open `parquet_transform/transforms.py` and write a decorated function:

```python
@register(
    "my_transform",            # internal registry key
    "→ My Target Type",        # label shown in the UI dropdown
    applicable_types=["int32"] # Arrow type strings for auto-suggest (or None to always show)
)
def my_transform(array: pa.Array, params: dict) -> pa.Array:
    """Convert int32 to string representation."""
    return array.cast(pa.string())
```

The function receives:
- `array` — a `pyarrow.Array` (the full column from the Parquet table)
- `params` — a `dict` for future parameterisation (currently always empty)

It must return a `pyarrow.Array`. The new transform will immediately appear in all column dropdowns the next time the app is started.

---

## Project Structure

```
change_parquet_schema/
├── parquet_transform/
│   ├── transforms.py   # Transform registry + all built-in transforms
│   ├── storage.py      # Azure Blob Storage client (list / download / upload)
│   └── processor.py    # Core logic: apply transforms to a PyArrow Table
├── gui/
│   ├── main_window.py  # Main application window
│   ├── schema_table.py # Schema display widget with per-column ComboBoxes
│   └── workers.py      # Background QThread workers (schema loading, batch processing)
├── main.py             # Entry point
└── requirements.txt
```

---

## Troubleshooting

**"No .parquet files found under prefix '...'"**
Check the folder prefix. Azure Blob uses the prefix as a string match — make sure it matches the actual path exactly, including trailing slashes (e.g. `raw/events/`).

**"Failed to load schema" — ResourceNotFoundError**
The container name or connection string is incorrect. Verify both in the Azure Portal under your storage account → Access keys.

**ArrowInvalid: Casting from timestamp[ns] to timestamp[ms] would lose data**
This error no longer occurs — `safe=False` is used internally to allow precision truncation. If you see it in a custom transform, add `safe=False` to your `.cast()` call.

**Transformed files look correct in PyArrow but Spark still fails**
Check the Parquet annotation with `pyarrow.parquet.read_schema(path).metadata`. If Databricks Autoloader still rejects the file, try the UTC variant of the timestamp transform (`→ timestamp[ms, UTC]`) and confirm the Spark schema matches the Parquet annotation.

---

## Security

**Never commit your Azure connection string to Git.** The connection string field in the UI is masked by default. To avoid accidental exposure, consider reading it from an environment variable and pre-filling the field programmatically rather than typing it directly.

---

## License

MIT — see [LICENSE](LICENSE).
