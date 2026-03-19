# uda-linter

A command-line tool for linting UDA (Unified Data Access) datasets across multiple shots and signals. Results are saved as a pivoted table in Parquet or CSV format.

## Installation

Requires Python >= 3.12. Install dependencies using [uv](https://github.com/astral-sh/uv):

```bash
uv sync
```

## Usage

```bash
python -m uda_linter.main --shots <shot1> [shot2 ...] --signals <signal1> [signal2 ...] [--output-file <path>]
```

### Arguments

| Argument | Short | Description | Default |
|---|---|---|---|
| `--shots` | | One or more shot numbers to lint | required |
| `--signals` | | One or more signal names to lint | required |
| `--output-file` | `-o` | Output file path (`.parquet` or `.csv`) | `linting_results.parquet` |

### Examples

Lint a single shot and signal:

```bash
python -m uda_linter.main --shots 12345 --signals /AMC/PLASMA_CURRENT
```

Lint multiple shots and signals, saving to CSV:

```bash
python -m uda_linter.main --shots 12345 12346 12347 --signals /AMC/PLASMA_CURRENT /EFM/EFM_BETAN -o results.csv
```

Save output as Parquet (default):

```bash
python -m uda_linter.main --shots 12345 --signals /AMC/PLASMA_CURRENT -o linting_results.parquet
```

## Output

The tool fetches each `(shot, signal)` combination via a `uda://<signal>:<shot>` URI, runs the xinter linter on it, and writes the aggregated results to the specified output file.

The output is a pivoted table indexed by `file_path`, `group`, `variable_name`, and `target_type`, with one column per checker.

## Dependencies

- [`uda-xarray`](https://github.com/samueljackson92/uda-xarray) — UDA dataset access via xarray
- [`xinter`](https://github.com/samueljackson92/xinter) — dataset linting engine
