# shot-linter

A command-line tool for linting plasma-physics shot data from UDA and SAL backends across multiple shots and signals. Results are saved as a pivoted table in Parquet or CSV format, with one column per xinter checker.

## Installation

Requires Python >= 3.12. Install dependencies using [uv](https://github.com/astral-sh/uv):

```bash
uv sync
```

For development (includes pytest):

```bash
uv sync --group dev
```

## Usage

```bash
python -m shot_linter.main --signals <signal1> [signal2 ...] \
    (--shots <shot1> [shot2 ...] | --shot-file <path> | --shot-min N --shot-max M) \
    [--transport {uda,sal}] [--check-coords] [-o OUTPUT] [-n WORKERS]
```

### Arguments

| Argument | Short | Description | Default |
|---|---|---|---|
| `--shots` | | One or more shot numbers | — |
| `--shot-file` | | CSV or Parquet file with shot numbers in the first column | — |
| `--shot-min` | | Start of shot range (inclusive) | — |
| `--shot-max` | | End of shot range (inclusive) | — |
| `--signals` | | One or more signal names (required) | required |
| `--transport` | | Data transport backend: `uda` or `sal` | `uda` |
| `--check-coords` | | Also lint coordinate variables, not just data variables | `False` |
| `--output-file` | `-o` | Output path — `.csv` or `.parquet` | `linting_results.csv` |
| `--num-workers` | `-n` | Number of parallel worker processes | `cpu_count()` |

Exactly one of `--shots`, `--shot-file`, or `--shot-min`/`--shot-max` must be provided.

### Examples

Lint a single shot via UDA:

```bash
python -m shot_linter.main --shots 12345 --signals /AMC/PLASMA_CURRENT
```

Lint multiple shots and signals, saving to Parquet:

```bash
python -m shot_linter.main \
    --shots 12345 12346 12347 \
    --signals /AMC/PLASMA_CURRENT /EFM/EFM_BETAN \
    -o results.parquet
```

Lint a shot range with 8 parallel workers:

```bash
python -m shot_linter.main \
    --shot-min 12000 --shot-max 12100 \
    --signals /AMC/PLASMA_CURRENT \
    -n 8 -o results.parquet
```

Lint from a shot list file:

```bash
python -m shot_linter.main \
    --shot-file shots.csv \
    --signals /AMC/PLASMA_CURRENT \
    -o results.csv
```

Lint via SAL transport, including coordinate variables:

```bash
python -m shot_linter.main \
    --shots 12345 \
    --signals AMC/PLASMA_CURRENT \
    --transport sal \
    --check-coords \
    -o results.csv
```

### URI formats

Each `(shot, signal)` pair is translated into a backend URI before linting:

| Transport | URI format | Example |
|---|---|---|
| UDA | `uda://<signal>:<shot>` | `uda:///AMC/PLASMA_CURRENT:12345` |
| SAL | `sal://pulse/<shot>/<signal>` | `sal://pulse/12345/AMC/PLASMA_CURRENT` |

## Output

The tool runs all xinter checkers over every `(shot, signal)` combination in parallel and writes the aggregated results to the output file.

The output is a pivoted table indexed by `(file_path, group, variable_name, target_type)`, with one column per checker. Example checkers include `nan_percent`, `mean`, `std`, `min`, `max`, `units`, `data_type`, and more — see the [xinter documentation](https://github.com/samueljackson92/xinter) for the full list.

## Running tests

```bash
uv sync --group dev
uv run pytest -v
```

## Dependencies

- [`uda-xarray`](https://github.com/samueljackson92/uda-xarray) — UDA dataset access via xarray
- [`sal-xarray`](https://github.com/samueljackson92/sal-xarray) — SAL dataset access via xarray
- [`xinter`](https://github.com/samueljackson92/xinter) — dataset linting engine
