import sys
import argparse
import multiprocessing as mp
from functools import partial
import pandas as pd
from loguru import logger
from xinter.core import lint_dataset, reports_to_dataframe


def process_signal(shot: int, signal: str, transport: str):
    uri = f"{transport}://{signal}:{shot}"
    try:
        report = lint_dataset(uri, engine=transport)
    except Exception:
        report = (shot, signal)
    return report


def gather_results(results):
    output = []
    for result in results:
        if isinstance(result, list):
            logger.info(f"Successfully linted {result[0].file_path}")
            output.append(result)
        else:
            logger.error(f"Error linting shot {result[0]} with signal {result[1]}")
    return output


def _process_signal(args, transport: str):
    return process_signal(*args, transport=transport)


def main():
    parser = argparse.ArgumentParser(description="A linting tool for shot data.")
    parser.add_argument("--shots", type=int, nargs="+", help="Shots to process")
    parser.add_argument(
        "--shot-file", type=str, help="File containing list of shots to process"
    )
    parser.add_argument("--shot-min", type=int, help="Minimum shot number")
    parser.add_argument("--shot-max", type=int, help="Maximum shot number")
    parser.add_argument(
        "--signals", type=str, nargs="+", help="Signals to process", required=True
    )
    parser.add_argument(
        "--transport",
        type=str,
        choices=["uda", "sal"],
        default="uda",
        help="Data transport method",
    )
    parser.add_argument(
        "-o",
        "--output-file",
        type=str,
        default="linting_results.csv",
        help="Output file path",
    )
    parser.add_argument(
        "-n",
        "--num-workers",
        type=int,
        default=mp.cpu_count(),
        help="Number of worker processes to use",
    )
    args = parser.parse_args()

    if args.shots:
        shots = args.shots
    elif args.shot_file:
        shot_file = args.shot_file
        if shot_file.endswith(".csv"):
            shots = pd.read_csv(shot_file, header=None).iloc[:, 0].astype(int).tolist()
        elif shot_file.endswith(".parquet"):
            shots = pd.read_parquet(shot_file).iloc[:, 0].map(int).tolist()
        else:
            logger.error("Error: Shot file must be in CSV or Parquet format")
            sys.exit(1)
    elif args.shot_min is not None and args.shot_max is not None:
        shots = list(range(args.shot_min, args.shot_max + 1))
    else:
        logger.error(
            "Error: You must specify either --shots, --shot-file, or both --shot-min and --shot-max"
        )
        sys.exit(1)

    signals = args.signals
    reports = []

    with mp.Pool(args.num_workers, maxtasksperchild=1) as pool:
        results = pool.imap_unordered(
            partial(_process_signal, transport=args.transport),
            ((shot, signal) for shot in shots for signal in signals),
        )
        reports = gather_results(results)

    df = reports_to_dataframe(reports)
    df = df.pivot(
        index=["file_path", "group", "variable_name", "target_type"],
        columns="checker_name",
        values="value",
    )

    if args.output_file.endswith(".parquet"):
        df.to_parquet(args.output_file, index=True)
    elif args.output_file.endswith(".csv"):
        df.to_csv(args.output_file, index=True)

    logger.info(f"Linting results saved to {args.output_file}")


if __name__ == "__main__":
    mp.set_start_method("fork")
    main()
