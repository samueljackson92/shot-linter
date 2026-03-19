import argparse
from rich.console import Console
from xinter.core import lint_dataset, reports_to_dataframe
from joblib import Parallel, delayed


def process_signal(shot, signal):
    uri = f"uda://{signal}:{shot}"
    try:
        report = lint_dataset(uri, engine="uda")
    except Exception as e:
        print(f"Error processing {uri}: {e}")
        report = None
    return report


def gather_results(console, results):
    output = []
    for result in results:
        if result is not None:
            console.print(
                f"[green]Successfully linted {result['data_vars']['file_path']}[/green]"
            )
            output.append(result)
        else:
            console.print(f"[red]Error linting {result}[/red]")
    return output


def main():
    parser = argparse.ArgumentParser(description="A simple command-line tool.")
    parser.add_argument("--shots", type=int, nargs="+", help="Shots to process")
    parser.add_argument("--signals", type=str, nargs="+", help="Signals to process")
    parser.add_argument(
        "-o",
        "--output-file",
        type=str,
        default="linting_results.parquet",
        help="Output file path",
    )
    args = parser.parse_args()

    shots = args.shots
    signals = args.signals
    reports = []

    console = Console()

    jobs = (
        delayed(process_signal)(shot, signal) for shot in shots for signal in signals
    )
    reports = Parallel(n_jobs=-1, return_as="generator_unordered")(jobs)
    reports = gather_results(console, reports)

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

    console.print(f"Linting results saved to {args.output_file}")


if __name__ == "__main__":
    main()
