"""Unit tests for shot_linter.main."""

from unittest.mock import patch, call
import pytest
import numpy as np
import xarray as xr

from xinter.core import Report, lint_dataset, reports_to_dataframe
from xinter.linters import LinterResult

from shot_linter.main import make_uri, process_signal, gather_results


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def simple_dataset():
    """In-memory xr.Dataset with one numeric variable; .load() is a no-op."""
    time = np.linspace(0, 1, 50)
    data = np.sin(2 * np.pi * time)
    return xr.Dataset(
        {"signal": xr.DataArray(data, dims=["time"], attrs={"units": "A"})},
        coords={"time": xr.DataArray(time, dims=["time"], attrs={"units": "s"})},
    )


def _make_report(file_path="uda:///AMC/PLASMA_CURRENT:12345"):
    """Minimal list[Report] matching what lint_dataset returns."""
    return [
        Report(
            file_path=file_path,
            group="N/A",
            type="data_vars",
            results={
                "nan_percent": {
                    "signal": LinterResult(value=0.0, message="0.00% NaNs found.", success=True)
                }
            },
        )
    ]


# ── make_uri ──────────────────────────────────────────────────────────────────


class TestMakeUri:
    def test_uda_format(self):
        assert make_uri(12345, "/AMC/PLASMA_CURRENT", "uda") == "uda:///AMC/PLASMA_CURRENT:12345"

    def test_sal_format(self):
        assert make_uri(12345, "/AMC/PLASMA_CURRENT", "sal") == "sal://pulse/12345//AMC/PLASMA_CURRENT"

    def test_sal_uri_matches_backend_regex(self):
        """Confirm the SAL URI satisfies the regex used inside sal_xarray."""
        import re
        uri = make_uri(99999, "AMC/PLASMA_CURRENT", "sal")
        assert re.search(r"sal://pulse/(\d+)/", uri), (
            f"SAL URI {uri!r} does not match sal_xarray's expected pattern"
        )

    def test_unknown_transport_raises(self):
        with pytest.raises(ValueError, match="Unknown transport"):
            make_uri(1, "/foo", "ftp")

    def test_uda_shot_embedded(self):
        uri = make_uri(42, "/EFM/BETAN", "uda")
        assert "42" in uri
        assert uri.startswith("uda://")

    def test_sal_shot_in_path(self):
        uri = make_uri(42, "EFM/BETAN", "sal")
        assert "42" in uri
        assert uri.startswith("sal://pulse/42/")


# ── process_signal ────────────────────────────────────────────────────────────


class TestProcessSignalUDA:
    def test_returns_list_on_success(self, simple_dataset):
        with patch("xarray.open_dataset", return_value=simple_dataset):
            result = process_signal(12345, "/AMC/PLASMA_CURRENT", "uda")

        assert isinstance(result, list)
        assert len(result) > 0

    def test_calls_open_dataset_with_uda_uri(self, simple_dataset):
        with patch("xarray.open_dataset", return_value=simple_dataset) as mock_open:
            process_signal(12345, "/AMC/PLASMA_CURRENT", "uda")

        args, kwargs = mock_open.call_args
        assert args[0] == "uda:///AMC/PLASMA_CURRENT:12345"
        assert kwargs.get("engine") == "uda"

    def test_check_coords_false_returns_one_report(self, simple_dataset):
        with patch("xarray.open_dataset", return_value=simple_dataset):
            result = process_signal(1, "/X", "uda", check_coords=False)
        assert len(result) == 1

    def test_check_coords_true_returns_extra_report(self, simple_dataset):
        with patch("xarray.open_dataset", return_value=simple_dataset):
            result_default = process_signal(1, "/X", "uda", check_coords=False)
            result_coords = process_signal(1, "/X", "uda", check_coords=True)
        # data_vars only vs data_vars + coords
        assert len(result_coords) > len(result_default)

    def test_file_path_reflects_uri(self, simple_dataset):
        with patch("xarray.open_dataset", return_value=simple_dataset):
            result = process_signal(12345, "/AMC/PLASMA_CURRENT", "uda")
        assert result[0].file_path == "uda:///AMC/PLASMA_CURRENT:12345"


class TestProcessSignalSAL:
    def test_returns_list_on_success(self, simple_dataset):
        with patch("xarray.open_dataset", return_value=simple_dataset):
            result = process_signal(12345, "AMC/PLASMA_CURRENT", "sal")

        assert isinstance(result, list)

    def test_calls_open_dataset_with_sal_uri(self, simple_dataset):
        with patch("xarray.open_dataset", return_value=simple_dataset) as mock_open:
            process_signal(12345, "AMC/PLASMA_CURRENT", "sal")

        args, kwargs = mock_open.call_args
        assert args[0] == "sal://pulse/12345/AMC/PLASMA_CURRENT"
        assert kwargs.get("engine") == "sal"


class TestProcessSignalErrors:
    def test_runtime_error_returns_tuple(self):
        with patch("xarray.open_dataset", side_effect=RuntimeError("network down")):
            result = process_signal(99, "/BAD/SIGNAL", "uda")

        assert result == (99, "/BAD/SIGNAL")

    def test_generic_exception_returns_tuple(self):
        with patch("xarray.open_dataset", side_effect=Exception("boom")):
            result = process_signal(42, "/SIG", "sal")

        assert result == (42, "/SIG")

    def test_tuple_values_match_inputs(self):
        with patch("xarray.open_dataset", side_effect=IOError("timeout")):
            shot, signal = process_signal(7777, "/SIGNAL/PATH", "uda")

        assert shot == 7777
        assert signal == "/SIGNAL/PATH"


# ── gather_results ────────────────────────────────────────────────────────────


class TestGatherResults:
    def test_successful_result_kept(self):
        reports = _make_report()
        assert gather_results([reports]) == [reports]

    def test_error_tuple_excluded(self):
        assert gather_results([(12345, "/BAD")]) == []

    def test_mixed_results_filters_errors(self):
        good = _make_report()
        bad = (1, "/X")
        output = gather_results([good, bad, good])
        assert len(output) == 2
        assert bad not in output

    def test_all_errors_returns_empty(self):
        errors = [(1, "/A"), (2, "/B"), (3, "/C")]
        assert gather_results(errors) == []

    def test_all_success_returns_all(self):
        reports = [_make_report(f"uda:///SIG:{i}") for i in range(5)]
        output = gather_results(reports)
        assert len(output) == 5


# ── DataFrame pivot (integration, no network) ─────────────────────────────────


class TestDataFramePivot:
    def test_pivot_produces_checker_columns(self, simple_dataset):
        with patch("xarray.open_dataset", return_value=simple_dataset):
            reports = lint_dataset("uda:///AMC/PLASMA_CURRENT:12345", engine="uda")

        df = reports_to_dataframe([reports])
        pivoted = df.pivot(
            index=["file_path", "group", "variable_name", "target_type"],
            columns="checker_name",
            values="value",
        )

        assert "nan_percent" in pivoted.columns
        assert "mean" in pivoted.columns
        assert pivoted.index.names == ["file_path", "group", "variable_name", "target_type"]

    def test_pivot_rows_match_variables(self, simple_dataset):
        with patch("xarray.open_dataset", return_value=simple_dataset):
            reports = lint_dataset("uda:///X:1", engine="uda")

        df = reports_to_dataframe([reports])
        pivoted = df.pivot(
            index=["file_path", "group", "variable_name", "target_type"],
            columns="checker_name",
            values="value",
        )

        # simple_dataset has one data variable: "signal"
        assert "signal" in pivoted.index.get_level_values("variable_name")

    def test_process_signal_then_pivot(self, simple_dataset):
        with patch("xarray.open_dataset", return_value=simple_dataset):
            result = process_signal(12345, "/AMC/PLASMA_CURRENT", "uda")

        df = reports_to_dataframe([result])
        pivoted = df.pivot(
            index=["file_path", "group", "variable_name", "target_type"],
            columns="checker_name",
            values="value",
        )

        assert not pivoted.empty
        assert "nan_percent" in pivoted.columns
