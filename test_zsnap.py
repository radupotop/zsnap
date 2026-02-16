from datetime import date
from importlib.machinery import SourceFileLoader
from importlib.util import module_from_spec, spec_from_loader
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, call


def _load_zsnap_module():
    script_path = Path(__file__).with_name("zsnap")
    loader = SourceFileLoader("zsnap_module", str(script_path))
    spec = spec_from_loader(loader.name, loader)
    module = module_from_spec(spec)
    loader.exec_module(module)
    return module


zsnap = _load_zsnap_module()


def test_create_snap_calls_run_cmd_and_returns_snapshot(monkeypatch):
    fixed_today = date(2026, 2, 16)
    run_cmd_mock = MagicMock(return_value=SimpleNamespace(stdout=""))
    monkeypatch.setattr(zsnap, "today", fixed_today)
    monkeypatch.setattr(zsnap, "run_cmd", run_cmd_mock)

    result = zsnap.create_snap("tank/data", dry_run=True)

    assert result == (fixed_today, "tank/data@2026-02-16")
    run_cmd_mock.assert_called_once_with(
        zsnap.ZFS_TAKE_SNAP,
        "tank/data@2026-02-16",
        dry_run=True,
    )


def test_get_all_snaps_parses_filters_and_sorts(monkeypatch):
    run_cmd_mock = MagicMock(
        return_value=SimpleNamespace(
            stdout="\n".join(
                [
                    "tank/data@2026-02-10",
                    "tank/data@not-a-date",
                    "tank/data@2026-02-01",
                ]
            )
        )
    )
    monkeypatch.setattr(zsnap, "run_cmd", run_cmd_mock)

    result = zsnap.get_all_snaps("tank/data", dry_run=False)

    assert result == [
        (date(2026, 2, 1), "tank/data@2026-02-01"),
        (date(2026, 2, 10), "tank/data@2026-02-10"),
    ]
    run_cmd_mock.assert_called_once_with(
        zsnap.ZFS_LS_SNAP,
        "tank/data",
        dry_run=False,
    )


def test_filter_older_snaps_returns_only_dates_before_cutoff():
    snaps = [
        (date(2026, 1, 1), "tank/data@2026-01-01"),
        (date(2026, 2, 10), "tank/data@2026-02-10"),
        (date(2026, 2, 20), "tank/data@2026-02-20"),
    ]
    cutoff = date(2026, 2, 10)

    result = zsnap.filter_older_snaps(snaps, cutoff)

    assert result == [(date(2026, 1, 1), "tank/data@2026-01-01")]


def test_remove_snaps_calls_run_cmd_for_each_snapshot(monkeypatch):
    snaps = [
        (date(2026, 1, 1), "tank/data@2026-01-01"),
        (date(2026, 1, 2), "tank/data@2026-01-02"),
    ]
    run_results = [object(), object()]
    run_cmd_mock = MagicMock(side_effect=run_results)
    monkeypatch.setattr(zsnap, "run_cmd", run_cmd_mock)

    result = zsnap.remove_snaps(snaps, dry_run=True)

    assert result == run_results
    assert run_cmd_mock.call_args_list == [
        call(zsnap.ZFS_DESTROY, "tank/data@2026-01-01", dry_run=True),
        call(zsnap.ZFS_DESTROY, "tank/data@2026-01-02", dry_run=True),
    ]
