from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pytest
import zsnap


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
    run_cmd_mock = MagicMock(side_effect=[object(), object()])
    monkeypatch.setattr(zsnap, "run_cmd", run_cmd_mock)

    result = zsnap.remove_snaps(snaps, dry_run=True)

    assert result is None
    assert run_cmd_mock.call_args_list == [
        call(zsnap.ZFS_DESTROY, "tank/data@2026-01-01", dry_run=True),
        call(zsnap.ZFS_DESTROY, "tank/data@2026-01-02", dry_run=True),
    ]


def test_snapshot_workflow_loop_integration(monkeypatch):
    cutoff_date = date(2026, 2, 10)
    fixed_today = date(2026, 2, 16)
    datasets = ("tank/alpha", "tank/beta")
    dry_run = True

    def run_cmd_side_effect(zfscmd, dataset, dry_run):
        if zfscmd == zsnap.ZFS_TAKE_SNAP:
            return SimpleNamespace(stdout="")
        if zfscmd == zsnap.ZFS_LS_SNAP:
            if dataset == "tank/alpha":
                return SimpleNamespace(
                    stdout="\n".join(
                        [
                            "tank/alpha@2026-01-01",
                            "tank/alpha@2026-01-20",
                            "tank/alpha@2026-02-12",
                            "tank/alpha@2026-02-16",
                        ]
                    )
                )
            if dataset == "tank/beta":
                return SimpleNamespace(
                    stdout="\n".join(
                        [
                            "tank/beta@2026-01-31",
                            "tank/beta@2026-02-12",
                            "tank/beta@2026-02-16",
                        ]
                    )
                )
        if zfscmd == zsnap.ZFS_DESTROY:
            return SimpleNamespace(stdout="")
        raise AssertionError(f"Unexpected command: {zfscmd} {dataset}")

    run_cmd_mock = MagicMock(side_effect=run_cmd_side_effect)
    info_mock = MagicMock()
    monkeypatch.setattr(zsnap, "today", fixed_today)
    monkeypatch.setattr(zsnap, "run_cmd", run_cmd_mock)
    monkeypatch.setattr(zsnap.log, "info", info_mock)

    for dset in datasets:
        zsnap.create_snap(dset, dry_run)
        snap_list = zsnap.get_all_snaps(dset, dry_run)
        old_snaps = zsnap.filter_older_snaps(snap_list, cutoff_date)
        if old_snaps:
            zsnap.remove_snaps(old_snaps, dry_run)
        else:
            zsnap.log.info("No snapshots to remove")

    assert run_cmd_mock.call_args_list == [
        call(zsnap.ZFS_TAKE_SNAP, "tank/alpha@2026-02-16", dry_run=True),
        call(zsnap.ZFS_LS_SNAP, "tank/alpha", dry_run=True),
        call(zsnap.ZFS_DESTROY, "tank/alpha@2026-01-01", dry_run=True),
        call(zsnap.ZFS_DESTROY, "tank/alpha@2026-01-20", dry_run=True),
        call(zsnap.ZFS_TAKE_SNAP, "tank/beta@2026-02-16", dry_run=True),
        call(zsnap.ZFS_LS_SNAP, "tank/beta", dry_run=True),
        call(zsnap.ZFS_DESTROY, "tank/beta@2026-01-31", dry_run=True),
    ]
    assert call("No snapshots to remove") not in info_mock.call_args_list


def test_has_zfs_returns_false_when_binary_missing(monkeypatch):
    run_mock = MagicMock(side_effect=FileNotFoundError)
    monkeypatch.setattr(zsnap.subprocess, "run", run_mock)

    result = zsnap.has_zfs()

    assert result is False
    run_mock.assert_called_once_with(("zfs", "version"), capture_output=True, check=True)


def test_has_dataset_returns_true_and_calls_run_cmd(monkeypatch):
    run_cmd_mock = MagicMock(return_value=SimpleNamespace(stdout=""))
    monkeypatch.setattr(zsnap, "run_cmd", run_cmd_mock)

    dataset = "tank/data"
    result = zsnap.has_dataset(dataset)

    assert result is True
    run_cmd_mock.assert_called_once_with(
        zsnap.ZFS_GET_DATASET,
        "tank/data",
        dry_run=False,
    )


def test_main_rejects_empty_dataset_entries(monkeypatch):
    args = SimpleNamespace(
        datasets=["tank/alpha", ""],
        retention_days=182,
        dry_run=True,
    )
    parse_args_mock = MagicMock(return_value=args)
    create_snap_mock = MagicMock()
    error_mock = MagicMock()

    monkeypatch.setattr(zsnap, "has_zfs", lambda: True)
    monkeypatch.setattr(zsnap.argparse.ArgumentParser, "parse_args", parse_args_mock)
    monkeypatch.setattr(zsnap, "create_snap", create_snap_mock)
    monkeypatch.setattr(zsnap.log, "error", error_mock)

    with pytest.raises(SystemExit) as exc:
        zsnap.main()

    assert exc.value.code == 1
    create_snap_mock.assert_not_called()
    error_mock.assert_called_once_with("Empty dataset name found")


def test_main_accepts_datasets_provided_multiple_times(monkeypatch):
    args = SimpleNamespace(
        datasets=["tank/alpha", "tank/beta"],
        retention_days=182,
        dry_run=True,
    )
    parse_args_mock = MagicMock(return_value=args)
    create_snap_mock = MagicMock()
    get_all_snaps_mock = MagicMock(side_effect=[[], []])
    filter_older_snaps_mock = MagicMock(side_effect=[[], []])
    remove_snaps_mock = MagicMock()
    has_dataset_mock = MagicMock(return_value=True)

    monkeypatch.setattr(zsnap, "has_zfs", lambda: True)
    monkeypatch.setattr(zsnap, "has_dataset", has_dataset_mock)
    monkeypatch.setattr(zsnap.argparse.ArgumentParser, "parse_args", parse_args_mock)
    monkeypatch.setattr(zsnap, "create_snap", create_snap_mock)
    monkeypatch.setattr(zsnap, "get_all_snaps", get_all_snaps_mock)
    monkeypatch.setattr(zsnap, "filter_older_snaps", filter_older_snaps_mock)
    monkeypatch.setattr(zsnap, "remove_snaps", remove_snaps_mock)

    result = zsnap.main()

    assert result is None
    assert has_dataset_mock.call_args_list == [
        call("tank/alpha"),
        call("tank/beta"),
    ]
    assert create_snap_mock.call_args_list == [
        call("tank/alpha", True),
        call("tank/beta", True),
    ]
    assert get_all_snaps_mock.call_args_list == [
        call("tank/alpha", True),
        call("tank/beta", True),
    ]
    remove_snaps_mock.assert_not_called()
