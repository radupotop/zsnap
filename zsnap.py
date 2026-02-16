#!/usr/bin/env python3

import argparse
import logging
import subprocess
from datetime import date, timedelta

# ZFS commands
ZFS_LS_SNAP = ("zfs", "list", "-H", "-t", "snapshot", "-o", "name", "-s", "creation")
ZFS_TAKE_SNAP = ("zfs", "snapshot")
ZFS_DESTROY = ("zfs", "destroy")

# Types
T_SNAP = tuple[date, str]

# Predefined vars
today = date.today()

# Logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("zsnap")


def get_cutoff_date(retention_days: int = 182) -> date:
    return today - timedelta(days=abs(retention_days))


def parse_snap_name(snap: str) -> T_SNAP | None:
    """
    Parse a single snapshot name
    """
    try:
        _, _date_iso = snap.split('@', 1)
        parsed_date = date.fromisoformat(_date_iso)
        return (parsed_date, snap)
    except ValueError:
        log.warning('Could not parse snapshot: %s', snap)


def get_all_snaps(dataset: str, dry_run=False) -> list[T_SNAP]:
    snapshot_names: list[str] = run_cmd(
        ZFS_LS_SNAP,
        dataset,
        dry_run=dry_run,
    ).stdout.splitlines()

    parsed = filter(None, (parse_snap_name(snap) for snap in snapshot_names))
    log.info("All snapshots for dataset %s: %s", dataset, parsed)
    return sorted(parsed)


def filter_older_snaps(snap_list: list[T_SNAP], cutoff_date: date) -> list[T_SNAP]:
    return list(filter(lambda d: d[0] < cutoff_date, snap_list))


def remove_snaps(snap_list: list[T_SNAP], dry_run=False):
    log.warning("Removing snapshots: %s", snap_list)
    return [
        run_cmd(
            ZFS_DESTROY,
            snap[1],
            dry_run=dry_run,
        )
        for snap in snap_list
    ]


def create_snap(dataset: str, dry_run=False) -> T_SNAP:
    new_snap_name = dataset + "@" + today.isoformat()
    log.info("Taking snapshot: %s", new_snap_name)
    run_cmd(
        ZFS_TAKE_SNAP,
        new_snap_name,
        dry_run=dry_run,
    )
    return (today, new_snap_name)


def run_cmd(zfscmd: tuple, dataset: str, dry_run: bool) -> subprocess.CompletedProcess:
    log.info("Running cmd %s %s", zfscmd, dataset)
    if dry_run:
        log.info("DRY RUN")
        return subprocess.CompletedProcess("", 0, stdout="")

    return subprocess.run(
        (*zfscmd, dataset),
        capture_output=True,
        check=True,
        text=True,
    )


def has_zfs() -> bool:
    try:
        subprocess.run(("zfs", "version"), capture_output=True, check=True)
    except (FileNotFoundError, subprocess.SubprocessError):
        return False
    return True


def main() -> int:
    if not has_zfs():
        log.error("ZFS module not loaded")
        return 1

    parser = argparse.ArgumentParser(
        description="Create ZFS snapshot and prune old snapshots"
    )
    parser.add_argument(
        "--datasets",
        "-ds",
        type=str,
        action="extend",
        nargs="+",
        help="Datasets to snapshot; accepts multiple values and can be repeated",
        required=True,
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=182,
    )
    parser.add_argument(
        "--dry-run",
        "-n",
        action="store_true",
        help="Print zfs snapshot/destroy commands without executing them.",
    )
    args = parser.parse_args()

    datasets = tuple(map(str.strip, args.datasets))
    log.info("Selected datasets: %s", datasets)

    if not all(datasets):
        log.error("Empty dataset name found")
        return 1

    dry_run = bool(args.dry_run)
    cutoff_date = get_cutoff_date(int(args.retention_days))

    log.info("Fresh snapshot suffix: %s", today)
    log.warning(
        "Deleting snapshots older than: %s (%s days ago)",
        cutoff_date,
        args.retention_days,
    )

    # Create a fresh snapshot only after selecting old snapshots for deletion,
    # but before actually deleting them.
    for dset in datasets:
        snap_list = get_all_snaps(dset, dry_run)
        old_snaps = filter_older_snaps(snap_list, cutoff_date)
        create_snap(dset, dry_run)
        if old_snaps:
            remove_snaps(old_snaps, dry_run)
        else:
            log.info("No snapshots to remove")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
