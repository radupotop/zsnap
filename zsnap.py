#!/usr/bin/env python3

import argparse
import logging
import subprocess
from datetime import date, timedelta
from typing import TypeAlias

# ZFS commands
ZFS_GET_DATASET = ("zfs", "list", "-H", "-t", "filesystem", "-o", "name")
ZFS_LS_SNAP = ("zfs", "list", "-H", "-t", "snapshot", "-o", "name", "-s", "creation")
ZFS_TAKE_SNAP = ("zfs", "snapshot")
ZFS_DESTROY = ("zfs", "destroy", "-n")

# Types
T_SNAP: TypeAlias = tuple[date, str]

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
        log.warning('Could not parse snapshot - skipping: %s', snap)


def has_dataset(dataset: str, dry_run=False) -> bool:
    try:
        run_cmd(
            ZFS_GET_DATASET,
            dataset.strip(),
            dry_run=dry_run,
        )
    except (FileNotFoundError, subprocess.SubprocessError):
        return False
    return True


def get_all_snaps(dataset: str, dry_run=False) -> list[T_SNAP]:
    try:
        snapshot_names: list[str] = run_cmd(
            ZFS_LS_SNAP,
            dataset.strip(),
            dry_run=dry_run,
        ).stdout.splitlines()
    except (FileNotFoundError, subprocess.SubprocessError) as err:
        log.error("Could not list snapshots for dataset: %s", dataset)
        raise SystemExit(err)

    parsed = sorted(
        filter(None, (parse_snap_name(snap) for snap in snapshot_names)),
    )
    log.info("All snapshots for dataset %s: %s", dataset, parsed)
    return parsed


def filter_older_snaps(snap_list: list[T_SNAP], cutoff_date: date) -> list[T_SNAP]:
    return list(filter(lambda d: d[0] < cutoff_date, snap_list))


def remove_snaps(snap_list: list[T_SNAP], dry_run=False):
    log.warning("Removing snapshots: %s", snap_list)
    for snap in snap_list:
        try:
            run_cmd(
                ZFS_DESTROY,
                snap[1],
                dry_run=dry_run,
            )
        except (FileNotFoundError, subprocess.SubprocessError) as err:
            log.error("Could not destroy snapshot: %s", snap[1])
            raise SystemExit(err)


def create_snap(dataset: str, dry_run=False) -> T_SNAP:
    new_snap_name = dataset.strip() + "@" + today.isoformat()
    log.info("Taking snapshot: %s", new_snap_name)
    try:
        run_cmd(
            ZFS_TAKE_SNAP,
            new_snap_name,
            dry_run=dry_run,
        )
    except (FileNotFoundError, subprocess.SubprocessError) as err:
        log.error("Could not create snapshot: %s", new_snap_name)
        raise SystemExit(err)
    return (today, new_snap_name)


def run_cmd(zfscmd: tuple, dataset: str, dry_run: bool) -> subprocess.CompletedProcess:
    log.info("Running cmd %s %s", " ".join(zfscmd), dataset)
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


def main():
    if not has_zfs():
        log.error("ZFS module not loaded")
        raise SystemExit(1)

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
        raise SystemExit(1)

    for ds in datasets:
        if not has_dataset(ds):
            log.error("Dataset not found: %s", ds)
            raise SystemExit(1)

    dry_run = bool(args.dry_run)
    cutoff_date = get_cutoff_date(int(args.retention_days))

    log.info("Fresh snapshot suffix: %s", today)

    # Create a fresh snapshot only after selecting old snapshots for deletion,
    # but before actually deleting them.
    for dset in datasets:
        snap_list = get_all_snaps(dset, dry_run)
        old_snaps = filter_older_snaps(snap_list, cutoff_date)
        create_snap(dset, dry_run)
        if old_snaps:
            log.warning(
                "Deleting snapshots older than: %s (%s days ago)",
                cutoff_date,
                args.retention_days,
            )
            remove_snaps(old_snaps, dry_run)
        else:
            log.info("No snapshots to remove")


if __name__ == "__main__":
    main()
