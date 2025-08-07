#!/usr/bin/env python3

import sys
import psycopg2
from db_config import db_params
from rich.console import Console
from rich.table import Table

LAMPORTS_PER_SOL = 1_000_000_000

def short_pubkey(pub):
    """First 7 + '...' + last 7 chars of pubkey, if it's longer than 14."""
    if len(pub) <= 14:
        return pub
    return pub[:7] + "..." + pub[-7:]

def main(epoch):
    # Connect to DB
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # 1) total epoch stake
    cur.execute(
        """
        SELECT COALESCE(SUM(activated_stake), 0)
        FROM validator_stats
        WHERE epoch = %s
        """,
        (epoch,)
    )
    total_epoch_stake_lamports = cur.fetchone()[0]
    total_epoch_stake_sol = total_epoch_stake_lamports / LAMPORTS_PER_SOL

    # 2) get validators for epoch, version starts with '0', descending stake
    cur.execute(
        """
        SELECT vs.identity_pubkey,
               vi.name,
               vs.version,
               vs.activated_stake
        FROM validator_stats vs
        JOIN validator_info vi
          ON vs.identity_pubkey = vi.identity_pubkey
        WHERE vs.epoch = %s
          AND vs.version LIKE '0%%'
        ORDER BY vs.activated_stake DESC
        """,
        (epoch,)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Print epoch
    print(f"\nEpoch: {epoch}\n")

    # Create a Rich table
    table = Table(
        show_header=True, 
        header_style="bold",
        # Optionally set a default style or box style
    )

    # Add columns (Rich can automatically adjust column widths, or we can specify them)
    # If you want *fixed* or *maximum* widths, you can do something like: width=60, no_wrap=True, etc.
    table.add_column("Name", justify="left", no_wrap=False)
    table.add_column("Version", justify="left", no_wrap=True)
    table.add_column("Pubkey", justify="left", no_wrap=True)
    table.add_column("Stake (SOL)", justify="right")
    table.add_column("% of Stake", justify="right")

    version_0_total_stake_lamports = 0

    for pubkey, raw_name, version, stake_lamports in rows:
        name = raw_name if raw_name else pubkey
        name = name.replace("🫨", "?")
        shortkey = short_pubkey(pubkey)
        stake_sol = stake_lamports / LAMPORTS_PER_SOL
        version_0_total_stake_lamports += stake_lamports

        if total_epoch_stake_lamports > 0:
            pct = (stake_lamports / total_epoch_stake_lamports) * 100
        else:
            pct = 0.0

        # Add a row to the Rich table
        table.add_row(
            name,
            version,
            shortkey,
            f"{stake_sol:,.2f}",
            f"{pct:,.2f}%",
        )

    # Print the table
    console = Console()
    console.print(table)

    # Summary
    print()
    print("Summary")
    print("-------")
    total_version_0_validators = len(rows)
    version_0_total_stake_sol = version_0_total_stake_lamports / LAMPORTS_PER_SOL
    if total_epoch_stake_lamports > 0:
        pct_version_0_stake = (version_0_total_stake_lamports / total_epoch_stake_lamports) * 100
    else:
        pct_version_0_stake = 0.0

    print(f"{'Total Version-0 Validators':<45} {total_version_0_validators}")
    print(f"{'Total Version-0 Stake (SOL)':<45} {version_0_total_stake_sol:,.2f}")
    print(f"{'Activated Stake Percentage for Version 0':<45} {pct_version_0_stake:,.2f}%\n")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <epoch>")
        sys.exit(1)

    main(sys.argv[1])
