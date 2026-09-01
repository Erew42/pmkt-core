from __future__ import annotations

from typer.testing import CliRunner

from pmkt.cli.app import app


PUBLIC_COMMANDS = {
    "backfill-venue-history",
    "build-groups",
    "collect-books",
    "collect-kalshi-books",
    "compute-features",
    "dataset",
    "discover-structures",
    "ingest-kalshi-markets",
    "ingest-markets",
    "ingest-markets-keyset",
    "markets",
    "query",
    "reconstruct-book-tape",
    "record-topbooks",
    "recover-stream-run",
    "resolve-market-resolutions",
    "schema",
    "stream-books",
    "stream-kalshi-books",
}

PRIVATE_COMMANDS = {
    "alerts",
    "canary-submit",
    "deployment",
    "drift-probe",
    "kill-switch",
    "ledger",
    "live-ladder",
    "match",
    "match-markets",
    "polymarket-auth",
    "runtime-backup",
    "soak",
}


def test_public_cli_registers_only_core_command_families() -> None:
    registered = {command.name for command in app.registered_commands}
    registered.update(group.name for group in app.registered_groups)

    assert PUBLIC_COMMANDS <= registered
    assert PRIVATE_COMMANDS.isdisjoint(registered)


def test_public_cli_help_is_available() -> None:
    result = CliRunner().invoke(app, ["--help"])

    assert result.exit_code == 0, result.output
    for command in PUBLIC_COMMANDS:
        assert command in result.output
