"""Parser edge cases."""

from __future__ import annotations

import pytest

from netlog_anomaly.parser import (
    ParseError,
    escape_for_storage,
    mask_message,
    parse_line,
    template_id,
)

NORMAL = (
    "- 1117838570 2005.06.03 R02-M1-N0-C:J12-U11 2005-06-03-15.42.50.363779 "
    "R02-M1-N0-C:J12-U11 RAS KERNEL INFO instruction cache parity error corrected"
)
ALERT = (
    "KERNDTLB 1117838573 2005.06.03 R02-M1-N0-C:J12-U11 2005-06-03-15.42.53.323456 "
    "R02-M1-N0-C:J12-U11 RAS KERNEL FATAL data TLB error interrupt"
)


def test_parses_normal_line_fields() -> None:
    event = parse_line(NORMAL, line_no=1)
    assert event.label == "-"
    assert event.is_alert is False
    assert event.epoch == 1117838570
    assert event.node == "R02-M1-N0-C:J12-U11"
    assert event.event_type == "RAS"
    assert event.component == "KERNEL"
    assert event.severity == "INFO"
    assert event.message == "instruction cache parity error corrected"
    assert event.line_no == 1


def test_alert_label_sets_is_alert() -> None:
    event = parse_line(ALERT, line_no=2)
    assert event.label == "KERNDTLB"
    assert event.is_alert is True
    assert event.severity == "FATAL"


def test_trailing_newline_and_carriage_return_are_stripped() -> None:
    assert parse_line(NORMAL + "\r\n", line_no=1).message.endswith("corrected")


def test_message_may_be_empty() -> None:
    header = NORMAL.rsplit(" INFO ", maxsplit=1)[0] + " INFO"
    event = parse_line(header, line_no=1)
    assert event.message == ""
    assert event.template == ""


def test_runs_of_whitespace_in_message_are_collapsed_in_template() -> None:
    line = NORMAL.replace("instruction cache", "instruction    cache")
    assert parse_line(line, line_no=1).template == "instruction cache parity error corrected"


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        ("", "empty_line"),
        ("   \n", "empty_line"),
        ("- 1117838570 2005.06.03 node", "too_few_fields"),
        (NORMAL.replace("1117838570", "notanumber"), "bad_epoch"),
        (NORMAL.replace("1117838570", "12345"), "bad_epoch"),
        (NORMAL.replace("1117838570", "-5"), "bad_epoch"),
        (NORMAL.replace("2005.06.03", "06/03/2005", 1), "bad_date"),
        (NORMAL.replace("2005-06-03-15.42.50.363779", "15:42:50"), "bad_datetime"),
        (NORMAL.replace(" INFO ", " IN-FO "), "unknown_severity"),
        (NORMAL.replace("instruction", "instruct\ufffdion"), "undecodable_bytes"),
        (NORMAL.replace("instruction", "instruct\x00ion"), "nul_byte"),
    ],
)
def test_malformed_lines_raise_with_reason(raw: str, reason: str) -> None:
    with pytest.raises(ParseError) as excinfo:
        parse_line(raw, line_no=1)
    assert excinfo.value.reason == reason


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("generating core.4356", "generating core.<NUM>"),
        ("63 ddr errors detected", "<NUM> ddr errors detected"),
        ("address 0x1fffffff out of range", "address <HEX> out of range"),
        ("connection from 192.168.1.10 closed", "connection from <IP> closed"),
        ("node R02-M1-N0-C:J12-U11 is down", "node <NODE> is down"),
        ("read /p/gb1/stella/raptor/input", "read <PATH>"),
        ("ciod status 0000000000f00000 seen", "ciod status <HEX> seen"),
    ],
)
def test_masking_replaces_variable_parts(message: str, expected: str) -> None:
    assert mask_message(message) == expected


def test_same_message_shape_shares_template_id() -> None:
    first = parse_line(NORMAL.replace("corrected", "corrected 1"), line_no=1)
    second = parse_line(NORMAL.replace("corrected", "corrected 2"), line_no=2)
    assert first.template_id == second.template_id


def test_different_message_shape_differs() -> None:
    first = parse_line(NORMAL, line_no=1)
    second = parse_line(ALERT, line_no=2)
    assert first.template_id != second.template_id


def test_template_id_is_stable_and_short() -> None:
    assert template_id("instruction cache parity error corrected") == template_id(
        "instruction cache parity error corrected"
    )
    assert len(template_id("x")) == 16


def test_shifted_columns_are_quarantined_not_silently_accepted() -> None:
    # Real BGL lines where the repeated-node column is missing, so the severity
    # position lands on message text.
    shifted = (
        "- 1119415930 2005.06.21 - 2005-06-21-21.52.10.214285 RAS KERNEL FATAL "
        "Kill job 20251 timed out. Block freed."
    )
    with pytest.raises(ParseError) as excinfo:
        parse_line(shifted, line_no=1)
    assert excinfo.value.reason == "unknown_severity"


def test_bglmaster_failure_level_is_accepted() -> None:
    # FAILURE is part of the BGL vocabulary and these lines are well formed.
    line = (
        "MASNORM 1119992001 2005.06.28 NULL 2005-06-28-13.53.21.974236 NULL RAS "
        "BGLMASTER FAILURE mmcs_server exited normally with exit code 13"
    )
    event = parse_line(line, line_no=1)
    assert event.severity == "FAILURE"
    assert event.is_alert is True
    assert event.component == "BGLMASTER"


def test_escape_for_storage_makes_a_nul_line_storable() -> None:
    raw = "- 1132324504 2005.11.18 UNKNOWN_LOCATION ... com.ibm\x00bgl"
    escaped = escape_for_storage(raw)
    assert "\x00" not in escaped
    assert "\\x00" in escaped


def test_escape_for_storage_leaves_ordinary_lines_alone() -> None:
    assert escape_for_storage(NORMAL) == NORMAL
