"""Parse raw BGL log lines into structured rows.

The BGL log from LogHub is whitespace delimited with nine header fields
followed by a free-form message::

    - 1117838570 2005.06.03 R02-M1-N0-C:J12-U11 \
2005-06-03-15.42.50.363779 R02-M1-N0-C:J12-U11 RAS KERNEL INFO \
instruction cache parity error corrected

The first field is the alert label: ``-`` for a normal line, otherwise an
alert category such as ``KERNDTLB``. The full BGL log ships without event
template identifiers, so templates here are derived by masking the variable
parts of the message and hashing the result.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

NORMAL_LABEL = "-"
HEADER_FIELDS = 9

_DATE_RE = re.compile(r"^\d{4}\.\d{2}\.\d{2}$")
_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}-\d{2}\.\d{2}\.\d{2}\.\d{6}$")
# BGL uses a fixed severity vocabulary. Restricting to it is what catches the
# small number of lines whose columns have shifted -- a missing node-repeat
# field, or message text spilled into a header position -- which would otherwise
# parse into a structurally valid row carrying nonsense.
SEVERITIES = frozenset({"INFO", "WARNING", "ERROR", "SEVERE", "FATAL", "FAILURE"})

# Epoch bounds wide enough for any plausible machine log, narrow enough to
# reject line numbers, byte counts and other junk in the timestamp column.
MIN_EPOCH = 946_684_800  # 2000-01-01
MAX_EPOCH = 2_524_608_000  # 2050-01-01

# Masking rules, applied in order. Order matters: hex and node identifiers are
# masked before bare integers, otherwise the digits inside them are eaten first.
_MASK_RULES: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"0x[0-9a-fA-F]+"), "<HEX>"),
    (re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"), "<IP>"),
    (re.compile(r"\bR\d{2}-M\d-N\d+(?:-[A-Za-z]+)?(?::J\d+-U\d+)?\b"), "<NODE>"),
    (re.compile(r"\b[0-9a-fA-F]{8,}\b"), "<HEX>"),
    (re.compile(r"(?:/[\w.\-]+){2,}/?"), "<PATH>"),
    (re.compile(r"\b\d+(?:\.\d+)?\b"), "<NUM>"),
)
_WS_RE = re.compile(r"\s+")


@dataclass(frozen=True, slots=True)
class LogEvent:
    """One successfully parsed log line."""

    line_no: int
    label: str
    is_alert: bool
    epoch: int
    node: str
    event_type: str
    component: str
    severity: str
    message: str
    template: str
    template_id: str


@dataclass(frozen=True, slots=True)
class RejectedLine:
    """One line that failed validation, kept for quarantine."""

    line_no: int
    reason: str
    raw_line: str


class ParseError(ValueError):
    """Raised when a line cannot be parsed into a LogEvent."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def mask_message(message: str) -> str:
    """Replace the variable parts of a message with placeholder tokens."""
    masked = message
    for pattern, placeholder in _MASK_RULES:
        masked = pattern.sub(placeholder, masked)
    return _WS_RE.sub(" ", masked).strip()


def template_id(template: str) -> str:
    """Stable short identifier for a masked template."""
    return hashlib.sha1(template.encode("utf-8")).hexdigest()[:16]


def parse_line(raw_line: str, line_no: int) -> LogEvent:
    """Parse one raw line, raising ParseError when it is malformed."""
    line = raw_line.rstrip("\n").rstrip("\r")

    if "�" in line:
        raise ParseError("undecodable_bytes")
    if not line.strip():
        raise ParseError("empty_line")

    parts = line.split(None, HEADER_FIELDS)
    if len(parts) < HEADER_FIELDS:
        raise ParseError("too_few_fields")

    label, epoch_raw, date, node, timestamp, _node_repeat, event_type, component, severity = parts[
        :HEADER_FIELDS
    ]
    message = parts[HEADER_FIELDS].strip() if len(parts) > HEADER_FIELDS else ""

    if not epoch_raw.isdigit():
        raise ParseError("bad_epoch")
    epoch = int(epoch_raw)
    if not MIN_EPOCH <= epoch <= MAX_EPOCH:
        raise ParseError("bad_epoch")

    if not _DATE_RE.match(date):
        raise ParseError("bad_date")
    if not _DATETIME_RE.match(timestamp):
        raise ParseError("bad_datetime")
    if severity.upper() not in SEVERITIES:
        raise ParseError("unknown_severity")

    template = mask_message(message)
    return LogEvent(
        line_no=line_no,
        label=label,
        is_alert=label != NORMAL_LABEL,
        epoch=epoch,
        node=node,
        event_type=event_type,
        component=component,
        severity=severity.upper(),
        message=message,
        template=template,
        template_id=template_id(template),
    )
