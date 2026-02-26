"""
exec_logger.py — Reusable colorful logger for Python projects
Author: Bryan Spillman & Claude AI | ( not taking full credit )
"""

import logging
import os
import sys
import re
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional


# ─────────────────────────────────────────────
#  ANSI Color / Style Codes
# ─────────────────────────────────────────────
class Colors:
    RESET       = "\033[0m"
    BOLD        = "\033[1m"
    DIM         = "\033[2m"

    # Foreground
    BLACK       = "\033[30m"
    RED         = "\033[31m"
    GREEN       = "\033[32m"
    YELLOW      = "\033[33m"
    BLUE        = "\033[34m"
    MAGENTA     = "\033[35m"
    CYAN        = "\033[36m"
    WHITE       = "\033[37m"
    BRIGHT_RED      = "\033[91m"
    BRIGHT_GREEN    = "\033[92m"
    BRIGHT_YELLOW   = "\033[93m"
    BRIGHT_BLUE     = "\033[94m"
    BRIGHT_MAGENTA  = "\033[95m"
    BRIGHT_CYAN     = "\033[96m"
    BRIGHT_WHITE    = "\033[97m"

    # Background
    BG_BLACK    = "\033[40m"
    BG_RED      = "\033[41m"
    BG_GREEN    = "\033[42m"
    BG_YELLOW   = "\033[43m"
    BG_BLUE     = "\033[44m"
    BG_MAGENTA  = "\033[45m"
    BG_CYAN     = "\033[46m"
    BG_WHITE    = "\033[47m"


# ─────────────────────────────────────────────
#  Strip ANSI codes (for file output)
# ─────────────────────────────────────────────
ANSI_ESCAPE = re.compile(r"\033\[[0-9;]*m")

def strip_ansi(text: str) -> str:
    return ANSI_ESCAPE.sub("", text)


# ─────────────────────────────────────────────
#  Hard-code your preferred datetime format here
#  This is used as the default for all loggers.
# ─────────────────────────────────────────────
DEFAULT_DATETIME_FMT = "%Y.%m.%d %H:%M:%S"


# ─────────────────────────────────────────────
#  Default Level → Color + Label mapping
# ─────────────────────────────────────────────
DEFAULT_LEVEL_STYLES: dict[str, dict] = {
    "DEBUG":    {"color": Colors.CYAN,          "label": "DEBUG  "},
    "INFO":     {"color": Colors.BRIGHT_GREEN,  "label": "INFO   "},
    "SUCCESS":  {"color": Colors.GREEN + Colors.BOLD, "label": "SUCCESS"},
    "WARNING":  {"color": Colors.BRIGHT_YELLOW, "label": "WARNING"},
    "ERROR":    {"color": Colors.BRIGHT_RED,    "label": "ERROR  "},
    "CRITICAL": {"color": Colors.BOLD + Colors.BG_RED + Colors.WHITE, "label": "CRITICAL"},
    "STEP":     {"color": Colors.BRIGHT_MAGENTA,"label": "STEP   "},
    "HEADER":   {"color": Colors.BRIGHT_CYAN + Colors.BOLD, "label": "HEADER "},
}

# Register custom levels
CUSTOM_LEVELS = {
    "SUCCESS": 25,
    "STEP":    22,
    "HEADER":  21,
}
for name, value in CUSTOM_LEVELS.items():
    if not hasattr(logging, name):
        logging.addLevelName(value, name)


# ─────────────────────────────────────────────
#  ColorLogger
# ─────────────────────────────────────────────
class ColorLogger:
    """
    A fully customizable colorful logger that writes rich color output to the
    console and plain text to log files simultaneously.

    Parameters
    ----------
    name        : Logger name (shown in log lines if include_name=True)
    log_dir     : Directory for log files. Defaults to ./logs
    log_file    : Base filename (no extension). Defaults to <name>
    datetime_fmt: strftime format for timestamps
    level       : Minimum log level (logging.DEBUG, logging.INFO, etc.)
    max_bytes   : Max log file size before rotation (default 5 MB)
    backup_count: Number of rotated files to keep
    col_widths  : Dict of column widths for timestamp / level / name columns
    level_styles: Override or extend default level→color/label mappings
    include_name: Whether to print the logger name in each line
    separator   : Character used between columns (default " │ ")
    console     : Enable/disable console output
    line_width  : Default width for divider() and header() lines (default 60)
    """

    def __init__(
        self,
        name: str                       = "app",
        log_dir: str | Path             = "logs",
        log_file: Optional[str]         = None,
        datetime_fmt: str               = DEFAULT_DATETIME_FMT,
        level: int                      = logging.DEBUG,
        max_bytes: int                  = 5 * 1024 * 1024,
        backup_count: int               = 5,
        col_widths: Optional[dict]      = None,
        level_styles: Optional[dict]    = None,
        include_name: bool              = False,
        separator: str                  = " │ ",
        console: bool                   = True,
        line_width: int                 = 60,
    ):
        self.name         = name
        self.datetime_fmt = datetime_fmt
        self.level        = level
        self.separator    = separator
        self.line_width   = line_width
        self.include_name = include_name
        self.console      = console

        # Merge custom level styles on top of defaults
        self.styles = {**DEFAULT_LEVEL_STYLES, **(level_styles or {})}

        # Column widths: timestamp | level | name
        # Level width is auto-calculated from the widest label — no need to set manually
        _defaults = {
            "timestamp": len(datetime.now().strftime(datetime_fmt)),
            "level":     self._max_label_width(),
            "name":      12,
        }
        self.col_widths = {**_defaults, **(col_widths or {})}

        # ── File handler setup ──────────────────────────────────────────
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        # log_filename = (log_file or name) + ".log"
        dtstamp = datetime.now().strftime(DEFAULT_DATETIME_FMT).replace(":", "").replace(" ", "_").replace(".", "")
        log_filename = dtstamp + "_" + (log_file or name) + ".log"
        self.log_path = self.log_dir / log_filename

        self._file_handler = RotatingFileHandler(
            self.log_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        self._file_handler.setLevel(logging.DEBUG)

        # Internal stdlib logger (file only — we handle console ourselves)
        self._logger = logging.getLogger(f"exec_logger.{name}")
        self._logger.setLevel(logging.DEBUG)
        self._logger.propagate = False
        if not self._logger.handlers:
            self._logger.addHandler(self._file_handler)

    # ─────────────────────────────────────────
    #  Internal helpers
    # ─────────────────────────────────────────
    def _max_label_width(self) -> int:
        """Return the character width of the longest level label in styles."""
        return max(len(s["label"].strip()) for s in self.styles.values())

    def _recalc_level_width(self):
        """Recalculate and update the level column width after styles change."""
        self.col_widths["level"] = self._max_label_width()

    # ─────────────────────────────────────────
    #  Core formatting
    # ─────────────────────────────────────────
    def _format(self, level_name: str, message: str, color: bool = True) -> str:
        style    = self.styles.get(level_name, {"color": Colors.WHITE, "label": level_name.ljust(7)})
        clr      = style["color"] if color else ""
        reset    = Colors.RESET if color else ""
        dim      = Colors.DIM if color else ""
        sep      = f"{dim}{self.separator}{reset}"

        ts        = datetime.now().strftime(self.datetime_fmt)
        ts_col    = ts.ljust(self.col_widths["timestamp"])
        lvl_col   = style["label"].strip().ljust(self.col_widths["level"])

        parts = [f"{dim}{ts_col}{reset}", f"{clr}{lvl_col}{reset}"]

        if self.include_name:
            nm_col = self.name[:self.col_widths["name"]].ljust(self.col_widths["name"])
            parts.append(f"{dim}{nm_col}{reset}")

        parts.append(f"{clr}{message}{reset}")
        return sep.join(parts)

    def _emit(self, level_name: str, message: str, exc_info=None):
        std_level = getattr(logging, level_name, logging.INFO)
        if std_level < self.level:
            return

        # Console (with color)
        if self.console:
            styled = self._format(level_name, message, color=True)
            stream = sys.stderr if std_level >= logging.ERROR else sys.stdout
            print(styled, file=stream)

        # File (plain)
        plain = strip_ansi(self._format(level_name, message, color=False))
        record = logging.LogRecord(
            name=self.name, level=std_level, pathname="", lineno=0,
            msg=plain, args=(), exc_info=exc_info,
        )
        self._file_handler.emit(record)

        if exc_info:
            import traceback
            tb = traceback.format_exc()
            tb_record = logging.LogRecord(
                name=self.name, level=std_level, pathname="", lineno=0,
                msg=tb, args=(), exc_info=None,
            )
            self._file_handler.emit(tb_record)

    # ─────────────────────────────────────────
    #  Public log methods
    # ─────────────────────────────────────────
    def debug(self, msg: str):                   self._emit("DEBUG",    msg)
    def info(self, msg: str):                    self._emit("INFO",     msg)
    def success(self, msg: str):                 self._emit("SUCCESS",  msg)
    def warning(self, msg: str):                 self._emit("WARNING",  msg)
    def error(self, msg: str, exc_info=False):   self._emit("ERROR",    msg, exc_info=exc_info)
    def critical(self, msg: str, exc_info=False):self._emit("CRITICAL", msg, exc_info=exc_info)
    def step(self, msg: str):                    self._emit("STEP",     msg)

    def header(self, msg: str, width: int = None):
        """Print a prominent section header banner."""
        w = width or self.line_width
        bar = "═" * w
        self._emit("HEADER", bar)
        self._emit("HEADER", msg.center(w))
        self._emit("HEADER", bar)

    def blank(self):
        """Print a blank line to console and file."""
        if self.console:
            print()
        plain_record = logging.LogRecord(
            name=self.name, level=logging.DEBUG, pathname="", lineno=0,
            msg="", args=(), exc_info=None,
        )
        self._file_handler.emit(plain_record)

    def divider(self, char: str = "─", width: int = None):
        """Print a horizontal divider line."""
        self._emit("DEBUG", char * (width or self.line_width))

    # ─────────────────────────────────────────
    #  Config helpers
    # ─────────────────────────────────────────
    def set_level(self, level: int):
        self.level = level

    def set_datetime_fmt(self, fmt: str):
        self.datetime_fmt = fmt
        self.col_widths["timestamp"] = len(datetime.now().strftime(fmt))

    def set_separator(self, sep: str):
        self.separator = sep

    def add_level_style(self, name: str, color: str, label: str, level_value: Optional[int] = None):
        """Register a brand-new custom log level with its color and label."""
        self.styles[name] = {"color": color, "label": label}
        self._recalc_level_width()
        if level_value is not None:
            logging.addLevelName(level_value, name)

    def silence_console(self):  self.console = False
    def enable_console(self):   self.console = True

    @property
    def log_file_path(self) -> Path:
        return self.log_path

    def __repr__(self):
        return (f"<ColorLogger name={self.name!r} level={logging.getLevelName(self.level)} "
                f"log={self.log_path}>")


# ─────────────────────────────────────────────
#  Registry — stores loggers by name
# ─────────────────────────────────────────────
_registry: dict[str, "ColorLogger"] = {}


def get_logger(
    name: str               = "app",
    log_dir: str | Path     = "logs",
    log_file: str           = None,
    datetime_fmt: str       = DEFAULT_DATETIME_FMT,
    level: int              = logging.DEBUG,
    **kwargs,
) -> ColorLogger:
    """
    Returns a ColorLogger for the given name.
    If one with that name already exists, returns the cached instance —
    all subsequent calls ignore configuration parameters.
    """
    if name not in _registry:
        _registry[name] = ColorLogger(
            name=name,
            log_dir=log_dir,
            log_file=log_file,
            datetime_fmt=datetime_fmt,
            level=level,
            **kwargs,
        )
    return _registry[name]


def clear_logger(name: str):
    """Remove a logger from the registry (useful for testing)."""
    _registry.pop(name, None)


# ─────────────────────────────────────────────
#  Demo / usage example
# ─────────────────────────────────────────────
# from colorlog import get_logger
# log = get_logger("nfs-test", log_dir="/var/log/storage", datetime_fmt="%Y-%m-%d %H:%M:%S")


















    # ── Basic usage ─────────────────────────────────────
    # log = get_logger(
    #     name         = "storage-demo",
    #     log_dir      = "logs",
    #     datetime_fmt = "%Y-%m-%d %H:%M:%S",
    #     separator    = " │ ",
    #     include_name = False,
    # )

    # log.header("NFS Protocol Validation Suite")
    # log.blank()

    # log.step("Connecting to cluster: cluster01.dc1.local")
    # log.debug("SSH handshake initiated on port 22")
    # log.info("Mounting NFS export: /ifs/data/prod")
    # log.success("NFS3 mount verified — latency 1.2 ms")
    # log.warning("NFS4 delegation not enabled on this export")
    # log.error("SMB2 share \\\\cluster01\\backup not reachable")
    # log.blank()

    # log.divider()

    # # ── Custom datetime format ──────────────────────────
    # log2 = get_logger(
    #     name         = "upgrade-check",
    #     log_dir      = "logs",
    #     datetime_fmt = "%m/%d %I:%M:%S %p",     # e.g.  02/24 03:45:22 PM
    #     separator    = "  |  ",
    #     include_name = True,
    #     col_widths   = {"name": 14},
    # )

    # log2.header("OneFS 9.7 → 9.8 Upgrade Validation")
    # log2.step("Phase 1 — Pre-flight checks")
    # log2.info("Verifying cluster quorum across 8 datacenters")
    # log2.success("All 32 nodes healthy")
    # log2.warning("Scheduled maintenance window closes in 45 min")
    # log2.critical("Node dc3-node04 failed health check — HALT upgrade")

    # # ── Add a custom level ──────────────────────────────
    # log2.add_level_style(
    #     name        = "AUDIT",
    #     color       = Colors.BRIGHT_BLUE + Colors.BOLD,
    #     label       = "AUDIT  ",
    #     level_value = 28,
    # )
    # log2._emit("AUDIT", "SMB3 session opened by user svc-backup@corp.local")

    # print(f"\n📁 Logs written to: {log.log_file_path}")
    # print(f"📁 Logs written to: {log2.log_file_path}")
