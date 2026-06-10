"""Shared CLI exit codes."""

from __future__ import annotations

from enum import IntEnum


class ExitCode(IntEnum):
    """Stable exit codes shared by all scripts."""

    SUCCESS = 0
    GENERAL_FAILURE = 1
    ARGUMENT_ERROR = 2
    CONFIG_ERROR = 3
    DEPENDENCY_ERROR = 4
    DATA_VALIDATION_ERROR = 5
    AUDIT_FAILURE = 6
    FILE_EXPORT_FAILURE = 7


EXIT_CODE_DESCRIPTIONS = {
    ExitCode.SUCCESS: "success",
    ExitCode.GENERAL_FAILURE: "general_failure",
    ExitCode.ARGUMENT_ERROR: "argument_error",
    ExitCode.CONFIG_ERROR: "config_error",
    ExitCode.DEPENDENCY_ERROR: "dependency_error",
    ExitCode.DATA_VALIDATION_ERROR: "data_validation_error",
    ExitCode.AUDIT_FAILURE: "audit_failure",
    ExitCode.FILE_EXPORT_FAILURE: "file_export_failure",
}


def describe_exit_code(exit_code: int | ExitCode) -> str:
    """Return a stable text label for an exit code."""

    normalized = ExitCode(exit_code)
    return EXIT_CODE_DESCRIPTIONS[normalized]
