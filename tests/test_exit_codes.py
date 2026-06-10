"""Basic tests for shared exit codes."""

from YM_data_collection.utils.exit_codes import ExitCode, describe_exit_code


def test_exit_codes_are_unique() -> None:
    values = {member.value for member in ExitCode}
    assert len(values) == len(ExitCode)


def test_exit_code_description_is_stable() -> None:
    assert describe_exit_code(ExitCode.SUCCESS) == "success"
