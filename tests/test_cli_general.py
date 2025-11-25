# Copyright 2025 deep-bi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from click.testing import CliRunner

from starrocks_br import cli


def test_cli_verbose_flag_enables_debug_logging(config_file, mocker):
    """Test that --verbose flag enables debug logging."""
    runner = CliRunner()

    setup_logging_mock = mocker.patch("starrocks_br.logger.setup_logging")

    runner.invoke(cli.cli, ["--verbose", "restore", "--help"])

    import logging

    setup_logging_mock.assert_called_once_with(level=logging.DEBUG)


def test_cli_without_verbose_uses_info_logging(config_file, mocker):
    """Test that CLI without --verbose uses INFO level logging."""
    runner = CliRunner()

    setup_logging_mock = mocker.patch("starrocks_br.logger.setup_logging")

    runner.invoke(cli.cli, ["restore", "--help"])

    setup_logging_mock.assert_called_once_with()


def test_cli_main_group_requires_subcommand():
    """Test the main CLI group command requires a subcommand."""
    runner = CliRunner()
    result = runner.invoke(cli.cli, [])
    assert result.exit_code in (0, 2)
    assert "Usage:" in result.output


def test_backup_group_requires_subcommand():
    """Test the backup group command requires a subcommand."""
    runner = CliRunner()
    result = runner.invoke(cli.backup, [])
    assert result.exit_code in (0, 2)
    assert "Usage:" in result.output
