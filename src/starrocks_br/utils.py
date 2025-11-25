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


def quote_identifier(identifier):
    """
    Quote a SQL identifier (database, table, or column name) with backticks.

    Args:
        identifier: The database, table, or column name to quote

    Returns:
        The identifier wrapped in backticks with internal backticks escaped

    Raises:
        ValueError: If identifier is None or empty string

    Examples:
        >>> quote_identifier("my_table")
        '`my_table`'
        >>> quote_identifier("select")
        '`select`'
        >>> quote_identifier("table`with`ticks")
        '`table``with``ticks`'
    """
    if identifier is None:
        raise ValueError("Identifier cannot be None")

    if identifier == "":
        raise ValueError("Identifier cannot be empty")

    escaped = identifier.replace("`", "``")
    return f"`{escaped}`"


def quote_value(value):
    """
    Quote and escape a SQL string value for safe query interpolation.

    Args:
        value: The string value to quote and escape

    Returns:
        The properly quoted and escaped SQL value

    Examples:
        >>> quote_value("test")
        "'test'"
        >>> quote_value("O'Brien")
        "'O''Brien'"
        >>> quote_value(None)
        'NULL'
    """
    if value is None:
        return "NULL"

    value = str(value)
    escaped = value.replace("\\", "\\\\")
    escaped = escaped.replace("'", "''")
    escaped = escaped.replace("\n", "\\n")
    escaped = escaped.replace("\t", "\\t")

    return f"'{escaped}'"


def build_qualified_table_name(database, table):
    """
    Build a fully qualified table name with proper quoting.

    Args:
        database: The database name
        table: The table name

    Returns:
        Fully qualified table name in format `database`.`table`

    Raises:
        ValueError: If database or table is None or empty

    Examples:
        >>> build_qualified_table_name("my_db", "my_table")
        '`my_db`.`my_table`'
    """
    if not database:
        raise ValueError("Database name cannot be empty or None")

    if not table:
        raise ValueError("Table name cannot be empty or None")

    return f"{quote_identifier(database)}.{quote_identifier(table)}"
