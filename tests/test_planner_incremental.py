from starrocks_br import planner


def test_should_find_partitions_updated_in_last_n_days(mocker):
    db = mocker.Mock()
    db.query.return_value = [
        ("sales_db", "fact_sales", "p20251015", "2025-10-15"),
        ("sales_db", "fact_sales", "p20251014", "2025-10-14"),
    ]
    
    partitions = planner.find_recent_partitions(db, days=7)
    
    assert len(partitions) == 2
    assert {"database": "sales_db", "table": "fact_sales", "partition_name": "p20251015"} in partitions
    assert {"database": "sales_db", "table": "fact_sales", "partition_name": "p20251014"} in partitions
    assert db.query.call_count == 1


def test_should_build_incremental_backup_command():
    partitions = [
        {"database": "sales_db", "table": "fact_sales", "partition_name": "p20251015"},
        {"database": "sales_db", "table": "fact_sales", "partition_name": "p20251014"},
    ]
    repository = "my_repo"
    label = "sales_db_20251015_inc"
    
    command = planner.build_incremental_backup_command(partitions, repository, label)
    
    expected = """
    BACKUP SNAPSHOT sales_db_20251015_inc
    TO my_repo
    ON (TABLE sales_db.fact_sales PARTITION (p20251015, p20251014))"""
    
    assert command == expected


def test_should_handle_empty_partitions_list():
    command = planner.build_incremental_backup_command([], "my_repo", "label")
    assert command == "" or "no partitions" in command.lower()


def test_should_handle_single_partition():
    partitions = [{"database": "db1", "table": "table1", "partition_name": "p1"}]
    command = planner.build_incremental_backup_command(partitions, "repo", "label")
    
    assert "TABLE db1.table1 PARTITION (p1)" in command
    assert "BACKUP SNAPSHOT label" in command
    assert "TO repo" in command


def test_should_format_date_correctly_in_query(mocker):
    db = mocker.Mock()
    db.query.return_value = []
    
    planner.find_recent_partitions(db, days=3)
    
    query = db.query.call_args[0][0]
    assert "information_schema.partitions" in query
    assert "WHERE" in query


def test_should_build_monthly_backup_command():
    command = planner.build_monthly_backup_command("sales_db", "my_repo", "sales_db_20251015_monthly")
    
    expected = """
    BACKUP DATABASE sales_db SNAPSHOT sales_db_20251015_monthly
    TO my_repo"""
    assert command == expected


def test_should_handle_different_database_names():
    command1 = planner.build_monthly_backup_command("orders_db", "repo", "label1")
    command2 = planner.build_monthly_backup_command("config_db", "repo", "label2")
    
    assert "BACKUP DATABASE orders_db SNAPSHOT label1" in command1
    assert "BACKUP DATABASE config_db SNAPSHOT label2" in command2
    assert "TO repo" in command1
    assert "TO repo" in command2


def test_should_handle_different_repositories():
    command = planner.build_monthly_backup_command("db", "s3_repo", "label")
    
    assert "BACKUP DATABASE db SNAPSHOT label" in command
    assert "TO s3_repo" in command


def test_should_handle_special_characters_in_label():
    command = planner.build_monthly_backup_command("test_db", "repo", "test_db_2025-10-15_monthly")
    
    assert "BACKUP DATABASE test_db SNAPSHOT test_db_2025-10-15_monthly" in command


def test_should_find_weekly_eligible_tables(mocker):
    db = mocker.Mock()
    db.query.return_value = [
        ("sales_db", "dim_customers"),
        ("sales_db", "dim_products"),
        ("orders_db", "dim_regions"),
    ]
    
    tables = planner.find_weekly_eligible_tables(db)
    
    assert len(tables) == 3
    assert {"database": "sales_db", "table": "dim_customers"} in tables
    assert {"database": "sales_db", "table": "dim_products"} in tables
    assert {"database": "orders_db", "table": "dim_regions"} in tables
    assert db.query.call_count == 1


def test_should_build_weekly_backup_command():
    tables = [
        {"database": "sales_db", "table": "dim_customers"},
        {"database": "sales_db", "table": "dim_products"},
        {"database": "orders_db", "table": "dim_regions"},
    ]
    repository = "my_repo"
    label = "weekly_backup_20251015"
    
    command = planner.build_weekly_backup_command(tables, repository, label)
    
    expected = """
    BACKUP SNAPSHOT weekly_backup_20251015
    TO my_repo
    ON (TABLE sales_db.dim_customers,
        TABLE sales_db.dim_products,
        TABLE orders_db.dim_regions)"""
    
    assert command == expected


def test_should_handle_empty_weekly_tables_list():
    command = planner.build_weekly_backup_command([], "my_repo", "label")
    assert command == "" or "no tables" in command.lower()


def test_should_handle_single_weekly_table():
    tables = [{"database": "config_db", "table": "ref_countries"}]
    command = planner.build_weekly_backup_command(tables, "repo", "label")
    
    assert "TABLE config_db.ref_countries" in command
    assert "BACKUP SNAPSHOT label" in command
    assert "TO repo" in command


def test_should_query_correct_weekly_eligible_condition(mocker):
    db = mocker.Mock()
    db.query.return_value = []
    
    planner.find_weekly_eligible_tables(db)
    
    query = db.query.call_args[0][0]
    assert "ops.table_inventory" in query
    assert "weekly_eligible = TRUE" in query
    assert "ORDER BY database_name, table_name" in query


def test_should_handle_different_database_names_in_weekly():
    tables = [
        {"database": "sales_db", "table": "dim_customers"},
        {"database": "orders_db", "table": "dim_regions"},
        {"database": "config_db", "table": "ref_countries"},
    ]
    command = planner.build_weekly_backup_command(tables, "repo", "label")
    
    assert "TABLE sales_db.dim_customers" in command
    assert "TABLE orders_db.dim_regions" in command
    assert "TABLE config_db.ref_countries" in command


def test_should_handle_special_characters_in_weekly_label():
    tables = [{"database": "test_db", "table": "test_table"}]
    command = planner.build_weekly_backup_command(tables, "repo", "weekly_2025-10-15_backup")
    
    assert "BACKUP SNAPSHOT weekly_2025-10-15_backup" in command
