from starrocks_br import labels


def test_should_generate_basic_label_format():
    label = labels.generate_label("mydb", "2025-10-15", "inc")
    assert label == "mydb_20251015_inc"


def test_should_handle_label_collision_with_retry_suffix():
    existing_labels = ["mydb_20251015_inc"]
    label = labels.generate_label("mydb", "2025-10-15", "inc", existing_labels)
    assert label == "mydb_20251015_inc_r1"


def test_should_handle_multiple_collisions():
    existing_labels = ["mydb_20251015_inc", "mydb_20251015_inc_r1"]
    label = labels.generate_label("mydb", "2025-10-15", "inc", existing_labels)
    assert label == "mydb_20251015_inc_r2"


def test_should_handle_different_backup_types():
    inc_label = labels.generate_label("mydb", "2025-10-15", "inc")
    weekly_label = labels.generate_label("mydb", "2025-10-15", "weekly")
    monthly_label = labels.generate_label("mydb", "2025-10-15", "monthly")
    
    assert inc_label == "mydb_20251015_inc"
    assert weekly_label == "mydb_20251015_weekly"
    assert monthly_label == "mydb_20251015_monthly"


def test_should_handle_empty_existing_labels():
    label = labels.generate_label("mydb", "2025-10-15", "inc", [])
    assert label == "mydb_20251015_inc"


def test_should_handle_different_database_names():
    label1 = labels.generate_label("db1", "2025-10-15", "inc")
    label2 = labels.generate_label("db2", "2025-10-15", "inc")
    
    assert label1 == "db1_20251015_inc"
    assert label2 == "db2_20251015_inc"
