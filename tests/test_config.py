import tempfile
import os
from starrocks_br import config


def test_should_load_valid_yaml_config():
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("""
        host: "127.0.0.1"
        port: 9030
        user: "root"
        password: ""
        database: "test_db"
        repository: "test_repo"
        """)
        f.flush()
        config_path = f.name
    
    try:
        cfg = config.load_config(config_path)
        assert cfg['host'] == "127.0.0.1"
        assert cfg['port'] == 9030
        assert cfg['user'] == "root"
        assert cfg['database'] == "test_db"
        assert cfg['repository'] == "test_repo"
    finally:
        os.unlink(config_path)


def test_should_raise_error_when_config_file_not_found():
    try:
        config.load_config("/nonexistent/config.yaml")
        assert False, "Should have raised FileNotFoundError"
    except FileNotFoundError:
        pass


def test_should_raise_error_when_yaml_is_invalid():
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("invalid: yaml: content: [")
        f.flush()
        config_path = f.name
    
    try:
        config.load_config(config_path)
        assert False, "Should have raised yaml.YAMLError"
    except Exception:
        pass
    finally:
        os.unlink(config_path)


def test_should_validate_config_with_all_required_fields():
    cfg = {
        'host': '127.0.0.1',
        'port': 9030,
        'user': 'root',
        'password': '',
        'database': 'test_db',
        'repository': 'test_repo'
    }
    
    config.validate_config(cfg)


def test_should_raise_error_when_required_field_missing():
    cfg = {
        'host': '127.0.0.1',
        'port': 9030,
        'user': 'root',
    }
    
    try:
        config.validate_config(cfg)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Missing required config field" in str(e)

