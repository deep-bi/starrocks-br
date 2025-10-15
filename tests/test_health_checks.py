def test_cluster_health_ok(mocker):
    from starrocks_br import health

    db = mocker.Mock()
    db.query.side_effect = [
        [
            ("fe1", "ALIVE", "TRUE", "TRUE"),
            ("fe2", "ALIVE", "TRUE", "FALSE"),
        ],
        [
            ("be1", "ALIVE", "TRUE"),
            ("be2", "ALIVE", "TRUE"),
        ],
    ]

    ok, msg = health.check_cluster_health(db)
    assert ok is True
    assert "healthy" in msg.lower()


def test_cluster_health_fail_when_any_dead(mocker):
    from starrocks_br import health

    db = mocker.Mock()
    db.query.side_effect = [
        [
            ("fe1", "ALIVE", "TRUE", "TRUE"),
            ("fe2", "DEAD", "FALSE", "FALSE"),
        ],
        [
            ("be1", "ALIVE", "TRUE"),
            ("be2", "DECOMMISSION", "FALSE"),
        ],
    ]

    ok, msg = health.check_cluster_health(db)
    assert ok is False
    assert "dead" in msg.lower() or "decommission" in msg.lower()


