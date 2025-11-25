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

import starrocks_br.health as health


def test_should_report_healthy_when_all_nodes_alive_and_ready(mocker):
    db = mocker.Mock()
    db.query.side_effect = [
        [
            # FE rows: (Id, Name, IP, EditLogPort, HttpPort, QueryPort, RpcPort, Role, ClusterId, Join, Alive, ...)
            (
                "1",
                "fe1",
                "127.0.0.1",
                "9010",
                "8030",
                "9030",
                "9020",
                "LEADER",
                "123",
                "true",
                "true",
            ),
            (
                "2",
                "fe2",
                "127.0.0.2",
                "9010",
                "8030",
                "9030",
                "9020",
                "FOLLOWER",
                "123",
                "true",
                "true",
            ),
        ],
        [
            # BE rows: (BackendId, IP, HeartbeatPort, BePort, HttpPort, BrpcPort, LastStartTime, LastHeartbeat, Alive, ...)
            (
                "10001",
                "127.0.0.1",
                "9050",
                "9060",
                "8040",
                "8060",
                "2025-10-16",
                "2025-10-16",
                "true",
            ),
            (
                "10002",
                "127.0.0.2",
                "9050",
                "9060",
                "8040",
                "8060",
                "2025-10-16",
                "2025-10-16",
                "true",
            ),
        ],
    ]

    ok, msg = health.check_cluster_health(db)
    assert ok is True
    assert "healthy" in msg.lower()


def test_should_report_unhealthy_when_any_node_dead_or_not_ready(mocker):
    db = mocker.Mock()
    db.query.side_effect = [
        [
            # FE rows: (Id, Name, IP, EditLogPort, HttpPort, QueryPort, RpcPort, Role, ClusterId, Join, Alive, ...)
            (
                "1",
                "fe1",
                "127.0.0.1",
                "9010",
                "8030",
                "9030",
                "9020",
                "LEADER",
                "123",
                "true",
                "true",
            ),
            (
                "2",
                "fe2",
                "127.0.0.2",
                "9010",
                "8030",
                "9030",
                "9020",
                "FOLLOWER",
                "123",
                "false",
                "false",
            ),
        ],
        [
            # BE rows: (BackendId, IP, HeartbeatPort, BePort, HttpPort, BrpcPort, LastStartTime, LastHeartbeat, Alive, ...)
            (
                "10001",
                "127.0.0.1",
                "9050",
                "9060",
                "8040",
                "8060",
                "2025-10-16",
                "2025-10-16",
                "true",
            ),
            (
                "10002",
                "127.0.0.2",
                "9050",
                "9060",
                "8040",
                "8060",
                "2025-10-16",
                "2025-10-16",
                "false",
            ),
        ],
    ]

    ok, msg = health.check_cluster_health(db)
    assert ok is False
    assert "unhealthy" in msg.lower()


def test_should_report_unhealthy_when_be_not_ready(mocker):
    db = mocker.Mock()
    db.query.side_effect = [
        [
            # FE rows: (Id, Name, IP, EditLogPort, HttpPort, QueryPort, RpcPort, Role, ClusterId, Join, Alive, ...)
            (
                "1",
                "fe1",
                "127.0.0.1",
                "9010",
                "8030",
                "9030",
                "9020",
                "LEADER",
                "123",
                "true",
                "true",
            )
        ],
        [
            # BE rows: (BackendId, IP, HeartbeatPort, BePort, HttpPort, BrpcPort, LastStartTime, LastHeartbeat, Alive, ...)
            (
                "10001",
                "127.0.0.1",
                "9050",
                "9060",
                "8040",
                "8060",
                "2025-10-16",
                "2025-10-16",
                "false",
            )
        ],
    ]

    ok, msg = health.check_cluster_health(db)
    assert ok is False
    assert "unhealthy" in msg.lower()
