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


def check_cluster_health(db) -> tuple[bool, str]:
    """Check FE/BE health via SHOW FRONTENDS/BACKENDS.

    Returns (ok, message).
    """
    fe_rows = db.query("SHOW FRONTENDS")
    be_rows = db.query("SHOW BACKENDS")

    def is_alive(value: str) -> bool:
        return str(value).upper() in {"ALIVE", "TRUE", "YES", "1"}

    any_dead = False
    for row in fe_rows:
        fe_joined_cluster = str(row[9]).upper() if len(row) > 9 else "TRUE"
        fe_is_alive = str(row[10]).upper() if len(row) > 10 else "TRUE"
        if not is_alive(fe_joined_cluster) or not is_alive(fe_is_alive):
            any_dead = True
            break

    if not any_dead:
        for row in be_rows:
            be_is_alive = str(row[8]).upper() if len(row) > 8 else "TRUE"
            if not is_alive(be_is_alive):
                any_dead = True
                break

    if any_dead:
        return False, "Cluster unhealthy: some FE/BE are DEAD or not READY"
    return True, "Cluster healthy: all FE/BE are ALIVE and READY"
