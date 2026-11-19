"""关键流程自测：探活、批准、多数票、幂等、结束、持久化、并发。"""
from __future__ import annotations

import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.main import create_app  # noqa: E402

MANIFEST = {
    "shards": {
        "shard-a": ["n1", "n2", "n3"],
        "shard-b": ["n2", "n3", "n4"],
        "shard-c": ["n3", "n4", "n5"],
    }
}
ALL_NODES = ["n1", "n2", "n3", "n4", "n5"]
T0 = datetime.now(timezone.utc) - timedelta(minutes=30)  # 初始探活采样时间


@pytest.fixture()
def make_client(tmp_path):
    """工厂：同一 db 路径可反复建 app，模拟服务重启。"""
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(MANIFEST))
    db_path = tmp_path / "permits.db"
    clients = []

    def _make():
        app = create_app(db_path=str(db_path), manifest_path=str(manifest_path))
        c = TestClient(app)
        clients.append(c)
        return c

    yield _make
    for c in clients:
        c.close()


@pytest.fixture()
def client(make_client):
    return make_client()


def report(client, node, healthy=True, boot="boot-1", sampled_at=None):
    sampled_at = sampled_at or datetime.now(timezone.utc)
    return client.post(
        "/liveness",
        json={
            "reports": [
                {
                    "node_id": node,
                    "sampled_at": sampled_at.isoformat(),
                    "healthy": healthy,
                    "boot_id": boot,
                }
            ]
        },
    )


def seed_all_healthy(client, boot="boot-1", sampled_at=T0):
    for n in ALL_NODES:
        r = report(client, n, healthy=True, boot=boot, sampled_at=sampled_at)
        assert r.status_code == 201, r.text


def request_permit(client, request_id, nodes):
    return client.post("/permits", json={"request_id": request_id, "nodes": nodes})


# ---------- 基础 ----------


def test_healthz_and_manifest(client):
    assert client.get("/healthz").json() == {"status": "ok"}
    m = client.get("/manifest").json()["shards"]
    assert m["shard-a"] == {"voters": ["n1", "n2", "n3"], "majority_needed": 2}
    assert m["shard-c"]["voters"] == ["n3", "n4", "n5"]


def test_liveness_latest_by_sample_time_wins(client):
    t1 = datetime(2026, 11, 19, 10, 0, tzinfo=timezone.utc)
    t0 = t1 - timedelta(hours=1)
    t2 = t1 + timedelta(hours=1)
    assert report(client, "n1", healthy=True, boot="b1", sampled_at=t1).status_code == 201
    # 更旧的采样时间后到达，不得覆盖
    assert report(client, "n1", healthy=False, boot="b0", sampled_at=t0).status_code == 201
    got = client.get("/liveness/n1").json()
    assert got["healthy"] is True and got["boot_id"] == "b1"
    # 更新的采样时间覆盖
    assert report(client, "n1", healthy=False, boot="b2", sampled_at=t2).status_code == 201
    got = client.get("/liveness/n1").json()
    assert got["healthy"] is False and got["boot_id"] == "b2"


def test_liveness_unknown_node_rejected(client):
    r = report(client, "ghost-node")
    assert r.status_code == 422
    assert r.json()["detail"]["unknown_nodes"] == ["ghost-node"]
    assert client.get("/liveness/ghost-node").status_code == 404


# ---------- 批准与幂等 ----------


def test_permit_happy_path_and_idempotent_replay(client):
    seed_all_healthy(client)
    r = request_permit(client, "req-1", ["n1"])
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["status"] == "open"
    assert body["boot_ids_at_approval"] == {"n1": "boot-1"}

    # 同号同批重发 -> 200 返回原许可（approved_at 不变）
    replay = request_permit(client, "req-1", ["n1"])
    assert replay.status_code == 200
    assert replay.json()["approved_at"] == body["approved_at"]

    # 节点顺序/重复不影响"同批"判定
    replay2 = request_permit(client, "req-1", ["n1", "n1"])
    assert replay2.status_code == 200

    # 同号不同批 -> 409
    conflict = request_permit(client, "req-1", ["n2"])
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["error"] == "request_id_conflict"


def test_permit_unknown_node_rejected(client):
    seed_all_healthy(client)
    r = request_permit(client, "req-x", ["n1", "ghost"])
    assert r.status_code == 422
    assert r.json()["detail"]["unknown_nodes"] == ["ghost"]


def test_reject_unhealthy_or_unknown_health(client):
    report(client, "n1", healthy=True, sampled_at=T0)
    report(client, "n3", healthy=False, sampled_at=T0)
    # n2 从未上报 -> 健康未知
    r = request_permit(client, "req-u1", ["n2"])
    assert r.status_code == 409
    assert r.json()["detail"]["health_unknown_nodes"] == ["n2"]
    r = request_permit(client, "req-u2", ["n3"])
    assert r.status_code == 409
    assert r.json()["detail"]["unhealthy_nodes"] == ["n3"]


def test_reject_node_occupied_by_open_permit(client):
    seed_all_healthy(client)
    assert request_permit(client, "req-1", ["n1"]).status_code == 201
    r = request_permit(client, "req-2", ["n1"])
    assert r.status_code == 409
    assert r.json()["detail"]["occupied_nodes"] == [{"node": "n1", "permit": "req-1"}]


# ---------- 多数票 ----------


def test_quorum_guard_lists_affected_shards(client):
    seed_all_healthy(client)
    # n1 只占 shard-a 一票，批准后 shard-a 剩 n2,n3 = 2/3，仍过半
    assert request_permit(client, "req-1", ["n1"]).status_code == 201
    # 再重启 n2：shard-a 扣除 n1(许可占用)、n2(本批) 后只剩 n3 = 1/3，不过半
    r = request_permit(client, "req-2", ["n2"])
    assert r.status_code == 409, r.text
    detail = r.json()["detail"]
    assert detail["error"] == "permit_rejected"
    shards = {s["shard"]: s for s in detail["affected_shards"]}
    assert set(shards) == {"shard-a"}  # shard-b 扣 n2 后仍有 n3,n4，不受影响
    sa = shards["shard-a"]
    assert sa["voters"] == 3 and sa["required_available"] == 2 and sa["available"] == 1
    assert sa["excluded"] == {"n1": "occupied_by:req-1", "n2": "in_batch"}
    # 不冲突的申请仍可批准（n5 只占 shard-c）
    assert request_permit(client, "req-3", ["n5"]).status_code == 201


def test_quorum_counts_unhealthy_and_unknown_as_unavailable(client):
    # n3 异常、n5 从未上报；申请重启 n4 时 shard-c(n3,n4,n5) 三票全部被扣
    report(client, "n1", healthy=True, sampled_at=T0)
    report(client, "n2", healthy=True, sampled_at=T0)
    report(client, "n3", healthy=False, sampled_at=T0)
    report(client, "n4", healthy=True, sampled_at=T0)
    r = request_permit(client, "req-q", ["n4"])
    assert r.status_code == 409
    shards = {s["shard"]: s for s in r.json()["detail"]["affected_shards"]}
    assert shards["shard-c"]["excluded"] == {
        "n3": "unhealthy",
        "n4": "in_batch",
        "n5": "health_unknown",
    }
    assert shards["shard-c"]["available"] == 0


# ---------- 结束许可 ----------


def test_finish_flow(client):
    seed_all_healthy(client, boot="boot-1", sampled_at=T0)
    assert request_permit(client, "req-1", ["n1"]).status_code == 201

    # 最新探活仍是获批前采集的 -> 拒绝
    r = client.post("/permits/req-1/finish")
    assert r.status_code == 409
    assert r.json()["detail"]["nodes"] == {"n1": "not_sampled_after_approval"}

    later = datetime.now(timezone.utc) + timedelta(minutes=10)
    # 获批后采集但启动标识未变 -> 拒绝
    report(client, "n1", healthy=True, boot="boot-1", sampled_at=later)
    r = client.post("/permits/req-1/finish")
    assert r.json()["detail"]["nodes"] == {"n1": "boot_id_unchanged"}

    # 新启动标识但不健康 -> 拒绝
    report(client, "n1", healthy=False, boot="boot-2", sampled_at=later + timedelta(minutes=1))
    r = client.post("/permits/req-1/finish")
    assert r.json()["detail"]["nodes"] == {"n1": "unhealthy"}

    # 获批后采集 + 健康 + 新启动标识 -> 结束
    report(client, "n1", healthy=True, boot="boot-2", sampled_at=later + timedelta(minutes=2))
    r = client.post("/permits/req-1/finish")
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "finished"
    assert r.json()["finished_at"]

    # 重复结束 -> 409
    assert client.post("/permits/req-1/finish").status_code == 409
    # 节点已释放，可再获许可
    assert request_permit(client, "req-2", ["n1"]).status_code == 201


def test_finish_requires_every_node_in_batch(client):
    seed_all_healthy(client, boot="boot-1", sampled_at=T0)
    # n1 在 shard-a，n4 在 shard-b/c；各分片扣除后仍过半
    assert request_permit(client, "req-1", ["n1", "n4"]).status_code == 201
    later = datetime.now(timezone.utc) + timedelta(minutes=10)
    report(client, "n1", healthy=True, boot="boot-2", sampled_at=later)
    # n4 尚未重启上报 -> 整批不能结束
    r = client.post("/permits/req-1/finish")
    assert r.status_code == 409
    assert r.json()["detail"]["nodes"] == {"n4": "not_sampled_after_approval"}
    report(client, "n4", healthy=True, boot="boot-9", sampled_at=later)
    assert client.post("/permits/req-1/finish").status_code == 200


def test_finish_unknown_permit_404(client):
    assert client.post("/permits/nope/finish").status_code == 404
    assert client.get("/permits/nope").status_code == 404


# ---------- 持久化 ----------


def test_state_survives_restart(make_client):
    c1 = make_client()
    seed_all_healthy(c1, boot="boot-1", sampled_at=T0)
    assert request_permit(c1, "req-1", ["n1"]).status_code == 201
    c1.close()

    # 模拟服务重启：同一 db 文件新建 app
    c2 = make_client()
    p = c2.get("/permits/req-1").json()
    assert p["status"] == "open" and p["nodes"] == ["n1"]
    lv = c2.get("/liveness/n1").json()
    assert lv["healthy"] is True and lv["boot_id"] == "boot-1"
    # 占用关系仍在：n1 不能再被新申请占用
    assert request_permit(c2, "req-2", ["n1"]).status_code == 409


# ---------- 并发 ----------


def test_concurrent_requests_keep_majority(client):
    seed_all_healthy(client)
    # 8 个并发申请，交替抢 n1 / n2（同属 shard-a，3 票最多扣 1 票）
    reqs = [(f"c-{i}", ["n1" if i % 2 == 0 else "n2"]) for i in range(8)]

    def submit(req):
        rid, nodes = req
        return request_permit(client, rid, nodes).status_code

    with ThreadPoolExecutor(max_workers=8) as pool:
        codes = list(pool.map(submit, reqs))

    assert codes.count(201) == 1, codes
    assert codes.count(409) == 7, codes
    opens = client.get("/permits", params={"status": "open"}).json()["permits"]
    assert len(opens) == 1
