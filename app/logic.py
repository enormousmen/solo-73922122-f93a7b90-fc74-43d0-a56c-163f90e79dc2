"""许可批准 / 结束的判定逻辑（纯函数，便于单测）。"""
from __future__ import annotations

from .manifest import Manifest
from .models import LatestReport, PermitRecord


def evaluate_permit_request(
    manifest: Manifest,
    batch: list[str],
    latest: dict[str, LatestReport],
    open_permits: list[PermitRecord],
) -> tuple[bool, dict]:
    """校验新申请。返回 (是否批准, 详情)。

    批准条件（同时满足）：
    1. 本批每个节点最新探活都健康；
    2. 本批节点未被任何未结束许可占用；
    3. 扣除本批节点、未结束许可占用节点、健康未知或异常节点后，
       本批涉及的每个分片剩余可用投票副本均过半。
    拒绝时详情里带上所有不满足的类别，票数不足时列出受影响分片。
    """
    batch_set = set(batch)

    # 条件 1：本批节点健康
    health_unknown = [n for n in batch if n not in latest]
    unhealthy = [n for n in batch if n in latest and not latest[n].healthy]

    # 条件 2：未结束许可占用
    node_owner: dict[str, str] = {}
    for p in open_permits:
        for n in p.nodes:
            node_owner[n] = p.request_id
    occupied = {n: node_owner[n] for n in batch if n in node_owner}

    # 条件 3：可用性扣除。excluded: node -> 扣除原因
    excluded: dict[str, str] = {}
    for n in batch:
        excluded[n] = "in_batch"
    for n, owner in sorted(node_owner.items()):
        excluded.setdefault(n, f"occupied_by:{owner}")
    for n in sorted(manifest.nodes):
        if n in excluded:
            continue
        rep = latest.get(n)
        if rep is None:
            excluded[n] = "health_unknown"
        elif not rep.healthy:
            excluded[n] = "unhealthy"

    affected_shards = []
    for shard, voters in sorted(manifest.shards.items()):
        if not batch_set.intersection(voters):
            continue  # 只检查本批涉及的分片
        available = [v for v in voters if v not in excluded]
        needed = manifest.majority_needed(shard)
        if len(available) < needed:
            affected_shards.append(
                {
                    "shard": shard,
                    "voters": len(voters),
                    "required_available": needed,
                    "available": len(available),
                    "available_nodes": available,
                    "excluded": {v: excluded[v] for v in voters if v in excluded},
                }
            )

    ok = not (health_unknown or unhealthy or occupied or affected_shards)
    detail: dict = {"ok": ok}
    if not ok:
        detail["error"] = "permit_rejected"
        if health_unknown:
            detail["health_unknown_nodes"] = health_unknown
        if unhealthy:
            detail["unhealthy_nodes"] = unhealthy
        if occupied:
            detail["occupied_nodes"] = [
                {"node": n, "permit": owner} for n, owner in sorted(occupied.items())
            ]
        if affected_shards:
            detail["affected_shards"] = affected_shards
    return ok, detail


def evaluate_finish(
    permit: PermitRecord, latest: dict[str, LatestReport]
) -> dict[str, str]:
    """结束许可的校验。返回 {节点: 不满足原因}，空 dict 表示可以结束。

    本批每个节点都须满足：最新探活在获批后采集、显示健康、
    启动标识与获批时不同。
    """
    problems: dict[str, str] = {}
    for n in permit.nodes:
        rep = latest.get(n)
        if rep is None:
            problems[n] = "no_liveness_report"
        elif rep.sampled_at <= permit.approved_at:
            problems[n] = "not_sampled_after_approval"
        elif not rep.healthy:
            problems[n] = "unhealthy"
        elif rep.boot_id == permit.boot_ids_at_approval[n]:
            problems[n] = "boot_id_unchanged"
    return problems
