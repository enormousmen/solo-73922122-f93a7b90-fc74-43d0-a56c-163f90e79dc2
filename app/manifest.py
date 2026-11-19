"""固定的分片投票副本清单（manifest）加载与校验。"""
from __future__ import annotations

import json
from dataclasses import dataclass


@dataclass(frozen=True)
class Manifest:
    # 分片名 -> 投票节点列表（有序、去重）
    shards: dict[str, tuple[str, ...]]

    @property
    def nodes(self) -> frozenset[str]:
        """清单中出现过的全部节点。"""
        out: set[str] = set()
        for voters in self.shards.values():
            out.update(voters)
        return frozenset(out)

    def majority_needed(self, shard: str) -> int:
        """该分片过半所需的最少可用投票副本数。"""
        return len(self.shards[shard]) // 2 + 1


def load_manifest(path: str) -> Manifest:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    shards_raw = raw.get("shards")
    if not isinstance(shards_raw, dict) or not shards_raw:
        raise ValueError("manifest 必须包含非空的 'shards' 对象")
    shards: dict[str, tuple[str, ...]] = {}
    for name, voters in shards_raw.items():
        if not isinstance(voters, list) or not voters:
            raise ValueError(f"分片 {name!r} 的投票副本列表必须是非空数组")
        uniq = sorted({str(v) for v in voters})
        shards[str(name)] = tuple(uniq)
    return Manifest(shards=shards)
