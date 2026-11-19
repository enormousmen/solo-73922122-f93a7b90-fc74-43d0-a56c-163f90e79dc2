"""请求体的 Pydantic 模型。"""
from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, Field, field_validator


class LivenessReportIn(BaseModel):
    node_id: str = Field(min_length=1, max_length=128)
    sampled_at: datetime = Field(description="采样时间，ISO8601")
    healthy: bool
    boot_id: str = Field(min_length=1, max_length=128, description="启动标识")

    @field_validator("sampled_at")
    @classmethod
    def _normalize_tz(cls, v: datetime) -> datetime:
        # 不带时区按 UTC 处理，统一归一到 UTC
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)


class LivenessBatchIn(BaseModel):
    reports: list[LivenessReportIn] = Field(min_length=1, max_length=5000)


class PermitRequestIn(BaseModel):
    request_id: str = Field(min_length=1, max_length=128, description="申请号")
    nodes: list[str] = Field(min_length=1, max_length=5000, description="拟重启节点")
