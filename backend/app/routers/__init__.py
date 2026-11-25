"""业务路由汇总：专用流程路由优先，通用资源路由兜底。"""
from __future__ import annotations

from app.routers import resource, write_flow

ROUTERS = [write_flow, resource]
