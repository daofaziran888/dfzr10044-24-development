"""写入撤回服务的兼容模块。

核心状态流转仍归属 write_job；此模块保留独立命名，方便按“写入撤回”领域对象导入。
"""
from __future__ import annotations

from app.services.write_job import WriteJobService, write_job_service


class WriteWithdrawService(WriteJobService):
    """写入撤回、幂等回执和快照回滚入口。"""


write_withdraw_service = WriteWithdrawService()

__all__ = ["WriteWithdrawService", "write_withdraw_service", "write_job_service"]
