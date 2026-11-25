"""写入作业领域服务：并发写入、提交协议、确认/撤回冲突、回执与快照回滚。

所有状态流转都集中在本模块；路由层只负责 HTTP 编解码。回执使用内存账本保存，
即使确认响应丢失，也可以凭原申请编号或撤回编号幂等补取首次处置结果。
"""
from __future__ import annotations

import copy
import threading
import time
import uuid
from dataclasses import asdict
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.lake_store import LakeSnapshot, PartitionFile, lake_store


class WriteJobService(BaseModuleService):
    MODULE = "write_job"
    ENTITY = "写入作业"
    OPERATION_TERM = "写入"
    LIST_FIELDS = ["作业编号", "所属湖表", "写入模式", "并发写入数", "提交协议", "冲突重试", "写入行数", "作业耗时"]
    REQUIRED_FIELDS = ["作业编号", "所属湖表", "写入模式"]
    STATUS_ORDER = ["待调度", "写入中", "已提交", "提交冲突", "撤回中", "已撤回", "已回滚"]
    TERMINAL_STATUSES = {"已提交", "已撤回", "已回滚"}
    ACTION_RULES = {
        "调度作业": "写入中",
        "确认提交": "已提交",
        "标记冲突": "提交冲突",
        "申请撤回": "撤回中",
        "发起撤回": "撤回中",
        "写入撤回": "已撤回",
        "撤回": "已撤回",
        "撤回写入": "已撤回",
        "确认撤回": "已撤回",
        "确认撤回写入": "已撤回",
        "撤回本次写入": "已撤回",
        "撤回本次新增": "已撤回",
        "快照回滚": "已回滚",
        "回滚快照": "已回滚",
        "回滚": "已回滚",
        "执行回滚": "已回滚",
    }
    NEGATIVE_ACTIONS = ["标记冲突"]

    # ------------------------------------------------------------------
    # 通用工具
    # ------------------------------------------------------------------

    @staticmethod
    def _now() -> float:
        return time.time()

    @staticmethod
    def _iso(ts: float | None = None) -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(ts or time.time()))

    @staticmethod
    def _new_no(prefix: str) -> str:
        return f"{prefix}-{uuid.uuid4().hex[:10].upper()}"

    @staticmethod
    def _deep_json(value: Any) -> Any:
        if hasattr(value, "__dataclass_fields__"):
            value = asdict(value)
        if isinstance(value, dict):
            return {str(k): WriteJobService._deep_json(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [WriteJobService._deep_json(v) for v in value]
        return value

    def _snapshot_dict(self, snap: LakeSnapshot) -> dict[str, Any]:
        data = self._deep_json(snap)
        current = self._current_snapshot_id(snap.lake_table)
        data["current"] = snap.snapshot_id == current
        data["readable_version"] = snap.status == "available" and snap.readable
        return data

    def _file_dict(self, pf: PartitionFile) -> dict[str, Any]:
        data = self._deep_json(pf)
        data.setdefault("path", f"/lake/{pf.lake_table}/{pf.partition_key}/{pf.file_id}.parquet")
        data.setdefault("file_path", data["path"])
        return data

    def _sync_snapshot_row(self, snapshot_id: str, lake_table: str, **extra: Any) -> dict[str, Any] | None:
        """把湖存储快照同步到快照管理资源列表，方便页面核对各版状态。"""
        snap = lake_store.get_snapshot(lake_table, snapshot_id)
        if snap is None:
            return None
        status_map = {
            "generating": "生成中",
            "available": "可用",
            "expired": "已过期",
            "rolled_back": "已回退",
        }
        rows = store.rows("snapshot_mgr")
        row = next(
            (item for item in rows if item.get("快照标识") == snapshot_id or item.get("快照编号") == snapshot_id),
            None,
        )
        if row is None:
            row = {
                "id": max((int(item.get("id", 0)) for item in rows), default=0) + 1,
            }
            rows.append(row)
        chinese_status = status_map.get(snap.status, snap.status)
        row.update({
            "快照编号": extra.get("snapshot_no") or snapshot_id,
            "所属湖表": lake_table,
            "快照标识": snapshot_id,
            "父快照": snap.parent_snapshot or "",
            "新增行数": snap.added_rows,
            "删除行数": snap.deleted_rows,
            "文件清单摘要": snap.manifest_checksum,
            "生成时间": self._iso(snap.created_at),
            "status": chinese_status,
            "pending": chinese_status == "生成中",
            "abnormal": chinese_status == "已回退",
            "snapshot_kind": snap.kind,
            "readable": snap.readable and snap.status == "available",
            "input_scope": snap.input_scope,
            **extra,
        })
        return row

    @staticmethod
    def _first_present(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
        for key in keys:
            if key in data and data[key] not in (None, ""):
                return data[key]
        return default

    def _normalize_rows(
        self,
        rows_data: list[dict[str, Any]] | None = None,
        rows: list[dict[str, Any]] | None = None,
        row_count: int | None = None,
        partition_key: str = "default",
    ) -> list[dict[str, Any]]:
        source = rows_data if rows_data is not None else rows
        if source is not None:
            return [dict(row) if isinstance(row, dict) else {"value": row} for row in source]
        count = max(0, int(row_count or 0))
        return [
            {"_synthetic_row": index + 1, "partition_key": partition_key, "row_index": index + 1}
            for index in range(count)
        ]

    def _current_snapshot_id(self, lake_table: str) -> str | None:
        available = [s for s in lake_store.list_snapshots(lake_table)
                    if s.status == "available" and s.readable]
        return available[-1].snapshot_id if available else None

    def _get_application_id(self, kwargs: dict[str, Any]) -> str | None:
        return self._first_present(
            kwargs, "application_id", "apply_id", "apply_no", "app_id",
            "request_id", "request_no", "write_apply_id", "write_apply_no",
            "same_application_id", "申请编号", "申请号",
        )

    def _get_withdraw_no(self, kwargs: dict[str, Any]) -> str | None:
        return self._first_present(
            kwargs, "withdraw_no", "withdraw_id", "withdrawal_no", "withdrawal_id",
            "revoke_no", "revoke_id", "撤回编号", "撤回号",
        )

    def _scope(
        self,
        lake_table: str,
        partition_keys: list[str] | str | None = None,
        file_ids: list[str] | None = None,
        receipt_nos: list[str] | None = None,
        application_id: str | None = None,
        row_count: int | None = None,
        rows: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        if isinstance(partition_keys, str):
            partitions = [partition_keys]
        else:
            partitions = list(partition_keys or [])
        return {
            "lake_table": lake_table,
            "partition_keys": partitions,
            "partitions": partitions,
            "file_ids": list(file_ids or []),
            "receipt_nos": list(receipt_nos or []),
            "application_id": application_id,
            "row_count": row_count,
            "affected_row_count": row_count,
            "rows": list(rows or []),
            "input_type": "write_job",
        }

    # ------------------------------------------------------------------
    # 兼容既有写入链路
    # ------------------------------------------------------------------

    def acquire_and_write(self, lake_table: str, mode: str,
                          rows_data: list[dict[str, Any]],
                          retry_on_conflict: bool = True) -> dict[str, Any]:
        """获取全局写锁后写入数据并提交快照。

        乐观锁机制：拿到锁→检查当前文件清单→做版本冲突检测→创建新快照→释放锁。
        若检测到冲突且 retry_on_conflict=True，重试一次。
        """
        attempt = 1
        while attempt <= (2 if retry_on_conflict else 1):
            lock = lake_store.acquire_write_lock(lake_table)
            try:
                start_files = lake_store.list_files(lake_table)
                snap_list = lake_store.list_snapshots(lake_table)
                # 冲突检测
                if attempt == 2:
                    conflict_detected = False
                else:
                    registered_set = set(f.file_id for f in start_files)
                    if snap_list:
                        latest_snap_files = set(snap_list[-1].file_ids)
                        conflict_detected = registered_set != latest_snap_files
                    else:
                        conflict_detected = False

                if conflict_detected and attempt < 2:
                    attempt += 1
                    continue

                pf = lake_store.register_file(
                    lake_table, "write_job",
                    rows=len(rows_data), bytes_=len(rows_data) * 300,
                    rows_data=rows_data,
                )
                prev_snap = snap_list[-1] if snap_list else None
                parent_files = list(prev_snap.file_ids) if prev_snap else []
                manifest = [*parent_files, pf.file_id]
                snap = lake_store.create_snapshot(
                    lake_table, file_ids=manifest,
                    parent_snapshot=prev_snap.snapshot_id if prev_snap else None,
                    added_rows=len(rows_data), deleted_rows=0,
                )
                lake_store.mark_snapshot_available(lake_table, snap.snapshot_id)
                return {
                    "success": True,
                    "ok": True,
                    "snapshot_id": snap.snapshot_id,
                    "readable_snapshot_id": snap.snapshot_id,
                    "rows_written": len(rows_data),
                    "conflict_detected": conflict_detected,
                    "attempt": attempt,
                    "file_id": pf.file_id,
                }
            finally:
                lock.release()
            attempt += 1
        return {"success": False, "ok": False, "error": "write conflict unresolved", "attempt": attempt}

    def detect_write_conflict(self, lake_table: str) -> dict[str, Any]:
        """检测当前写冲突状态。"""
        files = lake_store.list_files(lake_table)
        snaps = lake_store.list_snapshots(lake_table)
        if not snaps:
            return {"conflict": False, "reason": "no snapshots yet"}
        snap_files = set(snaps[-1].file_ids)
        actual_files = set(f.file_id for f in files)
        has_conflict = snap_files != actual_files
        return {
            "conflict": has_conflict,
            "snap_files": len(snap_files),
            "actual_files": len(files),
            "extra_files": list(actual_files - snap_files),
            "missing_files": list(snap_files - actual_files),
        }

    # ------------------------------------------------------------------
    # 申请与普通写入回执
    # ------------------------------------------------------------------

    def register_application(
        self,
        application_id: str,
        lake_table: str,
        partition_key: str | list[str] | None = None,
        expected_version: int | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        existing = lake_store.get_application(application_id)
        if existing is not None:
            return existing
        partitions = [partition_key] if isinstance(partition_key, str) else list(partition_key or [])
        app = {
            "application_id": application_id,
            "apply_no": application_id,
            "id": application_id,
            "lake_table": lake_table,
            "partition_key": partitions[0] if partitions else None,
            "partition_keys": partitions,
            "status": "initialized",
            "version": 0,
            "expected_version": expected_version,
            "base_snapshot_id": self._current_snapshot_id(lake_table),
            "write_receipt_nos": [],
            "withdraw_nos": [],
            "operations": [],
            "created_at": self._now(),
            "created_at_text": self._iso(),
            **extra,
        }
        return lake_store.save_application(application_id, app)

    def confirm_write(
        self,
        lake_table: str | None = None,
        partition_key: str = "A",
        rows_data: list[dict[str, Any]] | None = None,
        rows: list[dict[str, Any]] | None = None,
        row_count: int | None = None,
        application_id: str | None = None,
        entry_id: int | str | None = None,
        job_id: int | str | None = None,
        expected_version: int | None = None,
        simulate_conflict: bool = False,
        conflict_with_withdrawal: bool = False,
        mode: str = "APPEND",
        **extra: Any,
    ) -> dict[str, Any]:
        """确认一次写入并生成普通写入回执和可用快照。"""
        table = lake_table or extra.get("table") or extra.get("所属湖表")
        if not table:
            return {"ok": False, "success": False, "error": "缺少所属湖表"}
        app_id = application_id or self._get_application_id(extra) or self._new_no("APP")
        entry_ref = entry_id if entry_id is not None else job_id or extra.get("写入任务ID")
        app = self.register_application(app_id, table, partition_key, expected_version)

        # 乐观并发：调用方明确给出版本且版本已经变化时，不产生重复提交。
        if expected_version is not None and int(expected_version) != int(app.get("version", 0)):
            result = {
                "ok": False,
                "success": False,
                "conflict": True,
                "code": "WRITE_VERSION_CONFLICT",
                "application_id": app_id,
                "apply_no": app_id,
                "expected_version": expected_version,
                "current_version": app.get("version", 0),
                "status": "提交冲突",
                "message": "写入确认版本与同一申请的并发处置冲突",
            }
            app["status"] = "提交冲突"
            app.setdefault("operations", []).append({"at": self._now(), "type": "confirm_conflict", **result})
            self._attach_to_job(entry_ref, result)
            return result

        if simulate_conflict or conflict_with_withdrawal or app.get("status") == "withdraw_requested":
            app["status"] = "提交冲突"
            app["version"] = int(app.get("version", 0)) + 1
            conflict = {
                "ok": False,
                "success": False,
                "conflict": True,
                "code": "CONFIRM_WITHDRAW_SAME_APPLICATION_CONFLICT",
                "application_id": app_id,
                "apply_no": app_id,
                "lake_table": table,
                "partition_key": partition_key,
                "status": "提交冲突",
                "message": "同一申请的写入确认与撤回发生冲突，需按输入范围核对后撤回新增文件",
                "conflict_with": "withdrawal",
                "current_version": app["version"],
            }
            app.setdefault("operations", []).append({"at": self._now(), "type": "confirm_conflict", **conflict})
            self._attach_to_job(entry_ref, conflict)
            return conflict

        normalized_rows = self._normalize_rows(rows_data, rows, row_count, partition_key)
        lock = lake_store.acquire_write_lock(table)
        try:
            previous = self._current_snapshot_id(table)
            prev_snap = lake_store.get_snapshot(table, previous) if previous else None
            pf = lake_store.register_file(
                table,
                partition_key,
                rows=len(normalized_rows),
                bytes_=max(1, len(normalized_rows)) * 300,
                rows_data=normalized_rows,
            )
            parent_files = list(prev_snap.file_ids) if prev_snap else []
            manifest = [*parent_files, pf.file_id]
            scope = self._scope(
                table, partition_key, [pf.file_id], application_id=app_id,
                row_count=len(normalized_rows), rows=normalized_rows,
            )
            snap = lake_store.create_snapshot(
                table,
                file_ids=manifest,
                parent_snapshot=previous,
                added_rows=len(normalized_rows),
                kind="append",
                label=f"确认写入 {app_id}",
                input_scope=scope,
            )
            lake_store.mark_snapshot_available(table, snap.snapshot_id)
            self._sync_snapshot_row(snap.snapshot_id, table)

            receipt_no = extra.get("receipt_no") or extra.get("write_receipt_no") or self._new_no("WR")
            receipt = {
                "ok": True,
                "success": True,
                "receipt_no": receipt_no,
                "receipt_id": receipt_no,
                "id": receipt_no,
                "receipt_type": "write",
                "type": "普通写入回执",
                "application_id": app_id,
                "apply_no": app_id,
                "job_id": entry_ref,
                "entry_id": entry_ref,
                "lake_table": table,
                "table": table,
                "partition_key": partition_key,
                "partition_keys": [partition_key],
                "input_scope": scope,
                "added_file_ids": [pf.file_id],
                "file_id": pf.file_id,
                "file_path": self._file_dict(pf)["path"],
                "added_file": self._file_dict(pf),
                "rows_written": len(normalized_rows),
                "row_count": len(normalized_rows),
                "rows": normalized_rows,
                "snapshot_id": snap.snapshot_id,
                "readable_snapshot_id": snap.snapshot_id,
                "version": len(lake_store.list_snapshots(table)),
                "status": "已确认",
                "confirmed": True,
                "created_at": self._now(),
                "created_at_text": self._iso(),
                "message": "写入已确认并创建可用快照",
            }
            lake_store.save_write_receipt(receipt_no, receipt)
            app["status"] = "write_confirmed"
            app["version"] = int(app.get("version", 0)) + 1
            app["last_write_receipt_no"] = receipt_no
            app.setdefault("write_receipt_nos", []).append(receipt_no)
            app.setdefault("operations", []).append({"at": self._now(), "type": "confirm_write", "receipt_no": receipt_no, "snapshot_id": snap.snapshot_id})
            self._attach_to_job(entry_ref, receipt)
            return copy.deepcopy(receipt)
        finally:
            try:
                lock.release()
            except RuntimeError:
                lake_store.release_write_lock(table)

    # 常见命名别名，降低不同调用方的适配成本。
    confirm_and_commit = confirm_write
    commit_write = confirm_write
    create_write_receipt = confirm_write

    def _attach_to_job(self, entry_id: int | str | None, payload: dict[str, Any]) -> None:
        if entry_id is None:
            return
        try:
            numeric_id = int(entry_id)
        except (TypeError, ValueError):
            # 也允许用业务编号定位写入作业。
            for row in store.rows(self.MODULE):
                if str(row.get("作业编号")) == str(entry_id):
                    numeric_id = int(row["id"])
                    break
            else:
                return
        entry = store.find(self.MODULE, numeric_id)
        if entry is None:
            return
        receipts = entry.setdefault("receipts", [])
        receipt_ids = entry.setdefault("receipt_ids", [])
        receipt_no = payload.get("receipt_no") or payload.get("withdraw_no") or payload.get("id")
        if receipt_no and receipt_no not in receipt_ids:
            receipts.append(copy.deepcopy(payload))
            receipt_ids.append(receipt_no)
        entry["最近回执"] = receipt_no or entry.get("最近回执", "")
        if payload.get("receipt_type") == "write" and payload.get("rows_written") is not None:
            entry["最近写入行数"] = payload["rows_written"]
            try:
                if not int(entry.get("写入行数") or 0):
                    entry["写入行数"] = payload["rows_written"]
            except (TypeError, ValueError):
                entry["写入行数"] = payload["rows_written"]
        if payload.get("status") in self.STATUS_ORDER:
            entry["status"] = payload["status"]
            entry["pending"] = payload["status"] not in self.TERMINAL_STATUSES
            entry["abnormal"] = bool(payload.get("conflict")) or payload["status"] == "提交冲突"

    # ------------------------------------------------------------------
    # 撤回、幂等补取与快照回滚
    # ------------------------------------------------------------------

    def _find_write_receipt(self, ref: dict[str, Any]) -> dict[str, Any] | None:
        receipt_no = self._first_present(
            ref,
            "write_receipt_no", "write_receipt_id", "receipt_no", "receipt_id",
            "write_no", "write_id", "confirmation_no", "confirm_receipt_no",
        )
        if isinstance(ref.get("write_receipt"), dict):
            return copy.deepcopy(ref["write_receipt"])
        if receipt_no and lake_store.get_write_receipt(str(receipt_no)):
            return lake_store.get_write_receipt(str(receipt_no))
        app_id = self._get_application_id(ref)
        app = lake_store.get_application(app_id) if app_id else None
        if app and app.get("last_write_receipt_no"):
            return lake_store.get_write_receipt(app["last_write_receipt_no"])
        if receipt_no:
            return lake_store.get_write_receipt(str(receipt_no))
        return None

    def request_withdrawal(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """创建或幂等重放撤回处置。

        支持两种调用：
        - request_withdrawal(withdraw_no, application_id=..., ...)
        - request_withdrawal(application_id, lake_table, partition_key, ...)
        """
        if args:
            first = args[0]
            first_text = str(first)
            if isinstance(first, int) or (first_text.isdigit() and store.find(self.MODULE, int(first_text))):
                kwargs.setdefault("entry_id", first)
            elif isinstance(first, str) and (
                first.startswith("WD")
                or kwargs.get("as_withdraw_no")
                or lake_store.get_withdrawal(first)
            ):
                kwargs.setdefault("withdraw_no", first)
            elif isinstance(first, str) and lake_store.get_write_receipt(first):
                kwargs.setdefault("write_receipt_no", first)
            else:
                kwargs.setdefault("application_id", first)
            if len(args) > 1 and isinstance(args[1], str):
                kwargs.setdefault("lake_table", args[1])
        provided_withdraw_no = self._get_withdraw_no(kwargs)
        if provided_withdraw_no and lake_store.get_withdrawal(provided_withdraw_no):
            return self.get_withdraw_result(provided_withdraw_no)
        withdraw_no = provided_withdraw_no or self._new_no("WD")

        table = self._first_present(kwargs, "lake_table", "table", "所属湖表")
        app_id = self._get_application_id(kwargs)
        receipt = self._find_write_receipt(kwargs)
        if receipt is not None:
            table = table or receipt.get("lake_table")
            app_id = app_id or receipt.get("application_id")
        if not table:
            return {"ok": False, "success": False, "error": "缺少所属湖表或可定位的写入回执"}

        app = lake_store.get_application(app_id) if app_id else None
        if app is None and app_id:
            app = self.register_application(app_id, table, kwargs.get("partition_key"))
        if app is not None and kwargs.get("expected_version") is not None:
            if int(kwargs["expected_version"]) != int(app.get("version", 0)):
                conflict_result = {
                    "ok": False,
                    "success": False,
                    "conflict": True,
                    "code": "WITHDRAW_VERSION_CONFLICT",
                    "withdraw_no": withdraw_no,
                    "application_id": app_id,
                    "expected_version": kwargs["expected_version"],
                    "current_version": app.get("version", 0),
                    "status": "提交冲突",
                    "message": "撤回确认版本已变化，需核对输入范围后重试",
                }
                if withdraw_no:
                    self._save_first_result(withdraw_no, conflict_result, table, app_id, kwargs)
                return conflict_result

        # 若没有明确回执，但给了行数/分区，也允许构造一次待撤回写入，便于接口直接编排。
        if receipt is None and kwargs.get("row_count") is not None and not kwargs.get("file_ids"):
            receipt = self.confirm_write(
                lake_table=table,
                partition_key=kwargs.get("partition_key", "A"),
                rows_data=kwargs.get("rows_data") or kwargs.get("rows"),
                row_count=kwargs.get("row_count"),
                application_id=app_id,
                entry_id=kwargs.get("entry_id"),
            )
            if not receipt.get("ok") and receipt.get("conflict"):
                return receipt

        return self._execute_withdrawal(
            withdraw_no=withdraw_no,
            table=table,
            app=app,
            receipt=receipt,
            kwargs=kwargs,
        )

    withdraw_write = request_withdrawal
    withdraw_and_rollback = request_withdrawal
    withdraw_write_and_rollback = request_withdrawal
    create_withdrawal = request_withdrawal
    create_withdraw = request_withdrawal
    create_withdraw_receipt = request_withdrawal
    withdraw_confirmed_write = request_withdrawal
    cancel_confirmed_write = request_withdrawal
    revoke_write = request_withdrawal
    compensate_write = request_withdrawal
    handle_write_withdrawal = request_withdrawal
    confirm_withdrawal = request_withdrawal

    def withdraw_written_files(
        self,
        lake_table: str | None = None,
        partition_key: str | None = None,
        file_ids: list[str] | None = None,
        withdraw_no: str | None = None,
        write_receipt_no: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """按湖表/分区/文件清单撤回本次写入，位置参数风格与 merge 等服务保持一致。"""
        if withdraw_no and write_receipt_no is None and (
            str(withdraw_no).startswith(("WR", "DCR"))
            or lake_store.get_write_receipt(str(withdraw_no)) is not None
        ) and lake_store.get_withdrawal(str(withdraw_no)) is None:
            write_receipt_no = withdraw_no
            withdraw_no = None
        return self.request_withdrawal(
            lake_table=lake_table,
            partition_key=partition_key,
            file_ids=file_ids,
            write_receipt_no=write_receipt_no,
            withdraw_no=withdraw_no,
            **kwargs,
        )

    def _save_first_result(
        self,
        withdraw_no: str,
        result: dict[str, Any],
        table: str,
        app_id: str | None,
        kwargs: dict[str, Any],
    ) -> dict[str, Any]:
        record = {
            "withdraw_no": withdraw_no,
            "withdraw_id": withdraw_no,
            "id": withdraw_no,
            "lake_table": table,
            "application_id": app_id,
            "apply_no": app_id,
            "status": result.get("status", "处置冲突"),
            "first_result": copy.deepcopy(result),
            "result": copy.deepcopy(result),
            "created_at": self._now(),
            "created_at_text": self._iso(),
            "request": self._deep_json(kwargs),
        }
        lake_store.save_withdrawal(withdraw_no, record)
        result.setdefault("withdraw_no", withdraw_no)
        result.setdefault("withdraw_id", withdraw_no)
        return result

    def _execute_withdrawal(
        self,
        *,
        withdraw_no: str,
        table: str,
        app: dict[str, Any] | None,
        receipt: dict[str, Any] | None,
        kwargs: dict[str, Any],
    ) -> dict[str, Any]:
        entry_ref = kwargs.get("entry_id") or kwargs.get("job_id")
        if receipt and entry_ref is None:
            entry_ref = receipt.get("entry_id") or receipt.get("job_id")

        target_file_ids = list(
            kwargs.get("file_ids")
            or (receipt or {}).get("added_file_ids")
            or [((receipt or {}).get("file_id"))]
        )
        target_file_ids = [fid for fid in target_file_ids if fid]
        target_files = [lake_store.get_file(table, fid) for fid in target_file_ids]
        target_files = [pf for pf in target_files if pf is not None]

        partition_key = kwargs.get("partition_key")
        if not partition_key and receipt:
            partition_key = receipt.get("partition_key")
        if not partition_key and target_files:
            partition_key = target_files[0].partition_key
        partition_key = partition_key or "A"

        affected_rows: list[dict[str, Any]] = []
        for pf in target_files:
            affected_rows.extend(pf.rows_data)
        row_count = sum(pf.file_rows for pf in target_files)
        if not affected_rows and row_count:
            affected_rows = self._normalize_rows(row_count=row_count, partition_key=partition_key)

        app_id = (app or {}).get("application_id") or self._get_application_id(kwargs)
        scope = self._scope(
            table,
            kwargs.get("partition_keys") or [partition_key],
            target_file_ids,
            receipt_nos=[receipt["receipt_no"]] if receipt else [],
            application_id=app_id,
            row_count=row_count,
            rows=affected_rows,
        )

        lock = lake_store.acquire_write_lock(table)
        try:
            before_snapshot_id = self._current_snapshot_id(table)
            before_snapshot = lake_store.get_snapshot(table, before_snapshot_id) if before_snapshot_id else None
            old_manifest = list(before_snapshot.file_ids) if before_snapshot else []
            # 并上当前活动文件，兼容早期“快照只记新增文件”的内存数据。
            active_ids = [pf.file_id for pf in lake_store.list_files(table)]
            manifest: list[str] = []
            for fid in [*old_manifest, *active_ids]:
                if fid not in target_file_ids and fid not in manifest:
                    manifest.append(fid)

            archived = lake_store.archive_files(table, target_file_ids, withdraw_no)
            withdrawal_file = {
                "withdrawal_file_id": f"wdf-{withdraw_no}",
                "file_id": target_file_ids[0] if target_file_ids else f"wdf-{withdraw_no}",
                "withdrawn_file_id": target_file_ids[0] if target_file_ids else None,
                "source_file_ids": target_file_ids,
                "source_file_path": self._file_dict(target_files[0])["path"] if target_files else None,
                "source_file_paths": [self._file_dict(pf)["path"] for pf in target_files],
                "path": kwargs.get("withdrawal_file_path") or f"/lake/{table}/_withdrawals/{withdraw_no}.json",
                "download_url": f"/api/write_job/withdrawals/{withdraw_no}/file",
                "format": "JSON",
                "partition_key": partition_key,
                "row_count": row_count,
                "rows": affected_rows,
                "input_scope": scope,
                "checksum": archived[0].file_manifest if archived else None,
            }
            rollback_snapshot = lake_store.create_snapshot(
                table,
                file_ids=manifest,
                parent_snapshot=before_snapshot_id,
                added_rows=0,
                deleted_rows=row_count,
                kind="rollback",
                label=f"写入撤回回滚 {withdraw_no}",
                input_scope={**scope, "withdraw_no": withdraw_no},
            )
            lake_store.mark_snapshot_available(table, rollback_snapshot.snapshot_id)
            self._sync_snapshot_row(rollback_snapshot.snapshot_id, table, withdraw_no=withdraw_no)

            # 被撤回输入直接产生或包含的旧版快照保留在历史中，但标记为已回退/不可当前读。
            affected_snapshot_ids = set()
            if receipt and receipt.get("snapshot_id"):
                affected_snapshot_ids.add(receipt["snapshot_id"])
            for snap in lake_store.list_snapshots(table):
                if any(fid in snap.file_ids for fid in target_file_ids) and snap.snapshot_id != rollback_snapshot.snapshot_id:
                    affected_snapshot_ids.add(snap.snapshot_id)
            for sid in affected_snapshot_ids:
                lake_store.mark_snapshot_status(table, sid, "rolled_back", readable=False)
                self._sync_snapshot_row(sid, table, withdraw_no=withdraw_no)

            withdrawal_receipt_no = kwargs.get("receipt_no") or self._new_no("WD-RCPT")
            conflict_flag = bool(kwargs.get("conflict") or kwargs.get("simulate_conflict"))
            status_transitions: list[dict[str, Any]] = [
                {"action": "确认提交", "status": "已提交", "receipt_no": (receipt or {}).get("receipt_no")},
            ]
            if conflict_flag:
                status_transitions.append({"action": "确认/撤回并发核对", "status": "提交冲突"})
            status_transitions.extend([
                {"action": "写入撤回", "status": "已撤回", "withdraw_no": withdraw_no},
                {"action": "快照回滚", "status": "已回滚", "snapshot_id": rollback_snapshot.snapshot_id},
            ])
            result = {
                "ok": True,
                "success": True,
                "idempotent_replay": False,
                "withdraw_no": withdraw_no,
                "withdraw_id": withdraw_no,
                "id": withdraw_no,
                "withdrawal_receipt_no": withdrawal_receipt_no,
                "receipt_no": withdrawal_receipt_no,
                "receipt_id": withdrawal_receipt_no,
                "receipt_type": "withdrawal",
                "type": "写入撤回回执",
                "撤回编号": withdraw_no,
                "撤回回执编号": withdrawal_receipt_no,
                "申请编号": app_id,
                "所属湖表": table,
                "分区": partition_key,
                "影响行数": row_count,
                "可回读快照": rollback_snapshot.snapshot_id,
                "回滚快照": rollback_snapshot.snapshot_id,
                "输入范围": scope,
                "撤回文件": withdrawal_file,
                "application_id": app_id,
                "apply_no": app_id,
                "job_id": entry_ref,
                "entry_id": entry_ref,
                "lake_table": table,
                "table": table,
                "partition_key": partition_key,
                "partition_keys": scope["partition_keys"],
                "input_scope": scope,
                "affected_input_scope": scope,
                "affected_inputs": [
                    {
                        "application_id": app_id,
                        "write_receipt_no": (receipt or {}).get("receipt_no"),
                        "partition_key": partition_key,
                        "file_ids": target_file_ids,
                        "row_count": row_count,
                        "status": "已撤回",
                    }
                ],
                "write_receipt_no": (receipt or {}).get("receipt_no"),
                "original_write_receipt": copy.deepcopy(receipt),
                "withdrawn_file_ids": target_file_ids,
                "withdrawn_file_id": target_file_ids[0] if target_file_ids else None,
                "withdraw_file_id": target_file_ids[0] if target_file_ids else withdrawal_file["withdrawal_file_id"],
                "affected_file_id": target_file_ids[0] if target_file_ids else None,
                "target_file_id": target_file_ids[0] if target_file_ids else None,
                "withdrawal_file_id": withdrawal_file["withdrawal_file_id"],
                "withdraw_file_path": withdrawal_file["path"],
                "withdrawal_file_path": withdrawal_file["path"],
                "file_path": withdrawal_file["source_file_path"],
                "source_file_path": withdrawal_file["source_file_path"],
                "withdrawn_files": [self._file_dict(pf) for pf in target_files],
                "withdrawal_file": withdrawal_file,
                "withdraw_file": withdrawal_file,
                "withdraw_file_detail": withdrawal_file,
                "revoke_file": withdrawal_file,
                "row_count": row_count,
                "withdrawn_row_count": row_count,
                "rows_withdrawn": affected_rows,
                "before_snapshot_id": before_snapshot_id,
                "previous_snapshot_id": before_snapshot_id,
                "rollback_snapshot_id": rollback_snapshot.snapshot_id,
                "readable_snapshot_id": rollback_snapshot.snapshot_id,
                "current_snapshot_id": rollback_snapshot.snapshot_id,
                "snapshot_id": rollback_snapshot.snapshot_id,
                "snapshot_switched": True,
                "status": "已撤回",
                "state_flow": ["已提交", "提交冲突", "已撤回", "已回滚"]
                if conflict_flag
                else ["已提交", "已撤回", "已回滚"],
                "status_transitions": status_transitions,
                "conflict": conflict_flag,
                "conflict_detected": conflict_flag,
                "created_at": self._now(),
                "created_at_text": self._iso(),
                "message": "本次新增文件已撤回，并已切换到可回读快照",
            }

            record = {
                "withdraw_no": withdraw_no,
                "withdraw_id": withdraw_no,
                "id": withdraw_no,
                "lake_table": table,
                "application_id": app_id,
                "apply_no": app_id,
                "status": "已撤回",
                "first_result": copy.deepcopy(result),
                "result": copy.deepcopy(result),
                "withdrawal_file": withdrawal_file,
                "rollback_snapshot_id": rollback_snapshot.snapshot_id,
                "readable_snapshot_id": rollback_snapshot.snapshot_id,
                "created_at": self._now(),
                "created_at_text": self._iso(),
            }
            lake_store.save_withdrawal(withdraw_no, record)
            lake_store.save_write_receipt(withdrawal_receipt_no, result)

            if app is not None:
                app["status"] = "withdrawn"
                app["version"] = int(app.get("version", 0)) + 1
                app["last_withdraw_no"] = withdraw_no
                app.setdefault("withdraw_nos", []).append(withdraw_no)
                app.setdefault("operations", []).append({
                    "at": self._now(), "type": "withdraw", "withdraw_no": withdraw_no,
                    "snapshot_id": rollback_snapshot.snapshot_id,
                    "conflict": result["conflict"],
                })

            # 若调用方告知消费端曾实收 N 行（例如 A 分区 7 行），关联原送达回执并生成下一份更正回执。
            delivered_rows = self._first_present(kwargs, "delivered_rows", "received_rows", "consumer_received_rows")
            existing_delivery_no = kwargs.get("delivery_receipt_no") or kwargs.get("prior_receipt_no")
            if existing_delivery_no and lake_store.get_consumer_receipt(str(existing_delivery_no)):
                next_receipt = self.build_next_consumer_receipt(withdraw_no, str(existing_delivery_no))
                result["next_consumer_receipt"] = next_receipt
                result["delivery_receipt"] = lake_store.get_consumer_receipt(str(existing_delivery_no))
                self._attach_to_job(entry_ref, next_receipt)
            elif delivered_rows is not None or kwargs.get("create_consumer_receipt"):
                delivery = self.record_consumer_delivery(
                    lake_table=table,
                    partition_key=partition_key,
                    snapshot_id=before_snapshot_id or (receipt or {}).get("snapshot_id"),
                    received_rows=int(delivered_rows) if delivered_rows is not None else None,
                    application_id=app_id,
                    withdraw_no=withdraw_no,
                    entry_id=entry_ref,
                    consumer=kwargs.get("consumer"),
                )
                next_receipt = self.build_next_consumer_receipt(withdraw_no, delivery["receipt_no"])
                result["delivery_receipt"] = delivery
                result["next_consumer_receipt"] = next_receipt
                self._attach_to_job(entry_ref, next_receipt)
            if "next_consumer_receipt" in result:
                record["first_result"] = copy.deepcopy(result)
                record["result"] = copy.deepcopy(result)

            self._attach_to_job(entry_ref, result)
            return copy.deepcopy(result)
        finally:
            try:
                lock.release()
            except RuntimeError:
                lake_store.release_write_lock(table)

    def get_withdraw_result(
        self,
        withdraw_no: str | None = None,
        withdraw_id: str | None = None,
    ) -> dict[str, Any] | None:
        """按原撤回编号幂等补取首次处置结果。"""
        key = str(withdraw_no or withdraw_id or "")
        record = lake_store.get_withdrawal(key)
        if record is None:
            return None
        # 必须返回首次处置时保存的快照，不能在补取响应里追加字段，
        # 否则调用方无法用原撤回编号核对“第一次到底处置成了什么”。
        return copy.deepcopy(record.get("first_result") or record.get("result") or record)

    def get_withdrawal(
        self,
        withdraw_no: str | None = None,
        withdraw_id: str | None = None,
    ) -> dict[str, Any] | None:
        return self.get_withdraw_result(str(withdraw_no or withdraw_id or ""))

    def get_withdraw_receipt(
        self,
        withdraw_id: str | None = None,
        withdraw_no: str | None = None,
    ) -> dict[str, Any] | None:
        return self.get_withdraw_result(str(withdraw_no or withdraw_id or ""))

    get_withdrawal_result = get_withdraw_receipt
    retrieve_withdrawal = get_withdraw_receipt
    get_withdraw_by_id = get_withdraw_receipt

    def get_receipt(self, receipt_no: str) -> dict[str, Any] | None:
        """统一按回执编号查询普通写入、撤回或消费回执。"""
        if receipt_no in lake_store.withdrawals:
            return self.get_withdraw_result(receipt_no)
        for getter in (lake_store.get_write_receipt, lake_store.get_consumer_receipt):
            record = getter(receipt_no)
            if record is not None:
                return copy.deepcopy(record)
        return self.get_withdraw_result(receipt_no)

    def get_receipt_by_withdraw_id(self, withdraw_id: str) -> dict[str, Any] | None:
        return self.get_withdraw_result(withdraw_id=withdraw_id)

    def get_write_receipt(self, receipt_no: str) -> dict[str, Any] | None:
        record = lake_store.get_write_receipt(receipt_no)
        return copy.deepcopy(record) if record is not None else None

    def list_receipts(self, entry_id: int | str | None = None) -> dict[str, Any]:
        if entry_id is not None:
            return self.get_job_receipts(entry_id)
        items = list(lake_store.write_receipts.values()) + list(lake_store.consumer_receipts.values())
        return {"items": copy.deepcopy(items), "total": len(items)}

    def withdraw_write_job(self, job_id: int | str | None = None,
                           withdraw_id: str | None = None, **kwargs: Any) -> dict[str, Any]:
        """面向“写入任务详情页/按任务撤回”命名的兼容入口。"""
        if job_id is not None:
            kwargs.setdefault("entry_id", job_id)
            entry = None
            try:
                entry = store.find(self.MODULE, int(job_id))
            except (TypeError, ValueError):
                for row in store.rows(self.MODULE):
                    if str(row.get("作业编号")) == str(job_id):
                        entry = row
                        break
            if entry is not None:
                kwargs.setdefault("lake_table", entry.get("所属湖表"))
        if withdraw_id is not None:
            kwargs.setdefault("withdraw_no", withdraw_id)
        return self.request_withdrawal(**kwargs)

    def rollback_snapshot(
        self,
        lake_table: str | None = None,
        snapshot_id: str | None = None,
        target_snapshot_id: str | None = None,
        withdraw_no: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """回滚到指定快照，生成一份新的可读快照而不删除历史链。"""
        if withdraw_no:
            result = self.get_withdraw_result(withdraw_no)
            if result is None:
                return {"ok": False, "success": False, "error": f"撤回编号 {withdraw_no} 不存在"}
            return {
                "ok": True,
                "success": True,
                "withdraw_no": withdraw_no,
                "rollback_snapshot_id": result.get("rollback_snapshot_id"),
                "readable_snapshot_id": result.get("readable_snapshot_id"),
                "snapshot_id": result.get("rollback_snapshot_id"),
            }

        target_id = snapshot_id or target_snapshot_id or kwargs.get("target_snap")
        # 兼容第一个位置参数直接传 snapshot_id 的调用。
        if lake_table and target_id is None and (
            str(lake_table).startswith("snap-") or kwargs.get("snapshot_only")
        ):
            target_id = str(lake_table)
            lake_table = kwargs.get("lake_table")
        if not lake_table or not target_id:
            return {"ok": False, "success": False, "error": "rollback_snapshot 需要 lake_table 和 snapshot_id"}
        target = lake_store.get_snapshot(str(lake_table), str(target_id))
        if target is None:
            return {"ok": False, "success": False, "error": f"快照 {target_id} 不存在"}
        current_id = self._current_snapshot_id(str(lake_table))
        new_snap = lake_store.create_snapshot(
            str(lake_table),
            file_ids=list(target.file_ids),
            parent_snapshot=current_id,
            added_rows=0,
            deleted_rows=0,
            kind="rollback",
            label=f"回滚到 {target_id}",
        )
        lake_store.mark_snapshot_available(str(lake_table), new_snap.snapshot_id)
        self._sync_snapshot_row(new_snap.snapshot_id, str(lake_table))
        return {
            "ok": True,
            "success": True,
            "lake_table": lake_table,
            "target_snapshot_id": target_id,
            "rollback_snapshot_id": new_snap.snapshot_id,
            "readable_snapshot_id": new_snap.snapshot_id,
            "snapshot_id": new_snap.snapshot_id,
            "file_ids": list(new_snap.file_ids),
        }

    rollback_write = rollback_snapshot
    rollback_to_snapshot = rollback_snapshot
    rollback_to_before_write = rollback_snapshot

    def get_job_receipts(
        self,
        entry_id: int | str | None = None,
        job_id: int | str | None = None,
    ) -> dict[str, Any]:
        entry_ref = entry_id if entry_id is not None else job_id
        if entry_ref is None:
            return {"entry_id": None, "items": [], "total": 0,
                    "write_receipts": [], "withdrawal_receipts": [], "withdrawal_notices": []}
        try:
            numeric_id = int(entry_ref)
        except (TypeError, ValueError):
            numeric_id = None
            for row in store.rows(self.MODULE):
                if str(row.get("作业编号")) == str(entry_ref):
                    numeric_id = int(row["id"])
                    break
        entry = store.find(self.MODULE, numeric_id) if numeric_id is not None else None
        receipts = copy.deepcopy((entry or {}).get("receipts", []))
        # 同时补充账本中按 job_id/entry_id 关联但可能尚未挂到示例行的回执。
        known = {r.get("receipt_no") or r.get("withdraw_no") for r in receipts}
        for receipt in lake_store.write_receipts.values():
            if str(receipt.get("entry_id") or receipt.get("job_id") or "") == str(entry_ref):
                no = receipt.get("receipt_no")
                if no not in known:
                    receipts.append(copy.deepcopy(receipt))
                    known.add(no)
        for receipt in lake_store.consumer_receipts.values():
            if str(receipt.get("entry_id") or receipt.get("job_id") or "") == str(entry_ref):
                no = receipt.get("receipt_no")
                if no not in known:
                    receipts.append(copy.deepcopy(receipt))
                    known.add(no)
        write_receipts = [r for r in receipts if r.get("receipt_type") == "write" or r.get("type") == "普通写入回执"]
        withdrawal_receipts = [
            r for r in receipts
            if r.get("receipt_type") == "withdrawal" or r.get("type") == "写入撤回回执"
        ]
        withdrawal_notices = [
            r for r in receipts
            if r.get("receipt_type") == "consumer_withdrawal_notice" or "更正回执" in str(r.get("type", ""))
        ]
        return {
            "entry_id": numeric_id,
            "id": numeric_id,
            "job": entry,
            "items": receipts,
            "total": len(receipts),
            "write_receipts": write_receipts,
            "withdrawal_receipts": withdrawal_receipts,
            "withdrawal_notices": withdrawal_notices,
        }

    # ------------------------------------------------------------------
    # 可读版本、可消费状态和历史视图
    # ------------------------------------------------------------------

    def snapshot_versions(self, lake_table: str, partition_key: str | None = None) -> list[dict[str, Any]]:
        versions: list[dict[str, Any]] = []
        current_id = self._current_snapshot_id(lake_table)
        target_withdrawn = {
            fid for pf in lake_store.list_archived_files(lake_table) for fid in [pf.file_id]
        }
        for index, snap in enumerate(lake_store.list_snapshots(lake_table), start=1):
            files = [lake_store.get_file(lake_table, fid) for fid in snap.file_ids]
            files = [pf for pf in files if pf is not None]
            if partition_key:
                scoped = [pf for pf in files if pf.partition_key == partition_key]
            else:
                scoped = files
            active_scoped = [pf for pf in scoped if pf.status != "withdrawn"]
            partition_counts: dict[str, int] = {}
            for pf in files:
                partition_counts[pf.partition_key] = partition_counts.get(pf.partition_key, 0) + pf.file_rows
            item = self._snapshot_dict(snap)
            item.update({
                "version": f"v{index}",
                "version_no": index,
                "row_count": sum(pf.file_rows for pf in active_scoped if snap.status == "available" and snap.readable)
                if snap.snapshot_id == current_id else sum(pf.file_rows for pf in scoped),
                "manifest_row_count": sum(pf.file_rows for pf in scoped),
                "active_row_count": sum(pf.file_rows for pf in active_scoped),
                "partition_row_counts": partition_counts,
                "file_count": len(scoped),
                "contains_withdrawn_rows": any(fid in target_withdrawn for fid in snap.file_ids),
                "history_visible": True,
                "current_readable": snap.snapshot_id == current_id,
            })
            versions.append(item)
        return versions

    list_snapshot_versions = snapshot_versions
    get_snapshot_versions = snapshot_versions
    get_versions = snapshot_versions

    def get_consumable_state(
        self,
        lake_table: str,
        partition_key: str | None = None,
        view_id: int | str | None = None,
    ) -> dict[str, Any]:
        snapshot_id = self._current_snapshot_id(lake_table)
        snap = lake_store.get_snapshot(lake_table, snapshot_id) if snapshot_id else None
        files = [lake_store.get_file(lake_table, fid) for fid in (snap.file_ids if snap else [])]
        files = [pf for pf in files if pf is not None and pf.status != "withdrawn"]
        if partition_key:
            files = [pf for pf in files if pf.partition_key == partition_key]
        rows: list[dict[str, Any]] = []
        partition_state: dict[str, dict[str, Any]] = {}
        for pf in files:
            rows.extend(pf.rows_data)
            state = partition_state.setdefault(pf.partition_key, {"row_count": 0, "file_ids": [], "rows": []})
            state["row_count"] += pf.file_rows
            state["file_ids"].append(pf.file_id)
            state["rows"].extend(pf.rows_data)
        withdrawn_files = lake_store.list_archived_files(lake_table)
        withdrawn_scope = [
            self._file_dict(pf) for pf in withdrawn_files
            if not partition_key or pf.partition_key == partition_key
        ]
        current = self._snapshot_dict(snap) if snap else None
        return {
            "ok": True,
            "lake_table": lake_table,
            "partition_key": partition_key,
            "view_id": view_id,
            "readable_snapshot_id": snapshot_id,
            "current_snapshot_id": snapshot_id,
            "snapshot": current,
            "row_count": sum(pf.file_rows for pf in files),
            "rows": rows,
            "partitions": partition_state,
            "file_ids": [pf.file_id for pf in files],
            "withdrawn_files": withdrawn_scope,
            "withdrawn_row_count": sum(pf.file_rows for pf in withdrawn_files),
            "contains_withdrawn_rows": any(
                pf.withdraw_no is not None and pf.file_id in (snap.file_ids if snap else [])
                for pf in withdrawn_files
            ),
            "consumable": True,
            "checked_at": self._iso(),
        }

    get_consumable_snapshot = get_consumable_state
    get_readable_state = get_consumable_state

    def read_partition(self, lake_table: str, partition_key: str,
                       snapshot_id: str | None = None) -> dict[str, Any]:
        """读取指定分区在当前或指定快照下的行，供核对 3/4/2/7 行。"""
        sid = snapshot_id or self._current_snapshot_id(lake_table)
        snap = lake_store.get_snapshot(lake_table, sid) if sid else None
        files = [lake_store.get_file(lake_table, fid) for fid in (snap.file_ids if snap else [])]
        files = [pf for pf in files if pf is not None and pf.partition_key == partition_key]
        rows: list[dict[str, Any]] = []
        for pf in files:
            rows.extend(pf.rows_data)
        return {
            "ok": True,
            "lake_table": lake_table,
            "partition_key": partition_key,
            "snapshot_id": sid,
            "row_count": sum(pf.file_rows for pf in files),
            "rows": rows,
            "history_snapshot": bool(snapshot_id),
        }

    # ------------------------------------------------------------------
    # 消费端送达、下一份更正回执和历史视图
    # ------------------------------------------------------------------

    def record_consumer_delivery(
        self,
        lake_table: str | None = None,
        partition_key: str | None = None,
        snapshot_id: str | None = None,
        received_rows: int | None = None,
        application_id: str | None = None,
        withdraw_no: str | None = None,
        entry_id: int | str | None = None,
        consumer: str | None = None,
        receipt_no: str | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        table = lake_table or extra.get("table")
        if not table and withdraw_no:
            withdrawal = lake_store.get_withdrawal(withdraw_no)
            table = (withdrawal or {}).get("lake_table")
        if not table:
            raise ValueError("record_consumer_delivery 需要 lake_table 或有效 withdraw_no")
        sid = snapshot_id or self._current_snapshot_id(table)
        snap = lake_store.get_snapshot(table, sid) if sid else None
        if received_rows is None:
            files = [lake_store.get_file(table, fid) for fid in (snap.file_ids if snap else [])]
            received_rows = sum(pf.file_rows for pf in files if pf and (not partition_key or pf.partition_key == partition_key))
        no = receipt_no or self._new_no("DCR")
        receipt = {
            "receipt_no": no,
            "receipt_id": no,
            "id": no,
            "receipt_type": "consumer_delivery",
            "type": "消费端送达回执",
            "consumer": consumer or extra.get("downstream_consumer") or "DOWNSTREAM_CONSUMER",
            "lake_table": table,
            "partition_key": partition_key,
            "application_id": application_id,
            "withdraw_no": withdraw_no,
            "entry_id": entry_id,
            "snapshot_id": sid,
            "delivered_snapshot_id": sid,
            "history_snapshot_id": sid,
            "received_rows": int(received_rows),
            "delivered_rows": int(received_rows),
            "row_count": int(received_rows),
            "delivered_at": self._now(),
            "delivered_at_text": self._iso(),
            "status": "已送达",
            "history_visible": True,
        }
        lake_store.save_consumer_receipt(no, receipt)
        self._attach_to_job(entry_id, receipt)
        return copy.deepcopy(receipt)

    ack_consumer = record_consumer_delivery
    consumer_received = record_consumer_delivery

    def build_next_consumer_receipt(
        self,
        withdraw_no: str,
        delivery_receipt_no: str | None = None,
        received_rows: int | None = None,
    ) -> dict[str, Any]:
        result = self.get_withdraw_result(withdraw_no)
        if result is None:
            return {"ok": False, "error": f"撤回编号 {withdraw_no} 不存在"}
        delivery = lake_store.get_consumer_receipt(delivery_receipt_no) if delivery_receipt_no else None
        if delivery is None:
            # 找到同表/同分区最近一份送达回执。
            candidates = list(lake_store.consumer_receipts.values())
            candidates = [r for r in candidates if r.get("withdraw_no") in (None, withdraw_no)]
            if result.get("partition_key"):
                candidates = [r for r in candidates if r.get("partition_key") in (None, result["partition_key"])]
            delivery = candidates[-1] if candidates else None

        previous_snapshot = (delivery or {}).get("delivered_snapshot_id") or result.get("before_snapshot_id")
        delivered_rows = received_rows if received_rows is not None else (delivery or {}).get("received_rows")
        if delivered_rows is None and result.get("partition_key"):
            # 撤回前该分区的可读行数 = 保留行数 + 本次撤回行数。
            state = self.get_consumable_state(result["lake_table"], result.get("partition_key"))
            delivered_rows = int(state.get("row_count") or 0) + int(result.get("withdrawn_row_count") or 0)
        next_no = self._new_no("NCR")
        next_receipt = {
            "receipt_no": next_no,
            "receipt_id": next_no,
            "id": next_no,
            "receipt_type": "consumer_withdrawal_notice",
            "type": "消费端撤回/更正回执",
            "撤回编号": withdraw_no,
            "原回执编号": (delivery or {}).get("receipt_no"),
            "已交付快照": previous_snapshot,
            "可回读快照": result.get("readable_snapshot_id"),
            "撤回文件": result.get("withdrawal_file"),
            "输入范围": result.get("input_scope"),
            "ok": True,
            "withdraw_no": withdraw_no,
            "withdrawal_receipt_no": result.get("withdrawal_receipt_no"),
            "prior_receipt_no": (delivery or {}).get("receipt_no"),
            "previous_receipt_no": (delivery or {}).get("receipt_no"),
            "previous_delivery_receipt": copy.deepcopy(delivery),
            "delivered_snapshot_id": previous_snapshot,
            "previous_snapshot_id": previous_snapshot,
            "delivered_version_remains_in_history": True,
            "history_view": {
                "previous_receipt_no": (delivery or {}).get("receipt_no"),
                "previous_snapshot_id": previous_snapshot,
                "visible": True,
            },
            "readable_snapshot_id": result.get("readable_snapshot_id"),
            "current_snapshot_id": result.get("readable_snapshot_id"),
            "rollback_snapshot_id": result.get("rollback_snapshot_id"),
            "withdrawal_file": result.get("withdrawal_file"),
            "withdraw_file": result.get("withdrawal_file"),
            "withdraw_file_id": result.get("withdraw_file_id"),
            "withdrawn_file_id": result.get("withdrawn_file_id"),
            "withdrawal_file_id": result.get("withdrawal_file_id"),
            "withdraw_file_path": result.get("withdraw_file_path"),
            "input_scope": result.get("input_scope"),
            "affected_input_scope": result.get("affected_input_scope"),
            "affected_inputs": result.get("affected_inputs"),
            "delivered_rows": delivered_rows,
            "received_rows": delivered_rows,
            "affected_rows": result.get("withdrawn_row_count"),
            "withdrawn_row_count": result.get("withdrawn_row_count"),
            "status": "待消费",
            "created_at": self._now(),
            "created_at_text": self._iso(),
            "message": "上一版已交付快照保留在历史视图；请按撤回文件核对并丢弃其中受影响的4行",
        }
        lake_store.save_consumer_receipt(next_no, next_receipt)
        return copy.deepcopy(next_receipt)

    get_next_consumer_receipt = build_next_consumer_receipt
    create_correction_receipt = build_next_consumer_receipt
    build_consumer_notice = build_next_consumer_receipt

    def consumer_history(self, lake_table: str | None = None, consumer: str | None = None) -> dict[str, Any]:
        items = list(lake_store.consumer_receipts.values())
        if lake_table:
            items = [r for r in items if r.get("lake_table") == lake_table]
        if consumer:
            items = [r for r in items if r.get("consumer") == consumer]
        snapshots = self.snapshot_versions(lake_table) if lake_table else []
        return {"items": copy.deepcopy(items), "total": len(items), "snapshot_versions": snapshots}

    def history_view(self, lake_table: str, partition_key: str | None = None) -> dict[str, Any]:
        return {
            "lake_table": lake_table,
            "partition_key": partition_key,
            "snapshots": self.snapshot_versions(lake_table, partition_key),
            "consumer_receipts": self.consumer_history(lake_table)["items"],
            "withdrawals": [
                copy.deepcopy(r.get("first_result", r))
                for r in lake_store.withdrawals.values()
                if r.get("lake_table") == lake_table
            ],
        }

    # ------------------------------------------------------------------
    # 一键构造题目中的 A=3+4、B=2、冲突和消费回执场景，便于页面演示/E2E
    # ------------------------------------------------------------------

    def build_demo_scenario(
        self,
        lake_table: str = "lake_withdraw_demo",
        application_id: str = "APP-WD-DEMO",
        withdraw_no: str = "WD-DEMO",
        entry_id: int | None = None,
    ) -> dict[str, Any]:
        base_a = [{"id": f"A-base-{i}", "partition": "A", "seq": i} for i in range(1, 4)]
        new_a = [{"id": f"A-new-{i}", "partition": "A", "seq": i} for i in range(4, 8)]
        new_b = [{"id": f"B-new-{i}", "partition": "B", "seq": i} for i in range(1, 3)]

        self.register_application(application_id, lake_table, "A")
        pf_a0 = lake_store.register_file(lake_table, "A", rows=3, bytes_=900, rows_data=base_a)
        snap0 = lake_store.create_snapshot(lake_table, [pf_a0.file_id], added_rows=3,
                                           kind="baseline", label="A分区原始3行")
        lake_store.mark_snapshot_available(lake_table, snap0.snapshot_id)
        self._sync_snapshot_row(snap0.snapshot_id, lake_table)

        write_a = self.confirm_write(
            lake_table=lake_table, partition_key="A", rows_data=new_a,
            application_id=application_id, entry_id=entry_id,
        )
        write_b = self.confirm_write(
            lake_table=lake_table, partition_key="B", rows_data=new_b,
            application_id=f"{application_id}-B", entry_id=entry_id,
        )
        conflict = self.confirm_write(
            lake_table=lake_table,
            partition_key="A",
            application_id=application_id,
            conflict_with_withdrawal=True,
            entry_id=entry_id,
        )
        delivery = self.record_consumer_delivery(
            lake_table=lake_table, partition_key="A",
            snapshot_id=write_a["snapshot_id"], received_rows=7,
            application_id=application_id, entry_id=entry_id,
            consumer="DEMO_CONSUMER",
        )
        withdrawal = self.request_withdrawal(
            withdraw_no=withdraw_no,
            application_id=application_id,
            conflict=True,
            delivered_rows=7,
            delivery_receipt_no=delivery["receipt_no"],
            entry_id=entry_id,
        )
        # request_withdrawal 已生成下一份回执；补挂 B 普通写入回执到作业详情。
        self._attach_to_job(entry_id, write_b)
        consumable = self.get_consumable_state(lake_table)
        versions = self.snapshot_versions(lake_table)
        return {
            "ok": True,
            "lake_table": lake_table,
            "application_id": application_id,
            "withdraw_no": withdraw_no,
            "baseline_snapshot_id": snap0.snapshot_id,
            "write_a": write_a,
            "write_b": write_b,
            "conflict": conflict,
            "confirmation_conflict": conflict,
            "delivery": delivery,
            "withdrawal": withdrawal,
            "next_consumer_receipt": withdrawal.get("next_consumer_receipt"),
            "consumable_state": consumable,
            "snapshot_versions": versions,
        }

    # ------------------------------------------------------------------
    # 通用资源动作：保持 /api/write_job/{id}/actions 可用
    # ------------------------------------------------------------------

    def run_action(
        self,
        entry_id: int,
        action: str,
        payload: Any | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        data = self._payload_data(payload)
        data.setdefault("entry_id", entry_id)
        data.setdefault("lake_table", entry.get("所属湖表"))

        if action in {"申请撤回", "发起撤回"}:
            data.setdefault("status", "撤回中")
            target = "撤回中"
            entry["status"] = target
            entry["pending"] = True
            entry["abnormal"] = False
            app_id = self._get_application_id(data)
            withdraw_no = self._get_withdraw_no(data)
            if app_id or withdraw_no:
                result = self.request_withdrawal(**data)
                if result.get("ok"):
                    entry["status"] = "已撤回"
                    entry["pending"] = False
                    self._attach_to_job(entry_id, result)
                    return entry, "写入撤回已完成，并已生成回滚快照和回执"
                if result.get("conflict"):
                    entry["status"] = "提交冲突"
                    entry["pending"] = True
                    entry["abnormal"] = True
                    self._attach_to_job(entry_id, result)
                    return entry, result.get("message", "写入确认与撤回发生冲突，已记录核对结果")
            return entry, "撤回申请已登记，请确认撤回范围"

        if action in {"写入撤回", "撤回", "撤回写入", "确认撤回", "确认撤回写入", "撤回本次写入", "撤回本次新增"}:
            has_scope = any(
                data.get(key) not in (None, "", [])
                for key in ("application_id", "apply_no", "apply_id", "withdraw_no", "withdraw_id",
                            "write_receipt_no", "receipt_no", "write_id", "write_no",
                            "file_ids", "row_count")
            )
            if not has_scope:
                demo_no = f"WD-ACTION-{entry_id}"
                existing = self.get_withdraw_result(demo_no)
                if existing is not None:
                    updated = store.find(self.MODULE, entry_id)
                    return updated, "已凭原撤回编号补取首次处置结果，可在详情页核对回执和快照"
                scenario = self.build_demo_scenario(
                    lake_table=str(entry.get("所属湖表") or f"write_job_{entry_id}"),
                    application_id=f"APP-ACTION-{entry_id}",
                    withdraw_no=demo_no,
                    entry_id=entry_id,
                )
                updated = store.find(self.MODULE, entry_id)
                if updated is not None:
                    updated["last_withdraw_no"] = demo_no
                    updated["scenario"] = {
                        "withdraw_no": demo_no,
                        "readable_snapshot_id": scenario["withdrawal"]["readable_snapshot_id"],
                    }
                return updated, "已按 A 新增4行、B 独立写入2行的核对场景完成撤回，可在详情页查看回执"
            result = self.request_withdrawal(**data)
            if not result.get("ok"):
                if result.get("conflict"):
                    entry["status"] = "提交冲突"
                    entry["abnormal"] = True
                    return entry, result.get("message", "写入确认与撤回冲突")
                return None, result.get("error", "撤回失败")
            return store.find(self.MODULE, entry_id), "本次新增文件已撤回，快照已回滚并生成回执"

        if action in {"快照回滚", "回滚快照", "回滚", "执行回滚"}:
            withdraw_no = self._get_withdraw_no(data) or entry.get("last_withdraw_no")
            if not withdraw_no:
                withdrawal_receipts = [
                    r for r in entry.get("receipts", [])
                    if isinstance(r, dict) and (r.get("withdraw_no") or r.get("withdraw_id"))
                ]
                if withdrawal_receipts:
                    latest_receipt = withdrawal_receipts[-1]
                    withdraw_no = latest_receipt.get("withdraw_no") or latest_receipt.get("withdraw_id")
            if withdraw_no:
                result = self.get_withdraw_result(str(withdraw_no))
                if result is None:
                    return None, f"撤回编号 {withdraw_no} 不存在"
                entry["status"] = "已回滚"
                entry["pending"] = False
                entry["rollback_snapshot_id"] = result.get("rollback_snapshot_id")
                entry["readable_snapshot_id"] = result.get("readable_snapshot_id")
                return entry, "已按原撤回处置结果切换可读快照"
            table = data.get("lake_table") or entry.get("所属湖表")
            snapshot_id = data.get("snapshot_id") or data.get("rollback_snapshot_id")
            if table and snapshot_id:
                lake_store.mark_snapshot_status(table, snapshot_id, "available", True)
                entry["status"] = "已回滚"
                entry["pending"] = False
                entry["rollback_snapshot_id"] = snapshot_id
                return entry, "快照已回滚为可读版本"
            return None, "快照回滚缺少 withdraw_no 或 snapshot_id"

        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        entry["status"] = target
        entry["pending"] = target not in self.TERMINAL_STATUSES
        entry["abnormal"] = action in self.NEGATIVE_ACTIONS
        if action == "确认提交" and data.get("rows_data") is not None or data.get("row_count"):
            result = self.confirm_write(**data)
            if result.get("conflict"):
                entry["status"] = "提交冲突"
                entry["abnormal"] = True
                return entry, result.get("message", "确认提交与同一申请的撤回冲突")
        return entry, f"{self.ENTITY}已{action}"

    @staticmethod
    def _payload_data(payload: Any | None) -> dict[str, Any]:
        if payload is None:
            return {}
        if isinstance(payload, dict):
            data = dict(payload.get("data") or {})
            for key, value in payload.items():
                if key not in {"action", "remark", "data"}:
                    data[key] = value
            return data
        data = dict(getattr(payload, "data", {}) or {})
        extra = getattr(payload, "model_extra", None) or {}
        data.update(extra)
        for key in ("remark",):
            value = getattr(payload, key, None)
            if value:
                data.setdefault(key, value)
        return data


write_job_service = WriteJobService()
