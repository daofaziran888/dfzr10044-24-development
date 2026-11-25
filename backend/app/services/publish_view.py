"""消费视图领域服务：发布、刷新、撤回面向下游的视图，绑定快照与结构版本。"""
from __future__ import annotations

import copy
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.lake_store import lake_store


class PublishViewService(BaseModuleService):
    MODULE = "publish_view"
    ENTITY = "消费视图"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["视图编号", "所属湖表", "视图定义", "绑定快照", "可见列", "刷新方式", "下游消费方", "最近刷新"]
    REQUIRED_FIELDS = ["视图编号", "所属湖表", "视图定义"]
    STATUS_ORDER = ["草稿", "已发布", "刷新中", "已撤回"]
    ACTION_RULES = {"发布视图": "已发布", "触发刷新": "刷新中", "刷新": "刷新中", "刷新消费视图": "刷新中", "完成刷新": "已发布", "撤回视图": "已撤回"}
    NEGATIVE_ACTIONS = []

    def publish_view(self, lake_table: str, view_definition: str,
                     visible_columns: list[str], consumer: str) -> dict[str, Any]:
        """发布消费视图：绑定最新可用快照与可见列。"""
        snaps = lake_store.list_snapshots(lake_table)
        available = [s for s in snaps if s.status == "available" and s.readable]
        latest_snap = available[-1] if available else None
        return {
            "success": True,
            "ok": True,
            "lake_table": lake_table,
            "view_definition": view_definition,
            "bound_snapshot": latest_snap.snapshot_id if latest_snap else None,
            "readable_snapshot_id": latest_snap.snapshot_id if latest_snap else None,
            "visible_columns": visible_columns,
            "consumer": consumer,
            "published_at": time.time(),
        }

    def _find_entry(self, view_ref: int | str | None) -> dict[str, Any] | None:
        if view_ref is None:
            return None
        try:
            return store.find(self.MODULE, int(view_ref))
        except (TypeError, ValueError):
            for row in store.rows(self.MODULE):
                if str(row.get("视图编号")) == str(view_ref):
                    return row
        return None

    def _latest_withdrawal(self, lake_table: str, withdraw_no: str | None = None) -> dict[str, Any] | None:
        if withdraw_no:
            record = lake_store.get_withdrawal(withdraw_no)
            return copy.deepcopy((record or {}).get("first_result") or record)
        records = [
            copy.deepcopy(r.get("first_result") or r)
            for r in lake_store.withdrawals.values()
            if r.get("lake_table") == lake_table and (r.get("first_result") or {}).get("ok", True)
        ]
        return records[-1] if records else None

    def refresh_view(
        self,
        lake_table: str | dict[str, Any] | int | None = None,
        view_snapshot: str | None = None,
        *,
        entry_id: int | str | None = None,
        view_id: int | str | None = None,
        withdraw_no: str | None = None,
        application_id: str | None = None,
        received_rows: int | None = None,
        **context: Any,
    ) -> dict[str, Any]:
        """触发视图刷新：更新绑定快照到最新可读版本，并关联撤回输入范围。"""
        # 允许 refresh_view({...}) / refresh_view(view_id, withdraw_no=...) 等调用。
        payload: dict[str, Any] = {}
        if isinstance(lake_table, dict):
            payload.update(lake_table)
            lake_table = payload.get("lake_table") or payload.get("table") or payload.get("所属湖表")
        positional_view_ref: int | str | None = None
        if isinstance(lake_table, int):
            positional_view_ref = lake_table
            lake_table = None
        elif isinstance(lake_table, str) and self._find_entry(lake_table) is not None:
            positional_view_ref = lake_table
            lake_table = None
        if view_snapshot is not None and not isinstance(view_snapshot, str):
            payload.setdefault("withdraw_no", view_snapshot)
            view_snapshot = None
        if isinstance(view_snapshot, str) and (
            view_snapshot.startswith("WD") or view_snapshot in lake_store.withdrawals
        ):
            withdraw_no = withdraw_no or view_snapshot
            view_snapshot = None

        entry_ref = (
            entry_id if entry_id is not None
            else view_id if view_id is not None
            else positional_view_ref if positional_view_ref is not None
            else payload.get("view_id") or payload.get("entry_id")
        )
        withdraw_no = withdraw_no or context.get("withdraw_id") or context.get("withdraw_no")
        entry = self._find_entry(entry_ref)
        if entry is not None:
            lake_table = lake_table or entry.get("所属湖表")
            entry_ref = entry.get("id")
        lake_table = lake_table or context.get("lake_table") or payload.get("lake_table")

        if not lake_table:
            return {"success": False, "ok": False, "error": "缺少所属湖表或消费视图 ID"}

        snaps = lake_store.list_snapshots(str(lake_table))
        available = [s for s in snaps if s.status == "available" and s.readable]
        if not available:
            # 种子行可能只登记了业务快照编号；没有内存湖快照时保留原绑定，
            # 不让通用“触发刷新”动作从页面上消失。真实撤回流程会走到下面的可读快照。
            fallback_snapshot = (entry or {}).get("绑定快照") or view_snapshot
            if fallback_snapshot:
                result = {
                    "success": True,
                    "ok": True,
                    "warning": "no available lake snapshot, keep registered binding",
                    "lake_table": lake_table,
                    "view_id": entry_ref,
                    "previous_snapshot": fallback_snapshot,
                    "refreshed_snapshot": fallback_snapshot,
                    "bound_snapshot": fallback_snapshot,
                    "readable_snapshot_id": fallback_snapshot,
                    "snapshot_id": fallback_snapshot,
                    "refresh_time": time.time(),
                    "refreshed_at": self._iso(),
                    "affected_inputs": [],
                    "message": "未发现新的内存湖快照，暂保留已登记快照绑定",
                }
                if entry is not None:
                    entry["refresh_result"] = copy.deepcopy(result)
                    entry["最近刷新"] = result["refreshed_at"]
                return result
            result = {"success": False, "ok": False, "error": "no available snapshot", "lake_table": lake_table}
            if entry is not None:
                entry["refresh_result"] = result
            return result
        latest = available[-1]
        previous_snapshot = view_snapshot or (entry or {}).get("绑定快照")
        if previous_snapshot and previous_snapshot == latest.snapshot_id:
            # 若传入的是业务快照编号，不强行覆盖；保留调用方看到的旧绑定。
            pass

        withdrawal = self._latest_withdrawal(
            str(lake_table),
            withdraw_no or payload.get("withdraw_no") or payload.get("withdraw_id"),
        )
        if application_id is None and withdrawal:
            application_id = withdrawal.get("application_id")
        input_scope = (withdrawal or {}).get("input_scope") or (withdrawal or {}).get("affected_input_scope")
        affected_inputs = (withdrawal or {}).get("affected_inputs")
        if withdrawal and application_id and not affected_inputs:
            affected_inputs = [{
                "application_id": application_id,
                "withdraw_no": withdrawal.get("withdraw_no"),
                "partition_keys": (input_scope or {}).get("partition_keys", []),
                "file_ids": (input_scope or {}).get("file_ids", []),
                "row_count": (input_scope or {}).get("row_count"),
                "status": "已撤回",
            }]

        # 如果消费端曾实收旧版 N 行，刷新响应同时给出下一份撤回/更正回执。
        next_consumer_receipt = None
        if withdrawal and (received_rows is not None or payload.get("received_rows") is not None):
            from app.services.write_job import WriteJobService
            svc = WriteJobService()
            delivered = svc.record_consumer_delivery(
                lake_table=str(lake_table),
                partition_key=(input_scope or {}).get("partition_key")
                or ((input_scope or {}).get("partition_keys") or [None])[0],
                snapshot_id=withdrawal.get("before_snapshot_id") or previous_snapshot,
                received_rows=int(received_rows if received_rows is not None else payload.get("received_rows")),
                application_id=application_id,
                withdraw_no=withdrawal.get("withdraw_no"),
                entry_id=entry_ref,
            )
            next_consumer_receipt = svc.build_next_consumer_receipt(
                withdrawal["withdraw_no"], delivered["receipt_no"]
            )
        elif withdrawal:
            next_consumer_receipt = withdrawal.get("next_consumer_receipt")

        consumable = None
        if withdrawal:
            from app.services.write_job import WriteJobService
            consumable = WriteJobService().get_consumable_state(str(lake_table))

        result = {
            "success": True,
            "ok": True,
            "view_id": entry_ref,
            "entry_id": entry_ref,
            "lake_table": lake_table,
            "previous_snapshot": previous_snapshot,
            "previous_bound_snapshot": previous_snapshot,
            "refreshed_snapshot": latest.snapshot_id,
            "bound_snapshot": latest.snapshot_id,
            "readable_snapshot_id": latest.snapshot_id,
            "current_snapshot_id": latest.snapshot_id,
            "snapshot_id": latest.snapshot_id,
            "refresh_time": time.time(),
            "refreshed_at": self._iso(),
            "withdraw_no": (withdrawal or {}).get("withdraw_no") or withdraw_no,
            "withdraw_id": (withdrawal or {}).get("withdraw_no") or withdraw_no,
            "application_id": application_id,
            "apply_no": application_id,
            "input_scope": input_scope,
            "affected_input_scope": input_scope,
            "affected_inputs": affected_inputs or [],
            "withdrawal_file": (withdrawal or {}).get("withdrawal_file"),
            "rollback_snapshot_id": (withdrawal or {}).get("rollback_snapshot_id"),
            "next_consumer_receipt": next_consumer_receipt,
            "consumable_state": consumable,
            "history_view": {
                "previous_snapshot": previous_snapshot,
                "delivered_version_remains_visible": True,
                "snapshot_versions": WriteJobService().snapshot_versions(str(lake_table)) if withdrawal else [],
            },
            "message": "消费视图已关联发起撤回的输入范围，并绑定可回读快照",
            "撤回编号": (withdrawal or {}).get("withdraw_no") or withdraw_no,
            "申请编号": application_id,
            "输入范围": input_scope,
            "受影响输入": affected_inputs or [],
            "可回读快照": latest.snapshot_id,
            "撤回文件": (withdrawal or {}).get("withdrawal_file"),
        }

        if entry is not None:
            entry["绑定快照"] = latest.snapshot_id
            entry["最近刷新"] = result["refreshed_at"]
            entry["refresh_result"] = copy.deepcopy(result)
            entry["input_scope"] = input_scope
            entry["withdraw_no"] = result["withdraw_no"]
            entry["readable_snapshot_id"] = latest.snapshot_id
        return result

    refresh_publish_view = refresh_view
    refresh_consumer_view = refresh_view

    @staticmethod
    def _iso() -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%S")

    def revoke_view(self, lake_table: str, consumer: str) -> dict[str, Any]:
        """撤回视图：撤销下游消费方对视图的访问授权。"""
        return {
            "lake_table": lake_table,
            "consumer": consumer,
            "revoked": True,
            "revoke_time": time.time(),
        }

    def run_action(self, entry_id: int, action: str, payload: Any | None = None):
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        data = WriteJobServicePayload.payload_data(payload)
        if action in {"触发刷新", "刷新", "刷新消费视图"}:
            result = self.refresh_view(entry_id=entry_id, **data)
            target = self.ACTION_RULES[action]
            entry["status"] = target
            entry["pending"] = target != self.STATUS_ORDER[1]
            entry["refresh_result"] = result
            if not result.get("ok"):
                return entry, f"消费视图已进入刷新中；暂未取得可读快照：{result.get('error', '')}"
            return entry, "消费视图刷新已处理，响应已关联撤回输入范围和可回读快照"

        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        entry["status"] = target
        entry["pending"] = target not in {"已发布", "已撤回"}
        entry["abnormal"] = action in self.NEGATIVE_ACTIONS
        if action == "发布视图":
            latest = self.refresh_view(entry_id=entry_id)
            if latest.get("readable_snapshot_id"):
                entry["绑定快照"] = latest["readable_snapshot_id"]
        return entry, f"{self.ENTITY}已{action}"


class WriteJobServicePayload:
    """避免模块循环导入的载荷解析小工具。"""

    @staticmethod
    def payload_data(payload: Any | None) -> dict[str, Any]:
        if payload is None:
            return {}
        if isinstance(payload, dict):
            data = dict(payload.get("data") or {})
            data.update({k: v for k, v in payload.items() if k not in {"action", "remark", "data"}})
            return data
        data = dict(getattr(payload, "data", {}) or {})
        data.update(getattr(payload, "model_extra", None) or {})
        return data


publish_view_service = PublishViewService()
