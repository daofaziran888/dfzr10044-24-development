"""写入撤回、回执补取、快照版本和消费视图刷新的专用 HTTP 入口。

通用 CRUD/action 路由仍由 resource.py 提供；这里补齐需要返回丰富回执对象的路径。
所有路径都保持薄封装，业务判断位于 app.services。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from app.services.publish_view import PublishViewService
from app.services.write_job import WriteJobService

router = APIRouter(prefix="/api", tags=["写入撤回与消费刷新"])
write_service = WriteJobService()
view_service = PublishViewService()


async def _body(request: Request) -> dict[str, Any]:
    try:
        payload = await request.json()
    except Exception:
        return {}
    if payload is None:
        return {}
    if not isinstance(payload, dict):
        return {"value": payload}
    data = dict(payload.get("data") or {}) if isinstance(payload.get("data"), dict) else {}
    data.update({k: v for k, v in payload.items() if k != "data"})
    return data


# ----------------------------------------------------------------------
# 写入确认 / 撤回
# ----------------------------------------------------------------------

@router.post("/write_job/commits")
@router.post("/write_job/commit")
@router.post("/write_job/writes")
@router.post("/write-job/commits")
@router.post("/write-jobs/commits")
async def commit_write(request: Request) -> dict[str, Any]:
    return write_service.confirm_write(**await _body(request))


@router.post("/write_job/withdrawals")
@router.post("/write_job/withdraw-and-rollback")
@router.post("/write_job/withdrawals/rollback")
@router.post("/write_job/withdraw")
@router.post("/write_job/withdrawals/")
@router.post("/write-job/withdrawals")
@router.post("/write-jobs/withdrawals")
@router.post("/write_withdraw/withdrawals")
@router.post("/write_withdraw/withdraw")
@router.post("/write-withdraw/withdrawals")
async def create_withdrawal(request: Request) -> dict[str, Any]:
    return write_service.request_withdrawal(**await _body(request))


@router.get("/write_job/withdrawals/{withdraw_no}")
@router.get("/write_job/withdraw/{withdraw_no}")
@router.get("/write-job/withdrawals/{withdraw_no}")
@router.get("/write-jobs/withdrawals/{withdraw_no}")
@router.get("/write_withdraw/{withdraw_no}")
@router.get("/write_withdraw/withdrawals/{withdraw_no}")
async def get_withdrawal(withdraw_no: str) -> dict[str, Any]:
    result = write_service.get_withdraw_result(withdraw_no)
    if result is None:
        raise HTTPException(status_code=404, detail=f"撤回编号 {withdraw_no} 不存在")
    return result


@router.get("/write_job/withdrawals/{withdraw_no}/file")
@router.get("/write_job/withdrawals/{withdraw_no}/withdrawal-file")
def download_withdrawal_file(withdraw_no: str) -> JSONResponse:
    result = write_service.get_withdraw_result(withdraw_no)
    if result is None:
        raise HTTPException(status_code=404, detail=f"撤回编号 {withdraw_no} 不存在")
    file_ref = result.get("withdrawal_file") or {}
    return JSONResponse(
        content={
            "ok": True,
            "withdraw_no": withdraw_no,
            "file": file_ref,
            "rows": file_ref.get("rows", []),
        },
        headers={"Content-Disposition": f'attachment; filename="withdrawal-{withdraw_no}.json"'},
    )


@router.get("/write_job/receipts/{receipt_no}")
@router.get("/write_job/receipt/{receipt_no}")
@router.get("/receipts/{receipt_no}")
def get_receipt(receipt_no: str) -> dict[str, Any]:
    result = write_service.get_receipt(receipt_no)
    if result is None:
        raise HTTPException(status_code=404, detail=f"回执编号 {receipt_no} 不存在")
    return result


@router.get("/write_job/{entry_id}/receipts")
@router.get("/write-jobs/{entry_id}/receipts")
def job_receipts(entry_id: str) -> dict[str, Any]:
    return write_service.get_job_receipts(entry_id)


@router.get("/write_job/{entry_id}/receipts/write")
def job_write_receipts(entry_id: str) -> dict[str, Any]:
    result = write_service.get_job_receipts(entry_id)
    return {"items": result["write_receipts"], "total": len(result["write_receipts"])}


@router.get("/write_job/{entry_id}/receipts/withdrawal")
def job_withdrawal_receipts(entry_id: str) -> dict[str, Any]:
    result = write_service.get_job_receipts(entry_id)
    return {"items": result["withdrawal_receipts"], "total": len(result["withdrawal_receipts"])}


# ----------------------------------------------------------------------
# 可读状态 / 版本 / 历史
# ----------------------------------------------------------------------

@router.get("/write_job/state/{lake_table:path}")
@router.get("/write_job/consumable/{lake_table:path}")
def consumable_state(lake_table: str, partition_key: str | None = None) -> dict[str, Any]:
    return write_service.get_consumable_state(lake_table, partition_key)


@router.get("/write_job/read/{lake_table:path}")
def read_partition(lake_table: str, partition_key: str, snapshot_id: str | None = None) -> dict[str, Any]:
    return write_service.read_partition(lake_table, partition_key, snapshot_id)


@router.get("/write_job/versions/{lake_table:path}")
@router.get("/write_job/snapshots/{lake_table:path}")
def snapshot_versions(lake_table: str, partition_key: str | None = None) -> dict[str, Any]:
    items = write_service.snapshot_versions(lake_table, partition_key)
    return {"items": items, "total": len(items)}


@router.get("/write_job/history/{lake_table:path}")
def history_view(lake_table: str, partition_key: str | None = None) -> dict[str, Any]:
    return write_service.history_view(lake_table, partition_key)


# ----------------------------------------------------------------------
# 消费端回执
# ----------------------------------------------------------------------

@router.post("/write_job/consumer-deliveries")
@router.post("/consumer-receipts/delivery")
async def record_delivery(request: Request) -> dict[str, Any]:
    return write_service.record_consumer_delivery(**await _body(request))


@router.post("/write_job/consumer-receipts/next")
@router.post("/consumer-receipts/next")
async def next_consumer_receipt(request: Request) -> dict[str, Any]:
    data = await _body(request)
    withdraw_no = data.get("withdraw_no") or data.get("withdraw_id")
    if not withdraw_no:
        raise HTTPException(status_code=422, detail="缺少 withdraw_no")
    return write_service.build_next_consumer_receipt(
        withdraw_no,
        data.get("delivery_receipt_no") or data.get("prior_receipt_no"),
        data.get("received_rows") or data.get("delivered_rows"),
    )


@router.get("/write_job/consumer-history")
def consumer_history(lake_table: str | None = None, consumer: str | None = None) -> dict[str, Any]:
    return write_service.consumer_history(lake_table, consumer)


@router.post("/write_job/demo-withdrawal")
async def demo_scenario(request: Request) -> dict[str, Any]:
    data = await _body(request)
    return write_service.build_demo_scenario(**data)


# ----------------------------------------------------------------------
# 消费视图刷新
# ----------------------------------------------------------------------

@router.post("/publish_view/refresh")
@router.post("/publish-views/refresh")
@router.post("/consume_view/refresh")
async def refresh_publish_view(request: Request) -> dict[str, Any]:
    data = await _body(request)
    entry_id = data.pop("entry_id", None) or data.pop("view_id", None) or data.pop("id", None)
    if entry_id is not None:
        return view_service.refresh_view(entry_id=entry_id, **data)
    return view_service.refresh_view(**data)


@router.get("/publish_view/{entry_id}/refresh")
def refresh_publish_view_by_get(entry_id: int, withdraw_no: str | None = None,
                                received_rows: int | None = None) -> dict[str, Any]:
    return view_service.refresh_view(
        entry_id=entry_id, withdraw_no=withdraw_no, received_rows=received_rows
    )
