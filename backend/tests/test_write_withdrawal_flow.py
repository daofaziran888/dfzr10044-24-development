"""写入撤回、幂等回执、快照回滚和消费视图联动的验收场景。"""
from __future__ import annotations

from app.lake_store import lake_store
from app.services.publish_view import PublishViewService
from app.services.write_job import WriteJobService


def test_write_withdrawal_receipt_snapshot_and_consumer_refresh():
    svc = WriteJobService()
    table = "acceptance_write_withdraw_table"
    app_id = "APP-ACCEPTANCE-WD"
    wd_no = "WD-ACCEPTANCE-001"

    base_rows = [{"id": f"A{i}"} for i in range(1, 4)]
    base_file = lake_store.register_file(table, "A", 3, 900, rows_data=base_rows)
    base_snap = lake_store.create_snapshot(table, [base_file.file_id], added_rows=3)
    lake_store.mark_snapshot_available(table, base_snap.snapshot_id)

    write_a = svc.confirm_write(
        lake_table=table,
        partition_key="A",
        rows_data=[{"id": f"new-a-{i}"} for i in range(4)],
        application_id=app_id,
        entry_id=999001,
    )
    assert write_a["ok"]
    assert write_a["rows_written"] == 4

    write_b = svc.confirm_write(
        lake_table=table,
        partition_key="B",
        rows_data=[{"id": f"b-{i}"} for i in range(2)],
        application_id=f"{app_id}-B",
        entry_id=999001,
    )
    assert write_b["rows_written"] == 2

    conflict = svc.confirm_write(
        lake_table=table,
        partition_key="A",
        row_count=1,
        application_id=app_id,
        conflict_with_withdrawal=True,
    )
    assert conflict["ok"] is False
    assert conflict["conflict"] is True

    delivered = svc.record_consumer_delivery(
        lake_table=table,
        partition_key="A",
        snapshot_id=write_a["snapshot_id"],
        received_rows=7,
        application_id=app_id,
    )
    assert delivered["received_rows"] == 7

    first = svc.request_withdrawal(
        withdraw_no=wd_no,
        application_id=app_id,
        conflict=True,
        delivery_receipt_no=delivered["receipt_no"],
        entry_id=999001,
    )
    assert first["ok"]
    assert first["conflict"] is True
    assert first["withdrawn_row_count"] == 4
    assert first["readable_snapshot_id"] != write_a["snapshot_id"]

    replayed = svc.get_withdraw_result(wd_no)
    assert replayed == first

    state = svc.get_consumable_state(table)
    assert state["partitions"]["A"]["row_count"] == 3
    assert state["partitions"]["B"]["row_count"] == 2
    assert state["contains_withdrawn_rows"] is False

    next_receipt = first["next_consumer_receipt"]
    assert next_receipt["delivered_rows"] == 7
    assert next_receipt["affected_rows"] == 4
    assert next_receipt["withdrawal_file"]["row_count"] == 4
    assert next_receipt["previous_snapshot_id"] == write_a["snapshot_id"]
    assert next_receipt["delivered_version_remains_in_history"] is True

    history = svc.history_view(table, "A")
    snapshot_ids = {item["snapshot_id"] for item in history["snapshots"]}
    assert write_a["snapshot_id"] in snapshot_ids
    assert first["readable_snapshot_id"] in snapshot_ids

    receipts = svc.get_job_receipts(999001)
    assert len(receipts["write_receipts"]) >= 2
    assert len(receipts["withdrawal_receipts"]) == 1

    refresh = PublishViewService().refresh_view(
        lake_table=table,
        withdraw_no=wd_no,
    )
    assert refresh["ok"]
    assert refresh["withdraw_no"] == wd_no
    assert refresh["input_scope"]["row_count"] == 4
    assert refresh["readable_snapshot_id"] == first["readable_snapshot_id"]
