import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

export type ReceiptPayload = Record<string, unknown>

export async function getWriteJobReceipts(id: number | string): Promise<{
  items: EntryRow[]
  total: number
  write_receipts: EntryRow[]
  withdrawal_receipts: EntryRow[]
  job?: EntryRow | null
}> {
  const response = await request(`/api/write_job/${id}/receipts`)
  if (!response.ok) throw new Error('写入回执读取失败')
  return await response.json()
}

export async function createWriteWithdrawal(payload: ReceiptPayload): Promise<ActionResult & ReceiptPayload> {
  const response = await request('/api/write_job/withdrawals', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  return await response.json()
}

export async function fetchWithdrawal(withdrawNo: string): Promise<ActionResult & ReceiptPayload> {
  const response = await request(`/api/write_job/withdrawals/${encodeURIComponent(withdrawNo)}`)
  if (!response.ok) throw new Error('原撤回处置结果不存在')
  return await response.json()
}

export async function commitWrite(payload: ReceiptPayload): Promise<ActionResult & ReceiptPayload> {
  const response = await request('/api/write_job/commits', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  return await response.json()
}

export async function refreshPublishView(payload: ReceiptPayload): Promise<ActionResult & ReceiptPayload> {
  const response = await request('/api/publish_view/refresh', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  return await response.json()
}
