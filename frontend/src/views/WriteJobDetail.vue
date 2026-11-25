<template>
  <section class="page write-job-detail" data-testid="write-job-detail" :data-job-id="jobId">
    <header class="page-head">
      <div>
        <RouterLink class="link" to="/write_job" data-testid="back-to-write-jobs">← 返回写入作业</RouterLink>
        <h2 data-testid="detail-title">写入任务详情 · {{ jobNumber }}</h2>
        <p class="page-desc">核对确认回执丢失后的新增文件范围，发起撤回、快照回滚，并补取首次处置结果。</p>
      </div>
      <div class="page-actions">
        <button class="btn" type="button" data-testid="reload-detail" @click="reloadAll">刷新详情</button>
        <button class="btn primary" type="button" data-testid="run-demo-scenario" @click="runDemo">
          构造 A3+4 / B2 场景
        </button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="detail-error">{{ errorMessage }}</p>
    <p v-if="noticeMessage" class="success-text" data-testid="detail-notice">{{ noticeMessage }}</p>

    <div class="detail-grid">
      <article class="info-card" data-testid="job-summary">
        <h3>作业信息</h3>
        <dl>
          <div><dt>作业编号</dt><dd data-testid="job-number">{{ jobNumber }}</dd></div>
          <div><dt>所属湖表</dt><dd data-testid="job-table">{{ String(job['所属湖表'] || withdrawForm.lake_table || '—') }}</dd></div>
          <div><dt>当前状态</dt><dd data-testid="job-status">{{ String(job.status || '—') }}</dd></div>
          <div><dt>写入行数</dt><dd>{{ String(job['写入行数'] || '—') }}</dd></div>
        </dl>
      </article>

      <article class="info-card" data-testid="withdraw-form-card">
        <h3>发起写入撤回</h3>
        <div class="form-grid">
          <label><span>湖表</span><input v-model="withdrawForm.lake_table" data-testid="withdraw-table" /></label>
          <label><span>申请编号</span><input v-model="withdrawForm.application_id" data-testid="withdraw-application-no" /></label>
          <label><span>原撤回编号</span><input v-model="withdrawForm.withdraw_no" data-testid="withdraw-no-input" /></label>
          <label><span>分区</span><input v-model="withdrawForm.partition_key" data-testid="withdraw-partition" /></label>
          <label><span>本次新增行数</span><input v-model.number="withdrawForm.row_count" type="number" min="0" data-testid="withdraw-row-count" /></label>
          <label><span>消费端已收行数</span><input v-model.number="withdrawForm.delivered_rows" type="number" min="0" data-testid="delivered-row-count" /></label>
          <label class="checkbox-line">
            <input v-model="withdrawForm.conflict" type="checkbox" data-testid="withdraw-conflict" />
            <span>模拟同一申请的确认/撤回冲突</span>
          </label>
        </div>
        <div class="page-actions">
          <button class="btn primary" type="button" data-testid="submit-withdrawal" :disabled="submitting" @click="submitWithdrawal">
            {{ submitting ? '处置中…' : '写入撤回并回滚快照' }}
          </button>
          <button class="btn" type="button" data-testid="commit-b-write" :disabled="submitting" @click="commitBWrite">
            提交 B 独立写入 2 行
          </button>
        </div>
      </article>

      <article class="info-card" data-testid="replay-card">
        <h3>凭原撤回编号补取首次处置结果</h3>
        <div class="inline-form">
          <input v-model="replayNo" placeholder="输入 WD 开头的撤回编号" data-testid="replay-withdraw-no" />
          <button class="btn" type="button" data-testid="replay-withdrawal" @click="replayWithdrawal">补取结果</button>
        </div>
        <pre v-if="replayResult" class="json-box" data-testid="replay-result">{{ JSON.stringify(replayResult, null, 2) }}</pre>
      </article>

      <article class="info-card" data-testid="view-refresh-card">
        <h3>刷新消费视图并关联影响范围</h3>
        <div class="inline-form wrap">
          <label><span>消费视图 ID</span><input v-model.number="refreshForm.view_id" type="number" data-testid="refresh-view-id" /></label>
          <label><span>撤回编号</span><input v-model="refreshForm.withdraw_no" data-testid="refresh-withdraw-no" /></label>
          <label><span>实收行数</span><input v-model.number="refreshForm.received_rows" type="number" data-testid="refresh-received-rows" /></label>
          <button class="btn primary" type="button" data-testid="refresh-view" @click="refreshView">刷新消费视图</button>
        </div>
        <div v-if="refreshResult" class="linkage-box" data-testid="refresh-linkage">
          <p>撤回编号：<b data-testid="refresh-result-withdraw-no">{{ text(refreshResult.withdraw_no) }}</b></p>
          <p>输入范围：<b data-testid="refresh-result-scope">{{ formatScope(refreshResult.input_scope) }}</b></p>
          <p>可回读快照：<b data-testid="refresh-result-snapshot">{{ text(refreshResult.readable_snapshot_id) }}</b></p>
          <pre class="json-box" data-testid="refresh-result-json">{{ JSON.stringify(refreshResult, null, 2) }}</pre>
        </div>
      </article>
    </div>

    <div data-testid="receipts">
    <section class="receipt-section" data-testid="write-receipts">
      <div class="section-head">
        <h3>普通写入回执</h3>
        <span>共 {{ writeReceipts.length }} 份</span>
      </div>
      <p v-if="!writeReceipts.length" class="empty-state">尚未查到普通写入回执。</p>
      <article
        v-for="receipt in writeReceipts"
        :key="String(receipt.receipt_no || receipt.id)"
        class="receipt-card write"
        :data-testid="`write-receipt-${receipt.receipt_no || receipt.id}`"
      >
        <h4>{{ String(receipt.type || '普通写入回执') }} · {{ text(receipt.receipt_no) }}</h4>
        <div class="receipt-meta">
          <span>申请：{{ text(receipt.application_id || receipt.apply_no) }}</span>
          <span>分区：{{ text(receipt.partition_key) }}</span>
          <span>行数：{{ text(receipt.rows_written ?? receipt.row_count) }}</span>
          <span>快照：{{ text(receipt.readable_snapshot_id || receipt.snapshot_id) }}</span>
          <span>状态：{{ text(receipt.status) }}</span>
        </div>
        <pre class="json-box small">{{ JSON.stringify(receipt, null, 2) }}</pre>
      </article>
    </section>

    <section class="receipt-section" data-testid="withdrawal-receipts">
      <div class="section-head">
        <h3>原撤回与快照回滚回执</h3>
        <span>共 {{ withdrawalReceipts.length }} 份</span>
      </div>
      <p v-if="!withdrawalReceipts.length" class="empty-state">尚未查到写入撤回回执。</p>
      <article
        v-for="receipt in withdrawalReceipts"
        :key="String(receipt.withdraw_no || receipt.receipt_no || receipt.id)"
        class="receipt-card withdrawal"
        :data-testid="`withdrawal-receipt-${receipt.withdraw_no || receipt.receipt_no || receipt.id}`"
      >
        <h4>{{ String(receipt.type || '写入撤回回执') }} · {{ text(receipt.withdraw_no || receipt.receipt_no) }}</h4>
        <div class="receipt-meta">
          <span>原撤回编号：<b data-testid="receipt-withdraw-no">{{ text(receipt.withdraw_no || receipt.withdraw_id) }}</b></span>
          <span>影响行数：{{ text(receipt.withdrawn_row_count ?? receipt.row_count) }}</span>
          <span>回滚快照：<b data-testid="receipt-rollback-snapshot">{{ text(receipt.rollback_snapshot_id || receipt.readable_snapshot_id) }}</b></span>
          <span>撤回文件：<b data-testid="receipt-withdrawal-file">{{ text(receipt.withdrawal_file?.file_id || receipt.withdrawal_file?.path) }}</b></span>
        </div>
        <pre class="json-box small">{{ JSON.stringify(receipt, null, 2) }}</pre>
      </article>
    </section>
    </div>

    <section class="receipt-section" data-testid="consumer-receipts">
      <div class="section-head">
        <h3>消费端送达与下一份更正回执</h3>
        <span>共 {{ consumerReceipts.length }} 份</span>
      </div>
      <p v-if="!consumerReceipts.length" class="empty-state">尚无消费端回执；填写“消费端已收行数”后发起撤回会自动生成。</p>
      <article
        v-for="receipt in consumerReceipts"
        :key="String(receipt.receipt_no || receipt.id)"
        class="receipt-card consumer"
        :data-testid="`consumer-receipt-${receipt.receipt_no || receipt.id}`"
      >
        <h4>{{ text(receipt.type) }} · {{ text(receipt.receipt_no) }}</h4>
        <div class="receipt-meta">
          <span>已收/交付行数：{{ text(receipt.received_rows ?? receipt.delivered_rows) }}</span>
          <span>影响行数：{{ text(receipt.affected_rows ?? receipt.withdrawn_row_count) }}</span>
          <span>可回读快照：{{ text(receipt.readable_snapshot_id) }}</span>
          <span>撤回文件：{{ text(receipt.withdraw_file_id || receipt.withdrawal_file?.file_id) }}</span>
        </div>
        <pre class="json-box small">{{ JSON.stringify(receipt, null, 2) }}</pre>
      </article>
    </section>

    <section class="receipt-section" data-testid="snapshot-versions">
      <div class="section-head"><h3>各版快照与可读版本切换</h3><span>{{ versions.length }} 版</span></div>
      <table class="data-table">
        <thead><tr><th>版本</th><th>快照</th><th>状态</th><th>分区行数</th><th>当前可读</th></tr></thead>
        <tbody>
          <tr v-for="item in versions" :key="String(item.snapshot_id)" :data-testid="`version-${item.version}`">
            <td>{{ text(item.version) }}</td>
            <td>{{ text(item.snapshot_id) }}</td>
            <td>{{ text(item.status) }}</td>
            <td data-testid="version-partition-counts">{{ JSON.stringify(item.partition_row_counts || {}) }}</td>
            <td>{{ item.current_readable ? '是' : '历史视图' }}</td>
          </tr>
        </tbody>
      </table>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'

import {
  commitWrite,
  createWriteWithdrawal,
  fetchWithdrawal,
  getWriteJobReceipts,
  refreshPublishView,
} from '@/api/backend-service'

const props = defineProps<{ id: string }>()
const jobId = computed(() => props.id)

const job = ref<Record<string, any>>({ id: props.id })
const receipts = ref<Record<string, any>[]>([])
const versions = ref<Record<string, any>[]>([])
const errorMessage = ref('')
const noticeMessage = ref('')
const submitting = ref(false)
const replayNo = ref(`WD-ACTION-${props.id}`)
const replayResult = ref<Record<string, any> | null>(null)
const refreshResult = ref<Record<string, any> | null>(null)

const withdrawForm = reactive({
  lake_table: '',
  application_id: `APP-ACTION-${props.id}`,
  withdraw_no: `WD-ACTION-${props.id}`,
  partition_key: 'A',
  row_count: 4,
  delivered_rows: 7,
  conflict: true,
  entry_id: Number(props.id),
})
const refreshForm = reactive({
  view_id: 1,
  withdraw_no: `WD-ACTION-${props.id}`,
  received_rows: 7,
})

const jobNumber = computed(() => String(job.value['作业编号'] || `WJ-${props.id}`))
const writeReceipts = computed(() => receipts.value.filter((item) => item.receipt_type === 'write' || item.type === '普通写入回执'))
const withdrawalReceipts = computed(() => receipts.value.filter((item) => item.receipt_type === 'withdrawal' || item.type === '写入撤回回执'))
const consumerReceipts = computed(() => receipts.value.filter((item) => String(item.receipt_type || '').includes('consumer') || String(item.type || '').includes('回执') && String(item.type || '').includes('消费')))

function text(value: unknown): string {
  if (value === null || value === undefined || value === '') return '—'
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

function formatScope(scope: Record<string, unknown> | null | undefined): string {
  if (!scope) return '—'
  const partitions = scope.partition_keys || scope.partitions || scope.partition_key
  const files = Array.isArray(scope.file_ids) ? scope.file_ids.length : 0
  return `${String(partitions ?? '')} / ${String(scope.row_count ?? scope.affected_row_count ?? '')} 行 / ${files} 个文件`
}

async function loadJob() {
  const response = await fetch(`/api/write_job/${props.id}`)
  if (response.ok) job.value = await response.json()
  if (!withdrawForm.lake_table && job.value['所属湖表']) {
    withdrawForm.lake_table = String(job.value['所属湖表'])
  }
  const knownWithdrawNo = job.value.last_withdraw_no
    || job.value.receipt_ids?.find?.((id: unknown) => String(id).startsWith('WD'))
  if (knownWithdrawNo) {
    const wd = String(knownWithdrawNo)
    withdrawForm.withdraw_no = wd
    replayNo.value = wd
    refreshForm.withdraw_no = wd
  }
}

async function loadReceipts() {
  const payload = await getWriteJobReceipts(props.id)
  receipts.value = payload.items || []
  if (payload.job) job.value = payload.job
}

async function loadVersions() {
  const table = withdrawForm.lake_table || job.value['所属湖表']
  if (!table) return
  const response = await fetch(`/api/write_job/versions/${encodeURIComponent(String(table))}`)
  if (response.ok) {
    const payload = await response.json()
    versions.value = payload.items || []
  }
}

async function reloadAll() {
  errorMessage.value = ''
  noticeMessage.value = ''
  try {
    await Promise.all([loadJob(), loadReceipts()])
    await loadVersions()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '详情加载失败'
  }
}

async function submitWithdrawal() {
  submitting.value = true
  errorMessage.value = ''
  noticeMessage.value = ''
  try {
    const result = await createWriteWithdrawal({ ...withdrawForm })
    if (!result.ok) throw new Error(String(result.message || result.error || '撤回失败'))
    noticeMessage.value = '撤回已完成：新增文件已排除，可读快照已切换，回执可凭撤回编号补取。'
    replayNo.value = String(result.withdraw_no || withdrawForm.withdraw_no)
    refreshForm.withdraw_no = replayNo.value
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '撤回失败'
  } finally {
    submitting.value = false
  }
}

async function commitBWrite() {
  submitting.value = true
  errorMessage.value = ''
  try {
    const result = await commitWrite({
      lake_table: withdrawForm.lake_table,
      application_id: `${withdrawForm.application_id}-B`,
      partition_key: 'B',
      row_count: 2,
      entry_id: Number(props.id),
    })
    if (!result.ok) throw new Error(String(result.message || result.error || 'B 分区写入失败'))
    noticeMessage.value = 'B 分区独立写入的 2 行已继续有效。'
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : 'B 分区写入失败'
  } finally {
    submitting.value = false
  }
}

async function replayWithdrawal() {
  errorMessage.value = ''
  try {
    replayResult.value = await fetchWithdrawal(replayNo.value.trim())
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '补取失败'
    replayResult.value = null
  }
}

async function refreshView() {
  errorMessage.value = ''
  try {
    refreshResult.value = await refreshPublishView({ ...refreshForm })
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '消费视图刷新失败'
  }
}

async function runDemo() {
  submitting.value = true
  errorMessage.value = ''
  try {
    const result = await fetch('/api/write_job/demo-withdrawal', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ entry_id: Number(props.id), lake_table: withdrawForm.lake_table || undefined }),
    }).then(async (response) => response.json())
    if (!result.ok) throw new Error('场景构造失败')
    withdrawForm.withdraw_no = result.withdraw_no
    withdrawForm.application_id = result.application_id
    replayNo.value = result.withdraw_no
    refreshForm.withdraw_no = result.withdraw_no
    noticeMessage.value = '场景已构造：A 原有 3 行、新增 4 行，B 独立写入 2 行。'
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '场景构造失败'
  } finally {
    submitting.value = false
  }
}

onMounted(reloadAll)
</script>

<style scoped>
.detail-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
  gap: 16px;
  margin: 16px 0;
}
.info-card,
.receipt-card {
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 14px;
  padding: 16px;
}
.form-grid,
.inline-form {
  display: grid;
  gap: 10px;
}
.form-grid {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}
.inline-form {
  grid-template-columns: 1fr auto;
  align-items: end;
}
.inline-form.wrap {
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  margin-bottom: 12px;
}
label span,
.form-grid label span {
  display: block;
  font-size: 12px;
  opacity: 0.78;
  margin-bottom: 4px;
}
input {
  width: 100%;
  border-radius: 8px;
  border: 1px solid var(--border);
  background: #fff;
  color: #1f2937;
  padding: 8px 10px;
}
.checkbox-line {
  grid-column: 1 / -1;
  display: flex;
  align-items: center;
  gap: 8px;
}
.checkbox-line input {
  width: auto;
}
.receipt-section {
  margin: 22px 0;
}
.section-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 10px;
}
.receipt-card {
  margin: 12px 0;
}
.receipt-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  font-size: 13px;
  margin: 8px 0;
}
.json-box {
  max-height: 280px;
  overflow: auto;
  padding: 12px;
  border-radius: 10px;
  background: rgba(2, 6, 23, 0.55);
  border: 1px solid rgba(148, 163, 184, 0.18);
  white-space: pre-wrap;
  word-break: break-all;
}
.json-box.small {
  max-height: 180px;
  font-size: 12px;
}
.success-text {
  color: #86efac;
}
.linkage-box p {
  margin: 6px 0;
}
</style>
