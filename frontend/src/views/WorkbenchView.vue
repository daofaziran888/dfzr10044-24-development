<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">
            <RouterLink
              v-if="meta.key === 'write_job' && column === '作业编号'"
              class="link"
              :to="`/write_job/${row.id}`"
              :data-testid="`job-link-${row.id}`"
            >
              {{ row[column] ?? '—' }}
            </RouterLink>
            <template v-else>{{ row[column] ?? '—' }}</template>
          </td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <RouterLink
              v-if="meta.key === 'write_job'"
              class="link detail-link"
              :to="`/write_job/${row.id}`"
              :data-testid="`detail-${row.id}`"
            >
              详情
            </RouterLink>
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <section
      v-if="actionFeedback"
      class="receipt-panel"
      data-testid="action-feedback"
      :data-ok="String(actionFeedback.ok)"
    >
      <div class="panel-head">
        <h3>最近动作回执</h3>
        <button class="link" type="button" data-testid="close-feedback" @click="actionFeedback = null">关闭</button>
      </div>
      <p data-testid="action-feedback-message">{{ actionFeedback.message }}</p>
      <template v-if="feedbackLinkage">
        <div class="linkage-grid">
          <span>撤回编号：<b data-testid="feedback-withdraw-no">{{ feedbackLinkage.withdraw_no || '—' }}</b></span>
          <span>申请编号：<b>{{ feedbackLinkage.application_id || feedbackLinkage.apply_no || '—' }}</b></span>
          <span>可回读快照：<b data-testid="feedback-readable-snapshot">{{ feedbackLinkage.readable_snapshot_id || feedbackLinkage.bound_snapshot || '—' }}</b></span>
          <span>输入范围：<b data-testid="feedback-input-scope">{{ formatScope(feedbackLinkage.input_scope || feedbackLinkage.affected_input_scope) }}</b></span>
        </div>
        <pre class="json-box" data-testid="action-result-json">{{ JSON.stringify(actionFeedback.entry?.refresh_result || actionFeedback, null, 2) }}</pre>
      </template>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="(field, fieldIndex) in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input
            v-model="createValues[field]"
            :data-testid="`create-${field}`"
            :required="fieldIndex < 3"
          />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  createEntry,
  downloadEntries,
  listEntries,
  moduleMeta,
  runAction as applyAction,
} from '@/api/backend-service'
import type { ActionResult, EntryRow } from '@/data/types'

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const actionFeedback = ref<(ActionResult & { entry?: any }) | null>(null)
const feedbackLinkage = computed<Record<string, any> | null>(() => {
  const result = actionFeedback.value?.entry?.refresh_result
  return result && typeof result === 'object' ? result as Record<string, any> : null
})
const stats = computed(() => [
  { label: meta.metrics[0], value: total.value },
  { label: meta.metrics[1], value: countByMetric(meta.metrics[1], 1) },
  { label: meta.metrics[2], value: countByMetric(meta.metrics[2], 2) },
])

function countByMetric(label: string, index: number): number {
  const preferredStatus = meta.statuses.find((status) => label.includes(status.replace(/量$/, '')))
    || (label.includes('冲突') ? '提交冲突' : '')
    || (label.includes('已提交') ? '已提交' : '')
    || (index === 1 ? meta.statuses[1] : meta.statuses[meta.statuses.length - 1])
  return rows.value.filter((row) => String(row.status) === preferredStatus).length
}
const statusSummary = computed(() =>
  meta.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

function resetFilters() {
  filters.value = {}
  void reload()
}

function formatScope(scope: Record<string, unknown> | null | undefined): string {
  if (!scope) return '—'
  const partitions = scope.partition_keys || scope.partitions || scope.partition_key
  return `${String(partitions ?? '')} / ${String(scope.row_count ?? scope.affected_row_count ?? '')} 行 / ${(scope.file_ids as unknown[] | undefined)?.length ?? 0} 个文件`
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action)
    actionFeedback.value = result
    if (!result.ok) throw new Error(result.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

onMounted(reload)
</script>
