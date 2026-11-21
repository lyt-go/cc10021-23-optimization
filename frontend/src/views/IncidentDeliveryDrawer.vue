<template>
  <div v-if="open" class="drawer-backdrop" data-testid="incident-drawer" @click.self="emitClose">
    <section class="drawer" role="dialog" aria-label="质量事件处置记录">
      <header class="drawer-head">
        <div>
          <h3>事件处置记录</h3>
          <p class="drawer-sub">{{ incident?.['事件编号'] }} · {{ incident?.['关联规则'] }}</p>
        </div>
        <button class="btn ghost" type="button" data-testid="drawer-close" @click="emitClose">关闭</button>
      </header>

      <div v-if="loading" class="drawer-body">投递凭据加载中…</div>

      <div v-else class="drawer-body">
        <dl class="incident-grid">
          <div><dt>严重级别</dt><dd>{{ incident?.['严重级别'] ?? '—' }}</dd></div>
          <div><dt>偏离程度</dt><dd>{{ incident?.['偏离程度'] ?? '—' }}</dd></div>
          <div><dt>触发时间</dt><dd>{{ incident?.['触发时间'] ?? '—' }}</dd></div>
          <div><dt>处置状态</dt><dd>{{ incident?.['处理状态'] ?? incident?.status ?? '—' }}</dd></div>
        </dl>

        <h4 class="section-title">通知投递凭据</h4>
        <div v-if="!credential" class="empty-hint" data-testid="no-credential">
          {{ loadError || '该事件尚未登记通知投递凭据' }}
        </div>
        <table v-else class="credential-table" data-testid="credential-table">
          <tbody>
            <tr><th>投递身份</th><td data-testid="cred-delivery-id">{{ credential['投递身份'] }}</td></tr>
            <tr><th>投递状态</th><td>
              <span class="delivery-badge" :data-status="credential['投递状态']">
                {{ credential['投递状态'] }}
              </span>
            </td></tr>
            <tr><th>尝试次数</th><td>{{ credential['投递尝试次数'] }}</td></tr>
            <tr><th>通知编号</th><td>{{ credential['通知编号'] || '—' }}</td></tr>
            <tr><th>回执时间</th><td>{{ credential['回执时间'] || '—' }}</td></tr>
            <tr><th>失败原因</th><td>{{ credential['失败原因'] || '—' }}</td></tr>
            <tr><th>通知方式</th><td>{{ credential['通知方式'] }} / {{ credential['接收人'] }}</td></tr>
            <tr><th>网关实际通知数</th><td data-testid="gateway-count">{{ gatewayCount }}</td></tr>
          </tbody>
        </table>

        <div class="drawer-actions">
          <button
            class="btn primary"
            type="button"
            data-testid="recover-delivery"
            :disabled="recovering"
            @click="onRecover()"
          >
            {{ recovering ? '恢复中…' : '恢复投递' }}
          </button>
          <button
            class="btn"
            type="button"
            data-testid="simulate-fail"
            :disabled="recovering"
            @click="onRecover('fail_before_send')"
          >
            演练：发送前中止
          </button>
          <button
            class="btn"
            type="button"
            data-testid="simulate-lose-receipt"
            :disabled="recovering"
            @click="onRecover('lose_receipt')"
          >
            演练：送达后丢回执
          </button>
        </div>
        <p v-if="actionMessage" class="action-message" data-testid="recover-message">{{ actionMessage }}</p>
      </div>
    </section>
  </div>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue'

import {
  getDeliveryByIncident,
  recoverDelivery,
  type DeliveryCredential,
  type RecoverResult,
} from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const props = defineProps<{ open: boolean; incident: EntryRow | null }>()
const emit = defineEmits<{ (e: 'close'): void; (e: 'recovered'): void }>()

const credential = ref<DeliveryCredential | null>(null)
const gatewayCount = ref<number>(0)
const loading = ref(false)
const recovering = ref(false)
const loadError = ref('')
const actionMessage = ref('')

function emitClose() {
  emit('close')
}

async function loadCredential() {
  if (!props.incident) return
  const incidentId = String(props.incident['事件编号'] ?? '')
  if (!incidentId) return
  loading.value = true
  loadError.value = ''
  actionMessage.value = ''
  try {
    const data = await getDeliveryByIncident(incidentId)
    credential.value = data.credential ?? null
    gatewayCount.value = data['网关实际通知数'] ?? 0
    if (!data.ok) loadError.value = data.message ?? '暂无投递凭据'
  } catch (error) {
    loadError.value = error instanceof Error ? error.message : '投递凭据读取失败'
    credential.value = null
  } finally {
    loading.value = false
  }
}

async function onRecover(simulate?: string) {
  if (!props.incident) return
  const incidentId = String(props.incident['事件编号'] ?? '')
  recovering.value = true
  actionMessage.value = ''
  try {
    const result: RecoverResult = await recoverDelivery(incidentId, simulate)
    actionMessage.value = result.ok
      ? `恢复完成：${result['投递状态']}，通知编号 ${result['通知编号'] || '—'}，`
        + `本次实际投递 ${result['实际投递'] ? '是' : '否（复用既有回执）'}`
      : `恢复失败：${result.message ?? result['消息'] ?? '未知原因'}`
    await loadCredential()
    if (result.ok) emit('recovered')
  } catch (error) {
    actionMessage.value = error instanceof Error ? error.message : '恢复失败'
  } finally {
    recovering.value = false
  }
}

watch(
  () => [props.open, props.incident?.id] as const,
  ([isOpen]) => {
    if (isOpen) void loadCredential()
  },
  { immediate: true },
)
</script>

<style scoped>
.drawer-backdrop {
  position: fixed;
  inset: 0;
  background: rgba(15, 23, 42, 0.45);
  display: flex;
  justify-content: flex-end;
  z-index: 40;
}
.drawer {
  width: min(560px, 92vw);
  height: 100%;
  background: #fff;
  box-shadow: -8px 0 24px rgba(15, 23, 42, 0.18);
  display: flex;
  flex-direction: column;
  overflow-y: auto;
}
.drawer-head {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  padding: 20px 24px;
  border-bottom: 1px solid #e2e8f0;
}
.drawer-head h3 { margin: 0; font-size: 18px; }
.drawer-sub { margin: 4px 0 0; color: #64748b; font-size: 13px; }
.drawer-body { padding: 20px 24px; }
.incident-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px 20px;
  margin: 0 0 20px;
}
.incident-grid dt { color: #64748b; font-size: 12px; }
.incident-grid dd { margin: 2px 0 0; font-weight: 600; }
.section-title { margin: 8px 0 10px; font-size: 14px; }
.credential-table { width: 100%; border-collapse: collapse; font-size: 13px; }
.credential-table th {
  text-align: left; color: #64748b; font-weight: 500;
  padding: 8px 10px; width: 130px; vertical-align: top;
  border-bottom: 1px solid #f1f5f9;
}
.credential-table td { padding: 8px 10px; border-bottom: 1px solid #f1f5f9; }
.delivery-badge {
  display: inline-block; padding: 2px 10px; border-radius: 999px;
  font-size: 12px; background: #e2e8f0; color: #334155;
}
.delivery-badge[data-status='已投递'] { background: #dcfce7; color: #166534; }
.delivery-badge[data-status='投递中'] { background: #fef9c3; color: #854d0e; }
.delivery-badge[data-status='发送失败'] { background: #fee2e2; color: #991b1b; }
.delivery-badge[data-status='待投递'] { background: #e0f2fe; color: #075985; }
.drawer-actions { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 20px; }
.action-message { margin-top: 12px; font-size: 13px; color: #1d4ed8; }
.empty-hint { color: #94a3b8; font-size: 13px; padding: 12px 0; }
</style>
