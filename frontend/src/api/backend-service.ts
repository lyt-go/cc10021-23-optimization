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

export type DeliveryCredential = {
  投递身份: string
  关联事件: string
  关联规则集: string
  告警级别: string
  消息摘要: string
  通知方式: string
  接收人: string
  投递状态: string
  投递尝试次数: number
  通知编号: string
  回执时间: string
  失败原因: string
  登记时间: string
  更新时间: string
}

export type RecoverResult = {
  ok: boolean
  消息?: string
  message?: string
  投递身份?: string
  关联事件?: string
  通知编号?: string
  回执时间?: string
  投递状态?: string
  实际投递?: boolean
  网关实际通知数?: number
}

/** 按原事件编号查询持久化的投递凭据（事件落库后即可查到，哪怕通知尚未发出）。 */
export async function getDeliveryByIncident(incidentId: string): Promise<{
  ok: boolean
  credential?: DeliveryCredential
  网关实际通知数?: number
  message?: string
}> {
  const response = await request(`/api/delivery/by-incident/${encodeURIComponent(incidentId)}`)
  if (!response.ok) throw new Error('投递凭据读取失败')
  return await response.json()
}

/** 值班员从消息重开处置记录后提交恢复；并发恢复在后端按投递身份只确认一次。 */
export async function recoverDelivery(
  incidentId: string,
  simulate?: string,
): Promise<RecoverResult> {
  const response = await request(
    `/api/delivery/recover/${encodeURIComponent(incidentId)}`,
    { method: 'POST', body: JSON.stringify({ simulate: simulate ?? null }) },
  )
  if (!response.ok) throw new Error('投递恢复请求失败')
  return await response.json()
}
