# 数据质量监控与元数据治理平台

面向数据质量标准定义、自动巡检、异常告警、元数据采集与数据目录构建的数据质量治理平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 质量规则定义 | `dq_rule_def` | 质量规则 | 规则编号、规则名称、目标表名 |
| 巡检任务管理 | `dq_check_run` | 巡检任务 | 任务编号、关联规则集、数据范围 |
| 质量事件管理 | `dq_incident` | 质量事件 | 事件编号、关联规则、触发时间 |
| 元数据采集器 | `metadata_crawler` | 采集器配置 | 采集器编号、数据源类型、连接地址 |
| 数据表目录 | `table_catalog` | 表目录 | 表编号、表名称、所属库名 |
| 字段画像分析 | `column_profile` | 字段画像 | 画像编号、关联表名、字段名称 |
| 血缘图谱 | `lineage_graph` | 血缘关系 | 关系编号、上游表名、下游表名 |
| 指标波动监控 | `metric_monitor` | 监控指标 | 指标编号、目标表名、监控字段 |
| 质量评分卡 | `dq_scorecard` | 评分记录 | 记录编号、目标表名、完整性得分 |
| 数据分类标签 | `tag_classify` | 分类标签 | 标签编号、标签名称、分类维度 |
| 规则模板库 | `dq_template` | 规则模板 | 模板编号、模板名称、规则类型 |
| 异常模式识别 | `anomaly_pattern` | 异常模式 | 模式编号、模式名称、检测方法 |
| 质量报告生成 | `dq_report` | 质量报告 | 报告编号、报告周期、覆盖表数 |
| 元数据变更对比 | `metadata_diff` | 变更记录 | 变更编号、变更对象、变更类型 |
| 告警通知通道 | `dq_alert_channel` | 告警配置 | 配置编号、关联规则集、通知方式 |
| 质量基线管理 | `dq_baseline` | 基线定义 | 基线编号、目标表名、基线指标 |
| 主数据实体注册 | `mdm_entity` | 主数据实体 | 实体编号、实体名称、业务域 |
| 质量趋势看板 | `dq_trend` | 趋势记录 | 记录编号、目标表名、日期 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 指标波动告警的投递一致性（outbox）

指标波动超阈值时，按"事件落库 → 登记投递凭据 → 按投递身份发送"顺序执行，
解决旧实现"先写质量事件再写通知、中途停下两边不一致"的问题：

- `app/services/delivery_outbox.py`：通知投递登记箱。每条凭据带全局唯一
  **投递身份（delivery_id）**，状态机为 `待投递 → 投递中 → 已投递 / 发送失败`。
- 模拟通知网关按投递身份幂等：同一身份只产生一条实际通知，`inbox` 即实际收到的通知数。
- 中断可区分：`投递中 + 尝试0次` 为**发送前失败**（可安全重发）；
  `投递中 + 尝试≥1次` 为**送达后丢回执**（恢复只确认、不重发）。
- 恢复按投递身份加锁串行：同一次波动两笔并发恢复只有一笔实际投递，另一笔复用同一份回执；
  已送达事件重复恢复直接返回既有回执，兼容旧通知。
- 回执回写质量事件处置清单（`投递身份/投递状态/通知编号/回执时间`），
  告警通知按投递身份消费异常数据。

接口：

| 接口 | 说明 |
| --- | --- |
| `POST /api/metric-monitor/query` | 触发波动检测，`simulate` 可传 `fail_before_send` / `lose_receipt` 演练两种中断 |
| `POST /api/delivery/recover/{事件编号}` | 值班员按原事件恢复投递（并发安全、幂等） |
| `GET /api/delivery/by-incident/{事件编号}` | 查看事件持久化的投递凭据 |
| `GET /api/delivery/box` / `GET /api/delivery/snapshot` | 投递箱清单 / 凭据状态与网关实际通知数 |

前端在「质量事件管理」每行提供「处置记录」入口，抽屉里可查看凭据并提交恢复，
恢复后自动刷新处置单。

演练（先启动后端）：

```bash
backend/.venv/bin/python -I backend/demo/delivery_recovery_http.py            # 直连后端
backend/.venv/bin/python -I backend/demo/delivery_recovery_http.py http://127.0.0.1:5173  # 经前端代理
backend/.venv/bin/python -I backend/demo/delivery_recovery_e2e.py             # 进程内自建服务的完整演练
```

领域：数据工程
子方向：数仓与数据湖
