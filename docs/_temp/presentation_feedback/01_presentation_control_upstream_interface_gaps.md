# 展示控制系统反馈：数据采集系统上游接口缺口记录

本文档记录 `YM_presentation_control` 在 fake BFF 固定后，重新审核并收敛后的 `YM_data_collection` 上游能力缺口。

本文档仍是展示控制系统对数据采集系统的反馈，不是 `YM_data_collection` 的正式实现任务。后续回到数据采集系统时，应先进入数据采集系统自身的正式文档与 todolist 流程。

## 1. 使用边界

- 前端页面不会直连 `YM_data_collection`，只连接 `YM_presentation_control` 的 BFF。
- 展示控制 fake BFF endpoint 是展示层合同，不要求 `YM_data_collection` 实现同名接口。
- BFF 第一波允许 fan-out、短缓存、内存聚合和 mock 兜底，但不应长期替代数据采集系统的业务真相接口。
- 与展示控制系统自身相关的登录、权限、前端交互、BFF session、BFF WebSocket bridge 不属于数据采集系统缺口。
- 已由 data collection 现有接口覆盖的项目，应降级为 BFF 字段映射或 view model 组装。

## 2. 反馈来源

来源系统：

- `YM_presentation_control`

来源页面：

- 数据系统 / 系统总览
- 数据系统 / 标的列表
- 数据系统 / 标的详情
- 数据系统 / 数据同步状态
- 数据系统 / 数据质量与异常
- 数据系统 / 实时订阅状态
- 数据系统 / 数据集导出

来源工作区文档：

- `docs/_temp/08_presentation_control/65_hifi_page_element_api_contract.md`
- `docs/_temp/08_presentation_control/68_upstream_needed_summary_for_data_collection.md`
- `docs/_temp/08_presentation_control/69_fake_bff_upstream_needed_reaudit.md`
- `docs/08_presentation_and_control_system/04_BFF接口实时与上游缺口.md`

## 3. 当前已确认可由展示系统自身承担的内容

| 能力 | 当前归属 | 说明 |
|---|---|---|
| 登录、session、IP allowlist | `YM_presentation_control` | BFF config/session 处理 |
| 前端权限隐藏与按钮占位 | `YM_presentation_control` | 数据采集系统不关心页面权限 |
| 前端到 BFF 的 WebSocket client 状态 | `YM_presentation_control` | BFF bridge 本地状态 |
| BFF 对上游 WebSocket 的连接观测 | `YM_presentation_control` | BFF 自己连接 `/ws/v1/marketdata` 时可本地记录 |
| 页面筛选、分页、排序、toast、loading | `YM_presentation_control` | 展示层交互 |
| RSI / MACD / MA 等简单展示指标 | `YM_presentation_control` 第一波 BFF 可算 | 数据采集系统只需提供足够 K 线原始数据；复杂指标归因子系统 |
| issue resolve / ack 控制流转 | 暂不实现 | 未来控制层能力，不作为当前数据采集缺口 |
| dataset-export view model | `YM_presentation_control` | data collection 只提供 manifests/detail/download |
| instrument detail view model | `YM_presentation_control` | data collection 提供原始行情，BFF 组装页面结构 |

## 4. 当前 data collection 已确认能力

data collection 当前已可提供：

```text
GET /api/v1/system/health
GET /api/v1/system/runtime-status
GET /api/v1/metadata/instruments
GET /api/v1/metadata/coverage
GET /api/v1/metadata/status
GET /api/v1/metadata/quality-issues
GET /api/v1/marketdata/klines/recent
GET /api/v1/marketdata/klines/range
GET /api/v1/marketdata/snapshot/latest
GET /api/v1/marketdata/mark-price/latest
GET /api/v1/marketdata/index-price/latest
GET /api/v1/marketdata/open-interest/latest
GET /api/v1/marketdata/funding-rate/latest
GET /api/v1/marketdata/depth/latest
GET /api/v1/marketdata/slippage/estimate
GET /api/v1/datasets/manifests
GET /api/v1/datasets/manifests/detail
GET /api/v1/datasets/download
WS  /ws/v1/marketdata
```

## 5. 收敛后的缺口总览

| 编号 | 主题 | 当前结论 | 优先级 | 处理方式 |
|---|---|---|---|---|
| `PC-UP-001` | 运行健康、HTTP/WS 连接和全局 freshness | `/system/health` 已基本覆盖 data collection 自身状态 | P2 | 降级为 BFF 映射 + 可选字段增强 |
| `PC-UP-002` | topic/symbol 维度实时流新鲜度 | 第一波由 BFF bridge 内存维护 | P2 | 降级为扩展项 |
| `PC-UP-003` | 批量 status / coverage / matrix | 仍缺长期稳定批量能力 | P1 | 保留 upstream_needed |
| `PC-UP-004` | 质量事件和历史 issue 查询 | 已有 `/metadata/quality-issues`，字段和枚举需映射 | P2 | 降级为字段/语义对齐 |
| `PC-UP-005` | 进程、任务、角色、心跳、异常统计 | `/system/runtime-status` 字段不足 | P1 | 保留 upstream_needed，但优先增强现有 endpoint |
| `PC-UP-006` | 数据集 manifest / export index | 已有 manifests/detail/download | 已解决 | 从 upstream_needed 移除 |
| `PC-UP-007` | 标的详情原始市场数据核验 | 原始行情接口已基本覆盖 | 已覆盖 | 改为 BFF 组装项 |
| `DOC-API-DRIFT` | OpenAPI YAML 契约漂移 | formal OpenAPI schema 与 live API 存在漂移 | P1 | 文档契约修正 |

## 6. PC-UP-003：批量 status / coverage / matrix

来源页面 / 卡片：

- 数据系统 / 系统总览 `C-02`、`T-01`
- 数据系统 / 数据同步状态 `SYN-*`

需要的数据 / 能力：

- 批量查询多个标的、多个市场类型、多个数据类型、多个 interval 的同步状态。
- 批量查询覆盖范围、checkpoint、最新成功写入时间、缺口状态。
- 支持服务端筛选和分页。
- 输出矩阵和表格所需 summary。

当前判断：

- 当前已有 `metadata/status`、`metadata/coverage`、`metadata/instruments`。
- 这些是基础单项能力，可支撑第一波 BFF fan-out + cache。
- 长期仍需要 data collection 提供批量状态 / 覆盖矩阵能力。

建议接口方向：

```text
GET /api/v1/metadata/sync-status
```

或拆分为：

```text
GET /api/v1/metadata/coverage-matrix
GET /api/v1/metadata/status/batch
```

关键字段要求：

```text
filters
summary.ok_count
summary.lag_or_stale_count
summary.missing_or_error_count
summary.not_applicable_count
coverage_matrix.columns[]
coverage_matrix.rows[].cells[]
sync_items[]
thresholds.freshness_warn_threshold
thresholds.freshness_stale_threshold
thresholds.checkpoint_missing_threshold
pagination
```

建议状态口径：

| 状态 | 建议判断 |
|---|---|
| `ok` | 最近成功时间未超过 warn 阈值，且无当前错误 |
| `lag` | 超过 warn 阈值但未超过 stale 阈值 |
| `stale` | 超过 stale 阈值 |
| `missing` | 应有数据或 checkpoint 但不存在 |
| `error` | 上游、解析、写入或校验失败 |
| `n/a` | 该市场类型不适用该数据类型 |
| `todo` | 规划中但尚未接入 |

第一波展示系统处理：

- BFF fan-out 调用现有 status / coverage / instruments。
- BFF 短缓存并统一分页输出给前端。
- 该方式只作为第一波接入策略，不作为长期稳定接口方案。

## 7. PC-UP-005：runtime-status 增强

来源页面 / 卡片：

- 数据系统 / 系统总览 `T-03`

需要的数据 / 能力：

- 查询数据采集系统内关键进程、任务或 worker 的运行状态。
- 显示角色、PID、心跳时间、最近错误、异常数量。
- 区分 `api / collector / writer / websocket / scheduler` 等角色。

当前判断：

- 当前已有 `/api/v1/system/runtime-status`。
- 当前 response 已有简化 `processes[]`，但字段不足。
- 不建议新增同义 `/runtime/processes`，优先增强现有 endpoint。

建议增强 endpoint：

```text
GET /api/v1/system/runtime-status
```

建议新增字段：

```text
processes[].id
processes[].name
processes[].role
processes[].status
processes[].pid
processes[].last_heartbeat_at_utc
processes[].uptime_seconds
processes[].restart_count_24h
processes[].error_count_24h
processes[].last_error_message
summary.running_count
summary.lagging_count
summary.error_count
```

第一波展示系统处理：

- BFF 可展示有限 runtime summary。
- 缺失字段显示 `unknown` 或 mock 状态。
- 该缺口影响系统总览运行进程表准确性，但不阻塞高保真展示。

## 8. 降级项处理说明

### 8.1 PC-UP-001 health overview

BFF 直接读取：

```text
GET /api/v1/system/health
```

BFF 自己补充：

- BFF 到 data collection HTTP 的调用状态
- BFF 到 data collection WS 的 bridge 状态
- `meta.upstreams[]`
- 中文 message

不再要求 data collection 新增 `/runtime/health` 或 `/runtime/overview`。

### 8.2 PC-UP-002 realtime freshness

第一波由 BFF bridge 维护：

```text
GET /api/presentation/v1/data-system/realtime-status
```

未来如果多系统共享 topic freshness、BFF 重启后需要保留状态窗口，或 topic/symbol 数量明显扩大，再考虑 data collection 增强。

### 8.3 PC-UP-004 quality issues

优先复用：

```text
GET /api/v1/metadata/quality-issues
```

BFF 负责字段映射：

```text
id -> issue_id
severity 枚举映射
status 枚举映射
description -> title / msg / details
```

`ack / resolve` 控制动作第一波不实现。

### 8.4 PC-UP-006 dataset manifests

已由 data collection 覆盖：

```text
GET /api/v1/datasets/manifests
GET /api/v1/datasets/manifests/detail
GET /api/v1/datasets/download
```

展示控制的 dataset-export endpoint 是 BFF view model，不再算 data collection 缺口。

### 8.5 PC-UP-007 instrument detail raw market data

已由 data collection 原始行情接口基本覆盖。

BFF 负责：

- spot/perp detail 聚合
- 24h / 1h change 计算或派生
- basis / spread 计算
- MA / RSI / MACD 计算
- depth 20 截取和统一 shape
- fake WS patch 到页面 view model

## 9. DOC-API-DRIFT：OpenAPI YAML 契约漂移

复审发现 data collection live API 和 README 已对齐，但 formal OpenAPI YAML 部分 schema 仍有漂移。

建议单独记录为：

```text
DOC-API-DRIFT: 更新 data collection formal OpenAPI YAML，使其和 live API / README / tests 对齐。
```

重点包括：

- `SystemHealthResponse`
- `RuntimeStatusResponse`
- `CoverageResponse`
- `MetadataStatusResponse`

该项是契约文档修正，不是新的业务 upstream_needed。

## 10. 后续处理建议

建议后续只把 data collection 上游增强收敛成三类：

1. P1：批量 sync-status / coverage matrix
2. P1：runtime-status 增强
3. P1：OpenAPI YAML 契约漂移修正

其他早期缺口不再阻塞真实接入，由展示控制 BFF 做映射、组装、短缓存或 mock fallback。
