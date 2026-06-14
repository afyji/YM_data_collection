# YM_data_collection 使用文档

## 1. 这是什么项目

`YM_data_collection` 是个人量化系统的市场数据采集底座。

第一阶段面向 Binance：

- 现货 `spot`
- USDT 本位永续 `perp`
- 标的：`BTCUSDT`、`ETHUSDT`
- 数据类型：
  - kline
  - mark_price
  - index_price
  - open_interest
  - funding_rate
  - depth_snapshot

系统能力：

- 历史数据同步：通过 Binance REST API 回补历史数据
- 实时数据接入：通过 Binance WebSocket 接收实时行情
- 标准化与校验：统一字段、边界检查、质量问题记录
- 持久化：MySQL 作为主存储
- 缓存：Redis 存最新快照、高频实时状态
- 查询服务：HTTP API 查询最近窗口、区间数据、快照、滑点、元数据
- 实时推送：WebSocket 对外推送行情与系统事件
- 数据导出：导出 Parquet 数据集
- 质量检查：gap、重复、边界、新鲜度、缓存一致性、服务健康检查

一句话：

> 这是量化系统的数据地基。后面的因子、回测、策略、风控都应该读它产出的标准化数据。

---

## 2. 环境总览

你只需要理解三层环境：

### 2.1 Python / Conda 环境

项目使用 Conda。

Conda 环境文件：

```text
environment.yml
```

环境名：

```text
ym_quant
```

Python 版本：

```text
Python 3.11
```

### 2.2 MySQL

MySQL 是主存储。

默认配置在：

```text
YM_data_collection/config/base.yaml
```

配置加载规则：

1. 先读取 `base.yaml`
2. 再按 `--env` 叠加 `dev.yaml` 或 `prod.yaml`
3. 如果命令没有显式传 `--env`，则回退到 `base.yaml` 里的 `app.env`
4. 最后再应用 `YM_DATA_*` 环境变量覆盖

因此：

- `base.yaml` 是基础层，不建议通过改它来切换线上 / 本地环境
- 本地示例统一显式写 `--env dev`
- 线上、CI、生产环境统一显式写 `--env prod`

下面这个片段只是 `base.yaml` 的基础层配置：

```yaml
mysql:
  host: 127.0.0.1
  port: 3306
  database: quant_data
  username: quant_user
  password_secret_ref: MYSQL_PASSWORD
```

意思是：

- 数据库名：`quant_data`
- 用户名：`quant_user`
- 密码不写在配置文件里
- 密码从环境变量 `MYSQL_PASSWORD` 读取

如果命令使用 `--env dev`，还会继续叠加 `YM_data_collection/config/dev.yaml`，例如本地默认数据库会变成 `quant_data_dev`。

### 2.3 Redis

Redis 是缓存层。

默认配置：

```yaml
cache:
  enabled: true
  backend: redis
  host: 127.0.0.1
  port: 6379
  password_secret_ref: REDIS_PASSWORD
  db: 0
```

意思是：

- Redis 地址：`127.0.0.1:6379`
- 密码从环境变量 `REDIS_PASSWORD` 读取
- Redis DB：`0`

---

## 3. 第一次安装

以下命令默认你在项目根目录执行。

项目根目录指包含这些内容的目录：

```text
environment.yml
YM_data_collection/
docs/
```

### 3.1 创建 Conda 环境

只需要第一次做：

```bash
conda env create -f environment.yml
```

如果环境已经存在，跳过这一步。

### 3.2 进入 Conda 环境

以后每次使用项目前，都先执行：

```bash
conda activate ym_quant
```

确认 Python 来自这个环境：

```bash
which python
python --version
```

应该看到 Python 3.11。

### 3.3 安装项目本身

第一次创建环境后执行：

```bash
pip install -e YM_data_collection
```

说明：

- `-e` 是 editable install
- 修改代码后不用重新安装
- 这样可以使用 `pyproject.toml` 里定义的命令行入口

---

## 4. 环境变量配置

项目敏感信息不写死在 YAML 里，而是通过环境变量读取。

推荐创建这个文件：

```text
YM_data_collection/.env
```

示例：

```bash
MYSQL_PASSWORD='你的MySQL密码'
REDIS_PASSWORD='你的Redis密码'
DATA_API_TOKEN='dev-api-token'
DATA_WS_TOKEN='dev-ws-token'
INTERNAL_SERVICE_TOKEN='dev-internal-token'
```

说明：

- `YM_data_collection/apps/_cli_common.py` 会自动尝试读取 `YM_data_collection/.env`
- 如果你不想写 `.env`，也可以直接在 shell 里 export

例如：

```bash
export MYSQL_PASSWORD='你的MySQL密码'
export REDIS_PASSWORD='你的Redis密码'
```

当前 `base.yaml` 里 `auth.enabled: false`，所以 API token 默认不是强制的。
但建议仍然保留这些 token，后续打开鉴权时不用再补。

---

## 5. MySQL 准备

### 5.1 创建数据库和用户

如果你有 MySQL root 权限，可以执行：

```bash
mysql -u root -p
```

进入 MySQL 后执行：

```sql
CREATE DATABASE IF NOT EXISTS quant_data CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE USER IF NOT EXISTS 'quant_user'@'localhost' IDENTIFIED BY '你的MySQL密码';
GRANT ALL PRIVILEGES ON quant_data.* TO 'quant_user'@'localhost';
FLUSH PRIVILEGES;
```

然后把同一个密码写入：

```text
YM_data_collection/.env
```

```bash
MYSQL_PASSWORD='你的MySQL密码'
```

### 5.2 初始化表结构

确保已经：

```bash
conda activate ym_quant
```

然后执行：

```bash
python -m YM_data_collection.apps.init_mysql_schema \
  --config YM_data_collection/config/base.yaml \
  --env dev
```

线上环境把 `dev` 替换为 `prod`，不要通过改 `base.yaml` 做环境切换。

迁移 SQL 文件在：

```text
YM_data_collection/sql/migrations/
```

---

## 6. Redis 准备

### 6.1 启动 Redis

如果你的机器已经有 Redis 服务，确认它在运行即可。

常见检查命令：

```bash
redis-cli ping
```

如果设置了密码：

```bash
redis-cli -a '你的Redis密码' ping
```

返回：

```text
PONG
```

说明 Redis 正常。

### 6.2 配置 Redis 密码

如果 Redis 需要密码，把密码写入：

```text
YM_data_collection/.env
```

```bash
REDIS_PASSWORD='你的Redis密码'
```

如果你的本地 Redis 没有密码，当前代码读取 `REDIS_PASSWORD` 时可能会得到空值。
推荐做法是：

- 开发机 Redis 设置一个密码；或
- 仅在本地临时调试时，把 `YM_data_collection/config/base.yaml` 里的 `cache.password_secret_ref` 改成空字符串，并确保代码路径允许无密码连接。

更推荐第一种：开发环境也显式设置 Redis 密码。

---

## 7. 最小验证流程

这是每次确认项目能不能跑的最小流程。

### 7.1 进入环境

```bash
conda activate ym_quant
```

### 7.2 运行单元测试

```bash
python -m pytest YM_data_collection/tests/ -q
```

当前代码验证结果：

```text
779 passed, 0 xfailed
```

说明：

- 单元测试大部分不需要真实 MySQL / Redis
- 如果 MySQL / Redis 没配好，验收脚本会 SKIP 对应外部连接项，但不代表代码失败

### 7.3 运行验收清单

```bash
python -m YM_data_collection.scripts.acceptance_checklist
```

当前代码层面结果：

```text
PASSED: 12 | FAILED: 0 | SKIPPED: 4 | TOTAL: 16
```

其中 4 个 SKIP 通常是：

- MySQL 没连上
- Redis 没连上
- HTTP API 服务未启动
- runtime-status 服务未启动

这不是代码失败，是外部服务或长进程没启动。

---

## 8. 常用命令总览

所有命令默认都在项目根目录执行，并且先进入环境：

```bash
conda activate ym_quant
```

### 8.1 模块方式运行

```bash
python -m YM_data_collection.apps.<模块名> \
  --config YM_data_collection/config/base.yaml \
  --env dev
```

统一规则：

- 所有 `YM_data_collection/apps/` 下的 CLI 入口都支持 `--env {dev,prod}`
- `--env` 显式值优先
- 不传 `--env` 时，才回退到 `base.yaml` 中的 `app.env`

### 8.2 模块入口对照

README 统一使用 `python -m` 方式执行：

| 模块入口 | 用途 |
|---|---|
| `python -m YM_data_collection.apps.init_mysql_schema` | 初始化 MySQL 表结构 |
| `python -m YM_data_collection.apps.sync_instruments` | 同步交易标的元数据 |
| `python -m YM_data_collection.apps.run_historical_klines_sync` | 同步历史 K 线 |
| `python -m YM_data_collection.apps.run_historical_derivatives_sync` | 同步历史衍生品数据 |
| `python -m YM_data_collection.apps.run_recovery_sync` | 根据 checkpoint 恢复同步 |
| `python -m YM_data_collection.apps.run_resync_range` | 重同步指定时间区间 |
| `python -m YM_data_collection.apps.run_realtime_ingest` | 启动实时接入 |
| `python -m YM_data_collection.apps.run_data_api` | 启动 HTTP/WS API 服务 |
| `python -m YM_data_collection.apps.run_export_dataset` | 导出 Parquet 数据集 |
| `python -m YM_data_collection.apps.run_quality_check` | 数据质量检查 |
| `python -m YM_data_collection.apps.run_cache_consistency_check` | 缓存一致性检查 |
| `python -m YM_data_collection.apps.run_service_health_check` | 服务健康检查 |

查看任意命令帮助：

```bash
python -m YM_data_collection.apps.run_historical_klines_sync --help
```

---

## 9. 数据库初始化与元数据同步

### 9.1 初始化 MySQL Schema

```bash
python -m YM_data_collection.apps.init_mysql_schema \
  --config YM_data_collection/config/base.yaml \
  --env dev
```

### 9.2 同步 Binance 标的列表

同步永续标的：

```bash
python -m YM_data_collection.apps.sync_instruments \
  --config YM_data_collection/config/base.yaml \
  --env dev \
  --venue binance \
  --market-type perp
```

同步现货标的：

```bash
python -m YM_data_collection.apps.sync_instruments \
  --config YM_data_collection/config/base.yaml \
  --env dev \
  --venue binance \
  --market-type spot
```

---

## 10. 历史数据同步

### 10.1 历史 K 线同步

默认核心周期统一为 `1m`、`1h`、`1d`。这三个周期覆盖高频、小时级和日线级，其他周期可以从这三类数据快速聚合生成。

示例：同步 BTCUSDT / ETHUSDT 永续默认核心 K 线周期的一小段时间。

```bash
python -m YM_data_collection.apps.run_historical_klines_sync \
  --config YM_data_collection/config/base.yaml \
  --env dev \
  --venue binance \
  --market-type perp \
  --symbols BTCUSDT ETHUSDT \
  --start-ts-ms 2024-1-1 \
  --end-ts-ms 2024-1-7
```

如果确实需要临时拉其他周期，可以显式传 `--intervals`：

```bash
python -m YM_data_collection.apps.run_historical_klines_sync \
  --config YM_data_collection/config/base.yaml \
  --env dev \
  --venue binance \
  --market-type perp \
  --symbols BTCUSDT \
  --intervals 1m 1h 1d \
  --start-ts-ms 2024-1-1 \
  --end-ts-ms 2024-1-7
```

常用参数：

- `--venue`：交易所，当前用 `binance`
- `--market-type`：市场类型，`spot` 或 `perp`
- `--symbols`：标的列表，例如 `BTCUSDT ETHUSDT`
- `--intervals`：K 线周期列表；不传时默认 `1m 1h 1d`
- 当前命令支持值：`1m`、`5m`、`15m`、`1h`、`4h`、`12h`、`1d`
- 单个周期也要写成参数列表形式，例如 `--intervals 1h`
- `--start-ts-ms`：开始时间，支持毫秒时间戳或日期字符串，例如 `2024-1-1`
- `--end-ts-ms`：结束时间，支持毫秒时间戳或日期字符串，例如 `2024-1-7`
- 日期字符串支持 `2024-1-1`、`2024-1-1 12:30:00`、`2024-1-1T12:30:00+08:00`
- 对 `--start-ts-ms`，日期格式默认按当天 `00:00:00.000 UTC`
- 对 `--end-ts-ms`，日期格式默认按当天 `23:59:59.999 UTC`
- `--dry-run`：只拉取、标准化、校验，不写 MySQL

开发测试建议先跑很短区间。

### 10.2 历史衍生品数据同步

主入口：

- 批量历史回补：`python -m YM_data_collection.apps.run_historical_derivatives_sync`
- 局部补洞 / 手动重跑：`python -m YM_data_collection.apps.run_resync_range`

当前支持的永续衍生品数据：

- mark_price
- index_price
- open_interest
- funding_rate

字段含义：

- `mark_price`：标记价格。交易所用于永续合约盈亏计算、强平判断和风险控制的参考价格，不等同于最新成交价。
- `index_price`：指数价格。通常由多个现货市场价格聚合而成，是永续合约定价的底层参考价格。
- `funding_rate`：资金费率。永续合约多空双方按周期支付/收取的费率，用于让永续价格回归现货锚定。
- `open_interest`：持仓量。某一时刻市场上未平仓合约的总量，用来观察市场参与度和杠杆拥挤程度。

容易混淆的点：

- `mark_price` 和 `index_price` 都不是“最新成交价”
- `index_price` 更偏底层现货锚
- `mark_price` 更偏交易所风控和清算使用
- `funding_rate` 不是价格，而是一个周期性费率
- `open_interest` 不是成交量，而是未平仓总量

衍生品数据采集对照表：

| 数据类型 | 含义 | 批量历史回补 | 局部重同步 | 实时来源 | interval / period 语义 |
|---|---|---|---|---|---|
| `funding_rate` | 永续合约周期性资金费率 | `run_historical_derivatives_sync` | `run_resync_range` | perp `@markPrice@1s` 事件里的 `r` / `T` | 历史无 `interval`；实时也不是 K 线 interval |
| `mark_price` | 交易所风控使用的标记价格 | `run_historical_derivatives_sync` | `run_resync_range` | perp `@markPrice@1s` 事件里的 `p` | 历史固定拉 `1h` mark price kline |
| `index_price` | 多现货市场聚合出的指数价格 | `run_historical_derivatives_sync` | 当前不支持 | perp `@markPrice@1s` 事件里的 `i` | 历史固定拉 `1h` index price kline，使用 Binance `indexPriceKlines` |
| `open_interest` | 未平仓合约总量 | `run_historical_derivatives_sync` | `run_resync_range` | 当前 `run_realtime_ingest` 里未真正接上 live subscription | 历史固定拉 `5m` open interest history；Binance 仅提供最近 1 个月，auto 模式会裁剪到可用交集 |

说明：

- 当前历史衍生品同步命令支持 `mark_price`、`index_price`、`open_interest`、`funding_rate`
- `run_resync_range` 的定位是局部补洞，不是全量历史回补主入口
- `funding_rate` 在 `run_resync_range` 里是支持的；它没有额外 `interval`
- `mark_price` / `index_price` / `funding_rate` 在实时链路里共享同一个 Binance perp `@markPrice@1s` 流
- `open_interest` 历史接口是 Binance 特殊限制项，只能拉最近约 1 个月；auto 模式只要请求区间包含最近 1 个月窗口的一部分，就会自动拉交集部分

示例：

```bash
python -m YM_data_collection.apps.run_historical_derivatives_sync \
  --config YM_data_collection/config/base.yaml \
  --env dev \
  --venue binance \
  --symbols BTCUSDT \
  --start-ts-ms 2024-1-1 \
  --end-ts-ms 2024-1-7
```

最佳实践：

- 默认不要传 `--data-types`
- 脚本会按 `auto` 模式自动选择当前时间区间内“能拉的都尽量拉”
- 例如较老区间会自动跳过 Binance 无法提供的 `open_interest`
- 只有在你明确想限制字段范围时，才手动传 `--data-types`

常用参数：

- `--symbols`：可选；不传时默认使用 `config.binance.symbols`
- `--data-types`：支持 `auto`、`funding_rate`、`mark_price`、`index_price`、`open_interest`
- `auto`：默认模式；脚本自动决定当前区间能拉哪些字段，默认包含 `mark_price`、`index_price`、`funding_rate`；当请求区间和最近 1 个月窗口有交集时也包含 `open_interest`
- `funding_rate`：走 funding rate 历史接口，没有 `interval`
- `mark_price`：固定拉 `1h`
- `index_price`：固定拉 `1h`，写入 `perp_index_prices`
- `open_interest`：固定拉 `5m`，且 Binance 只提供最近约 1 个月；auto 模式会自动裁剪到可用交集

---

## 11. 断点恢复与区间重同步

### 11.1 从 checkpoint 恢复

```bash
python -m YM_data_collection.apps.run_recovery_sync \
  --config YM_data_collection/config/base.yaml \
  --env dev
```

用途：

- 读取 ingest_checkpoints
- 找出上次失败或未完成位置
- 尝试恢复最近缺口

### 11.2 指定区间重同步

```bash
python -m YM_data_collection.apps.run_resync_range \
  --config YM_data_collection/config/base.yaml \
  --env dev \
  --venue binance \
  --market-type perp \
  --symbol BTCUSDT \
  --data-type kline \
  --interval 1h \
  --start-ts-ms 2024-1-1 \
  --end-ts-ms 2024-1-7
```

用途：

- 手动修复某个标的、某个时间段的数据
- 可用于质量检查发现 gap 后的局部重跑
- `--data-type` 当前支持：`kline`、`funding_rate`、`mark_price`、`open_interest`
- 如果 `--data-type kline`，则 `--interval` 当前支持：`1m`、`5m`、`15m`、`1h`、`4h`、`12h`、`1d`
- 如果 `--data-type funding_rate`，不需要 `--interval`
- 如果 `--data-type mark_price`，当前代码内部固定按 `1h` 拉取
- 如果 `--data-type open_interest`，当前代码内部固定按 `5m` 拉取
- `index_price` 当前不支持这个命令

---

## 12. 实时数据接入

启动实时接入：

```bash
python -m YM_data_collection.apps.run_realtime_ingest \
  --config YM_data_collection/config/base.yaml \
  --env dev
```

它会：

- 连接 Binance WebSocket
- 订阅配置里的 symbols 和核心 K 线周期（默认 `1m`、`1h`、`1d`）以及数据流
- 高频数据优先写 Redis
- 低频/闭合数据写 MySQL
- 按 realtime_persistence 配置刷盘

当前和衍生品相关的实时语义：

- `mark_price`、`index_price`、`funding_rate` 共享同一个 perp `@markPrice@1s` 流
- `funding_rate` 虽然来自这个实时流，但它不是 K 线周期概念，没有 `1h/4h` 这种 interval
- `open_interest` 当前在 `--topics` 中有保留名字，但 `run_realtime_ingest` 里还没有真正建立 live subscription

注意：

- 这是长驻进程
- 开发测试只做短时间 smoke test
- 看到数据进入 Redis/MySQL 后即可停止
- 不要无意中开多个重复实时接入进程

停止方式：

```bash
Ctrl+C
```

---

## 13. 启动 HTTP / WebSocket API 服务

### 13.1 启动服务

```bash
python -m YM_data_collection.apps.run_data_api \
  --config YM_data_collection/config/base.yaml \
  --env dev \
  --http-host 127.0.0.1 \
  --http-port 18081 \
  --ws-host 127.0.0.1 \
  --ws-port 8001
```

默认 HTTP 地址：

```text
http://127.0.0.1:18081
```

默认 WebSocket 地址：

```text
ws://127.0.0.1:18081/ws/v1/marketdata
```

说明：当前 FastAPI HTTP 与 WebSocket 运行在同一个 uvicorn 服务端口上；`/ws/v1/marketdata` 是实际注册的 WS endpoint。

### 13.2 检查 HTTP 服务

```bash
curl http://127.0.0.1:18081/api/v1/system/health
```

```bash
curl http://127.0.0.1:18081/api/v1/system/runtime-status
```

如果后续打开 token 鉴权，需要加：

```bash
curl -H "X-API-Token: dev-api-token" \
  http://127.0.0.1:18081/api/v1/system/health
```

---

## 14. HTTP API 使用

### 14.1 行情数据接口

| 端点 | 说明 |
|---|---|
| `GET /api/v1/marketdata/klines/recent` | 最近 N 根 K 线 |
| `GET /api/v1/marketdata/klines/range` | 区间 K 线查询 |
| `GET /api/v1/marketdata/snapshot/latest` | 最新综合快照 |
| `GET /api/v1/marketdata/mark-price/latest` | 最新标记价格 |
| `GET /api/v1/marketdata/index-price/latest` | 最新指数价格 |
| `GET /api/v1/marketdata/open-interest/latest` | 最新持仓量 |
| `GET /api/v1/marketdata/funding-rate/latest` | 最新资金费率 |
| `GET /api/v1/marketdata/depth/latest` | 最新盘口快照 |
| `GET /api/v1/marketdata/slippage/estimate` | 滑点估算 |

示例：最近 K 线：

```bash
curl "http://127.0.0.1:18081/api/v1/marketdata/klines/recent?venue=binance&market_type=perp&symbol=BTCUSDT&interval=1h&count=20"
```

示例：区间 K 线：

```bash
curl "http://127.0.0.1:18081/api/v1/marketdata/klines/range?venue=binance&market_type=perp&symbol=BTCUSDT&interval=1h&start_ts_ms=1704067200000&end_ts_ms=1704672000000"
```

示例：最新快照：

```bash
curl "http://127.0.0.1:18081/api/v1/marketdata/snapshot/latest?venue=binance&market_type=perp&symbol=BTCUSDT"
```

示例：latest 单项行情：

```bash
curl "http://127.0.0.1:18081/api/v1/marketdata/mark-price/latest?venue=binance&market_type=perp&symbol=BTCUSDT"
curl "http://127.0.0.1:18081/api/v1/marketdata/index-price/latest?venue=binance&market_type=perp&symbol=BTCUSDT"
curl "http://127.0.0.1:18081/api/v1/marketdata/open-interest/latest?venue=binance&market_type=perp&symbol=BTCUSDT"
curl "http://127.0.0.1:18081/api/v1/marketdata/funding-rate/latest?venue=binance&market_type=perp&symbol=BTCUSDT"
```

示例：滑点估算：

```bash
curl "http://127.0.0.1:18081/api/v1/marketdata/slippage/estimate?venue=binance&market_type=perp&symbol=BTCUSDT&side=buy&quote_asset_amount=1000"
```

### 14.2 元数据接口

| 端点 | 说明 |
|---|---|
| `GET /api/v1/metadata/instruments` | 标的列表 |
| `GET /api/v1/metadata/coverage` | 数据覆盖情况 |
| `GET /api/v1/metadata/status` | 同步状态 |
| `GET /api/v1/metadata/quality-issues` | 质量问题列表 |

示例：

```bash
curl "http://127.0.0.1:18081/api/v1/metadata/instruments?venue=binance&market_type=perp"
curl "http://127.0.0.1:18081/api/v1/metadata/coverage?venue=binance&market_type=perp&symbol=BTCUSDT&data_type=kline&interval=1h"
curl "http://127.0.0.1:18081/api/v1/metadata/status?venue=binance&market_type=perp&symbol=BTCUSDT&data_type=kline&interval=1h"
curl "http://127.0.0.1:18081/api/v1/metadata/quality-issues?symbol=BTCUSDT&data_type=kline&status_filter=open"
```

### 14.3 系统接口

| 端点 | 说明 |
|---|---|
| `GET /api/v1/system/health` | 健康检查 |
| `GET /api/v1/system/runtime-status` | 运行时状态 |

### 14.4 数据集接口

| 端点 | 说明 |
|---|---|
| `GET /api/v1/datasets/manifests` | 导出文件清单 |
| `GET /api/v1/datasets/manifests/detail` | 单个导出文件详情 |
| `GET /api/v1/datasets/download` | 下载导出文件 |

`datasets` 系列统一使用 `manifest_id` 作为外部稳定标识。

示例：

```bash
curl "http://127.0.0.1:18081/api/v1/datasets/manifests?symbol=BTCUSDT&data_type=kline"
curl "http://127.0.0.1:18081/api/v1/datasets/manifests/detail?manifest_id=1"
curl -OJ "http://127.0.0.1:18081/api/v1/datasets/download?manifest_id=1"
```

### 14.5 标准响应格式

```json
{
  "success": true,
  "code": "OK",
  "message": "...",
  "data": {},
  "meta": {
    "source": "cache",
    "fallback_used": false,
    "cache_refreshed": false
  }
}
```

---

## 15. WebSocket API 使用

默认地址：

```text
ws://127.0.0.1:18081/ws/v1/marketdata
```

### 15.1 订阅消息格式

```json
{
  "action": "subscribe",
  "request_id": "req-001",
  "topics": [
    "marketdata.kline:binance:perp:BTCUSDT:1h"
  ]
}
```

### 15.2 取消订阅

```json
{
  "action": "unsubscribe",
  "request_id": "req-002",
  "topics": [
    "marketdata.kline:binance:perp:BTCUSDT:1h"
  ]
}
```

### 15.3 心跳

```json
{
  "action": "ping",
  "request_id": "ping-001",
  "ts_ms": 1710000000000
}
```

服务端返回：

```json
{
  "type": "pong",
  "request_id": "ping-001",
  "ts_ms": 1710000000000
}
```

### 15.4 可用 Topic

| Topic | 说明 |
|---|---|
| `marketdata.kline` | K 线更新 |
| `marketdata.mark_price` | 标记价格 |
| `marketdata.index_price` | 指数价格 |
| `marketdata.open_interest` | 持仓量 |
| `marketdata.funding_rate` | 资金费率 |
| `marketdata.depth_snapshot` | 深度快照 |
| `system.quality_event` | 质量事件 |
| `system.stream_status` | 流状态事件 |

常见 topic key 形态：

```text
marketdata.kline:binance:perp:BTCUSDT:1h
marketdata.mark_price:binance:perp:BTCUSDT
marketdata.index_price:binance:perp:BTCUSDT
marketdata.open_interest:binance:perp:BTCUSDT
marketdata.funding_rate:binance:perp:BTCUSDT
marketdata.depth_snapshot:binance:perp:BTCUSDT
system.quality_event:binance:perp:BTCUSDT
system.stream_status
```

---

## 16. 数据导出

### 16.1 导出 K 线 Parquet

```bash
python -m YM_data_collection.apps.run_export_dataset \
  --config YM_data_collection/config/base.yaml \
  --env dev \
  --dataset-name btcusdt_1h_kline \
  --symbol BTCUSDT \
  --data-type kline \
  --source-interval 1h \
  --start-ts-ms 2024-1-1 \
  --end-ts-ms 2024-1-7 \
  --output-dir artifacts/datasets
```

### 16.2 重采样导出

例如从 1h 重采样到 4h：

```bash
python -m YM_data_collection.apps.run_export_dataset \
  --config YM_data_collection/config/base.yaml \
  --env dev \
  --dataset-name btcusdt_4h_from_1h \
  --symbol BTCUSDT \
  --data-type kline \
  --source-interval 1h \
  --target-interval 4h \
  --start-ts-ms 2024-1-1 \
  --end-ts-ms 2024-1-7 \
  --output-dir artifacts/datasets \
  --version v1
```

常用参数：

- `--dataset-name`：数据集名称
- `--symbol`：标的
- `--data-type`：数据类型，当前支持 `kline`、`mark_price`、`index_price`、`open_interest`、`funding_rate`
- `--source-interval`：源 K 线周期；常见值见命令 `--help`
- `--target-interval`：目标周期，可选；当前支持 `1m`、`5m`、`15m`、`1h`、`4h`、`8h`、`1d`
- `--offset-minutes`：边界偏移分钟数
- `--aggregation-mode`：聚合模式，当前只支持 `default`
- `--version`：版本号
- `--output-dir`：输出目录

---

## 17. 质量检查和健康检查

### 17.1 数据质量检查

```bash
python -m YM_data_collection.apps.run_quality_check \
  --config YM_data_collection/config/base.yaml \
  --env dev \
  --venue binance \
  --market-type perp \
  --symbols BTCUSDT \
  --data-types kline \
  --intervals 1h \
  --start-ts-ms 2024-1-1 \
  --end-ts-ms 2024-1-7
```

检查内容包括：

- 是否有 gap
- 是否重复
- K 线边界是否正确
- 数据新鲜度是否满足阈值
- `--data-types` 当前支持：`kline`、`depth_snapshot`
- `--intervals` 用于指定要检查的 K 线周期；如果只检查一个周期，写成 `--intervals 1h`
- 当前质量检查命令支持的 K 线周期：`1m`、`5m`、`15m`、`1h`、`4h`、`1d`

### 17.2 Redis / MySQL 缓存一致性检查

```bash
python -m YM_data_collection.apps.run_cache_consistency_check \
  --config YM_data_collection/config/base.yaml \
  --env dev
```

当前 `--data-types` 支持：

- `kline`
- `mark_price`
- `index_price`
- `open_interest`
- `funding_rate`
- `depth_snapshot`

### 17.3 服务健康检查

只检查本地模块和依赖：

```bash
python -m YM_data_collection.apps.run_service_health_check \
  --config YM_data_collection/config/base.yaml \
  --env dev
```

如果 API 服务已经启动，可以加 HTTP 地址：

```bash
python -m YM_data_collection.apps.run_service_health_check \
  --config YM_data_collection/config/base.yaml \
  --env dev \
  --http-url http://127.0.0.1:18081
```

---

## 18. 推荐日常使用流程

### 18.1 只开发 / 跑测试

```bash
conda activate ym_quant
python -m pytest YM_data_collection/tests/ -q
```

### 18.2 第一次准备完整本地环境

```bash
conda env create -f environment.yml
conda activate ym_quant
pip install -e YM_data_collection
```

然后：

1. 配置 `YM_data_collection/.env`
2. 准备 MySQL 数据库和用户
3. 准备 Redis
4. 初始化 MySQL schema
5. 跑测试和验收

```bash
python -m YM_data_collection.apps.init_mysql_schema \
  --config YM_data_collection/config/base.yaml \
  --env dev
python -m pytest YM_data_collection/tests/ -q
python -m YM_data_collection.scripts.acceptance_checklist
```

### 18.3 采集一小段历史数据

```bash
python -m YM_data_collection.apps.sync_instruments \
  --config YM_data_collection/config/base.yaml \
  --env dev \
  --venue binance \
  --market-type perp

python -m YM_data_collection.apps.run_historical_klines_sync \
  --config YM_data_collection/config/base.yaml \
  --env dev \
  --venue binance \
  --market-type perp \
  --symbols BTCUSDT \
  --intervals 1h \
  --start-ts-ms 2024-1-1 \
  --end-ts-ms 2024-1-1
```

### 18.4 启动查询服务

```bash
python -m YM_data_collection.apps.run_data_api \
  --config YM_data_collection/config/base.yaml \
  --env dev \
  --http-host 127.0.0.1 \
  --http-port 18081 \
  --ws-host 127.0.0.1 \
  --ws-port 8001
```

另开一个终端测试：

```bash
curl http://127.0.0.1:18081/api/v1/system/health
```

### 18.5 短时间启动实时接入

```bash
python -m YM_data_collection.apps.run_realtime_ingest \
  --config YM_data_collection/config/base.yaml \
  --env dev
```

观察一小段时间，确认数据流正常后 `Ctrl+C` 停止。

---

## 19. 测试

### 19.1 全量测试

```bash
python -m pytest YM_data_collection/tests/ -q
```

当前状态：

```text
779 passed, 0 xfailed, 51 个测试文件
```

### 19.2 指定测试文件

```bash
python -m pytest YM_data_collection/tests/test_contract_ws.py -q
```

当前 WS 契约测试状态：

```text
91 passed, 0 xfailed
```

### 19.3 验收清单

```bash
python -m YM_data_collection.scripts.acceptance_checklist
```

输出 16 项检查的 PASS / FAIL / SKIP 状态。

### 19.4 测试结果如何理解

- `passed`：通过
- `failed`：失败，需要修
- `xfailed`：预期失败，当前应为 0
- `skipped`：跳过，通常是外部服务没启动或没配置
- warnings：警告，不等于失败，但后续可逐步清理

---

## 20. 常见问题排查

| 问题 | 优先排查 |
|---|---|
| `conda activate ym_quant` 找不到环境 | 先执行 `conda env create -f environment.yml` |
| import `YM_data_collection` 失败 | 确认在项目根目录，或执行 `pip install -e YM_data_collection` |
| MySQL 连接失败 | 检查 MySQL 是否启动、`quant_user` 是否存在、`MYSQL_PASSWORD` 是否正确 |
| Redis 连接失败 | 检查 Redis 是否启动、密码是否正确、`REDIS_PASSWORD` 是否设置 |
| Binance 请求失败 | 检查网络、代理、防火墙、请求时间范围是否合理 |
| API 返回 401 | 如果开启鉴权，检查 `X-API-Token` 是否等于 `DATA_API_TOKEN` |
| API 端口占用 | 换 `--http-port`，或停止旧进程 |
| WebSocket 端口占用 | 换 `--ws-port`，或停止旧进程 |
| 历史同步很慢 | 缩短时间区间，减少 symbols/intervals |
| 导出文件为空 | 确认 MySQL 中对应区间有数据 |
| 验收脚本有 SKIP | 通常是 MySQL/Redis/API 服务没启动，不一定是代码错 |

---

## 21. 长进程管理（supervisord）

以下命令是长进程或可能运行较久：

- `python -m YM_data_collection.apps.run_realtime_ingest`
- `python -m YM_data_collection.apps.run_data_api`
- 大时间范围的 `python -m YM_data_collection.apps.run_historical_klines_sync`
- 大时间范围的 `python -m YM_data_collection.apps.run_historical_derivatives_sync`
- 大数据量 `python -m YM_data_collection.apps.run_export_dataset`

如果你只管理 1 到 2 个进程，直接开两个终端也行。

如果你未来会有很多常驻进程，Ubuntu 上更推荐直接用 `supervisord`。

它的优势很直接：

- `apt install supervisor` 就能装
- 一个命令看全部进程状态
- 一个命令重启单个进程
- 配置比纯 `systemd` 批量管理更省事
- 适合单机托管多个 Python 常驻进程

### 21.1 Ubuntu 安装

```bash
sudo apt update
sudo apt install -y supervisor
sudo systemctl enable --now supervisor
sudo systemctl status supervisor
```

### 21.2 路径约定

下面所有命令都按“绝对路径”写，不再使用相对路径。

先约定你自己的部署路径：

```bash
PROJECT_ROOT=/home/<your_user>/quant_system
CONFIG_PATH=${PROJECT_ROOT}/YM_data_collection/config/base.yaml
CONDA_BIN=/home/<your_user>/miniconda3/bin/conda
LOG_DIR=/var/log/ym_data_collection
```

创建日志目录：

```bash
sudo mkdir -p ${LOG_DIR}
sudo chown -R $USER:$USER ${LOG_DIR}
```

说明：

- `PROJECT_ROOT` 必须是项目根目录绝对路径
- `CONFIG_PATH` 必须是配置文件绝对路径
- `CONDA_BIN` 指向 conda 可执行文件绝对路径
- 这里优先使用 `conda run -n ym_quant`，不是直接写环境里的 `python` 路径

### 21.3 supervisord 配置

推荐把项目配置放到：

```text
/etc/supervisor/conf.d/ym_data_collection.conf
```

也可以先参考仓库里的样例文件：

- [ym_data_collection.conf.example](deploy/supervisor/ym_data_collection.conf.example)

如果你打算直接从样例文件开始，可以先复制一份：

```bash
sudo cp ${PROJECT_ROOT}/YM_data_collection/deploy/supervisor/ym_data_collection.conf.example /etc/supervisor/conf.d/ym_data_collection.conf
```

最小示例配置：

```ini
[program:ym_realtime_ingest]
directory=/home/<your_user>/quant_system
command=/home/<your_user>/miniconda3/bin/conda run --no-capture-output -n ym_quant python -m YM_data_collection.apps.run_realtime_ingest --config /home/<your_user>/quant_system/YM_data_collection/config/base.yaml --env dev
autostart=true
autorestart=true
startsecs=5
stopasgroup=true
killasgroup=true
stdout_logfile=/var/log/ym_data_collection/realtime_ingest.log
stderr_logfile=/var/log/ym_data_collection/realtime_ingest.err.log
environment=PYTHONUNBUFFERED="1"

[program:ym_data_api]
directory=/home/<your_user>/quant_system
command=/home/<your_user>/miniconda3/bin/conda run --no-capture-output -n ym_quant python -m YM_data_collection.apps.run_data_api --config /home/<your_user>/quant_system/YM_data_collection/config/base.yaml --env dev
autostart=true
autorestart=true
startsecs=5
stopasgroup=true
killasgroup=true
stdout_logfile=/var/log/ym_data_collection/data_api.log
stderr_logfile=/var/log/ym_data_collection/data_api.err.log
environment=PYTHONUNBUFFERED="1"
```

说明：

- `directory` 必须是项目根目录绝对路径
- `command` 里使用 `conda run --no-capture-output -n ym_quant python -m ...`
- `--config` 使用配置文件绝对路径
- `autostart=true` 表示开机后由 supervisor 自动拉起
- `autorestart=true` 表示异常退出后自动重启
- `stopasgroup=true` 和 `killasgroup=true` 用来避免子进程残留
- `PYTHONUNBUFFERED=1` 让日志及时刷到文件

如果 `conda run` 在你的机器上不稳定，再退回到环境里的 Python 绝对路径。也就是把 `command=` 改成下面这种：

```ini
command=/home/<your_user>/miniconda3/envs/ym_quant/bin/python -m YM_data_collection.apps.run_realtime_ingest --config /home/<your_user>/quant_system/YM_data_collection/config/base.yaml --env dev
```

### 21.4 让配置生效

```bash
sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl status
```

### 21.5 常用命令

查看全部进程状态：

```bash
sudo supervisorctl status
```

启动单个进程：

```bash
sudo supervisorctl start ym_realtime_ingest
sudo supervisorctl start ym_data_api
```

停止单个进程：

```bash
sudo supervisorctl stop ym_realtime_ingest
sudo supervisorctl stop ym_data_api
```

重启某个进程：

```bash
sudo supervisorctl restart ym_realtime_ingest
sudo supervisorctl restart ym_data_api
```

重启全部受管进程：

```bash
sudo supervisorctl restart all
```

看日志：

```bash
tail -f /var/log/ym_data_collection/realtime_ingest.log
tail -f /var/log/ym_data_collection/data_api.log
```

如果你改了 `/etc/supervisor/conf.d/ym_data_collection.conf`，记得重新加载：

```bash
sudo supervisorctl reread
sudo supervisorctl update
```

### 21.6 适合什么场景

推荐：

- 单机部署
- Ubuntu 常驻运行
- 你未来会有 5 到 10 个常驻进程
- 你希望安装简单、运维动作简单

不推荐只靠它解决的场景：

- 强依赖更细粒度的系统权限隔离
- 你已经有完整容器编排体系

一个常见折中方案是：

- Ubuntu 机器上直接用 `supervisord` 管项目常驻进程
- 更底层的 MySQL、Redis 继续交给系统服务自己管理

开发阶段原则：

- 先用小时间区间验证
- 不要直接跑多年数据
- 实时接入只做 smoke test 时，确认数据流后立即停止
- 不要同时启动多个相同采集进程

---

## 22. 项目结构

```text
YM_data_collection/
  adapters/              # Binance REST/WS adapter、rate limiter
  apps/                  # CLI 入口
  api/                   # FastAPI HTTP routes
  cache/                 # Redis client 和 keyspace
  config/                # Pydantic 配置模型和 base/dev/prod YAML
  domain/                # 核心领域对象 / DTO
  export/                # Parquet 导出和重采样
  ingestion/             # 历史/实时接入处理链路
  normalization/         # 标准化逻辑
  persistence/           # MySQL、migration、repository
  quality/               # 数据质量检查
  services/              # 查询、快照、滑点、覆盖范围服务
  tests/                 # 自动化测试
  validation/            # 校验逻辑
  ws/                    # 对外 WebSocket 协议和推送
```

---

## 23. 架构简图

```text
Binance REST / WebSocket
        |
        v
  adapters
        |
        v
  normalization
        |
        v
  validation
        |
        +------------------+
        |                  |
        v                  v
      MySQL              Redis
  主历史存储            最新状态/缓存
        |                  |
        +---------+--------+
                  |
                  v
              services
                  |
        +---------+---------+
        |                   |
        v                   v
     HTTP API          WebSocket Push
```

---

## 24. 最重要的三条命令

如果你忘了所有细节，只记这三条：

```bash
conda activate ym_quant
pip install -e YM_data_collection
python -m pytest YM_data_collection/tests/ -q
```

如果这三条能跑通，Python 项目环境基本就是好的。
