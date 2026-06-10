CREATE TABLE IF NOT EXISTS `instruments` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键，仅用于数据库内部管理',
    `venue` VARCHAR(32) NOT NULL COMMENT '交易所标识，第一阶段固定为binance',
    `market_type` VARCHAR(16) NOT NULL COMMENT '市场类型，例如spot、perp',
    `symbol` VARCHAR(64) NOT NULL COMMENT '交易所原始交易对，例如BTCUSDT',
    `base_asset` VARCHAR(32) NOT NULL COMMENT '基础资产，例如BTC',
    `quote_asset` VARCHAR(32) NOT NULL COMMENT '计价资产，例如USDT',
    `instrument_code` VARCHAR(128) NOT NULL COMMENT '系统统一标的编码，例如crypto.binance.perp.BTCUSDT',
    `is_active` TINYINT(1) NOT NULL DEFAULT 1 COMMENT '当前是否启用',
    `tick_size` DECIMAL(20,8) NOT NULL COMMENT '最小价格变动单位',
    `step_size` DECIMAL(20,8) NOT NULL COMMENT '最小数量步长',
    `min_qty` DECIMAL(20,8) NOT NULL COMMENT '最小可下单数量',
    `min_notional` DECIMAL(24,8) NOT NULL COMMENT '最小名义金额',
    `contract_type` VARCHAR(32) NULL COMMENT '合约类型，现货可为空',
    `created_at_utc` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '记录创建UTC时间',
    `updated_at_utc` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最近更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_instruments_venue_market_symbol` (`venue`, `market_type`, `symbol`),
    UNIQUE KEY `uk_instruments_code` (`instrument_code`),
    KEY `idx_instruments_active` (`is_active`),
    KEY `idx_instruments_base_quote` (`base_asset`, `quote_asset`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='标准化交易标的元数据';

CREATE TABLE IF NOT EXISTS `ingest_checkpoints` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `venue` VARCHAR(32) NOT NULL COMMENT '交易所来源',
    `market_type` VARCHAR(16) NOT NULL COMMENT '市场类型',
    `symbol` VARCHAR(64) NOT NULL COMMENT '交易所原始交易对',
    `data_type` VARCHAR(32) NOT NULL COMMENT '数据类型，例如kline、mark_price、depth_snapshot',
    `interval_code` VARCHAR(16) NULL COMMENT 'K线类周期，非K线数据可为空',
    `interval_code_key` VARCHAR(16) GENERATED ALWAYS AS (COALESCE(`interval_code`, '')) STORED COMMENT '唯一键辅助字段，归一化空interval',
    `last_event_ts_ms` BIGINT UNSIGNED NULL COMMENT '最近成功处理的事件时间戳',
    `last_event_dt_utc` DATETIME(3) NULL COMMENT '最近成功处理的事件UTC时间',
    `last_trade_id` BIGINT UNSIGNED NULL COMMENT '为未来trade扩展预留的最近trade_id',
    `last_kline_open_ts_ms` BIGINT UNSIGNED NULL COMMENT 'K线同步最近成功的open_ts_ms',
    `status` VARCHAR(32) NOT NULL DEFAULT 'pending' COMMENT '当前同步状态，例如pending、running、ok、error',
    `last_success_at_utc` DATETIME(3) NULL COMMENT '最近一次成功时间',
    `last_error_message` VARCHAR(1024) NULL COMMENT '最近错误摘要',
    `updated_at_utc` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最近更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_checkpoints_scope` (`venue`, `market_type`, `symbol`, `data_type`, `interval_code_key`),
    KEY `idx_checkpoints_status` (`status`),
    KEY `idx_checkpoints_updated_at` (`updated_at_utc`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='同步推进位置与状态记录';

CREATE TABLE IF NOT EXISTS `data_quality_issues` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `venue` VARCHAR(32) NOT NULL COMMENT '交易所来源',
    `market_type` VARCHAR(16) NOT NULL COMMENT '市场类型',
    `symbol` VARCHAR(64) NOT NULL COMMENT '交易所原始交易对',
    `data_type` VARCHAR(32) NOT NULL COMMENT '问题对应的数据类型',
    `interval_code` VARCHAR(16) NULL COMMENT 'K线类问题对应周期，其余可为空',
    `issue_type` VARCHAR(64) NOT NULL COMMENT '问题类型，例如missing_bar、boundary_error',
    `severity` VARCHAR(16) NOT NULL DEFAULT 'warning' COMMENT '严重程度，例如warning、error',
    `detected_at_utc` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '检测到问题的UTC时间',
    `start_ts_ms` BIGINT UNSIGNED NULL COMMENT '问题起始时间戳',
    `end_ts_ms` BIGINT UNSIGNED NULL COMMENT '问题结束时间戳',
    `description` VARCHAR(2048) NOT NULL COMMENT '问题描述',
    `status` VARCHAR(32) NOT NULL DEFAULT 'open' COMMENT '问题状态，例如open、resolved',
    `resolution_note` VARCHAR(2048) NULL COMMENT '修复说明',
    `resolved_at_utc` DATETIME(3) NULL COMMENT '问题解决UTC时间',
    PRIMARY KEY (`id`),
    KEY `idx_quality_symbol_type_time` (`symbol`, `data_type`, `detected_at_utc`),
    KEY `idx_quality_status` (`status`),
    KEY `idx_quality_severity` (`severity`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='数据质量问题记录';

CREATE TABLE IF NOT EXISTS `file_manifests` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `dataset_name` VARCHAR(128) NOT NULL COMMENT '数据集名称',
    `venue` VARCHAR(32) NOT NULL COMMENT '数据来源交易所',
    `market_type` VARCHAR(16) NOT NULL COMMENT '市场类型',
    `symbol` VARCHAR(64) NOT NULL COMMENT '交易对',
    `data_type` VARCHAR(32) NOT NULL COMMENT '数据集类型，例如kline、depth_snapshot',
    `interval_code` VARCHAR(16) NULL COMMENT '数据集周期',
    `time_boundary_rule` VARCHAR(64) NULL COMMENT '导出层使用的时间边界规则，例如1d@03:42:00',
    `file_format` VARCHAR(16) NOT NULL COMMENT '文件格式，第一阶段默认为parquet',
    `file_path` VARCHAR(1024) NOT NULL COMMENT '导出文件路径',
    `partition_key` VARCHAR(128) NULL COMMENT '导出分区键',
    `start_ts_ms` BIGINT UNSIGNED NOT NULL COMMENT '数据集覆盖起始时间戳',
    `end_ts_ms` BIGINT UNSIGNED NOT NULL COMMENT '数据集覆盖结束时间戳',
    `row_count` BIGINT UNSIGNED NOT NULL COMMENT '文件行数',
    `file_size_bytes` BIGINT UNSIGNED NOT NULL COMMENT '文件字节大小',
    `content_hash` VARCHAR(128) NOT NULL COMMENT '文件内容哈希',
    `version` INT UNSIGNED NOT NULL COMMENT '数据集版本号',
    `generated_by` VARCHAR(128) NOT NULL COMMENT '生成脚本或任务名',
    `generated_at_utc` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '导出完成UTC时间',
    `status` VARCHAR(32) NOT NULL DEFAULT 'ready' COMMENT '当前状态，例如ready、replaced',
    PRIMARY KEY (`id`),
    KEY `idx_manifest_dataset_symbol_time` (`dataset_name`, `symbol`, `start_ts_ms`, `end_ts_ms`),
    KEY `idx_manifest_path` (`file_path`(255)),
    KEY `idx_manifest_status` (`status`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='导出数据集与文件清单';
