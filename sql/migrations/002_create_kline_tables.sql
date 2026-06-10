CREATE TABLE IF NOT EXISTS `spot_klines` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键，仅用于数据库内部管理',
    `venue` VARCHAR(32) NOT NULL COMMENT '数据来源交易所',
    `symbol` VARCHAR(64) NOT NULL COMMENT '交易所原始symbol，例如BTCUSDT',
    `instrument_code` VARCHAR(128) NOT NULL COMMENT '系统统一标的编码',
    `interval_code` VARCHAR(16) NOT NULL COMMENT 'K线周期，例如1h、4h、12h、1d',
    `open_ts_ms` BIGINT UNSIGNED NOT NULL COMMENT 'K线开始时间戳',
    `close_ts_ms` BIGINT UNSIGNED NOT NULL COMMENT 'K线结束时间戳',
    `open_dt_utc` DATETIME(3) NOT NULL COMMENT '开始时间对应UTC时间',
    `close_dt_utc` DATETIME(3) NOT NULL COMMENT '结束时间对应UTC时间',
    `open_price` DECIMAL(20,8) NOT NULL COMMENT '开盘价',
    `high_price` DECIMAL(20,8) NOT NULL COMMENT '最高价',
    `low_price` DECIMAL(20,8) NOT NULL COMMENT '最低价',
    `close_price` DECIMAL(20,8) NOT NULL COMMENT '收盘价',
    `volume` DECIMAL(20,8) NOT NULL COMMENT '基础资产成交量',
    `quote_volume` DECIMAL(24,8) NOT NULL COMMENT '计价资产成交量',
    `trade_count` BIGINT UNSIGNED NOT NULL COMMENT '该时间桶内成交笔数',
    `taker_buy_base_volume` DECIMAL(20,8) NOT NULL COMMENT '主动买入方成交的基础资产数量',
    `taker_buy_quote_volume` DECIMAL(24,8) NOT NULL COMMENT '主动买入方成交的计价资产数量',
    `source` VARCHAR(32) NOT NULL COMMENT '数据来源类型，例如exchange_rest或repair',
    `ingested_at_utc` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '写入本系统的UTC时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_spot_klines_scope` (`venue`, `symbol`, `interval_code`, `open_ts_ms`),
    KEY `idx_spot_klines_symbol_interval_time` (`symbol`, `interval_code`, `open_ts_ms`),
    KEY `idx_spot_klines_instrument_interval_time` (`instrument_code`, `interval_code`, `open_ts_ms`),
    KEY `idx_spot_klines_close_time` (`close_ts_ms`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='标准化现货K线主表';

CREATE TABLE IF NOT EXISTS `perp_klines` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键，仅用于数据库内部管理',
    `venue` VARCHAR(32) NOT NULL COMMENT '数据来源交易所',
    `symbol` VARCHAR(64) NOT NULL COMMENT '交易所原始symbol，例如BTCUSDT',
    `instrument_code` VARCHAR(128) NOT NULL COMMENT '系统统一标的编码',
    `interval_code` VARCHAR(16) NOT NULL COMMENT 'K线周期，例如1h、4h、12h、1d',
    `open_ts_ms` BIGINT UNSIGNED NOT NULL COMMENT 'K线开始时间戳',
    `close_ts_ms` BIGINT UNSIGNED NOT NULL COMMENT 'K线结束时间戳',
    `open_dt_utc` DATETIME(3) NOT NULL COMMENT '开始时间对应UTC时间',
    `close_dt_utc` DATETIME(3) NOT NULL COMMENT '结束时间对应UTC时间',
    `open_price` DECIMAL(20,8) NOT NULL COMMENT '开盘价',
    `high_price` DECIMAL(20,8) NOT NULL COMMENT '最高价',
    `low_price` DECIMAL(20,8) NOT NULL COMMENT '最低价',
    `close_price` DECIMAL(20,8) NOT NULL COMMENT '收盘价',
    `volume` DECIMAL(20,8) NOT NULL COMMENT '基础资产成交量',
    `quote_volume` DECIMAL(24,8) NOT NULL COMMENT '计价资产成交量',
    `trade_count` BIGINT UNSIGNED NOT NULL COMMENT '该时间桶内成交笔数',
    `taker_buy_base_volume` DECIMAL(20,8) NOT NULL COMMENT '主动买入方成交的基础资产数量',
    `taker_buy_quote_volume` DECIMAL(24,8) NOT NULL COMMENT '主动买入方成交的计价资产数量',
    `source` VARCHAR(32) NOT NULL COMMENT '数据来源类型，例如exchange_rest或repair',
    `ingested_at_utc` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '写入本系统的UTC时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_perp_klines_scope` (`venue`, `symbol`, `interval_code`, `open_ts_ms`),
    KEY `idx_perp_klines_symbol_interval_time` (`symbol`, `interval_code`, `open_ts_ms`),
    KEY `idx_perp_klines_instrument_interval_time` (`instrument_code`, `interval_code`, `open_ts_ms`),
    KEY `idx_perp_klines_close_time` (`close_ts_ms`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='标准化永续K线主表';
