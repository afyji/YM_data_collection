CREATE TABLE IF NOT EXISTS `perp_funding_rates` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `venue` VARCHAR(32) NOT NULL COMMENT '交易所来源',
    `symbol` VARCHAR(64) NOT NULL COMMENT '原始交易对',
    `instrument_code` VARCHAR(128) NOT NULL COMMENT '系统统一标的编码',
    `funding_time_ts_ms` BIGINT UNSIGNED NOT NULL COMMENT '资金费率生效时间戳',
    `funding_time_dt_utc` DATETIME(3) NOT NULL COMMENT '资金费率生效UTC时间',
    `funding_rate` DECIMAL(20,10) NOT NULL COMMENT '资金费率',
    `mark_price` DECIMAL(20,8) NULL COMMENT '对应时点标记价格，若来源可提供则保留',
    `source` VARCHAR(32) NOT NULL COMMENT '来源说明',
    `ingested_at_utc` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '写入UTC时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_perp_funding_scope` (`venue`, `symbol`, `funding_time_ts_ms`),
    KEY `idx_funding_symbol_time` (`symbol`, `funding_time_ts_ms`),
    KEY `idx_funding_instrument_time` (`instrument_code`, `funding_time_ts_ms`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='永续资金费率主表';

CREATE TABLE IF NOT EXISTS `perp_open_interest` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `venue` VARCHAR(32) NOT NULL COMMENT '交易所来源',
    `symbol` VARCHAR(64) NOT NULL COMMENT '原始交易对',
    `instrument_code` VARCHAR(128) NOT NULL COMMENT '系统统一标的编码',
    `event_ts_ms` BIGINT UNSIGNED NOT NULL COMMENT '事件时间戳',
    `event_dt_utc` DATETIME(3) NOT NULL COMMENT '事件对应UTC时间',
    `open_interest` DECIMAL(24,8) NOT NULL COMMENT '持仓量',
    `open_interest_value` DECIMAL(24,8) NULL COMMENT '若来源可给出则保留名义价值',
    `source` VARCHAR(32) NOT NULL COMMENT '来源说明',
    `ingested_at_utc` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '写入UTC时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_perp_oi_scope` (`venue`, `symbol`, `event_ts_ms`),
    KEY `idx_oi_symbol_time` (`symbol`, `event_ts_ms`),
    KEY `idx_oi_instrument_time` (`instrument_code`, `event_ts_ms`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='永续持仓量主表';

CREATE TABLE IF NOT EXISTS `perp_mark_prices` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `venue` VARCHAR(32) NOT NULL COMMENT '交易所来源',
    `symbol` VARCHAR(64) NOT NULL COMMENT '原始交易对',
    `instrument_code` VARCHAR(128) NOT NULL COMMENT '系统统一标的编码',
    `event_ts_ms` BIGINT UNSIGNED NOT NULL COMMENT '事件时间戳',
    `event_dt_utc` DATETIME(3) NOT NULL COMMENT '事件对应UTC时间',
    `mark_price` DECIMAL(20,8) NOT NULL COMMENT '标记价格',
    `funding_rate` DECIMAL(20,10) NULL COMMENT '当前资金费率，若来源可提供则保留',
    `next_funding_time_ts_ms` BIGINT UNSIGNED NULL COMMENT '下一次资金费率时间戳，若来源可提供则保留',
    `source` VARCHAR(32) NOT NULL COMMENT '来源说明',
    `ingested_at_utc` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '写入UTC时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_perp_mark_scope` (`venue`, `symbol`, `event_ts_ms`),
    KEY `idx_mark_symbol_time` (`symbol`, `event_ts_ms`),
    KEY `idx_mark_instrument_time` (`instrument_code`, `event_ts_ms`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='永续标记价格主表';

CREATE TABLE IF NOT EXISTS `perp_index_prices` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `venue` VARCHAR(32) NOT NULL COMMENT '交易所来源',
    `symbol` VARCHAR(64) NOT NULL COMMENT '原始交易对',
    `instrument_code` VARCHAR(128) NOT NULL COMMENT '系统统一标的编码',
    `event_ts_ms` BIGINT UNSIGNED NOT NULL COMMENT '事件时间戳',
    `event_dt_utc` DATETIME(3) NOT NULL COMMENT '事件对应UTC时间',
    `index_price` DECIMAL(20,8) NOT NULL COMMENT '指数价格',
    `source` VARCHAR(32) NOT NULL COMMENT '来源说明',
    `ingested_at_utc` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '写入UTC时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_perp_index_scope` (`venue`, `symbol`, `event_ts_ms`),
    KEY `idx_index_symbol_time` (`symbol`, `event_ts_ms`),
    KEY `idx_index_instrument_time` (`instrument_code`, `event_ts_ms`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='永续指数价格主表';
