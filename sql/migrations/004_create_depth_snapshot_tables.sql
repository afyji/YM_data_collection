CREATE TABLE IF NOT EXISTS `spot_depth_snapshots` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `venue` VARCHAR(32) NOT NULL COMMENT '交易所来源',
    `symbol` VARCHAR(64) NOT NULL COMMENT '原始交易对',
    `instrument_code` VARCHAR(128) NOT NULL COMMENT '系统统一标的编码',
    `event_ts_ms` BIGINT UNSIGNED NOT NULL COMMENT '盘口快照事件时间戳',
    `event_dt_utc` DATETIME(3) NOT NULL COMMENT '事件对应UTC时间',
    `best_bid_price` DECIMAL(20,8) NOT NULL COMMENT '第一档买价',
    `best_bid_qty` DECIMAL(20,8) NOT NULL COMMENT '第一档买量',
    `best_ask_price` DECIMAL(20,8) NOT NULL COMMENT '第一档卖价',
    `best_ask_qty` DECIMAL(20,8) NOT NULL COMMENT '第一档卖量',
    `mid_price` DECIMAL(20,8) NOT NULL COMMENT '中间价，通常为(best_bid+best_ask)/2',
    `spread_abs` DECIMAL(20,8) NOT NULL COMMENT '绝对价差',
    `spread_bps` DECIMAL(20,10) NOT NULL COMMENT '基点价差',
    `depth_levels` INT UNSIGNED NOT NULL COMMENT '保存的盘口档位数，第一阶段默认10',
    `bid_depth_json` JSON NOT NULL COMMENT '买盘档位数组，格式如[[price,qty],...]',
    `ask_depth_json` JSON NOT NULL COMMENT '卖盘档位数组，格式如[[price,qty],...]',
    `source` VARCHAR(32) NOT NULL COMMENT '来源说明',
    `ingested_at_utc` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '写入UTC时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_spot_depth_scope` (`venue`, `symbol`, `event_ts_ms`),
    KEY `idx_spot_depth_symbol_time` (`symbol`, `event_ts_ms`),
    KEY `idx_spot_depth_instrument_time` (`instrument_code`, `event_ts_ms`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='现货深度快照主表';

CREATE TABLE IF NOT EXISTS `perp_depth_snapshots` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `venue` VARCHAR(32) NOT NULL COMMENT '交易所来源',
    `symbol` VARCHAR(64) NOT NULL COMMENT '原始交易对',
    `instrument_code` VARCHAR(128) NOT NULL COMMENT '系统统一标的编码',
    `event_ts_ms` BIGINT UNSIGNED NOT NULL COMMENT '盘口快照事件时间戳',
    `event_dt_utc` DATETIME(3) NOT NULL COMMENT '事件对应UTC时间',
    `best_bid_price` DECIMAL(20,8) NOT NULL COMMENT '第一档买价',
    `best_bid_qty` DECIMAL(20,8) NOT NULL COMMENT '第一档买量',
    `best_ask_price` DECIMAL(20,8) NOT NULL COMMENT '第一档卖价',
    `best_ask_qty` DECIMAL(20,8) NOT NULL COMMENT '第一档卖量',
    `mid_price` DECIMAL(20,8) NOT NULL COMMENT '中间价，通常为(best_bid+best_ask)/2',
    `spread_abs` DECIMAL(20,8) NOT NULL COMMENT '绝对价差',
    `spread_bps` DECIMAL(20,10) NOT NULL COMMENT '基点价差',
    `depth_levels` INT UNSIGNED NOT NULL COMMENT '保存的盘口档位数，第一阶段默认10',
    `bid_depth_json` JSON NOT NULL COMMENT '买盘档位数组，格式如[[price,qty],...]',
    `ask_depth_json` JSON NOT NULL COMMENT '卖盘档位数组，格式如[[price,qty],...]',
    `source` VARCHAR(32) NOT NULL COMMENT '来源说明',
    `ingested_at_utc` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '写入UTC时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_perp_depth_scope` (`venue`, `symbol`, `event_ts_ms`),
    KEY `idx_perp_depth_symbol_time` (`symbol`, `event_ts_ms`),
    KEY `idx_perp_depth_instrument_time` (`instrument_code`, `event_ts_ms`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='永续深度快照主表';
