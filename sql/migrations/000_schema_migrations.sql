CREATE TABLE IF NOT EXISTS `schema_migrations` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `version` VARCHAR(32) NOT NULL COMMENT 'migration版本号，例如001、002',
    `name` VARCHAR(128) NOT NULL COMMENT 'migration名称',
    `checksum` VARCHAR(128) NOT NULL COMMENT 'SQL文件内容哈希，用于校验漂移',
    `executed_at_utc` DATETIME(3) NOT NULL COMMENT '执行完成UTC时间',
    `status` VARCHAR(32) NOT NULL COMMENT '执行状态，例如success或failed',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_schema_migrations_version` (`version`),
    KEY `idx_schema_migrations_executed_at` (`executed_at_utc`),
    KEY `idx_schema_migrations_status` (`status`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='SQL migration执行历史表';
