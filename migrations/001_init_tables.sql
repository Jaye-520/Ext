-- Ext项目数据库表结构
-- 抖音字幕提取 + 视频指纹去重

-- 字幕表
CREATE TABLE IF NOT EXISTS `dy_subtitle` (
    `id` INT NOT NULL AUTO_INCREMENT COMMENT '自增ID',
    `aweme_id` VARCHAR(64) NOT NULL COMMENT '抖音视频ID',
    `video_url` VARCHAR(1024) NOT NULL COMMENT '视频下载URL',
    `fingerprint` VARCHAR(64) COMMENT '视频指纹(pHash)',
    `language` VARCHAR(8) DEFAULT 'zh' COMMENT '语言',
    `duration` FLOAT COMMENT '视频时长(秒)',
    `subtitle_text` LONGTEXT COMMENT '完整字幕文本',
    `segments` JSON COMMENT '字幕分段JSON',
    `confidence` FLOAT COMMENT '平均置信度',
    `status` TINYINT DEFAULT 0 COMMENT '状态:0待处理,1成功,2失败,3重复(复制字幕)',
    `error_msg` TEXT COMMENT '错误信息',
    `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `processed_at` DATETIME COMMENT '处理完成时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_aweme_id` (`aweme_id`),
    KEY `idx_status` (`status`),
    KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='抖音字幕表';

-- 指纹表
CREATE TABLE IF NOT EXISTS `dy_fingerprint` (
    `id` INT NOT NULL AUTO_INCREMENT COMMENT '自增ID',
    `aweme_id` VARCHAR(64) NOT NULL COMMENT '抖音视频ID',
    `video_url` VARCHAR(1024) NOT NULL COMMENT '视频URL',
    `phash` VARCHAR(64) NOT NULL COMMENT 'pHash指纹',
    `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_aweme_id` (`aweme_id`),
    KEY `idx_phash` (`phash`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='抖音视频指纹表';