-- 视频指纹表（pHash 去重）
CREATE TABLE IF NOT EXISTS fingerprint (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    video_id VARCHAR(64) NOT NULL COMMENT '原始视频ID',
    platform ENUM('bilibili', 'douyin') NOT NULL COMMENT '平台',
    p_hash VARCHAR(64) NOT NULL COMMENT '感知哈希值',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_p_hash (p_hash),
    INDEX idx_video_platform (video_id, platform)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 字幕分段表
CREATE TABLE IF NOT EXISTS subtitle_segment (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    video_id VARCHAR(64) NOT NULL COMMENT '原始视频ID',
    platform ENUM('bilibili', 'douyin') NOT NULL COMMENT '平台',
    start_time FLOAT NOT NULL COMMENT '开始时间（秒）',
    end_time FLOAT NOT NULL COMMENT '结束时间（秒）',
    text TEXT NOT NULL COMMENT '字幕文本',
    confidence FLOAT DEFAULT NULL COMMENT '置信度(0-1)',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_video_start (video_id, platform, start_time),
    INDEX idx_video_id (video_id),
    INDEX idx_video_start (video_id, start_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 字幕表（全量文本）
CREATE TABLE IF NOT EXISTS subtitle (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    video_id VARCHAR(64) NOT NULL COMMENT '原始视频ID',
    platform ENUM('bilibili', 'douyin') NOT NULL COMMENT '平台',
    full_text TEXT NOT NULL COMMENT '完整字幕文本',
    confidence_avg FLOAT DEFAULT NULL COMMENT '平均置信度(0-1)',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_video_platform (video_id, platform),
    INDEX idx_video_id (video_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 任务状态表（方案B：不触碰原表）
CREATE TABLE IF NOT EXISTS task_status (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    video_id VARCHAR(64) NOT NULL,
    platform ENUM('bilibili', 'douyin') NOT NULL,
    status ENUM('PENDING', 'PROCESSING', 'SUCCESS', 'FAILED') NOT NULL DEFAULT 'PENDING',
    retry_count TINYINT UNSIGNED DEFAULT 0,
    error_msg TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_video_platform (video_id, platform),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 游标维护表
CREATE TABLE IF NOT EXISTS sync_cursor (
    id INT PRIMARY KEY DEFAULT 1,
    bilibili_last_id BIGINT UNSIGNED DEFAULT 0,
    douyin_last_id BIGINT UNSIGNED DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
