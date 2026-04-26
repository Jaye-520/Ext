-- 源数据表示例（根据实际业务调整）
-- Producer 会读取这两个表拉取待处理视频任务

-- Bilibili 视频源表
CREATE TABLE IF NOT EXISTS bilibili_video (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    bvid VARCHAR(32) NOT NULL COMMENT 'BV号',
    video_url VARCHAR(512) NOT NULL COMMENT '视频链接（yt-dlp 使用）',
    title VARCHAR(255) DEFAULT NULL COMMENT '视频标题',
    author VARCHAR(128) DEFAULT NULL COMMENT 'UP主',
    duration INT DEFAULT NULL COMMENT '时长（秒）',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_bvid (bvid),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Bilibili 视频源表';

-- 抖音视频源表
CREATE TABLE IF NOT EXISTS douyin_aweme (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    aweme_id VARCHAR(64) NOT NULL COMMENT '抖音视频ID',
    video_download_url VARCHAR(1024) NOT NULL COMMENT '直链下载地址',
    desc_text VARCHAR(512) DEFAULT NULL COMMENT '视频描述',
    author_id VARCHAR(64) DEFAULT NULL COMMENT '作者ID',
    duration INT DEFAULT NULL COMMENT '时长（秒）',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_aweme_id (aweme_id),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='抖音视频源表';
