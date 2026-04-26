# 视频字幕系统代码审查报告
## 整体评估
项目整体架构清晰，模块划分合理，核心功能的业务逻辑正确，使用了异步IO、连接池、幂等设计等最佳实践，但存在多个高优先级的致命问题需要修复，否则程序无法正常运行或导致严重后果。
---
## 问题汇总
### 🔥 高优先级问题（必须修复，否则程序无法运行或导致严重后果）
| 问题描述 | 位置 | 修复建议 |
| --- | --- | --- |
| 数据库表名不匹配，缺少subtitle表：<br>1. SQL脚本创建的表名是`dy_fingerprint`、`dy_subtitle_segment`，但代码中使用的是`fingerprint`、`subtitle_segment`<br>2. 代码中使用了`subtitle`表，但SQL脚本中没有创建，部署新环境时会直接报错 | [init.sql](file:///home/jaye/project/Ext/video_subtitle_system/sql/init.sql)、[storage.py](file:///home/jaye/project/Ext/video_subtitle_system/src/storage.py) | 统一表名：<br>1. 修改SQL脚本中的表名和代码保持一致<br>2. 在init.sql中添加subtitle表的创建语句 |
| 敏感信息硬编码：配置文件中明文存储了数据库密码和Hugging Face Token，提交到代码仓库会导致敏感信息泄露 | [config.yaml#L5, L14](file:///home/jaye/project/Ext/video_subtitle_system/config.yaml#L5) | 1. 创建config.example.yaml示例模板，去除敏感信息<br>2. 将config.yaml加入.gitignore，避免敏感信息提交<br>3. 可选：支持从环境变量读取敏感配置 |
| ASR模型加载阻塞事件循环：同步的load_model方法在异步流程中直接调用，加载大模型时会阻塞整个事件循环，导致服务无响应 | [asr_engine.py#L20-L30](file:///home/jaye/project/Ext/video_subtitle_system/src/asr_engine.py#L20)、[main.py#L47](file:///home/jaye/project/Ext/video_subtitle_system/main.py#L47) | 将load_model放到线程池中运行，避免阻塞异步事件循环 |
---
### ⚠️ 中优先级问题（影响性能或稳定性，建议修复）
| 问题描述 | 位置 | 修复建议 |
| --- | --- | --- |
| 视频指纹去重性能极差：每次去重都查询10000条指纹记录，逐行计算汉明距离，数据量大时会严重阻塞服务 | [fingerprint.py#L62-L68](file:///home/jaye/project/Ext/video_subtitle_system/src/fingerprint.py#L62) | 使用支持向量检索的数据库（如RedisStack、Milvus），或者在数据库层面使用汉明距离函数做过滤 |
| 日志trace_id绑定时机错误：get_logger时就bind了trace_id，后续设置的trace_id不会更新到日志中，导致日志trace_id混乱 | [logger.py#L43](file:///home/jaye/project/Ext/video_subtitle_system/src/logger.py#L43) | 通过structlog processor动态获取trace_id，而不是提前bind |
| 视频指纹计算阻塞事件循环：compute方法是CPU密集型同步操作，直接在异步协程中调用会阻塞事件循环 | [fingerprint.py#L18](file:///home/jaye/project/Ext/video_subtitle_system/src/fingerprint.py#L18) | 放到线程池中运行，避免阻塞事件循环 |
---
### 💡 低优先级问题（优化建议）
| 问题描述 | 修复建议 |
| --- | --- |
| 测试覆盖不足：现有测试只覆盖了存储逻辑，缺少下载、音频提取、ASR、任务调度等核心模块的测试，也没有错误场景和边界情况的测试 | 补充单元测试和集成测试，提高测试覆盖率 |
| 游标更新时机可能导致重复拉取任务：任务推送到队列后立即更新游标，如果程序在推送后更新前崩溃，下次启动会重复拉取相同任务 | 将推送任务和更新游标放到同一个数据库事务中，或者在任务成功处理后再更新游标 |
---
## 修复优先级建议
1. 优先修复4个高优先级问题，确保程序可以正常运行
2. 然后修复中优先级问题，提高性能和稳定性
3. 最后逐步完成低优先级的优化
