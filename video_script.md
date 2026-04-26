# 视频讲解稿：视频字幕自动化提取系统

> 目标时长：约 12-15 分钟  
> 目标受众：具备基础编程知识，想理解工程化设计思路的开发者  
> 风格：从生活类比切入，逐层深入代码实现

---

## 【开场：30秒】

大家好，今天我来分享一个我自己开发的**视频字幕自动化提取系统**。

简单说，这个系统干的事情就是：
- 你数据库里存了一堆 B 站和抖音的视频链接；
- 系统自己把这些视频下载下来；
- 把里面的语音提取出来；
- 用 AI 转成带时间轴的字幕；
- 最后存回数据库，供下游业务使用。

整个过程是**全自动、离线运行、可并发、可容错**的。

那它是怎么做到的？我们今天从架构设计到代码细节，一层一层把它拆开来看。

---

## 【第一部分：为什么这样设计架构？—— 2分钟】

在看代码之前，我们先想一个问题：

如果你要处理几万条视频，最直接的写法是什么？

可能是写一个 `for` 循环，从数据库里读一条，下载、转码、识别、保存，然后再读下一条。

但这个写法有个致命问题：
- 下载视频可能要十几秒，这时候 CPU 在干等；
- ASR 识别可能要几十秒，内存和 GPU 占用很高；
- 如果某个环节崩溃了，你根本不知道哪些做了、哪些没做。

所以我的设计思路是：**把"发现任务"和"执行任务"拆成两个独立角色。**

我用了一个非常经典的模式，叫做 **Producer-Worker 模式**。大家可以把整个系统想象成一家快递公司：

- **Producer（分拣员）**：不停地到仓库（MySQL）里看看有没有新包裹（新视频），有就贴个标签，放到传送带（Redis 队列）上。
- **Worker（快递员）**：站在传送带尽头，来一个包裹就处理一个，负责下载、拆包、录入系统。

两者通过 **Redis 列表** 解耦。Producer 只管往上放，Worker 只管往下取，互不干扰。

**为什么这么设计？三个好处：**
1. **解耦**：Producer 跑多快和 Worker 跑多快没关系；
2. **削峰**：如果突然来了一万条视频，Redis 队列能缓冲，Worker 按自己的节奏消费；
3. **可扩展**：Worker 慢了就多加几个 Worker，Producer 不会受到任何影响。

好，这个设计的骨架清楚了，我们来看代码是怎么落地的。

---

## 【第二部分：一条视频的一生 —— 3分钟】

我们打开 `main.py`，这是整个系统的入口文件。

（切换屏幕，展示 main.py）

大家看，main 函数里干了这么几件事：
- 加载配置；
- 初始化数据库、Redis、各种引擎；
- 启动 Producer 和 Worker 两个协程；
- 注册信号处理器，保证按 Ctrl+C 能优雅退出。

重点看最后这段：

```python
producer_task = asyncio.create_task(producer.start())
worker_task = asyncio.create_task(worker.run())
```

Producer 和 Worker 是**并行跑在同一个事件循环里的**。这不是多进程，而是 Python asyncio 的协程并发，开销非常小。

---

### Producer 在做什么？（展示 producer.py）

Producer 的核心逻辑就一个字：**"轮询"**。

它每 5 秒钟去 MySQL 查一次：
- B 站视频表里，有没有 id 比我上次记录的游标更大的视频？
- 抖音视频表里，有没有新的？
- 而且要排除掉那些我已经处理成功过的。

大家看这段 SQL：

```python
bilibili_tasks = await self.db.fetch_all(
    """SELECT b.id as video_id, 'bilibili' as platform, b.video_url as url
       FROM bilibili_video b
       WHERE b.id > %s
         AND NOT EXISTS (
             SELECT 1 FROM task_status ts
             WHERE ts.video_id = b.id AND ts.platform = 'bilibili' AND ts.status = 'SUCCESS'
         )
       ORDER BY b.id
       LIMIT %s""",
    (cursor.bilibili_last_id, self.config.batch_size // 2)
)
```

这里有三个非常关键的工程细节：

**第一，游标机制。**
我在数据库里维护了一张 `sync_cursor` 表，只存两个数字：B 站上次处理到哪个 id 了，抖音上次处理到哪个 id 了。
这样每次查询都是**增量查询**，不会全表扫描，性能非常好。

**第二，不碰源表。**
注意我的 SQL 里查的是 `bilibili_video` 和 `douyin_aweme`，这是业务方的原始数据表。
但我没有在这些表里加任何状态字段。任务状态全部记录在独立的 `task_status` 表里。
为什么这么设计？因为源表可能是别的业务系统维护的，你往里加字段，会破坏别人的约定。这叫**零侵入设计**。

**第三，反压机制。**
在推任务之前，Producer 会先检查 Redis 队列的长度：

```python
queue_len = await self.redis.llen("task_queue")
if queue_len > self.backpressure_threshold:
    return
```

如果队列已经超过 1000 个任务，Producer 就先不推了，等 Worker 消化一下。
这就像一个水库的闸门，下游扛不住的时候，上游自动限流。没有这个机制，Redis 会被撑爆。

查出来的新任务，Producer 会转成 JSON，推进 Redis 的 `task_queue` 列表里。

---

### Worker 在做什么？（展示 worker.py）

Worker 的核心逻辑是一个**无限循环**，不停地从 Redis 取任务。

它用的是 Redis 的一个原子命令：`BRPOPLPUSH`。

```python
task = await self.redis.brpoplpush("task_queue", "processing_queue", timeout=5)
```

这个命令非常关键，它做了两件事，而且是**原子操作**：
1. 从 `task_queue` 的右边弹出一个任务；
2. 立刻把它推到 `processing_queue` 的左边。

为什么要有 `processing_queue`？

假设 Worker 刚取出一个任务，正在下载视频，这时候服务器突然断电了。等重启之后，这个任务还在 `processing_queue` 里。我在 `main.py` 的 `recover_crashed_tasks` 函数里，会把 `processing_queue` 里的任务全部搬回 `task_queue`，同时把数据库里状态为 `PROCESSING` 的任务改回 `PENDING`。

这就是**崩溃自愈**。视频不会丢，任务不会漏。

好，任务取出来了，Worker 开始处理。一条视频要经历五个步骤：

---

#### 步骤 1：下载

```python
async with self.download_sem:
    video_path = await self.downloader.download(url, platform)
```

这里有一个 `async with self.download_sem`，它是一个 asyncio 信号量，最多允许 6 个下载同时进行。

为什么限制并发？因为下载是 IO 密集型，开太多会占满带宽，而且对方的 CDN 可能会把你封掉。

下载模块里还有一个有趣的细节：B 站和抖音的下载策略是不一样的。

（展示 downloader.py）

抖音给的是直链，我直接用 `httpx` 做流式下载，边下边写，内存占用很低。而且我设了 500MB 的上限，防止恶意大文件把磁盘撑爆。

B 站现在用的是 DASH 格式，视频和音频是分开的。我直接调 `yt-dlp` 这个命令行工具，让它帮我合并成 MP4。yt-dlp 是 yt-dlp 社区维护的，对国内平台的支持非常好。

---

#### 步骤 2：视频去重

下载完之后，我不急着转码。我先算一个**视频指纹**。

（展示 fingerprint.py）

为什么？因为同一个视频可能被不同的人上传，URL 不一样，但内容完全一样。我不希望浪费算力去做重复识别。

这里用的是一个计算机视觉的经典算法，叫 **pHash（感知哈希）**。

大家 familiar 的 MD5 是文件级哈希，文件改一个字节就变。但视频经过不同平台转码后，编码参数可能不同，MD5 会完全不同。

pHash 不一样，它算的是**图像内容的感知特征**。只要画面内容一样，哪怕分辨率、码率不同，算出来的哈希值也很接近。

具体怎么算的？我取视频的第 1 秒、第 3 秒、第 5 秒这三帧，分别算 pHash，然后用**按位与**合并成一个最终哈希。

```python
merged = hashes[0].hash
for h in hashes[1:]:
    merged = merged & h.hash
```

去重的时候，我不查全表，只查最近 7 天的 1000 条记录。为什么？因为重复视频通常出现在短期内，而且查全表性能太差。

比对的时候，我用的不是精确匹配，而是**汉明距离**：

```python
if self.hamming_distance(p_hash, fp["p_hash"]) <= 5:
    return True
```

两个哈希有几位不一样，如果不超过 5 位，就认为是同一个视频。这个阈值是工程经验值，大家可以按自己场景调。

如果是重复视频，Worker 直接跳过 ASR，在数据库里记一条空字幕和指纹，避免下次再来。

---

#### 步骤 3：音频提取

视频确认不是重复之后，下一步是把音频抽出来。

（展示 audio_extractor.py）

这里调的是 `FFmpeg`，把视频里的音频轨道转成 16kHz 单声道 WAV。为什么是 16kHz 单声道？因为 Whisper 模型训练时用的就是这个格式，转换后识别率最高。

注意这段代码：

```python
def extract_sync(self, video_path: Path) -> bytes:
    return self._extract_impl(video_path)
```

Worker 里是这样调用的：

```python
audio_data = await loop.run_in_executor(
    self.transcode_pool,
    lambda: self.audio_extractor.extract_sync(video_path)
)
```

`run_in_executor` 加上 `ThreadPoolExecutor`，把同步的 FFmpeg 调用放到线程池里跑。**不阻塞主事件循环**。这是 asyncio 里处理 CPU 密集型或同步 IO 的标准做法。

转码并发我也限制了，线程池最多 4 个 worker。因为 FFmpeg 转码也是吃 CPU 的，开太多会互相抢资源。

---

#### 步骤 4：ASR 语音识别

音频准备好了，进入最核心的环节：语音转文字。

（展示 asr_engine.py）

我用的是 **Faster-Whisper**。它是 OpenAI Whisper 的一个高性能复刻版，底层用 CTranslate2 做了加速。

看初始化：

```python
self.model = WhisperModel(
    self.model_size,
    device="auto",
    compute_type=compute_type,
)
```

`device="auto"` 会自动检测你有没有 NVIDIA GPU。有 GPU 就用 CUDA 跑，没有就回退到 CPU。

`compute_type` 我根据模型大小做了区分：base 模型用 int8 量化，更大的模型用 float32。量化可以减少显存占用，提升推理速度。

识别的时候：

```python
segments, _ = self.model.transcribe(
    str(tmp_wav),
    language=None,
    vad_filter=True,
    vad_parameters=dict(min_silence_duration_ms=500),
)
```

`language=None` 让模型自动检测语言，支持中文、英文、日文混合场景。

`vad_filter=True` 是开启**语音活动检测**。如果音频里有大段空白，Whisper 会自动跳过，既省算力又减少幻觉。

返回的 `segments` 是一个生成器，每个片段包含开始时间、结束时间、文本内容和置信度。Worker 把它们整理成字典列表，传给存储模块。

ASR 这里我也加了信号量限制，最多同时跑 2 个识别任务。因为 Whisper 模型吃 GPU 显存，并发高了会 OOM。

---

#### 步骤 5：存储与任务完成

（展示 storage.py，如果文件不长的话）

存储模块会把三段数据写入 MySQL：
1. `subtitle_segment`：按时间轴的分段字幕；
2. `subtitle`：完整字幕文本的聚合视图；
3. `fingerprint`：视频指纹，供未来去重用；
4. `task_status`：把状态从 `PROCESSING` 更新为 `SUCCESS`。

写入完成后，Worker 把这个任务从 `processing_queue` 里移除，叫做 **ACK（确认）**。

如果这五个步骤里任何一步抛异常了，Worker 不会直接丢弃任务，而是进入**重试逻辑**：

```python
retry_count = task.get("retry_count", 0) + 1
if retry_count >= self.max_retries:
    await self.storage.mark_failed(...)
else:
    task["retry_count"] = retry_count
    await self.redis.lpush("task_queue", json.dumps(task))
```

最多重试 3 次。3 次都失败，就标记为 `FAILED`，记录错误原因，人工介入排查。

---

## 【第三部分：三个容易被忽略的工程细节 —— 3分钟】

好，主流程讲完了。但我特别想强调三个代码里的小细节，它们体现了真正的工程思维。

---

### 细节 1：三级并发控制

如果你仔细看 Worker 的初始化，会发现我用了三种不同的并发控制手段：

```python
self.download_sem = asyncio.Semaphore(concurrency.download)      # 6
self.transcode_pool = ThreadPoolExecutor(max_workers=concurrency.transcode)  # 4
self.asr_sem = asyncio.Semaphore(concurrency.asr)                # 2
```

- **下载用 asyncio.Semaphore**：下载是网络 IO，asyncio 的协程切换效率最高；
- **转码用 ThreadPoolExecutor**：FFmpeg 是同步阻塞的 CPU 任务，必须放线程里；
- **ASR 又用 asyncio.Semaphore**：虽然 Faster-Whisper 内部也有并行，但从系统层面我再包一层，防止 GPU 爆显存。

这三种并发手段**不是随便选的**，而是根据任务的 IO/CPU 特征精准匹配的。如果你全用线程池或者全用协程，性能都会大打折扣。

---

### 细节 2：优雅退出

```python
stop_event = asyncio.Event()

for sig in (signal.SIGTERM, signal.SIGINT):
    asyncio.get_event_loop().add_signal_handler(sig, lambda s=sig: handle_signal(s))
```

这是给 Docker 和 Kubernetes 准备的。当容器收到停止信号时，系统不会立刻被杀死，而是：
1. 设置 stop_event；
2. Producer 停止轮询；
3. Worker 把当前任务做完再退出；
4. 关闭数据库连接池、Redis 连接、HTTP 客户端。

这避免了"任务做到一半被强杀"导致的数据不一致。

---

### 细节 3：结构化日志

整个系统没有一行 `print`，全部用的是 `structlog`。

```python
logger.info("task_completed",
    video_id=video_id, platform=platform,
    download_time_ms=download_time_ms,
    asr_time_ms=asr_time_ms,
    segments_count=len(segments))
```

输出的不是人读的文本，而是 JSON：

```json
{
  "event": "task_completed",
  "video_id": "123456",
  "download_time_ms": 2300,
  "asr_time_ms": 12000,
  "segments_count": 42
}
```

这种日志可以直接被 ELK、Grafana Loki、Datadog 等日志平台采集和分析。你可以很方便地画一个 Grafana 面板，看每分钟的吞吐量、平均处理耗时、失败率。

---

## 【第四部分：部署与扩展 —— 2分钟】

讲完代码，我们看看怎么跑起来。

项目提供了 `docker-compose.yml`，里面定义了三个服务：
- **MySQL**：存数据和任务状态；
- **Redis**：任务队列和崩溃恢复；
- **App**：我们的 Python 守护进程。

App 服务里还有一段 GPU 预留配置：

```yaml
deploy:
  resources:
    reservations:
      devices:
        - driver: nvidia
          count: all
          capabilities: [gpu]
```

如果你有 NVIDIA GPU 和 Docker 的 nvidia-runtime，Whisper 会自动跑在 GPU 上，速度比 CPU 快 5 到 10 倍。没有 GPU 的话，注释掉这段就行，代码里 `device="auto"` 会自动 fallback 到 CPU。

配置文件是独立的 YAML，包含数据库连接、并发参数、轮询间隔等。生产环境和测试环境只需要换一份配置文件，不需要改代码。

**未来可以怎么扩展？**
- Worker 可以水平扩展：多开几个容器实例，共享同一个 Redis 队列，天然就是分布式；
- 可以接入更多平台：YouTube、快手，只需要在 Producer 里加一段 SQL；
- ASR 可以换模型：Whisper 只是默认实现，你也可以接入国内的讯飞、阿里云 ASR，只需要实现同样的 `ASREngine` 接口。

---

## 【结尾：30秒】

总结一下，这个项目的核心设计亮点：

1. **Producer-Worker + Redis 队列**，实现了解耦、削峰和可扩展；
2. **三级并发控制**，根据 IO/CPU/GPU 特征精准匹配并发模型；
3. **pHash 去重**，避免重复浪费算力；
4. **崩溃自愈**，Redis 双队列 + DB 状态回滚，保证任务不丢；
5. **零侵入源表**，独立任务状态表，不破坏原有业务数据。

代码已经开源在 GitHub，感兴趣的同学可以 clone 下来研究，也欢迎提 Issue 和 PR。

以上就是全部内容，感谢观看！

---

## 【附：录屏建议的画面切换节奏】

| 时间段 | 画面内容 |
|--------|----------|
| 0:00-0:30 | 项目首页 / 演示效果（如果有） |
| 0:30-2:30 | 手绘或 PPT 展示 Producer-Worker 架构图 |
| 2:30-3:30 | 打开 main.py，整体过一遍入口逻辑 |
| 3:30-5:00 | 打开 producer.py，重点展示 SQL 和反压代码 |
| 5:00-8:00 | 打开 worker.py，结合下载、指纹、转码、ASR 逐段展开 |
| 8:00-9:30 | 展示 fingerprint.py 和 asr_engine.py 的核心算法 |
| 9:30-11:00 | 回到 worker.py，讲并发控制、优雅退出、日志 |
| 11:00-12:00 | 展示 docker-compose.yml 和部署说明 |
| 12:00-12:30 | PPT 总结页 |
