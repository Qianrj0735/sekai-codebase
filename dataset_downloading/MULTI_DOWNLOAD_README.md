# 多进程YouTube视频下载器

这个工具可以将YouTube URL列表分割成多个部分，然后使用多个yt-dlp进程并行下载，大大提高下载效率。

## 文件说明

- `multi_download.py` - Python版本的多进程下载器
- `multi_download.sh` - Shell版本的多进程下载器
- `ytdlp.sh` - 原始的单进程下载脚本

## 使用方法

### Python版本 (推荐)

```bash
# 基本使用 - 4个进程，每个进程间隔10秒启动
python3 multi_download.py

# 自定义参数
python3 multi_download.py --workers 8 --start-delay 15 --output-dir ./downloads

# 查看所有选项
python3 multi_download.py --help
```

#### Python版本参数说明

- `--urls-file` / `-u`: URL列表文件路径 (默认: sekai-real-walking-hq_urls.txt)
- `--workers` / `-w`: 工作进程数量 (默认: 4)
- `--output-dir` / `-o`: 下载输出目录 (默认: ./videos)
- `--start-delay` / `-d`: 进程间启动延迟秒数 (默认: 10)
- `--extra-args` / `-e`: 额外的yt-dlp参数
- `--keep-temp`: 保留临时分割文件

### Shell版本

```bash
# 基本使用 - 4个进程，每个进程间隔10秒启动
./multi_download.sh

# 自定义进程数和延迟
./multi_download.sh 8 15
```

## 工作原理

1. **文件分割**: 将原始URL文件按照进程数平均分割成多个小文件
2. **进程启动**: 按指定的时间间隔启动各个yt-dlp进程
3. **并行下载**: 每个进程独立下载分配给它的URL列表
4. **日志记录**: 每个进程的输出会保存到单独的日志文件
5. **自动清理**: 下载完成后自动清理临时文件

## 推荐配置

### 根据你的网络环境调整：

- **家庭网络**: 4-6个进程，启动延迟10-15秒
- **VPS/服务器**: 6-10个进程，启动延迟5-10秒
- **企业网络**: 2-4个进程，启动延迟15-20秒

### 示例配置

```bash
# 保守配置 - 适合大多数情况
python3 multi_download.py --workers 4 --start-delay 10

# 激进配置 - 服务器环境
python3 multi_download.py --workers 8 --start-delay 5

# 超保守配置 - 避免IP被限制
python3 multi_download.py --workers 2 --start-delay 20
```

## 特性

- ✅ 多进程并行下载，大幅提升效率
- ✅ 智能启动延迟，避免IP被ban
- ✅ 断点续传支持
- ✅ 自动跳过已下载文件
- ✅ 详细的日志记录
- ✅ 错误容忍，单个失败不影响整体
- ✅ 自动清理临时文件
- ✅ 灵活的参数配置

## 注意事项

1. **启动延迟**: 进程间的启动延迟很重要，避免同时发送大量请求被YouTube限制
2. **进程数量**: 不要设置过多进程，建议根据网络带宽和CPU核心数合理设置
3. **存储空间**: 确保有足够的磁盘空间存储下载的视频
4. **网络稳定性**: 建议在网络稳定的环境下运行

## 故障排除

### 如果下载速度仍然很慢：
- 增加启动延迟时间
- 减少工作进程数量
- 检查网络连接

### 如果出现大量错误：
- 检查yt-dlp是否为最新版本: `yt-dlp --update`
- 增加进程间的启动延迟
- 检查URL文件格式是否正确

### 查看详细日志：
每个进程的详细输出会保存在 `download_log_worker_X.txt` 文件中。

## 性能对比

以你的3880个URL为例：

- **单进程**: 预计需要数天到数周
- **4进程**: 预计时间缩短到原来的1/4
- **8进程**: 预计时间缩短到原来的1/8 (如果网络允许)

实际效果取决于网络条件和YouTube的限制策略。
