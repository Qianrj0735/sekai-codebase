# 集成下载-处理脚本使用说明

这个脚本结合了视频下载和处理功能，实现边下载边处理的工作流程，有效节省存储空间。

## 功能特点

- ✅ **边下载边处理**: 下载完一个视频立即处理，然后删除原视频
- ✅ **多进程并行**: 支持多个进程同时工作
- ✅ **空间高效**: 避免存储大量原始视频文件
- ✅ **错误容忍**: 单个视频失败不影响整体流程
- ✅ **GPU加速**: 使用NVIDIA GPU进行视频处理
- ✅ **智能重试**: 下载失败时自动重试

## 工作流程

1. **分配任务**: 将URL列表按进程数分割
2. **进程启动**: 各进程按延迟时间启动
3. **下载视频**: 从YouTube下载单个视频
4. **检查clip**: 确认是否有对应的clip文件
5. **视频处理**: 使用GPU将视频处理成vstream格式
6. **清理空间**: 删除原始视频文件
7. **继续下一个**: 重复处理下一个URL

## 使用方法

### 基本使用

```bash
python3 download_and_process.py \
    --urls-file sekai-real-walking-hq_urls.txt \
    --input-clip-dir /path/to/clip/files \
    --output-dir /path/to/output/vstreams
```

### 完整参数示例

```bash
python3 download_and_process.py \
    --urls-file sekai-real-walking-hq_urls.txt \
    --input-clip-dir /workspace/sekai-codebase/clips \
    --output-dir /workspace/sekai-codebase/vstreams \
    --workers 2 \
    --start-delay 20 \
    --width 1280 \
    --height 720 \
    --fps 30 \
    --device-id 0 \
    --log-level INFO
```

## 参数说明

### 必需参数
- `--urls-file` / `-u`: YouTube URL列表文件
- `--input-clip-dir` / `-c`: 包含clip时间戳文件的目录
- `--output-dir` / `-o`: vstream输出目录

### 可选参数
- `--workers` / `-w`: 工作进程数量 (默认: 2)
- `--start-delay` / `-d`: 进程间启动延迟秒数 (默认: 15)
- `--width`: 输出视频宽度 (默认: 1280)
- `--height`: 输出视频高度 (默认: 720)
- `--fps`: 输出视频FPS (默认: 30)
- `--device-id`: GPU设备ID (默认: 0)
- `--log-level`: 日志级别 (默认: INFO)

## 目录结构要求

### 输入clip目录结构
```
input_clip_dir/
├── mJ90XoEPbjA.txt     # YouTube视频ID对应的clip文件
├── 8ls8xBn70gM.txt
└── ...
```

### 输出vstream目录结构
```
output_dir/
├── mJ90XoEPbjA/
│   ├── mJ90XoEPbjA_0000010_0000050.hevc
│   ├── mJ90XoEPbjA_0000100_0000140.hevc
│   └── ...
├── 8ls8xBn70gM/
│   └── ...
└── ...
```

## 推荐配置

### 根据资源情况调整：

#### 内存充足的服务器
```bash
--workers 4 --start-delay 10
```

#### 内存有限的环境
```bash
--workers 2 --start-delay 20
```

#### 网络带宽有限
```bash
--workers 1 --start-delay 30
```

## 存储空间优化

这个脚本的主要优势是空间效率：

- **传统方式**: 需要存储所有原始视频 (可能几TB)
- **本脚本**: 只需要存储临时单个视频 + 最终vstream文件

### 临时空间计算
- 单个视频: ~100-500MB
- 同时处理的视频数 = workers数量
- 临时空间需求: workers × 500MB

### 示例
- 2个进程: ~1GB临时空间
- 4个进程: ~2GB临时空间

## 错误处理

脚本会自动处理以下情况：

1. **下载失败**: 自动重试最多3次
2. **缺少clip文件**: 跳过该视频并记录警告
3. **处理异常**: 清理临时文件并继续
4. **GPU内存不足**: 自动垃圾回收

## 监控和日志

### 实时监控
```bash
# 监控进程状态
ps aux | grep download_and_process

# 监控磁盘使用
df -h /tmp

# 监控GPU使用
nvidia-smi
```

### 日志级别
- **DEBUG**: 详细的执行信息
- **INFO**: 常规处理信息
- **WARNING**: 警告信息
- **ERROR**: 错误信息

## 故障排除

### 常见问题

1. **GPU内存不足**
   ```bash
   # 减少进程数或使用更小的batch_size
   --workers 1
   ```

2. **下载速度慢**
   ```bash
   # 增加启动延迟
   --start-delay 30
   ```

3. **存储空间不足**
   ```bash
   # 检查临时目录空间
   df -h /tmp
   ```

4. **找不到clip文件**
   ```bash
   # 确认clip目录路径和文件名格式
   ls /path/to/clip/dir/
   ```

### 性能优化建议

1. **使用SSD存储临时文件**
2. **确保足够的GPU内存**
3. **合理设置进程数**
4. **监控网络带宽使用**

## 与原脚本的对比

| 特性 | multi_download.py | download_and_process.py |
|------|-------------------|------------------------|
| 存储需求 | 所有视频 (几TB) | 临时空间 (~GB) |
| 处理方式 | 先下载后处理 | 边下载边处理 |
| 空间效率 | 低 | 高 |
| 内存使用 | 较少 | 适中 |
| GPU需求 | 无 | 必需 |
| 适用场景 | 存储充足 | 存储有限 |
