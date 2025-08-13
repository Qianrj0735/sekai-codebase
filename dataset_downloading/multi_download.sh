#!/bin/bash

# 多进程YouTube视频下载脚本
# 使用方法: ./multi_download.sh [工作进程数] [启动延迟秒数]

# 默认参数
NUM_WORKERS=${1:-4}
START_DELAY=${2:-10}
URLS_FILE="sekai-real-walking-hq_urls.txt"
OUTPUT_DIR="./videos"
TEMP_DIR="temp_splits"

echo "======================================"
echo "多进程YouTube视频下载器 (Shell版本)"
echo "======================================"
echo "工作进程数: $NUM_WORKERS"
echo "启动延迟: ${START_DELAY}秒"
echo "URL文件: $URLS_FILE"
echo "输出目录: $OUTPUT_DIR"
echo "======================================"

# 检查输入文件
if [ ! -f "$URLS_FILE" ]; then
    echo "错误: URL文件不存在: $URLS_FILE"
    exit 1
fi

# 创建必要的目录
mkdir -p "$OUTPUT_DIR"
mkdir -p "$TEMP_DIR"

# 计算总URL数量
TOTAL_URLS=$(wc -l < "$URLS_FILE")
URLS_PER_WORKER=$((($TOTAL_URLS + $NUM_WORKERS - 1) / $NUM_WORKERS))

echo "总共 $TOTAL_URLS 个URL，每个进程处理约 $URLS_PER_WORKER 个URL"

# 分割URL文件
echo "正在分割URL文件..."
split -l $URLS_PER_WORKER "$URLS_FILE" "$TEMP_DIR/urls_part_"

# 重命名分割文件
SPLIT_FILES=()
WORKER_ID=0
for file in $TEMP_DIR/urls_part_*; do
    new_name="$TEMP_DIR/urls_worker_${WORKER_ID}.txt"
    mv "$file" "$new_name"
    SPLIT_FILES+=("$new_name")
    echo "进程 $WORKER_ID: $(wc -l < "$new_name") 个URL -> $new_name"
    ((WORKER_ID++))
done

# 启动下载进程
PIDS=()
for i in "${!SPLIT_FILES[@]}"; do
    URLS_FILE_WORKER="${SPLIT_FILES[$i]}"
    DELAY=$((i * START_DELAY))
    
    echo "启动进程 $i，延迟 ${DELAY}秒..."
    
    (
        if [ $DELAY -gt 0 ]; then
            echo "进程 $i: 等待 ${DELAY}秒..."
            sleep $DELAY
        fi
        
        echo "进程 $i: 开始下载..."
        yt-dlp \
            -N 5 \
            -f "299+bestaudio" \
            -a "$URLS_FILE_WORKER" \
            -o "$OUTPUT_DIR/%(id)s.%(ext)s" \
            --sleep-interval 3 \
            --max-sleep-interval 6 \
            --continue \
            --no-overwrites \
            --write-description \
            --write-info-json \
            --ignore-errors \
            > "download_log_worker_${i}.txt" 2>&1
        
        echo "进程 $i: 下载完成"
    ) &
    
    PIDS+=($!)
    echo "启动进程 $i (PID: ${PIDS[$i]})"
done

echo ""
echo "所有 ${#PIDS[@]} 个进程已启动，等待完成..."

# 等待所有进程完成
for i in "${!PIDS[@]}"; do
    wait ${PIDS[$i]}
    echo "进程 $i 已完成"
done

echo ""
echo "所有下载进程已完成！"

# 清理临时文件
echo "清理临时文件..."
rm -f $TEMP_DIR/urls_worker_*.txt
rmdir "$TEMP_DIR" 2>/dev/null

echo "下载完成！检查 $OUTPUT_DIR 目录获取下载的文件。"
echo "查看各进程的日志文件: download_log_worker_*.txt"
