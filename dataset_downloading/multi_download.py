#!/usr/bin/env python3
"""
多进程YouTube视频下载器
将URL列表分割成多个文件，然后启动多个yt-dlp进程并行下载
"""

import os
import sys
import time
import subprocess
import multiprocessing
from pathlib import Path
import argparse
import math


def split_urls_file(input_file, num_workers, output_dir="temp_splits"):
    """
    将URL文件分割成多个小文件
    
    Args:
        input_file: 输入的URL文件路径
        num_workers: 工作进程数量
        output_dir: 分割文件的输出目录
    
    Returns:
        list: 分割后的文件路径列表
    """
    # 创建临时目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 读取所有URL
    with open(input_file, 'r', encoding='utf-8') as f:
        urls = [line.strip() for line in f if line.strip()]
    
    total_urls = len(urls)
    urls_per_worker = math.ceil(total_urls / num_workers)
    
    print(f"总共 {total_urls} 个URL，分配给 {num_workers} 个进程")
    print(f"每个进程处理约 {urls_per_worker} 个URL")
    
    split_files = []
    for i in range(num_workers):
        start_idx = i * urls_per_worker
        end_idx = min((i + 1) * urls_per_worker, total_urls)
        
        if start_idx >= total_urls:
            break
            
        worker_urls = urls[start_idx:end_idx]
        split_file = os.path.join(output_dir, f"urls_worker_{i}.txt")
        
        with open(split_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(worker_urls))
        
        split_files.append(split_file)
        print(f"进程 {i}: {len(worker_urls)} 个URL -> {split_file}")
    
    return split_files


def download_worker(worker_id, urls_file, output_dir, start_delay, extra_args=""):
    """
    单个工作进程的下载函数
    
    Args:
        worker_id: 工作进程ID
        urls_file: 该进程负责的URL文件
        output_dir: 下载输出目录
        start_delay: 启动延迟时间（秒）
        extra_args: 额外的yt-dlp参数
    """
    # 延迟启动，避免同时请求
    if start_delay > 0:
        print(f"进程 {worker_id}: 等待 {start_delay} 秒后开始下载...")
        time.sleep(start_delay)
    
    print(f"进程 {worker_id}: 开始下载...")
    
    # 转换为绝对路径
    urls_file = os.path.abspath(urls_file)
    output_dir = os.path.abspath(output_dir)
    
    # 构建yt-dlp命令
    cmd = [
        "yt-dlp",
        "-N", "5",  # 减少并发连接数
        "-f", "299+bestaudio",  # 视频格式
        "-a", urls_file,  # 从文件读取URL（使用绝对路径）
        "-o", f"{output_dir}/%(id)s.%(ext)s",  # 输出格式（使用绝对路径）
        "--sleep-interval", "3",  # 请求间隔
        "--max-sleep-interval", "6",  # 最大请求间隔
        "--continue",  # 断点续传
        "--no-overwrites",  # 不覆盖已存在文件
        "--ignore-errors",  # 忽略错误继续下载
        "--merge-output-format", "mp4"
    ]
    
    # 添加额外参数
    if extra_args:
        cmd.extend(extra_args.split())
    
    try:
        # 执行下载命令（移除cwd参数，使用当前工作目录）
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        
        print(f"进程 {worker_id}: 下载完成，返回码: {result.returncode}")
        
        # 保存日志
        os.makedirs("/workspace/sekai-codebase/dataset_downloading/logs", exist_ok=True)
        log_dir = "/workspace/sekai-codebase/dataset_downloading/logs"
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"download_log_worker_{worker_id}.txt")
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write(f"Worker {worker_id} - Return Code: {result.returncode}\n")
            f.write("=" * 50 + "\n")
            f.write(result.stdout)
            
    except Exception as e:
        print(f"进程 {worker_id}: 发生错误 - {e}")


def cleanup_temp_files(split_files):
    """清理临时分割文件"""
    for file_path in split_files:
        try:
            os.remove(file_path)
            print(f"已删除临时文件: {file_path}")
        except Exception as e:
            print(f"删除临时文件失败 {file_path}: {e}")


def main():
    parser = argparse.ArgumentParser(description="多进程YouTube视频下载器")
    parser.add_argument("--urls-file", "-u", 
                       default="sekai-real-walking-hq_urls.txt",
                       help="URL列表文件 (默认: sekai-real-walking-hq_urls.txt)")
    parser.add_argument("--workers", "-w", type=int, 
                       default=4,
                       help="工作进程数量 (默认: 4)")
    parser.add_argument("--output-dir", "-o", 
                       default="./videos",
                       help="下载输出目录 (默认: ./videos)")
    parser.add_argument("--start-delay", "-d", type=int, 
                       default=10,
                       help="进程间启动延迟秒数 (默认: 10)")
    parser.add_argument("--extra-args", "-e", 
                       default="",
                       help="额外的yt-dlp参数")
    parser.add_argument("--keep-temp", action="store_true",
                       help="保留临时分割文件")
    
    args = parser.parse_args()
    
    # 检查输入文件
    if not os.path.exists(args.urls_file):
        print(f"错误: URL文件不存在: {args.urls_file}")
        sys.exit(1)
    
    # 创建输出目录
    os.makedirs(args.output_dir, exist_ok=True)
    
    print("=" * 60)
    print("多进程YouTube视频下载器")
    print("=" * 60)
    print(f"URL文件: {args.urls_file}")
    print(f"工作进程数: {args.workers}")
    print(f"输出目录: {args.output_dir}")
    print(f"启动延迟: {args.start_delay}秒")
    print("=" * 60)
    
    # 分割URL文件
    print("正在分割URL文件...")
    split_files = split_urls_file(args.urls_file, args.workers)
    
    if not split_files:
        print("错误: 没有生成分割文件")
        sys.exit(1)
    
    print(f"生成了 {len(split_files)} 个分割文件")
    
    # 创建进程池
    processes = []
    
    try:
        # 启动所有工作进程
        for i, urls_file in enumerate(split_files):
            delay = i * args.start_delay  # 每个进程延迟启动
            
            process = multiprocessing.Process(
                target=download_worker,
                args=(i, urls_file, args.output_dir, delay, args.extra_args)
            )
            process.start()
            processes.append(process)
            print(f"启动进程 {i} (PID: {process.pid})")
        
        print(f"\n所有 {len(processes)} 个进程已启动，等待完成...")
        
        # 等待所有进程完成
        for i, process in enumerate(processes):
            process.join()
            print(f"进程 {i} 已完成")
        
        print("\n所有下载进程已完成！")
        
    except KeyboardInterrupt:
        print("\n收到中断信号，正在终止所有进程...")
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
                if process.is_alive():
                    process.kill()
    
    finally:
        # 清理临时文件
        if not args.keep_temp:
            print("\n清理临时文件...")
            cleanup_temp_files(split_files)
            # 删除临时目录
            try:
                os.rmdir("temp_splits")
            except:
                pass
        else:
            print(f"\n临时文件保留在: {split_files}")


if __name__ == "__main__":
    main()
