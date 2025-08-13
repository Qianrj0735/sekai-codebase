#!/usr/bin/env python3
"""
集成下载-处理脚本
下载YouTube视频后立即处理成vstream，然后删除原视频文件以节省空间
"""

import gc
import logging
import os
import sys
import time
import subprocess
import multiprocessing
import argparse
import math
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# 导入CUDA相关模块
import pycuda.driver as cuda
import cvcuda
import torch

# 导入nvtranscoding的工具
sys.path.append("/workspace/sekai-codebase/clip_extracting")
from utils.nvvpf_utils import (
    VideoBatchDecoder,
    VideoMemoryEncoder,
)


def process_one_video(
    video_filename,
    vstream_filename_format,
    clips,
    decoder,
    encoder,
    cvcuda_stream,
    torch_stream,
):
    """
    处理单个视频的函数 (从3_nvtranscoding.py移植)
    """
    files = []
    if len(clips) == 0:
        return files

    decoder.initialize(video_filename)

    clip_idx, s, e = 0, clips[0][0], clips[0][1]
    with cvcuda_stream, torch.cuda.stream(torch_stream):
        for frame_idx, frames in enumerate(decoder):
            if frame_idx == s:
                encoder.initialize()
                encoder(frames)
            elif s < frame_idx < e - 1:
                encoder(frames)
            elif frame_idx == e - 1:
                encoder(frames)
                file = encoder.finish()

                with open(
                    os.path.join(vstream_filename_format.format(s, e)), "wb"
                ) as f:
                    f.write(file)
                del file
                files.append(vstream_filename_format.format(s, e))

                clip_idx += 1
                if clip_idx == len(clips):
                    break
                s, e = clips[clip_idx]
    assert clip_idx == len(clips) == len(files)

    decoder.finish()
    gc.collect()
    return files


def download_single_video(url, temp_video_dir, max_retries=3):
    """
    下载单个视频

    Args:
        url: YouTube URL
        temp_video_dir: 临时视频存储目录
        max_retries: 最大重试次数

    Returns:
        tuple: (success, video_path, video_id)
    """
    for attempt in range(max_retries):
        try:
            # 构建yt-dlp命令
            cmd = [
                "yt-dlp",
                "-f",
                "299+bestaudio",
                "-o",
                f"{temp_video_dir}/%(id)s.%(ext)s",
                "--merge-output-format",
                "mp4",
                "--no-overwrites",
                "--quiet",  # 减少输出
                url,
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

            if result.returncode == 0:
                # 查找下载的文件
                for file in os.listdir(temp_video_dir):
                    if file.endswith(".mp4"):
                        video_id = file.replace(".mp4", "")
                        return True, os.path.join(temp_video_dir, file), video_id

                return False, None, None
            else:
                logging.warning(f"下载失败 (尝试 {attempt + 1}/{max_retries}): {url}")
                if attempt < max_retries - 1:
                    time.sleep(5)  # 等待后重试

        except subprocess.TimeoutExpired:
            logging.warning(f"下载超时 (尝试 {attempt + 1}/{max_retries}): {url}")
        except Exception as e:
            logging.error(
                f"下载异常 (尝试 {attempt + 1}/{max_retries}): {url}, 错误: {e}"
            )

    return False, None, None


def process_worker(
    worker_id,
    urls,
    input_clip_dir,
    output_dir,
    start_delay,
    width=1280,
    height=720,
    fps=30,
    device_id=0,
):
    """
    工作进程：下载并处理视频
    """
    # 延迟启动
    if start_delay > 0:
        logging.info(f"进程 {worker_id}: 等待 {start_delay} 秒后开始...")
        time.sleep(start_delay)

    logging.info(f"进程 {worker_id}: 开始处理 {len(urls)} 个视频...")

    # 创建临时目录
    temp_video_dir = f"/tmp/temp_videos_worker_{worker_id}"
    os.makedirs(temp_video_dir, exist_ok=True)

    # 在子进程中初始化CUDA环境
    cuda_ctx = None
    decoder = None
    encoder = None

    try:
        # 设置CUDA环境变量（在子进程中）
        os.environ["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
        os.environ["CUDA_VISIBLE_DEVICES"] = str(device_id)

        # 初始化CUDA
        logging.info(f"进程 {worker_id}: 初始化CUDA...")

        # 初始化CUDA驱动（这在每个进程中都需要调用）
        cuda.init()

        # 获取设备并创建上下文
        cuda_device = cuda.Device(device_id)
        cuda_ctx = cuda_device.make_context()

        # 创建CUDA流
        cvcuda_stream = cvcuda.Stream().current
        torch_stream = torch.cuda.default_stream()

        # 初始化编码器和解码器
        decoder = VideoBatchDecoder(
            width, height, fps, 1, device_id, cuda_ctx, cvcuda_stream
        )
        encoder = VideoMemoryEncoder(
            width, height, fps, 1, device_id, cuda_ctx, cvcuda_stream
        )

        logging.info(f"进程 {worker_id}: CUDA初始化成功")

    except Exception as e:
        logging.error(f"进程 {worker_id}: CUDA初始化失败 - {e}")
        # 如果CUDA初始化失败，清理并退出
        if cuda_ctx:
            try:
                cuda_ctx.pop()
            except:
                pass
        return

    processed_count = 0
    failed_count = 0

    try:
        for i, url in enumerate(urls):
            try:
                logging.info(f"进程 {worker_id}: 处理 {i+1}/{len(urls)} - {url}")

                # 1. 下载视频
                success, video_path, video_id = download_single_video(
                    url, temp_video_dir
                )

                if not success:
                    logging.error(f"进程 {worker_id}: 下载失败 - {url}")
                    failed_count += 1
                    continue

                logging.info(f"进程 {worker_id}: 下载完成 - {video_id}")

                # 2. 检查是否有对应的clip文件
                clip_file = os.path.join(input_clip_dir, f"{video_id}.txt")
                if not os.path.exists(clip_file):
                    logging.warning(f"进程 {worker_id}: 找不到clip文件 - {clip_file}")
                    os.remove(video_path)  # 删除下载的视频
                    failed_count += 1
                    continue

                # 3. 读取clip信息
                with open(clip_file, "r") as f:
                    clips = [tuple(map(int, line.strip().split(" "))) for line in f]

                if len(clips) == 0:
                    logging.warning(f"进程 {worker_id}: clip文件为空 - {video_id}")
                    os.remove(video_path)
                    continue

                # 4. 创建输出目录
                video_output_dir = os.path.join(output_dir, video_id)
                os.makedirs(video_output_dir, exist_ok=True)

                # 5. 处理视频
                logging.info(f"进程 {worker_id}: 开始处理视频 - {video_id}")
                vstream_format = os.path.join(
                    video_output_dir, f"{video_id}_{{:07d}}_{{:07d}}.hevc"
                )

                processed_files = process_one_video(
                    video_path,
                    vstream_format,
                    clips,
                    decoder,
                    encoder,
                    cvcuda_stream,
                    torch_stream,
                )

                logging.info(
                    f"进程 {worker_id}: 处理完成 - {video_id}, 生成 {len(processed_files)} 个片段"
                )
                processed_count += 1

                # 6. 删除原始视频文件以节省空间
                os.remove(video_path)
                logging.info(f"进程 {worker_id}: 已删除临时视频文件 - {video_path}")

                # 7. 清理内存
                gc.collect()

            except Exception as e:
                logging.error(
                    f"进程 {worker_id}: 处理视频时发生错误 - {url}, 错误: {e}"
                )
                # 清理可能的临时文件
                try:
                    if "video_path" in locals() and os.path.exists(video_path):
                        os.remove(video_path)
                except:
                    pass
                failed_count += 1

    finally:
        # 清理CUDA上下文
        logging.info(f"进程 {worker_id}: 清理CUDA资源...")
        try:
            if cuda_ctx:
                cuda_ctx.pop()
                logging.info(f"进程 {worker_id}: CUDA上下文已清理")
        except Exception as e:
            logging.warning(f"进程 {worker_id}: 清理CUDA上下文时出错 - {e}")

        # 清理临时目录
        try:
            import shutil

            shutil.rmtree(temp_video_dir, ignore_errors=True)
            logging.info(f"进程 {worker_id}: 临时目录已清理")
        except Exception as e:
            logging.warning(f"进程 {worker_id}: 清理临时目录时出错 - {e}")

    logging.info(
        f"进程 {worker_id}: 完成处理，成功: {processed_count}, 失败: {failed_count}"
    )


def process_worker_thread(
    worker_id,
    urls,
    input_clip_dir,
    output_dir,
    shared_decoder,
    shared_encoder,
    shared_cuda_resources,
    thread_lock,
):
    """
    线程工作函数
    """
    for i, url in enumerate(urls):
        try:
            logging.info(f"线程 {worker_id}: 处理 {i+1}/{len(urls)} - {url}")

            # 下载视频
            temp_video_dir = f"/tmp/temp_videos_thread_{worker_id}"
            os.makedirs(temp_video_dir, exist_ok=True)

            success, video_path, video_id = download_single_video(url, temp_video_dir)

            if not success:
                continue

            # 读取clip文件
            clip_file = os.path.join(input_clip_dir, f"{video_id}.txt")
            if not os.path.exists(clip_file):
                os.remove(video_path)
                continue

            with open(clip_file, "r") as f:
                clips = [tuple(map(int, line.strip().split(" "))) for line in f]

            if len(clips) == 0:
                os.remove(video_path)
                continue

            # 创建输出目录
            video_output_dir = os.path.join(output_dir, video_id)
            os.makedirs(video_output_dir, exist_ok=True)

            # 使用锁保护CUDA资源的访问
            with thread_lock:
                logging.info(f"线程 {worker_id}: 开始处理视频 - {video_id}")
                vstream_format = os.path.join(
                    video_output_dir, f"{video_id}_{{:07d}}_{{:07d}}.hevc"
                )

                processed_files = process_one_video(
                    video_path,
                    vstream_format,
                    clips,
                    shared_decoder,
                    shared_encoder,
                    shared_cuda_resources["cvcuda_stream"],
                    shared_cuda_resources["torch_stream"],
                )

            # 删除临时视频
            os.remove(video_path)
            logging.info(f"线程 {worker_id}: 处理完成 - {video_id}")

        except Exception as e:
            logging.error(f"线程 {worker_id}: 处理错误 - {e}")


def split_urls(urls, num_workers):
    """分割URL列表"""
    urls_per_worker = math.ceil(len(urls) / num_workers)
    split_urls = []

    for i in range(num_workers):
        start_idx = i * urls_per_worker
        end_idx = min((i + 1) * urls_per_worker, len(urls))

        if start_idx >= len(urls):
            break

        worker_urls = urls[start_idx:end_idx]
        split_urls.append(worker_urls)

    return split_urls


def main():
    parser = argparse.ArgumentParser(description="集成下载-处理脚本")
    parser.add_argument("--urls-file", "-u", required=True, help="URL列表文件")
    parser.add_argument("--input-clip-dir", "-c", required=True, help="输入clip目录")
    parser.add_argument("--output-dir", "-o", required=True, help="输出vstream目录")
    parser.add_argument(
        "--workers", "-w", type=int, default=2, help="工作进程数量 (默认: 2)"
    )
    parser.add_argument(
        "--start-delay",
        "-d",
        type=int,
        default=15,
        help="进程间启动延迟秒数 (默认: 15)",
    )
    parser.add_argument(
        "--width", type=int, default=1280, help="输出视频宽度 (默认: 1280)"
    )
    parser.add_argument(
        "--height", type=int, default=720, help="输出视频高度 (默认: 720)"
    )
    parser.add_argument("--fps", type=int, default=30, help="输出视频FPS (默认: 30)")
    parser.add_argument("--device-id", type=int, default=0, help="GPU设备ID (默认: 0)")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="日志级别 (默认: INFO)",
    )

    args = parser.parse_args()

    # 设置日志
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # 检查输入文件和目录
    if not os.path.exists(args.urls_file):
        logging.error(f"URL文件不存在: {args.urls_file}")
        sys.exit(1)

    if not os.path.exists(args.input_clip_dir):
        logging.error(f"Clip目录不存在: {args.input_clip_dir}")
        sys.exit(1)

    # 创建输出目录
    os.makedirs(args.output_dir, exist_ok=True)

    # 读取URL列表
    with open(args.urls_file, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    logging.info(f"总共 {len(urls)} 个URL需要处理")
    logging.info(f"使用 {args.workers} 个工作进程")
    logging.info(f"输出目录: {args.output_dir}")

    # 分割URL
    split_url_lists = split_urls(urls, args.workers)
    logging.info(f"URL分配: {[len(url_list) for url_list in split_url_lists]}")

    # 启动工作进程
    processes = []

    try:
        for i, worker_urls in enumerate(split_url_lists):
            delay = i * args.start_delay

            process = multiprocessing.Process(
                target=process_worker,
                args=(
                    i,
                    worker_urls,
                    args.input_clip_dir,
                    args.output_dir,
                    delay,
                    args.width,
                    args.height,
                    args.fps,
                    args.device_id,
                ),
            )
            process.start()
            processes.append(process)
            logging.info(
                f"启动进程 {i} (PID: {process.pid}), 处理 {len(worker_urls)} 个URL"
            )

        logging.info(f"所有 {len(processes)} 个进程已启动，等待完成...")

        # 等待所有进程完成
        for i, process in enumerate(processes):
            process.join()
            logging.info(f"进程 {i} 已完成")

        logging.info("所有处理进程已完成！")

    except KeyboardInterrupt:
        logging.info("收到中断信号，正在终止所有进程...")
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=10)
                if process.is_alive():
                    process.kill()


def main_threaded():
    parser = argparse.ArgumentParser(description="集成下载-处理脚本")
    parser.add_argument("--urls-file", "-u", required=True, help="URL列表文件")
    parser.add_argument("--input-clip-dir", "-c", required=True, help="输入clip目录")
    parser.add_argument("--output-dir", "-o", required=True, help="输出vstream目录")
    parser.add_argument(
        "--workers", "-w", type=int, default=2, help="工作进程数量 (默认: 2)"
    )
    parser.add_argument(
        "--start-delay",
        "-d",
        type=int,
        default=15,
        help="进程间启动延迟秒数 (默认: 15)",
    )
    parser.add_argument(
        "--width", type=int, default=1280, help="输出视频宽度 (默认: 1280)"
    )
    parser.add_argument(
        "--height", type=int, default=720, help="输出视频高度 (默认: 720)"
    )
    parser.add_argument("--fps", type=int, default=30, help="输出视频FPS (默认: 30)")
    parser.add_argument("--device-id", type=int, default=0, help="GPU设备ID (默认: 0)")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="日志级别 (默认: INFO)",
    )

    args = parser.parse_args()

    # 设置日志
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # 检查输入文件和目录
    if not os.path.exists(args.urls_file):
        logging.error(f"URL文件不存在: {args.urls_file}")
        sys.exit(1)

    if not os.path.exists(args.input_clip_dir):
        logging.error(f"Clip目录不存在: {args.input_clip_dir}")
        sys.exit(1)

    # 创建输出目录
    os.makedirs(args.output_dir, exist_ok=True)

    # 读取URL列表
    with open(args.urls_file, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    logging.info(f"总共 {len(urls)} 个URL需要处理")
    logging.info(f"使用 {args.workers} 个工作进程")
    logging.info(f"输出目录: {args.output_dir}")

    # 初始化CUDA（只在主进程中一次）
    cuda.init()
    cuda_device = cuda.Device(args.device_id)
    cuda_ctx = cuda_device.make_context()
    cvcuda_stream = cvcuda.Stream().current
    torch_stream = torch.cuda.default_stream()

    # 创建共享的编码器和解码器
    decoder = VideoBatchDecoder(
        args.width, args.height, args.fps, 1, args.device_id, cuda_ctx, cvcuda_stream
    )
    encoder = VideoMemoryEncoder(
        args.width, args.height, args.fps, 1, args.device_id, cuda_ctx, cvcuda_stream
    )

    shared_cuda_resources = {
        "cvcuda_stream": cvcuda_stream,
        "torch_stream": torch_stream,
    }

    # 创建线程锁
    cuda_lock = threading.Lock()

    # 分割URL
    split_url_lists = split_urls(urls, args.workers)

    # 使用线程池
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        for i, worker_urls in enumerate(split_url_lists):
            future = executor.submit(
                process_worker_thread,
                i,
                worker_urls,
                args.input_clip_dir,
                args.output_dir,
                decoder,
                encoder,
                shared_cuda_resources,
                cuda_lock,
            )
            futures.append(future)

        # 等待所有线程完成
        for future in futures:
            future.result()

    # 清理CUDA资源
    cuda_ctx.pop()


if __name__ == "__main__":
    main()
