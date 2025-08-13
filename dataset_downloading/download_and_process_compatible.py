#!/usr/bin/env python3
"""
简化版下载-处理脚本
如果CUDA相关库不可用，提供基础的下载功能
"""

import os
import sys
import time
import subprocess
import multiprocessing
import argparse
import math
import logging
import shutil
from pathlib import Path

# 尝试导入CUDA相关库
try:
    import pycuda.driver as cuda
    import cvcuda
    import torch

    CUDA_AVAILABLE = True

    # 尝试导入nvvpf工具
    try:
        sys.path.append("/workspace/sekai-codebase/clip_extracting")
        from utils.nvvpf_utils import VideoBatchDecoder, VideoMemoryEncoder

        NVVPF_AVAILABLE = True
    except ImportError:
        NVVPF_AVAILABLE = False
        print("警告: nvvpf工具不可用，将使用FFmpeg处理")

except ImportError:
    CUDA_AVAILABLE = False
    NVVPF_AVAILABLE = False
    print("警告: CUDA库不可用，将使用CPU处理")


def process_with_ffmpeg(video_path, clips, output_dir, video_id):
    """
    使用FFmpeg处理视频（备用方案）
    """
    processed_files = []

    for i, (start_frame, end_frame) in enumerate(clips):
        # 假设30fps
        start_time = start_frame / 30.0
        duration = (end_frame - start_frame) / 30.0

        output_file = os.path.join(
            output_dir, f"{video_id}_{start_frame:07d}_{end_frame:07d}.mp4"
        )

        cmd = [
            "ffmpeg",
            "-i",
            video_path,
            "-ss",
            str(start_time),
            "-t",
            str(duration),
            "-c:v",
            "libx264",
            "-c:a",
            "aac",
            "-y",
            output_file,
        ]

        try:
            subprocess.run(cmd, check=True, capture_output=True)
            processed_files.append(output_file)
            logging.info(f"使用FFmpeg处理完成: {output_file}")
        except subprocess.CalledProcessError as e:
            logging.error(f"FFmpeg处理失败: {e}")

    return processed_files


def process_one_video_cuda(
    video_filename,
    vstream_filename_format,
    clips,
    decoder,
    encoder,
    cvcuda_stream,
    torch_stream,
):
    """
    使用CUDA处理视频的函数
    """
    import gc

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
    """
    for attempt in range(max_retries):
        try:
            cmd = [
                "yt-dlp",
                "-f",
                "299+bestaudio/best",
                "-o",
                f"{temp_video_dir}/%(id)s.%(ext)s",
                "--merge-output-format",
                "mp4",
                "--no-overwrites",
                "--quiet",
                url,
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

            if result.returncode == 0:
                for file in os.listdir(temp_video_dir):
                    if file.endswith(".mp4"):
                        video_id = file.replace(".mp4", "")
                        return True, os.path.join(temp_video_dir, file), video_id

                return False, None, None
            else:
                logging.warning(f"下载失败 (尝试 {attempt + 1}/{max_retries}): {url}")
                if attempt < max_retries - 1:
                    time.sleep(5)

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
    use_cuda=True,
):
    """
    工作进程：下载并处理视频
    """
    if start_delay > 0:
        logging.info(f"进程 {worker_id}: 等待 {start_delay} 秒后开始...")
        time.sleep(start_delay)

    logging.info(f"进程 {worker_id}: 开始处理 {len(urls)} 个视频...")

    # 创建临时目录
    temp_video_dir = f"/tmp/temp_videos_worker_{worker_id}"
    os.makedirs(temp_video_dir, exist_ok=True)

    # 初始化处理器
    decoder = None
    encoder = None
    cuda_ctx = None
    cvcuda_stream = None
    torch_stream = None

    if use_cuda and CUDA_AVAILABLE and NVVPF_AVAILABLE:
        try:
            cuda_device = cuda.Device(device_id)
            cuda_ctx = cuda_device.retain_primary_context()
            cuda_ctx.push()
            cvcuda_stream = cvcuda.Stream().current
            torch_stream = torch.cuda.default_stream(device=cuda_device)

            decoder = VideoBatchDecoder(
                width, height, fps, 1, device_id, cuda_ctx, cvcuda_stream
            )
            encoder = VideoMemoryEncoder(
                width, height, fps, 1, device_id, cuda_ctx, cvcuda_stream
            )
            logging.info(f"进程 {worker_id}: 使用CUDA加速处理")
        except Exception as e:
            logging.warning(f"进程 {worker_id}: CUDA初始化失败，使用FFmpeg: {e}")
            use_cuda = False
    else:
        use_cuda = False
        logging.info(f"进程 {worker_id}: 使用FFmpeg处理")

    try:
        processed_count = 0
        failed_count = 0

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

                # 2. 检查clip文件
                clip_file = os.path.join(input_clip_dir, f"{video_id}.txt")
                if not os.path.exists(clip_file):
                    logging.warning(f"进程 {worker_id}: 找不到clip文件 - {clip_file}")
                    os.remove(video_path)
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

                if use_cuda:
                    vstream_format = os.path.join(
                        video_output_dir, f"{video_id}_{{:07d}}_{{:07d}}.hevc"
                    )
                    processed_files = process_one_video_cuda(
                        video_path,
                        vstream_format,
                        clips,
                        decoder,
                        encoder,
                        cvcuda_stream,
                        torch_stream,
                    )
                else:
                    processed_files = process_with_ffmpeg(
                        video_path, clips, video_output_dir, video_id
                    )

                logging.info(
                    f"进程 {worker_id}: 处理完成 - {video_id}, 生成 {len(processed_files)} 个片段"
                )
                processed_count += 1

                # 6. 删除原始视频
                os.remove(video_path)
                logging.info(f"进程 {worker_id}: 已删除临时视频 - {video_path}")

            except Exception as e:
                logging.error(
                    f"进程 {worker_id}: 处理视频时发生错误 - {url}, 错误: {e}"
                )
                try:
                    if "video_path" in locals() and os.path.exists(video_path):
                        os.remove(video_path)
                except:
                    pass
                failed_count += 1

        logging.info(
            f"进程 {worker_id}: 完成处理，成功: {processed_count}, 失败: {failed_count}"
        )

    finally:
        # 清理CUDA环境
        if cuda_ctx:
            try:
                cuda_ctx.pop()
            except:
                pass

        # 清理临时目录
        try:
            shutil.rmtree(temp_video_dir, ignore_errors=True)
        except:
            pass


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
    parser = argparse.ArgumentParser(description="下载-处理脚本 (兼容版)")
    parser.add_argument("--urls-file", "-u", required=True, help="URL列表文件")
    parser.add_argument("--input-clip-dir", "-c", required=True, help="输入clip目录")
    parser.add_argument("--output-dir", "-o", required=True, help="输出目录")
    parser.add_argument("--workers", "-w", type=int, default=2, help="工作进程数量")
    parser.add_argument(
        "--start-delay", "-d", type=int, default=15, help="进程间启动延迟秒数"
    )
    parser.add_argument("--width", type=int, default=1280, help="输出视频宽度")
    parser.add_argument("--height", type=int, default=720, help="输出视频高度")
    parser.add_argument("--fps", type=int, default=30, help="输出视频FPS")
    parser.add_argument("--device-id", type=int, default=0, help="GPU设备ID")
    parser.add_argument(
        "--no-cuda", action="store_true", help="强制使用FFmpeg而不是CUDA"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="日志级别",
    )

    args = parser.parse_args()

    # 设置日志
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # 检查环境
    if args.no_cuda:
        use_cuda = False
        logging.info("强制使用FFmpeg处理")
    else:
        use_cuda = CUDA_AVAILABLE and NVVPF_AVAILABLE
        if use_cuda:
            logging.info("将使用CUDA加速处理")
        else:
            logging.info("将使用FFmpeg处理")

    # 检查输入
    if not os.path.exists(args.urls_file):
        logging.error(f"URL文件不存在: {args.urls_file}")
        sys.exit(1)

    if not os.path.exists(args.input_clip_dir):
        logging.error(f"Clip目录不存在: {args.input_clip_dir}")
        sys.exit(1)

    # 创建输出目录
    os.makedirs(args.output_dir, exist_ok=True)

    # 读取URL
    with open(args.urls_file, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    logging.info(f"总共 {len(urls)} 个URL需要处理")
    logging.info(f"使用 {args.workers} 个工作进程")

    # 分割URL
    split_url_lists = split_urls(urls, args.workers)

    # 启动进程
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
                    use_cuda,
                ),
            )
            process.start()
            processes.append(process)
            logging.info(
                f"启动进程 {i} (PID: {process.pid}), 处理 {len(worker_urls)} 个URL"
            )

        logging.info("等待所有进程完成...")

        for i, process in enumerate(processes):
            process.join()
            logging.info(f"进程 {i} 已完成")

        logging.info("所有处理进程已完成！")

    except KeyboardInterrupt:
        logging.info("收到中断信号，正在终止进程...")
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=10)
                if process.is_alive():
                    process.kill()


if __name__ == "__main__":
    main()
