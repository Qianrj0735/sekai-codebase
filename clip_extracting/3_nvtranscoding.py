# NOTE: One must import PyCuda driver first, before CVCUDA or VPF otherwise things may throw unexpected errors.
import gc
import logging
import os
import argparse

import pycuda.driver as cuda  # noqa: F401
import cvcuda

# import tqdm
import torch

from utils.nvvpf_utils import (
    VideoBatchDecoder,
    VideoMemoryEncoder,
)


def process_one_video(video_filename, vstream_filename_format, clips):
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
    # torch.cuda.empty_cache()
    # nvcv.clear_cache()
    return files


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_clip_dir", type=str)
    parser.add_argument("--input_video_dir", type=str)
    parser.add_argument("--output_dir", type=str)
    parser.add_argument(
        "--width", type=int, default=1280, help="Width of the output video."
    )
    parser.add_argument(
        "--height", type=int, default=720, help="Height of the output video."
    )
    parser.add_argument("--fps", type=int, default=30, help="FPS of the output video.")
    parser.add_argument(
        "--batch_size",
        type=int,
        default=1,
        help="BS of the Decoder, set to 1 for transcodeing.",
    )
    parser.add_argument(
        "--device_id",
        type=int,
        default=0,
        help="Specify the GPU ID if you have multiple GPUs.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    if args.output_dir is None:
        args.output_dir = args.input_clip_dir + "_vstreams"

    os.makedirs(args.output_dir, exist_ok=False)

    logging.info(f"Using CUDA device: {args.device_id}.")

    cuda_device = cuda.Device(args.device_id)
    cuda_ctx = cuda_device.retain_primary_context()
    cuda_ctx.push()
    cvcuda_stream = cvcuda.Stream().current
    torch_stream = torch.cuda.default_stream(device=cuda_device)

    decoder = VideoBatchDecoder(
        args.width,
        args.height,
        args.fps,
        args.batch_size,
        args.device_id,
        cuda_ctx,
        cvcuda_stream,
    )
    assert decoder.fps == 30

    encoder = VideoMemoryEncoder(
        args.width,
        args.height,
        args.fps,
        args.batch_size,
        args.device_id,
        cuda_ctx,
        cvcuda_stream,
    )

    vids = sorted([os.path.splitext(vid)[0] for vid in os.listdir(args.input_clip_dir)])
    logging.info(f"Total {len(vids)} file(s) to process.")

    for idx, vid in enumerate(vids, start=1):
        with open(os.path.join(args.input_clip_dir, f"{vid}.txt"), "r") as f:
            clips = [tuple(map(int, line.strip().split(" "))) for line in f]

        logging.info(f"[{idx}/{len(vids)}] Start processing '{vid}'.")

        os.makedirs(os.path.join(args.output_dir, vid), exist_ok=False)

        files = process_one_video(
            os.path.join(args.input_video_dir, f"{vid}.mp4"),
            os.path.join(args.output_dir, vid, f"{vid}_{{:07d}}_{{:07d}}.hevc"),
            clips,
        )

        logging.info(f"Finish process {len(files)} clips of '{vid}'.")

    cuda_ctx.pop()
