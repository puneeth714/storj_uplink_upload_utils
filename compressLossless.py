#!/usr/bin/env python
# 7z compression of each subsource in a source (recursive)
import os
import sys
import subprocess
import time
from loguru import logger
from get_files import get_files

# # Set up logging to out.log file
# logger.add("out.log", enqueue=True, encoding='utf-8')


def condition(file):
    # return true if file is a source
    return os.path.isdir(file)


def getTotalSize(folders, directory):
    # use du command to get size of files
    total_size = 0
    for folder in folders:
        total_size += int(subprocess.check_output(
            f"du -sb {directory}/{folder}", shell=True).split()[0])
    return total_size


def archivesource(source, destination, compression_type="7z"):
    # check if destination source exists and create if not
    if not os.path.exists(destination):
        os.makedirs(destination)
    # compress source
    if compression_type == "7z":
        subprocess.run(
            f"7z a -t7z -m0=lzma2 -mx=9 -mfb=64 -md=32m -ms=on {os.path.join(destination,source)}.7z {source}",
            shell=True,
        )
        # -si{dictionary_size} - set dictionary size to {dictionary_size} bytes
        # -mx=9 - set compression level to 9 (max)
        # -m0=lzma - use LZMA compression
        # -mfb=64 - set LZMA fast bytes to 64
        # -md=32m - set LZMA dictionary size to 32 MB
        # -ms=on - use solid compression
        # -mmt=on - use multithreading
        # -mhe=on - use high entropy encoding
        # -r - recursive
        # -t7z - use 7z format
        # -si{dictionary_size} - set dictionary size to {dictionary_size} bytes
        return f"{os.path.join(destination,source)}.7z"
    elif compression_type == "tar.gz":
        subprocess.run(
            f"tar -czvf {destination}/{source}.tar.gz {source}",
            shell=True,
        )
        return f"{destination}/{source}.tar.gz"

    elif compression_type == "tar.bz2":
        subprocess.run(
            f"tar -cjvf {destination}/{source}.tar.bz2 {source}",
            shell=True,
        )
        return f"{destination}/{source}.tar.bz2"
    elif compression_type == "tar.xz":
        subprocess.run(
            f"tar -cJvf {destination}/{source}.tar.xz {source}",
            shell=True,
        )
        return f"{destination}/{source}.tar.xz"
    elif compression_type == "tar":
        subprocess.run(
            f"tar -cvf {destination}/{source}.tar {source}",
            shell=True,
        )
        return f"{destination}/{source}.tar"




