#!/usr/bin/env python

from genericpath import isfile
import os
import sys
import time
import yaml
from loguru import logger
from PIL import Image
import subprocess
from get_files import get_files, readConfig

# log to output.log file
logger.add("output.log", format="{time} {level} {message}", level="INFO")


def condition(file):
    # return if the given file is an image/video
    # using the file command
    file_type = subprocess.check_output(f"file {file}", shell=True)
    return b"image" in file_type.lower() or b"video" in file_type.lower() or \
        ".jpg" in file or ".jpeg" in file or ".png" in file or ".mp4" in file or \
        ".mov" in file or b"media" in file_type.lower()


def checkImageVideo(file):
    # check if the given file is an image or a video
    # using the file command
    file_type = subprocess.check_output(f"file {file}", shell=True)
    if b"image" in file_type.lower():
        return "image"
    elif b"video" in file_type.lower():
        return "video"
    else:
        file = file.lower()
        if ".jpg" in file or ".jpeg" in file or ".png" in file:
            return "image"
        elif ".mp4" in file or ".mov" in file:
            return "video"
        else:
            return "other"


def getTotalSize(file, directory):
    # os.path.getsize() returns the size of the file in megabytes
    if os.path.isdir(file):
        # get size with du command
        size = subprocess.check_output(
            f"du -s {file}", shell=True).decode().split()[0]
        return int(size)/1024
    return os.path.getsize(os.path.join(directory, file))/(1024*1024)

# it will get list of file paths,destination directory and config file


def compressFilesLossy(files, destination, config):
    # check if destination directory exists and create if not
    if not os.path.exists(destination):
        os.makedirs(destination)
    # compress each file in the list
    for file in files:
        # check if the file is an image or a video
        file_type = checkImageVideo(file)
        # if the file is an image
        if file_type == "image":
            # compress the image
            compressPIL(file, destination, config)
        # if the file is a video
        elif file_type == "video":
            # compress the video
            compressFFMPEG(file, destination, config)
        # if the file is neither an image nor a video
        else:
            # log the file type
            logger.info(f"File type: {file_type}")
            # log the file name
            logger.info(f"File name: {file}")
            # log the file size
            logger.info(
                f"File size: {getTotalSize(file,os.path.dirname(file))} MB")
            # log the file is not compressed
            logger.info("File is not compressed")
            # copy the file to the destination directory
            subprocess.run(f"cp {file} {destination}", shell=True)
            # log the file is copied
            logger.info("File is copied")


def compressFFMPEG(source: str, destination: str, config: dict):
    # compressFileLossy will call this function if the source is a video file
    # convet the video to a lower resolution and lower bitrate with given codec in config file
    # and save it in the destination directory
    # source contains the path to the source file and file name
    # store file name in source_file variable and store the file path in source_path
    source_file = os.path.basename(source)
    source_path = os.path.dirname(source)
    dest_file = source_file
    # convert the video to a lower resolution and lower bitrate with given codec in config file
    # and save it in the destination directory
    # use video_format in config file
    # check if input and output file formats are the same
    if not os.path.isfile(os.path.join(destination, dest_file)):
        if config['video_format'] == source_file.split('.')[-1]:
            # if input and output file formats are the same
            logger.info(
                f"Input and output file formats are the same. Converting {source_file} to {config['video_format']} format")
            cmd = f"ffmpeg -i {source} -c:v {config['codec']} -b:v {config['bitrate']}  {os.path.join(destination,source_file)}"
        else:
            # if input and output file formats are different
            # remove the extension from the source file and add the video_format from config file
            logger.info(
                f"Input and output file formats are different. Converting {source_file} to {config['video_format']} format")
            dest_file = source_file.split(
                '.')[0] + '.' + config['video_format']
            cmd = f"ffmpeg -i {source} -c:v {config['codec']} -b:v {config['bitrate']}  {os.path.join(destination,dest_file)}"
        subprocess.run(cmd, shell=True)
    # log the source and destination file sizes and compression ratio
    source_size = getTotalSize(source, source_path)
    destination_size = getTotalSize(os.path.join(
        destination, dest_file), destination)
    logger.info(f"Source file size: {source_size} MB")
    logger.info(f"Destination file size: {destination_size} MB")
    logger.info(
        f"Compression ratio: {(source_size)*100/destination_size} %")
    # return the destination file path
    return os.path.join(dest_file, destination)


def compressPIL(source: str, destination: str, config: dict):
    # compressFileLossy will call this function if the source is an image file
    # convert the image to a lower quality and save it in the destination directory
    # use given quality ,extension and format in config file
    # source contains the path to the source file and file name
    # store file name in source_file variable and store the file path in source_path
    source_file = os.path.basename(source)
    source_path = os.path.dirname(source)
    dest_file = source_file
    # convert the image to a lower quality and save it in the destination directory
    # use given quality ,extension and format in config file
    if not os.path.isfile(os.path.join(destination, dest_file)):
        img = Image.open(source)
        if config['image_format'] == source_file.split('.')[-1]:
            # if input and output file formats are the same
            logger.info(
                f"Input and output file formats are the same. Converting {source_file} to {config['image_format']} format")
            img.save(os.path.join(destination, source_file),
                     format=config['image_format'], quality=config['quality'])
        else:
            # if input and output file formats are different
            # remove the extension from the source file and add the image_format from config file
            logger.info(
                f"Input and output file formats are different. Converting {source_file} to {config['image_format']} format")
            dest_file = source_file.split(
                '.')[0] + '.' + config['image_format']
            img.save(os.path.join(destination, dest_file),
                     format=config['image_format'], quality=config['quality'])
    # log the source and destination file sizes and compression ratio
    source_size = getTotalSize(source, source_path)
    destination_size = getTotalSize(os.path.join(
        destination, dest_file), destination)
    logger.info(f"Source file size: {source_size} MB")
    logger.info(f"Destination file size: {destination_size} MB")
    logger.info(
        f"Compression ratio: {(source_size)*100/destination_size} %")
    # return the destination file path
    return os.path.join(dest_file, destination)


def main():
    # get source folder ,destination folder read config file
    # call compressFileLossy function
    # do logging accordinglly
    source = sys.argv[1]
    destination = sys.argv[2]
    config_file = sys.argv[3] if len(sys.argv) == 4 else "config.yaml"
    config = readConfig(config_file)
    logger.info(f"Source folder: {source}")
    logger.info(f"Destination folder: {destination}")
    # get file list from source folder
    file_list = get_files(source, condition)
    # get the size of the source folder and log it
    source_size = getTotalSize(source, source)
    logger.info(f"Source folder size: {source_size} MB")
    # call compressFileLossy function
    compressFilesLossy(file_list, destination, config)
    # get the size of the destination folder and log it
    destination_size = getTotalSize(destination, destination)
    logger.info(f"Destination folder size: {destination_size} MB")
    # log the compression ratio
    logger.info(
        f"Compression ratio: {(source_size-destination_size)*100/source_size} %")


if __name__ == "__main__":
    main()
