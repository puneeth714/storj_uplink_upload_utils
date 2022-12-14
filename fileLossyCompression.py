#!/usr/bin/env python

from genericpath import isfile
import os
import sys
import time
import yaml
import sqlite3
from loguru import logger
from PIL import Image
import subprocess
from PIL import UnidentifiedImageError
from get_files import get_files, get_files_custom, readConfig, removeSpaces, get_files_recurse, get_files_scandir

# log to output.log file
logger.add("output.log", format="{time} {level} {message}", level="INFO")


def condition(file):
    # return if the given file is an image/video
    # using the file command
    # thier should be virtually no condition because file types are handled in the respective functions
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
        return round(int(size)/1024, 2)
    return round(os.path.getsize(os.path.join(directory, file))/(1024*1024), 2)

# it will get list of file paths,destination directory and config file


def addTodatabase(fileName, filePath, destinationName, destinationPath, compressionRatio, connection: sqlite3.Connection, table):
    # add the file name, file path, destination name, destination path and compression ratio to the database
    # create table if not exists
    # add the data to the table with fileName as the primary key
    query = f"CREATE TABLE IF NOT EXISTS {table} (fileName TEXT PRIMARY KEY, filePath TEXT, destinationName TEXT, destinationPath TEXT, compressionRatio REAL)"
    connection.execute(query)
    # insert only if the given fileName is not already in the database
    query = f"INSERT OR IGNORE INTO {table} VALUES (\"{fileName}\",\"{filePath}\",\"{destinationName}\",\"{destinationPath}\",{compressionRatio})"
    # query = f"INSERT INTO {table} VALUES (\'{fileName}\',\'{filePath}\',\'{destinationName}\',\'{destinationPath}\',{compressionRatio})"
    logger.info(query)
    connection.execute(query)


def copyFile(sourceFile, destinationFile):
    # copy the file to the destination directory
    cmd = f"cp  -p {sourceFile}   {destinationFile}"
    runs = subprocess.run(cmd, shell=True)
    if runs.returncode != 0:
        logger.error("Error in copy command")
        return False
    # log the source and destination file sizes and compression ratio
    logger.info(f"Done using copy command {cmd}")
    return True


def compressionAverage(avg, now):
    # calculate the average compression ratio
    return now if avg == 0 else (avg+now)/2


def customCheckConditionFFMPEG(fileName: str, config: dict) -> bool:
    # check if coustom condition is in fileName
    return config["custom_file"] in fileName.lower()


def customParamsFFMPEG(config: dict) -> str:
    # return the custom parameters for ffmpeg command
    # use custom_params in config file
    # which is in list format convert to string format with spaces
    return " ".join(config["custom_params"])


def compressFilesLossy(files, root, destination, config, connection: sqlite3.Connection, table):
    # check if destination directory exists and create if not
    if not os.path.exists(destination):
        os.makedirs(destination)
    # compress each file in the list
    total_count = len(files)
    average_compress_ratio = 0
    for file in files:
        # check if the file is an image or a video
        file_type = checkImageVideo(file)
        # if the file is an image
        if file_type == "image":
            # compress the image
            source_file, source_path, destination_folder, dest_file, compression = compressPIL(
                file, root, destination, config)
        # if the file is a video
        elif file_type == "video":
            # compress the video
            source_file, source_path, destination_folder, dest_file, compression = compressFFMPEG(
                file, root, destination, config)
        # if the file is neither an image nor a video
        else:
            source_file, source_path, destination_folder, dest_file, compression = os.path.basename(
                file), os.path.dirname(file), destination, os.path.basename(file), 1
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
            copyFile(file, destination)
            # log the file is copied
            logger.info("File is copied")
        average_compress_ratio = compressionAverage(
            average_compress_ratio, compression)
        # write the data to the database
        addTodatabase(source_file, source_path, dest_file,
                      destination_folder, compression, connection, table)
        # show count remaining in red colour
        logger.opt(ansi=True).info(
            "<blue>{}</blue> files remaining", total_count)
        total_count -= 1
        # log the average compression ratio
        logger.info(f"Average compression ratio: {average_compress_ratio}")
    # log the total number of files compressed
    logger.info(f"Total files compressed: {len(files)}")
    # log the average compression ratio
    logger.info(f"Average compression ratio Overall: {average_compress_ratio}")


def compressFFMPEG(source: str, root: str, destination: str, config: dict):
    # compressFileLossy will call this function if the source is a video file
    # convet the video to a lower resolution and lower bitrate with given codec in config file
    # and save it in the destination directory
    # source contains the path to the source file and file name
    # store file name in source_file variable and store the file path in source_path
    source_file = os.path.basename(source)
    source_path = os.path.dirname(source)
    dest_file = source.replace(root, "")
    try:
        os.makedirs(os.path.join(destination, os.path.dirname(dest_file)))
    except FileExistsError:
        logger.info("Directory already exists")
    # convert the video to a lower resolution and lower bitrate with given codec in config file
    # and save it in the destination directory
    # use video_format in config file
    # check if input and output file formats are the same
    if not os.path.isfile(os.path.join(destination, dest_file)):
        if os.path.isfile(os.path.join(destination, dest_file[:dest_file.rfind(".")+1]+config["video_format"])) and not customCheckConditionFFMPEG(source_file, config):
            logger.info(f"Already converted to {config['video_format']}")
            dest_file = dest_file[:dest_file.rfind(
                ".")+1]+config["video_format"]
        else:
            if config['video_format'] == source_file.split('.')[-1]:
                # if input and output file formats are the same
                logger.info(
                    f"Input and output file formats are the same. Converting {source_file} to {config['video_format']} format")
            else:
                # if input and output file formats are different
                # remove the extension from the source file and add the video_format from config file
                logger.info(
                    f"Input and output file formats are different. Converting {source_file} to {config['video_format']} format")
                dest_file = dest_file.split(
                    '.')[0] + '.' + config['video_format']
            if customCheckConditionFFMPEG(source_file, config):
                logger.info(
                    "Custom condition is true. Using custom parameters")
                cmd = f"ffmpeg -v error -n -i {source} {customParamsFFMPEG(config)} \'{os.path.join(destination, dest_file)}\'"
                logger.info(f"Using custom command {cmd}")
            else:
                cmd = f"ffmpeg -v error -i {source} -c:v {config['codec']} -b:v {config['bitrate']}  \'{os.path.join(destination,dest_file)}\'"
            subprocess.run(cmd, shell=True)
    # log the source and destination file sizes and compression ratio
    source_size = getTotalSize(source, source_path)
    destination_size = getTotalSize(os.path.join(
        destination, dest_file), destination)
    compression = round((source_size)/destination_size, 2)
    logger.info(f"Source file size: {source_size} MB")
    logger.info(f"Destination file size: {destination_size} MB")
    logger.info(
        f"Compression ratio: {compression} ")
    # return
    return source_file, source_path, destination, dest_file, compression


def compress_convert(sourceFile, destinationFile, config):
    # use convert command to convert the image to a lower quality and given format
    # use image_format in and quality in config file
    cmd = f"convert {sourceFile} -quality {config['quality']} {destinationFile}"
    runs = subprocess.run(cmd, shell=True)
    if runs.returncode != 0:
        logger.error("Error in convert command")
        return False
    # log the source and destination file sizes and compression ratio
    logger.info(f"Done using convert command {cmd}")
    return True


def compressPIL(source: str, root: str, destination: str, config: dict):
    # compressFileLossy will call this function if the source is an image file
    # convert the image to a lower quality and save it in the destination directory
    # use given quality ,extension and format in config file
    # source contains the path to the source file and file name
    # store file name in source_file variable and store the file path in source_path
    source_file = os.path.basename(source)
    source_path = os.path.dirname(source)
    # dest_file is file with deestination file,removing the root from source and source_file
    dest_file = source.replace(root, "")
    try:
        os.makedirs(os.path.join(destination, os.path.dirname(dest_file)))
    except FileExistsError:
        logger.debug("Directory already exists")

    # convert the image to a lower quality and save it in the destination directory
    # use given quality ,extension and format in config file
    if not os.path.isfile(os.path.join(destination, dest_file)):
        if os.path.isfile(os.path.join(destination, dest_file[:dest_file.rfind(".")+1]+config["image_format"])):
            logger.info(
                f"Already converted to {config['image_format']} format")
            dest_file = dest_file[:dest_file.rfind(
                ".")+1]+config["image_format"]
        else:
            if source_file.split('.')[-1] in [config['image_format'], "jpg"]:
                # if input and output file formats are the same
                logger.info(
                    f"Input and output file formats are the same. Converting {source_file} to {config['image_format']} format")
            else:
                # if input and output file formats are different
                # remove the extension from the source file and add the image_format from config file
                logger.info(
                    f"Input and output file formats are different. Converting {source_file} to {config['image_format']} format")
                dest_file = dest_file.split(
                    '.')[0] + '.' + config['image_format']
            try:
                img = Image.open(source)
                img.save(os.path.join(destination, dest_file),
                         format=config['image_format'], quality=config['quality'])
            except Exception as e:
                logger.warning(
                    f"Unidentified image file {source_file}\/ or format not supported")
                logger.warning(f"Error is {e}\n\n\n")
                logger.warning("using convert command")
                if compress_convert(source, os.path.join(destination, dest_file), config) and os.path.isfile(os.path.join(destination, dest_file)):
                    logger.info("Done using convert command")
                else:
                    logger.error("Error in convert command")
                    # copy the file to the destination directory as it is
                    # here we should use source_file because we want to copy the file with the extension as it is not changing extension
                    # but dest_file is the file name which extension might be changed
                    copyFile(source, os.path.join(destination, os.path.dirname(source_file)))
    # log the source and destination file sizes and compression ratio
    source_size = getTotalSize(source, source_path)
    print(f"{os.path.join(destination, dest_file)} , {destination}")
    destination_size = getTotalSize(
        os.path.join(destination, dest_file), destination)
    compression = round((source_size)/destination_size, 2)
    logger.info(f"Source file size: {source_size} MB")
    logger.info(f"Destination file size: {destination_size} MB")
    logger.info(
        f"Compression ratio: {compression}")
    # return
    return source_file, source_path, destination, dest_file, compression


def main():  # sourcery skip: do-not-use-bare-except
    # get source folder ,destination folder read config file
    # call compressFileLossy function
    # do logging accordinglly
    source = sys.argv[1]
    destination = sys.argv[2]
    config_file = sys.argv[3] if len(sys.argv) == 4 else "config.yaml"
    config = readConfig(config_file)
    logger.info(f"Source folder: {source}")
    logger.info(f"Destination folder: {destination}")
    # replace the source names with removing special characters
    # removeSpaces(source)
    # get file list from source folder
    get_files_start = time.time()
    file_list = get_files_custom(source)
    # print(file_list)
    logger.info(
        f"Time taken to get file list: {time.time()-get_files_start} seconds")
    # get the size of the source folder and log it
    logger.info("Getting the size of the source folder")
    source_size = getTotalSize(source, source)
    logger.info(f"Source folder size: {source_size} MB")
    # connect to the database
    conn = sqlite3.connect(config['database'])
    # call compressFileLossy function
    try:
        compressFilesLossy(file_list, source, destination,
                           config, conn, table=config['files_table'])
        # get the size of the destination folder and log it
        destination_size = getTotalSize(destination, destination)
        logger.info(f"Source folder size: {source_size} MB")
        logger.info(f"Destination folder size: {destination_size} MB")
        # log the compression ratio
        logger.info(
            f"Compression ratio: {round((source_size)/destination_size,2)}")
        # commit the changes to the database
        conn.commit()
        # close the connection to the database
        conn.close()
    # except any exception or keyboard interrupt
    # log the exception
    except Exception as e:
        logger.exception(f"Exception occured: {e}")

        # if the user presses ctrl+c
        # commit the changes to the database
        conn.commit()
        # close the connection to the database
        conn.close()
        # exit the program
        sys.exit(0)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt")
        # commit the changes to the database
        conn.commit()
        # close the connection to the database
        conn.close()
        # exit the program
        sys.exit(0)


if __name__ == "__main__":
    main()
