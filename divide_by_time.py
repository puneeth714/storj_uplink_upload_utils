#!/usr/bin/env python3

# divide file in a given directory by their creation/modification date and move them to folders named YYYY_MM

import os
import shutil
import re
import sys
from loguru import logger
from datetime import datetime
from get_files import *

# # set logger file to output.log
# logger.add("outputs1.log", format="{time} {level} {message}")


# paths = sys.argv
# if len(paths) == 1:
#     logger.error(
#         "usage \"python divide_by_time.py -force_use stat [source] [destination]\"")
#     exit(1)

# if paths[1] == "-force_use":
#     use = paths[2]
# source = paths[3]
# destination = paths[4]
# is_root = True


def get_date_stat(file):
    # DATE = os.stat(file)
    DATE = datetime.fromtimestamp(os.stat(file)[8])
    return f"{DATE.year}_{DATE.month}"


def file_type(file: str):
    # check if the file is an image format using file command
    return os.popen(f"file \"{file}\"").read().lower().find("image") != -1


def get_date_exif(file, regex=None):
    # \" is used because the file name may contain spaces which will make the command fail!
    meta = os.popen(f"exiftool \"{file}\"")
    DATE = meta.read()
    if regex == None:
        regex = "Date\/Time Original *: ([0-9:]{7})"
    date: str = re.findall(regex, DATE)
    if (date == []):
        return get_date_exif(file, regex="File Modification Date\/Time *: ([0-9:]{7})")
    date = date[0].replace(":", "_")
    meta.close()
    return date


def get_date(file, force=False):
    # if the file format is in the from FILETYPE_XXXX.Extension then call get_date_name else get_date_exif
    logger.debug(f"Getting date for {file}")
    if force != False and force == "stat":
        logger.debug(f"stat : {file}")
        return get_date_exif(file=file)
    regex = "(^[a-zA-Z_ ]*)([0-9_-]*[0-9])"
    check = re.findall(regex, file)
    if(check == [] or check[0][0] == '' or check[0][1] == ''):
        logger.debug(f"stat : {file}")
        get_date_stat(file=file)
    elif check[0][1].find("-") != -1:
        logger.debug(f"name : {file}")
        return get_date_name(check[0][1].replace("-", ""))
    return get_date_name(check[0][1])


def get_date_name(filetime):
    # The file name contains FILETYPEXXX*.EXTENSION XXX denots the timing of creation of the file

    return f"{filetime[:4]}_{filetime[4:6]}"


def move_file(file: str, date):
    return shutil.move(file, date)

# logger.debug the status of the file copied also



def screenshotCamera(file: str, path: str):
    # check if the given file is a screenshot or a camera photo based on the file name ,exif data and file type(Most screenshots are of type png)
    # files from folders named "Screenshots" or "Record" are considered screenshots
    # files from folders named "Camera" are considered camera photos
    # files from folders named "WhatsApp" are considered camera photos
    # Naming are not case sensitive
    real_file = os.path.join(path, file)
    file = file.lower()
    path = path.lower()
    if file.find("screenshot") != -1 or path.find("screenshots") != -1 or path.find("record") != -1 or file.find("record") != -1:
        return "Screenshots"
    elif file.find("camera") != -1 or path.find("camera") != -1:
        return "Camera"
    elif file.find("whatsapp") != -1 or path.find("whatsapp") != -1:
        return "Camera"
    # if exiftool returns have "exposure" in the output then the file is a camera photo or gps data is present in the file then the file is a camera photo
    elif os.popen(f"exiftool \"{real_file}\"").read().find("Exposure") != -1 or os.popen(f"exiftool \"{real_file}\"").read().find("GPS") != -1:
        return "Camera"
    # logger.error(f"Could not determine if {file} is a screenshot or a camera photo")
    # exit(1)
    logger.warning(
        f"Could not determine if {file} is a screenshot or a camera photo")
    return "Camera"


def main(source, destination, force=False):
    # change path to the directory you want to divide
    os.chdir(source)
    logger.debug(f"Current directory: {source}")
    files = os.listdir(source)
    global is_root
    if is_root:
        try:
            os.mkdir("Screenshots")
            os.mkdir("Camera")
        except FileExistsError:
            pass
        destination = os.path.join(destination, "Screenshots")
        is_root = False
    for file in files:
        if os.path.isdir(os.path.join(source, file)):
            main(os.path.join(source, file), destination, force)
            os.chdir("../")
            logger.debug(f"changed to {os.getcwd()}")
            continue
        # check if the file is an image format
        if not file_type(file):
            if ".mp4" in file.lower() or ".MOV" in file or ".avi" in file or ".mkv" in file or ".webm" in file:
                logger.debug(f"Video file: {file}")
            else:
                logger.debug(f"skipping {file}")
                continue

        if screenshotCamera(file, source) == "Screenshots":
            destination = destination.replace("Camera", "Screenshots")
        else:
            destination = destination.replace("Screenshots", "Camera")
        date = get_date(file, force)
        logger.debug(f"{file} created on {date}")
        # check if the folder is present
        sour = os.path.join(source, file)
        dest = os.path.join(destination, date)
        logger.debug(f"source: {sour} destination: {dest}")
        if os.path.isdir(dest) != True:
            if os.path.isdir(destination) != True:
                os.makedirs(destination)
            os.mkdir(dest)
            move_file(sour, dest)
            logger.debug(f"{file} copied to {date}")

        elif os.path.isfile(os.path.join(dest, file)) != True:
            move_file(sour, dest)
            logger.debug(f"{file} copied to {dest}")
        else:
            logger.info(f"{file} is already present in  {dest}")
    # removeSpaces(source=paths[1])
    logger.debug(f"Done removing spaces from {source}")
    logger.debug("Done!")


# main(source=source, destination=destination, force=use)
# # "/home/puneeth/Pictures/Mobile/tmp/"
