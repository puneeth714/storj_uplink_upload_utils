#!/usr/bin/env python3

# get each folder in source, compress it and move it to destination folder (recursive)
# in the another process ,pipe the compressed file to queue and use this queue to upload to uplink storj
# read the config.yaml file to get the all the parameters
# if the file is already compressed then skip it
import os
import sys
import time
import yaml
from loguru import logger
from multiprocessing import Process, Queue, Pipe
from get_files import removeSpaces, get_files, delete_files
from compressLossless import *
from upload import parallelsRun, countFilesInDestination, makeCmd


# Get source ,destination,compression_type,uplink_path_type,uplink_path_destination,log_file
# parallel_upload
def makePath(path):
    # replace environment variables in the path with their values
    return os.path.expandvars(path)

def losslesscompression(source, destination, sender, compression_type="7z"):
    global_start_time = time.time()
    # # Get source to compress
    # source = sys.argv[1]
    # # Get destination source
    # destination = sys.argv[2]
    # # Get compression type
    # if len(sys.argv) > 3:
    #     if sys.argv[3] in ["7z", "tar.gz", "tar.bz2", "tar.xz", "tar"]:
    #         compression_type = sys.argv[3]
    #     else:
    #         logger.warning("Invalid compression type")
    #         logger.warning(
    #             "Valid compression types are: 7z, tar.gz, tar.bz2, tar.xz, tar")
    #         logger.warning("Defaulting to 7z")
    #         compression_type = "7z"
    # Get list of files in source
    files = get_files(source, condition, isDir=True)
    logger.info(f"Compressing {len(files)} files")
    # Get total size of files
    total_size = getTotalSize(files, source)
    # Loop through files
    output_files = []
    for file in files:
        # Compress file
        logger.info(f"Compressing {file}")
        start = time.time()
        # append only the file name to the output_files list
        output = archivesource(
            os.path.join(source, file), destination, compression_type).split("/")[-1]
        # send the compressed file to the Pipe
        sender.send(output)
        output_files.append(output)
        # log time taken
        logger.info(
            f"Compressed {file} to {output} in {round(time.time() - start)} seconds")
        # print original size and compressed size in MB with ratio in %
        logger.info(
            f"Original size: {getTotalSize([file], source) / 1024 / 1024} MB")
        logger.info(
            f"Compressed size: {getTotalSize([output], destination) / 1024 / 1024} MB")
        logger.info(
            f"Compression ratio: {getTotalSize([output], destination) / getTotalSize([file], source) * 100}%")
    sender.send("Finished")
    # get total size of compressed files
    total_size_compressed = getTotalSize(output_files, destination)
    # print total size of original files and total size of compressed files in MB with ratio in %
    logger.info(
        f"Total original size: {round(total_size / 1024 / 1024, 2)} MB")
    logger.info(
        f"Total compressed size: {round(total_size_compressed / 1024 / 1024, 2)} MB")
    logger.info(
        f"Total compression ratio: {round(total_size_compressed / total_size * 100, 2)} %")
    # print total time taken
    logger.info(f"Total time taken: {time.time() - global_start_time} seconds")


def sendFilesUplink(source, destination, parallels, receiver, uplink_path_type="sj://"):
    # receiver is the pipe receiver
    # wait untill the length of receiver is equal to the length of parallels
    files = []
    while True:
        while len(files) != parallels:
            recieve = receiver.recv()
            logger.info(f"Received {recieve}")
            if recieve == "Finished":
                logger.info("Finished receiving files")
                return
            files.append(recieve)
        # upload the files to uplink
        count = countFilesInDestination(destination, uplink_path_type)
        print(f"files in destination: {count}")
        # make the cmd to upload the files
        cmd = makeCmd(files, source, destination, uplink_path_type)
        # call the parallelsRun function to upload the files to uplink
        parallelsRun(cmd)
        # delete the files from the destination
        delete_files(files, source)
        # remove the files from the list
        files = []


def onGoCompressionUpload(config_file_path):
    onGoCompression_start_time = time.time()
    # get the parameters from config.yaml
    logger.info("Reading config.yaml file")
    with open(config_file_path, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    source = makePath(config["source"])
    destination = makePath(config["destination"])
    compression_type = config["compression_type"]
    uplink_path_type = config["uplink_path_type"]
    uplink_path_destination = makePath(config["uplink_path_destination"])
    log_file = makePath(config["log_file"])
    parallel_upload = config["parallel_upload"]
    parallel_upload_wait_time = config["parallel_upload_wait_time"]
    # add logger for logger.info to file counts.log
    logger.add(log_file, format="{time} {level} {message}", level="INFO")

    # remove spaces from the file name recursively including subdirectories
    # removeSpaces(source)
    # change the current working directory to source
    # os.chdir(source)
    # losslesscompression(source, destination, compression_type)


if __name__ == "__main__":
    config_file_path = sys.argv[1]
    onGoCompression_start_time = time.time()

    # get the parameters from config.yaml
    logger.info("Reading config.yaml file")
    with open(config_file_path, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    source = config["source"]
    destination = config["destination"]
    compression_type = config["compression_type"]
    uplink_path_type = config["uplink_path_type"]
    uplink_path_destination = config["uplink_path_destination"]
    log_file = config["log_file"]
    parallel_upload = config["parallel_upload"]
    parallel_upload_wait_time = config["parallel_upload_wait_time"]
    # add logger for logger.info to file counts.log
    logger.add(log_file, format="{time} {level} {message}", level="INFO")

    # remove spaces from the file name recursively including subdirectories
    # removeSpaces(source)
    # change the current working directory to source
    # os.chdir(source)
    # losslesscompression(source, destination, compression_type)

    # create pipe
    logger.info("Creating pipe")
    sender, receiver = Pipe()
    # create process for losslesscompression and sendFilesUplink
    logger.info("Creating process")
    p1 = Process(target=losslesscompression, args=(
        source, destination, sender, compression_type))
    p2 = Process(target=sendFilesUplink, args=(
        destination, uplink_path_destination, parallel_upload, receiver, uplink_path_type))
    # start process
    logger.info("Starting process")
    p1.start()
    p2.start()
    # join process
    p1.join()
    p2.join()
    # close pipe
    logger.info("Closing pipe")
    sender.close()
    receiver.close()
    # print total time taken
    logger.info(
        f"Total time taken: {time.time() - onGoCompression_start_time} seconds")
