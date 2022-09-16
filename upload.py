#!/usr/bin/env python
import os
import sys
from multiprocessing import Process
import subprocess
import asyncio
from loguru import logger
import time
import json

# upload files in source folder to destination folder in stroj.io using uplink command line tool,and monitor the progress
# usage: python upload.py source_folder destination_folder uplink_path_type
# uplink_path_type: sj:// default.

# # add logger for logger.info to file counts.log
# logger.add("counts.log", format="{time} {level} {message}", level="INFO")


def upload(source, destination, uplink_path_type):
    # print count of uploaded files
    # upload the files in source folder to destination folder in stroj.io using uplink command line tool,and monitor the progress
    # check if file is in uploaded.txt, if not, upload it
    try:
        count = 0
        skip = 0
        uploaded = open("uploaded.txt", "r").read().split()
        print(f"uploading {source} to {destination}")
        for root, dirs, files in os.walk(source):
            for f in files:
                if f in uploaded:
                    print(f"file {f} is already uploaded \n Skipping")
                    skip += 1
                    continue
                source_file = os.path.join(root, f)
                # send in the source file
                destination_file = os.path.join(
                    destination, root.replace(source, ""), f)
                cmd = f"/home/$USER/programmes/uplink cp \"{source_file}\" \"{uplink_path_type}{destination_file}\""
                print(cmd)
                subprocess.run(cmd, shell=True)
                uploaded.append(f)
                count += 1
                print(f"uploaded {count} files\n\n")
                print(f"skipped {skip} files")
                logger.info(f"uploaded {count} files\n\n")
                logger.info(f"skipped {skip} files")
                logger.info(
                    f"remaining files: {countFilesInSource(source) - count}")
                time_taken = time.time() - start_time
                print(f"time taken: {time_taken}")
                start_time = time.time()
    except KeyboardInterrupt:
        file = open("uploaded.txt", "a+")
        for each in uploaded:
            file.write(each+"\n")
        return "Interupt"


def uploadParallelSystem(cmd: str):
    # run the commands in cmd in parallel
    return os.system(cmd)


def parallelsRun(cmd: list):
    # run the commands in cmd in parallel
    start_time = time.time()

    processes = []
    for each in cmd:
        p = Process(target=uploadParallelSystem, args=(each,))
        p.start()
        processes.append(p)
    for p in processes:
        p.join()
    # print time taken in seconds
    logger.debug(f"time taken: {time.time() - start_time}")


def findUplink(path, uplink_path_type):
    # get all files in path recursively in uplink
    # return the list only the files not the path
    cmd = f"/home/$USER/programmes/uplink ls {uplink_path_type}{path} --recursive -o json"
    name = os.popen(cmd).read().split("\n")
    files = []
    for each in name:
        if each == "":
            continue
        files.append(json.loads(each)["key"].split("/")[-1])
    return files


def uploadLog(files: list):
    # log the files going to be uploaded with its size
    for each in files:
        file = each.split("\"")[1]
        logger.info(f"uploading {file} with size {int(os.path.getsize(file))/(1024*1024)}M")


def uploadParallel(source, destination, parallels, uplink_path_type):
    # # upload the files in source folder to destination folder in stroj.io using uplink command line tool,and monitor the progress
    # check if file is in uploaded.txt, if not, upload it
    # upload files with multiple processes based on the values of parallels
    files_cmd = []
    # files_cmd will store the commands to be run in parallel each value is a list
    # each list contains the commands to be run in parallel
    total_files = countFilesInSource(source)

    tmp_cmd = []
    skip = 0
    os.chdir(source)
    uploaded = findUplink(destination, uplink_path_type)
    logger.info(f"uploading {source} to {destination}")
    for root, dirs, files in os.walk(source):
        for f in files:
            if f in uploaded:
                logger.warning(f"file {f} is already uploaded Skipping")
                skip += 1
                continue
            source_file = os.path.join(root, f)
            # send in the source file
            destination_file = os.path.join(
                destination, root.replace(source, ""), f)
            cmd = f"/home/$USER/programmes/uplink cp  \"{source_file}\" \"{uplink_path_type}{destination_file}\""
            tmp_cmd.append(cmd)
            if len(tmp_cmd) == parallels:
                files_cmd.append(tmp_cmd)
                tmp_cmd = []
    if len(tmp_cmd) > 0:
        files_cmd.append(tmp_cmd)
    logger.info(f"total files: {total_files}")
    logger.info(f"total files to be uploaded: {total_files - skip}")
    logger.info(f"total files to be skipped: {skip}\n")
    try:
        for each in files_cmd:
            uploadLog(each)
            parallelsRun(each)
            # appned the files to uploaded.txt
            for file in each:
                uploaded.append(file[file.rindex("/")+1:])
            logger.info(f"uploaded {parallels} files")
            logger.info(f"remaining files: {total_files-parallels}\n")
            # print remaining files
            total_files -= parallels
    except KeyboardInterrupt:
        file = open("uploaded.txt", "a+")
        for each in uploaded:
            file.write(each+"\n")
        return "Interrupt"


async def uploadParallelAsync(cmd: list):
    # run the commands in cmd in parallel
    processes = []
    for each in cmd:
        # run the command in parallel using asyncio
        p = await asyncio.create_subprocess_shell(each)
        processes.append(p)
    for p in processes:
        await p.wait()


def uploadParallelCmd(source, destination, parallels, uplink_path_type):
    # first move the files into a new folder
    # now upload each folder using --recursive of uplink with parallels (-t parameter)
    # after each upload, check the number of files in destination folder and count the uploads and skips
    count = 0
    os.chdir(source)
    if not os.path.isdir("tmp"):
        os.mkdir("tmp")
    for each in os.listdir(source):
        if os.path.isfile(each):
            os.rename(each, f"tmp/{each}")
    folders = os.listdir()
    for each in folders:
        cmd = f"/home/$USER/programmes/uplink cp --recursive --progress  \"{source}\" \"{uplink_path_type}{destination}\" -t {parallels}"
        print(cmd)
        subprocess.run(cmd, shell=True)
        count += countFilesInSource(each)
        print(f"uploaded {count} files\n\n")


def countFilesInSource(source):
    count = 0
    for root, dirs, files in os.walk(source):
        for _ in files:
            count += 1
    return count


def countFilesInDestination(destination, uplink_path_type):
    # destination might have subfolders, so we need to count all files in destination folder
    cmd = f"/home/$USER/programmes/uplink ls {uplink_path_type}{destination} --recursive | wc -l"
    print(cmd)
    count = os.popen(cmd).read()
    return int(count)-1


def main(source,destination,parallels,uplink_path_type="sj://"):
    # source = sys.argv[1]
    # destination = sys.argv[2]
    # parallels = int(sys.argv[3])
    # uplink_path_type = sys.argv[4] if len(sys.argv) > 4 else "sj://"
    total = countFilesInSource(source)
    print(f"total files: {total}")
    count = countFilesInDestination(destination, uplink_path_type)
    print(f"files in destination: {count}")
    print(uploadParallel(source, destination, parallels, uplink_path_type))


# if __name__ == "__main__":
#     asyncio.run(main())
# main()
