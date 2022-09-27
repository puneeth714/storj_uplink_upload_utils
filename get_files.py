import os
import time
import yaml
import json
import pandas as pd
import sqlite3
from loguru import logger


def readConfig(config):
    # read the config file
    with open(config, "r") as file:
        return yaml.safe_load(file)


def replace(file_name: str):
    # replace all the speciall characteres other than _ with _
    return file_name.replace(" ", "_").replace("(", "_").replace(")", "_").replace("[", "_").replace("]", "_").replace("{", "_").replace("}", "_").replace("'", "_").replace('"', "_").replace(",", "_").replace(":", "_").replace(";", "_").replace("!", "_").replace("?", "_").replace("&", "_").replace("$", "_").replace("#", "_").replace("@", "_").replace("%", "_").replace("^", "_").replace("*", "_").replace("+", "_").replace("=", "_").replace("|", "_").replace("\\", "_").replace("/", "_").replace("<", "_").replace(">", "_").replace("`", "_").replace("~", "_")


def removeSpaces(source: str):
    # remove spaces from the file name recursively including subdirectories
    os.chdir(source)
    files = os.listdir(source)
    logger.debug(f"Current directory: {source}")

    for file in files:
        if os.path.isdir(os.path.join(source, file)):
            removeSpaces(os.path.join(source, file))
            os.chdir("../")
            logger.debug(f"changed to {os.getcwd()}")
            continue
        new_file_name = replace(file)
        os.rename(os.path.join(source, file), os.path.join(
            source, new_file_name))
        logger.debug(f"{file} renamed to {new_file_name}")

    logger.debug(f"Done removing spaces from {source}")


def removeSpacesDir(source: str):
    # remove spaces from the directory name recursively including subdirectories
    for root, dirs, _ in os.walk(source, topdown=False):
        for dirName in dirs:
            new_dir_name = replace(dirName)
            os.rename(os.path.join(root, dirName), os.path.join(
                root, new_dir_name))
            logger.debug(f"{dirName} renamed to {new_dir_name}")


def get_metadata(folder: str):
    # get the metadata of the file and append it to a json file using exiftool
    metadata = os.popen(f"exiftool -j {folder} -r").read()
    metadata = json.loads(metadata)
    # convert the json file to sql files table file_data
    convertJSONtoSQL(metadata, "file_data")


def convertJSONtoSQL(data: dict, table_name: str):
    # convert the list of dictionaries to a pandas dataframe and then to a sql file
    # table with index of keys
    df = pd.DataFrame(data)
    # convert the dataframe to a sql file
    conn = sqlite3.connect("file_data.db")
    df.to_sql(table_name, conn, if_exists="replace", index=False)


def get_files(path, condition, isDir=False):
    # return all the files in the path recursively
    get_files_start = time.time()
    files_are = []
    if isDir:
        files_are.extend(file for file in os.listdir(
            path) if condition(os.path.join(path, file)))
        return files_are
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            file_path = os.path.join(root, name)
            if condition(file_path):
                files_are.append(file_path)
    logger.info(f"Time taken to get files: {time.time()-get_files_start}")
    return files_are


def get_files_recurse(path, condition, isDir=False):
    # return all the files in the path recursively
    files_are = []
    if isDir:
        files_are.extend(file for file in os.listdir(
            path) if condition(os.path.join(path, file)))
        return files_are
    for file in os.listdir(path):
        file_path = os.path.join(path, file)
        if os.path.isdir(file_path):
            files_are.extend(get_files_recurse(file_path, condition, isDir))
        elif condition(file_path):
            files_are.append(file_path)
    return files_are


def get_files_scandir(path, condition, isDir=False):
    # return all the files in the path recursively
    files_are = []
    if isDir:
        files_are.extend(file for file in os.listdir(
            path) if condition(os.path.join(path, file)))
        return files_are
    for entry in os.scandir(path):
        if entry.is_dir():
            files_are.extend(get_files_scandir(entry.path, condition, isDir))
        elif condition(entry.path):
            files_are.append(entry.path)
    return files_are


def get_files_notIn(source, destinataion):
    # get the files in source but not in destination
    # using find command
    # get files in source sort get files in destination sort and find the difference
    source_files = get_files_custom(source, "-printf '%f\n")
    destination_files = get_files_custom(destinataion, "-printf '%f\n")
    return set(source_files), set(destination_files)


def get_files_custom(path, params=None):
    # just get the files recursively
    # using find command
    if params != None:
        return os.popen(f"find {path} -type f  {params}").read().split("\n")
    return os.popen(f"find {path} -type f").read().split("\n")


def delete_files(files: list, source: str):
    # delete the files in the list
    for file in files:
        path = os.path.join(source, file)
        os.remove(path)
        logger.info(f"{path} deleted")


# get_metadata("/home/puneeth/Pictures/Files")
