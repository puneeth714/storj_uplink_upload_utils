import os
import yaml
from loguru import logger


def readConfig(config):
    # read the config file
    with open(config, "r") as file:
        return yaml.safe_load(file)


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
        os.rename(os.path.join(source, file), os.path.join(
            source, file.replace(" ", "")))
        logger.debug(f"{file} renamed to {file.replace(' ', '')}")

    logger.debug(f"Done removing spaces from {source}")


def get_files(path, condition, isDir=False):
    # return all the files in the path recursively
    files_are = []
    if isDir:
        files_are.extend(file for file in os.listdir(
            path) if condition(os.path.join(path, file)))
        return files_are
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            file_path=os.path.join(root,name)
            if condition(file_path):
                files_are.append(file_path)
    return files_are


def delete_files(files: list, source: str):
    # delete the files in the list
    for file in files:
        path = os.path.join(source, file)
        os.remove(path)
        logger.info(f"{path} deleted")
