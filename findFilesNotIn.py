#!/usr/bin/env python3

# find the files which are in current directory but not in the given directory as two command line arguments
# usage: python3 findFilesNotIn.py <directory1> <directory2>
import os        
import sys

def findIn(folder,file):
    # check if file is present in folder or subfolders
    for each_file in os.listdir(folder):
        if os.path.isdir(folder+each_file):
            if findIn(folder+each_file, file):
                return True
        else:
            if each_file==file:
                return True
    return False
def findFilesNotIn(checkWith, checkIn):
    for each_file in os.listdir(checkWith):
        if os.path.isdir(each_file):
            findFilesNotIn(each_file, checkIn)
        else:
            if not findIn(checkIn, each_file):
                print(f"{each_file} in folder {checkWith}")


findFilesNotIn(
    "/home/puneeth/Documents/record/Recordings/Call Recordings/", "/home/puneeth/Documents/record/Recordings/records/")
