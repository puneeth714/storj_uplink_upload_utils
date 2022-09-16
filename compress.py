#!/usr/bin/env python

import os
import sys
import cv2
from get_files import get_files
from PIL import Image, ImageFilter


def compressImg(file, file_path, destination):
    image = Image.open(file_path)
    #image = image.filter(ImageFilter.GaussianBlur(0.05))
    # Set quality= to the preferred quality.
    # 85 has no difference in low size (MB) files and 65 is the lowest reasonable number
    file = file.split(".")[0] + ".jpg"
    # denoise
    image = image.filter(ImageFilter.DETAIL)

    dest = os.path.join(destination, file)
    image.save(dest, optimize=True, quality=65)


def compressImgCV(file, file_path, destination):
    image = cv2.imread(file_path)
    dest = os.path.join(destination, file)
    # add guassian blur
    image = cv2.GaussianBlur(image, sigmaX=0.05, ksize=(0, 0), sigmaY=0.05)
    # denoise
    image = cv2.fastNlMeansDenoisingColored(image, None, 10, 10, 7, 21)
    cv2.imwrite(dest, image, [cv2.IMWRITE_JPEG_QUALITY, 65])


def compressImgConvert(file, file_path, destination):
    dest = os.path.join(destination, file)
    os.system(
        f"convert {file_path} -strip  -quality 65 -interlace JPEG -gaussian-blur 0.05  {dest}")


def isImage(file):
    if os.popen(f"file -b --mime-type {file}").read().split("/")[0] == "image":
        return True


def main():
    # get source and destinaton folders
    source = sys.argv[1]
    destination = sys.argv[2]
    os.chdir(source)
    # get all files in source folder with image type
    files = get_files(source, isImage)
    # create destination folder if not exists
    if not os.path.exists(destination):
        os.makedirs(destination)
    # compress and save files
    for file in files:
        compressImgConvert(file, os.path.join(source, file), destination)
        print(f"Compressed: {file}")


main()
