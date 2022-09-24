#!/usr/bin/python3

import os
import re
# copy files
import shutil
from plotly.graph_objs import Bar, Layout
from plotly import offline

persons = {}


def find_person(file):
    regex = "([a-zA-Z+0-9 ]*)"
    return re.findall(regex, file)[0]


def findAllPersons(persons: dict):
    for each_file in os.listdir():
        if os.path.isdir(each_file):
            os.chdir(each_file)
            findAllPersons(persons)
            os.chdir("..")
        person = find_person(each_file)
        if person in persons.keys():
            persons[person] += 1
        else:
            persons[person] = 1


findAllPersons(persons)

# copy files with the same key to a folder with name of key


def copyFiles(persons: dict):
    for each_file in os.listdir():
        if os.path.isdir(each_file):
            if each_file == "records":
                continue
            os.chdir(each_file)
            copyFiles(persons)
            os.chdir("..")
        else:
            person = find_person(each_file)
            if person in persons.keys():
                print(f"Copying \"{each_file}\" to \"../records/{person}\"")
                # create a folder with name of person if not exists
                if not os.path.exists(f"../records/{person}"):
                    os.mkdir(f"../records/{person}")
                shutil.copy2(each_file, f"../records/{person}")
# copyFiles(persons)

# copy files with the same key to a folder with name of key


multiples = []


def movFiles(persons: dict):
    for each_file in os.listdir():
        if os.path.isdir(each_file):
            if each_file == "records":
                continue
            os.chdir(each_file)
            movFiles(persons)
            os.chdir("..")
        elif ".py" in each_file:
            continue
        else:
            person = find_person(each_file)
            if person in persons.keys():
                print(f"moving \"{each_file}\" to \"../records/{person}\"")
                # create a folder with name of person if not exists
                if not os.path.exists(f"../records/{person}"):
                    os.mkdir(f"../records/{person}")
                # check if the file is already present in the folder with name of person and add it to multiples list
                if os.path.exists(f"../records/{person}/{each_file}"):
                    multiples.append(f"../records/{person}/{each_file}")
                else:
                    shutil.move(each_file, f"../records/{person}")


# movFiles(persons)

persons = sorted(((v, k) for k, v in persons.items()), reverse=True)

# for each in persons:
#     print(f"{each[1]} : {each[0]}")
# print(f"Multiples: {multiples}")
# plot the graph for the number of files for each person


def plotGraph(persons: list):
    x = []
    y = []
    for each in persons:
        x.append(each[1])
        y.append(each[0])
    data = [Bar(x=x, y=y)]
    x_axis_config = {"title": "Person"}
    y_axis_config = {"title": "Number of files"}
    my_layout = Layout(title="Number of files for each person",
                       xaxis=x_axis_config, yaxis=y_axis_config)
    offline.plot({"data": data, "layout": my_layout}, filename="persons.html")


plotGraph(persons)
