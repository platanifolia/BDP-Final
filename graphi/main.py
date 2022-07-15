# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import csv


def graphi():
    nodes = [];
    labels = {};
    labelsnodes = [];
    edges = {};
    f = open("labels", encoding="utf-8")
    while True:
        line = f.readline();
        if not line:
            break;
        s = line.split("\t");
        nodes.append(s[0]);
        labels[s[0]] = s[1];
        if s[1] not in labelsnodes:
            labelsnodes.append(s[1]);
    f.close();
    f = open("graph", encoding="utf-8")
    while True:
        line = f.readline();
        if not line:
            break;
        l = [];
        s = line.split("\t");
        s2 = s[1].split("|");
        for i in s2:
            s3 = i.split(",");
            l.append((s3[0], s3[1]));
        edges[s[0]] = l;
    f.close();
    with open("edges.csv", "w", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Source", "Target", "Weight"]);
        for key in edges.keys():
            for value in edges[key]:
                writer.writerow([key, value[0], value[1]]);
    with open("labels.csv", "w", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Id", "Source", "Label"])
        for key in labels.keys():
            writer.writerow([key, key, labels[key]]);


graphi();

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
