# -*- coding: utf-8 -*-
"""
Created on Sun May 24 00:28:06 2020

@author: Lenovo
"""

import csv
import pandas as pd


file = r"E:\java1\sparkBigProject\trainend\src\main\resources\tmdb_5000_movies.csv"
fb = open(file, encoding='ISO-8859-1')
reader = csv.reader(fb, delimiter=',')
l = []
#for i in fb.readlines():
#    l.append(str(i).split(",(?=([^\']*\'[^\']*\')*[^\']*$)"))
for i in reader:
    l.append(i)


df = pd.DataFrame(l)
df = df.loc[df.iloc[:, 21] == '']
df = df.loc[df.iloc[:, 20] != '']
df = df.iloc[:, :21]
df.to_csv(r'E:\java1\sparkBigProject\trainend\src\main\resources\tmdb.csv', index=False)
