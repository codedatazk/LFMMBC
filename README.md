# LFMMBC
# README

1  Install conda Environment.
​		LFMMBC.yml

2  import csv or xlsx Files
python LFMMBC.py import -f (filePath) -t (TableNames)

3 Initialize table datasets. 
python LFMMBC.py genpcode -t (TableNames)

4 Read/Write/Update/Inspect
Read：
python LFMMBC.py read -t (TableNames) -n (The nTh record)

Write:
python LFMMBC.py write -t (TableNames)

Update:
python LFMMBC.py write -t (TableNames) -n (The nTh record)

Inspect：
python LFMMBC.py inspect -t (TableNames)
