import os
import time
import json
import pathlib
import sys
import getpass

config_file = sys.argv[1]

with open(config_file) as f:
    data = json.load(f)

for i in data:
    value = data[i]
    if "$USER" in str(value):
        value = value.replace("$USER",getpass.getuser()) 
        data[i] = value


path_to_namenodes=data["path_to_namenodes"]
sync_period=data["sync_period"]


try:
    while 1:
        with open("exit.txt","r") as fp:
            A = fp.readlines()
            if A[0] =='1':
                exit()
        fp.close()
        time.sleep(sync_period)
        if not pathlib.Path(path_to_namenodes+"namenode.json").exists():
            with open(os.path.join(path_to_namenodes,"namenode.json"),'w') as firstfile, open(os.path.join(path_to_namenodes,"secondary.json"),'r') as secondfile:
                for line in secondfile:
                    firstfile.write(line)
        else:
            with open(os.path.join(path_to_namenodes,"namenode.json"),'r') as firstfile, open(os.path.join(path_to_namenodes,"secondary.json"),'w') as secondfile:
                for line in firstfile:
                    secondfile.write(line)
except Exception as e:
    exit()