import os
import time
import json
import pathlib
from datetime import datetime
import sys
import getpass

config_file=sys.argv[1]
with open(config_file) as f:
    data = json.load(f)

for i in data:
    value = data[i]
    if "$USER" in str(value):
        value = value.replace("$USER",getpass.getuser()) 
        data[i] = value
# exit()

path_to_namenodes=data["path_to_namenodes"]
sync_period=data["sync_period"]
check_point_time = 2
namenode_log_path=data["namenode_log_path"]
fs_path=data["fs_path"]
namenode_checkpoints=data["namenode_checkpoints"]


try:
    while 1:
        with open("exit.txt","r") as fp:
            A = fp.readlines()
            if A[0] =='1':
                exit()
        fp.close()
        time.sleep(2)
        if not pathlib.Path(path_to_namenodes+"namenode.json").exists():
            print(1)
            if not pathlib.Path(path_to_namenodes+"secondary.json").exists():
                try:
                    with open(os.path.join(path_to_namenodes,"namenode.json"), 'w') as fp:
                        dic={fs_path:{}}
                        json.dump(dic,fp)
                    with open(os.path.join(namenode_log_path,"namenode_log.txt"), 'w') as fp:
                        fp.write("Name node was created.""\n")    
                        fp.close()
                except:
                    print("Unable to create Namenode.")
                try:
                    with open(os.path.join(path_to_namenodes,"secondary.json"), 'w') as fp:
                        dic={fs_path:{}}
                        json.dump(dic,fp)
                        fp.close()

                    with open(os.path.join(namenode_log_path,"namenode_log.txt"), 'a') as fp:
                        fp.write("Secondary Namenode was created.""\n")    
                        fp.close()
                except:
                    print("Unable to create Namenode.")
            else:
                with open(os.path.join(path_to_namenodes,"namenode.json"),'w') as firstfile, open(os.path.join(path_to_namenodes,"secondary.json"),'r') as secondfile:
                    for line in secondfile:
                        firstfile.write(line)
                        fp.close()
                with open(os.path.join(namenode_log_path,"namenode_log.txt"), 'a') as fp:
                    fp.write(datetime.now()+"  - Copied Secondary to Primary Namenode.""\n")    
                    fp.close()
        else:
            try:
                os.mkdir(namenode_checkpoints)
            except:
                pass
            with open(os.path.join(namenode_checkpoints,"checkpoints.txt"), 'a') as fp:
                fp.write(str(datetime.now())+"  - Checked Namenode existence.""\n")    
                fp.close()

except Exception as e:
    print(e)
    exit()