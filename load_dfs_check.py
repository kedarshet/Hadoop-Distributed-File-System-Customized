import os
import pathlib
import json
from datanode import remove

def check_datanode(path_to_namenodes,datanode_log_path,namenode_log_path):
    with open(os.path.join(path_to_namenodes,"namenode.json"), 'r') as fp:
        data=json.load(fp)
    
    for i in data.keys():
        if i[-1]=="/":
            continue
        else:
            for j in data[i]:
                # total_rep = len(data[i][j])
                doesnt_exist = []
                exist = []
                for k in data[i][j]:
                    if not pathlib.Path(k).exists():
                        doesnt_exist.append(k)
                    else:
                        exist.append(k)
                if not len(exist):
                    remove(i,path_to_namenodes,datanode_log_path,namenode_log_path)
                    print("is corrupted. Please upload once again.")
                else:
                    for ne in doesnt_exist:
                        with open(ne,"w") as fp1,open(exist[0],"r") as fp2:
                            fp1.writelines(fp2.readlines())
                        # print("rewritten")
# check_datanode("/home/akanksha/BD_YAH/NameNode/")