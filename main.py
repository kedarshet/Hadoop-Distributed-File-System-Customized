import sys
# import time
import json
from pathlib import Path
import namenode
import datanode
import hadoopjobs
import load_dfs_check
import shutil
import os


# global data
config_file  = sys.argv[1]
import getpass

with open(config_file) as f:
   data = json.load(f)

for i in data:
    value = data[i]
    if "$USER" in str(value):
        value = value.replace("$USER",getpass.getuser()) 
        data[i] = value
# exit()


path_to_datanodes=data["path_to_datanodes"]
path_to_namenodes=data["path_to_namenodes"]
sync_period=data["sync_period"]
fs_path=data["fs_path"]
temp_dir = data["temporary_directory"]
num_datanodes=data["num_datanodes"]
datanode_log_path=data["datanode_log_path"]
namenode_log_path=data["namenode_log_path"]
dfs_setup_config=data["dfs_setup_config"]
replication_factor=data["replication_factor"]
datanode_size=data["datanode_size"]
block_size=data["block_size"]


if not Path(dfs_setup_config).exists():
    dfs_setup_config_dir = dfs_setup_config.replace(dfs_setup_config.split('/')[-1],"")
    os.makedirs(dfs_setup_config_dir)
    name = "./configs/"+str(len(os.listdir("./configs")))+".json"
    with open(dfs_setup_config,"w") as fp1, open(config_file,"r") as fp2 :
        fp1.writelines(fp2.readlines())
    fp1.close()
    fp2.close()
    with open(config_file,"r") as fp2 , open(name,"w") as fp3:
        fp3.writelines(fp2.readlines())
    fp2.close()
    fp3.close()
with open(config_file,"r") as fp3:
     data = json.load(fp3)


print("Welcome to HDFS\n")  
for i in data:
    print(str(i)+"\t"+str(data[i]).rjust(10))

if not Path(path_to_datanodes).exists():
    datanode.create_data_nodes(path_to_datanodes,temp_dir,num_datanodes,datanode_log_path)
if not Path(path_to_namenodes).exists():             
    namenode.create_namenodes(path_to_namenodes,fs_path,namenode_log_path)
elif not Path(path_to_namenodes+'namenode.json').exists():
    if Path(path_to_namenodes+'secondary.json').exists():
        shutil.copyfile(path_to_namenodes+'secondary.json',path_to_namenodes+'namenode.json')
    else:
        namenode.create_namenodes(path_to_namenodes,fs_path,namenode_log_path)

load_dfs_check.check_datanode(path_to_namenodes,datanode_log_path,namenode_log_path)

while 1:
    x=input(">>> ")
    if(x==""):
        continue
    if(x=="exit"):
        with open("exit.txt","w") as fp:
            fp.write("1")
        fp.close()
        exit()
    else:
        y=x.split()
        if(y[0]=="mkdir"):
            if(len(y)<2):
                print("missing arguements")
            elif(len(y)>2):
                print("Found more arguements")
            elif(len(y)==2):
                if(y[1][-1]!='/'):
                    print("Invalid format. Add '/' in the end. For example: mkdir newdir/")
                else:
                    namenode.mkdir(path_to_namenodes,y[1],fs_path,namenode_log_path)
        elif(y[0]=="put"):
            if(len(y)<3):
                print("missing arguements")
            elif(len(y)>3):
                print("Found more arguements")
            elif(len(y)==3):
                datanode.put(block_size,path_to_datanodes,path_to_namenodes,replication_factor,num_datanodes,datanode_size,datanode_log_path,namenode_log_path,temp_dir,y[2],fs_path,y[1])
        elif(y[0]=="cat"):
            if(len(y)<2):
                print("missing arguements")
            elif(len(y)>2):
                print("Found more arguements")
            elif(len(y)==2):
                datanode.cat(y[1],path_to_namenodes,fs_path)

        elif(y[0]=="rm"):
            if(len(y)<2):
                print("missing arguements")
            elif(len(y)>2):
                print("Found more arguements")
            elif(len(y)==2):
                datanode.remove(fs_path+y[1],path_to_namenodes,datanode_log_path,namenode_log_path)
        elif(y[0]=="rmdir"):
            if(len(y)<2):
                print("missing arguements")
            elif(len(y)>2):
                print("Found more arguements")
            elif(len(y)==2):
                if(y[1][-1]!='/'):
                    print("Invalid format. Add '/' in the end. For example: rmdir newdir/")
                else:
                    datanode.rmdir(fs_path,y[1],path_to_namenodes,datanode_log_path,namenode_log_path) 
        elif(y[0]=="ls"):
            if(len(y)<2):
                print("missing arguements")
            elif(len(y)>2):
                print("Found more arguements")
            elif(len(y)==2):
                if(y[1][-1]!='/'):
                    print("Invalid format. Add '/' in the end. For example: rmdir newdir/")
                else:
                    datanode.ls(fs_path,y[1],path_to_namenodes)
        elif(y[0]=="mapreduce"):
            if(len(y)<9):
                print("Missing arguements")
            elif(len(y)>9):
                args=y[9:]
                hadoopjobs.mapreduce(y[2],y[4],y[6],y[8],args,path_to_namenodes,fs_path)  
            elif(len(y)==9):
                if(y[1]=="--input" and y[3]=="--output" and y[5]=="--mapper" and y[7]=="--reducer"):
                    hadoopjobs.mapreduce(y[2],y[4],y[6],y[8],[],path_to_namenodes,fs_path)            
        elif(y[0]=="help"):
            print("\nRecognized commands:")
            print("-  cat path-to-file")
            print("-  ls path-to-directory")
            print("-  rm path-to-file")
            print("-  put absolute-local-file-path absolute-path-in-dfs")
            print("-  mkdir path-to-new-directory ")
            print("-  rmdir path-to-directory\n")
            print("-  mapreduce --input path-to-input --output absolute-path-to-output --mapper absolute-path-to-mapper --reducer absolute-path-to-reducer arguements(optional)")
            print("Note: Do remember to add '/' in the end whenever working with directories in commands\n")
            
        else:
            print("Command not recognized. Enter help to know more")