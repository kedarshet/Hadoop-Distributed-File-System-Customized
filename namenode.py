#importing required modules
import os
import json
from csv import writer

# variables required for creating namenodes and mkdir 

#creates namenode folder and namenode(json file)
def create_namenodes(path_to_namenodes,fs_path,namenode_log_path):
    try:
        os.makedirs(namenode_log_path)
    except:
        pass
    try:
        os.makedirs(path_to_namenodes)
    except:
        pass
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
        with open(os.path.join(namenode_log_path,"namenode_log.txt"), 'a') as fp:
            fp.write("Secondary Namenode was created.""\n")    
            fp.close()
    except:
        print("Unable to create Secondary Namenode.")
            # print("NameNode log is created"+"\n")
# create_namenodes(path_to_namenodes,fs_path,namenode_log_path)

#mkdir func: used for creating virtual directory
def mkdir(path_to_namenodes,name_of_new_dir,fs_path,namenode_log_path):
    if(name_of_new_dir=='/' or name_of_new_dir[-2]=='/'):
        print("Provide name for the directory")
        return
    lst_dir = name_of_new_dir.split('/')
    lst_dir = lst_dir[:-1]
    prev_dir = ''
    for i in range(len(lst_dir)-1):
        prev_dir+=lst_dir[i]+'/'
    
    with open(os.path.join(path_to_namenodes,"namenode.json"), 'r') as fp:
        data=json.load(fp)
    #print(fs_path+prev_dir)
    #checking if previous path exists
    if data.get(fs_path+prev_dir) is None:
        print("Directory Not Found")
        return
    
    #creating an object in namenode.
    data[fs_path+name_of_new_dir]={}
    with open(os.path.join(path_to_namenodes,"namenode.json"), 'w') as fp:
        json.dump(data,fp)
    with open(os.path.join(namenode_log_path,"namenode_log.txt"), 'a') as fp:
        fp.write(fs_path+name_of_new_dir+" path was added.""\n")    
        fp.close()

   