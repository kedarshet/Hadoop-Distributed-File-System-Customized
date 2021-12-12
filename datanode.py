# importing modules
import os
# from posixpath import split
from fsplit.filesplit import Filesplit
import shutil
import json
from pathlib import Path
import string
import random

fs = Filesplit()



#creating datanode
def create_data_nodes(data_dir,temp_dir,num_nodes,log_dir): 
    os.makedirs(log_dir) #creating log file directory
    os.makedirs(data_dir) #creating datanode directory
    #creating datanodes as folders
    for i in range(num_nodes):
        directory = "node"+str(i)
        path = os.path.join(data_dir, directory) 
        os.makedirs(path) 
        with open(os.path.join(log_dir, directory+".txt"), 'w') as fp:
            fp.write(directory+" is created"+"\n")#creating datanode log files 
    #creating temp file path directory
    directory = "temp"
    path = os.path.join(temp_dir, directory)     
    os.makedirs(path) 
    # print("Directory '% s' created" % directory) 

# create_data_nodes(path_to_datanodes,temp_dir,number_of_datanodes,datanode_log_path)

#put command for adding file to given virtual path(takes several parameters)
def put(block_size,data_dir,path_to_namenodes,replication_factor,num_nodes,datanode_size,datanode_log_path,namenode_log_path,temp_dir,name_of_new_dir,fs_path,local_path):
    # checking if file exists in local path
    if not Path(local_path).exists():
        print("File Not Found.")
        return
    #accessing namenode
    with open(os.path.join(path_to_namenodes,"namenode.json"), 'r') as fp:
        data=json.load(fp)
    #check if given hadoop path exists
    if data.get(fs_path+name_of_new_dir) is None and name_of_new_dir!='/':
        print("Hadoop Path Not Found")
        return
    file_name = local_path.split('/')[-1]
    
    if fs_path+name_of_new_dir+file_name in data:
        print('File Already exists.')
        return
    total_blocks=0
    #counting total blocks present in directory
    try:
        for i in range(num_nodes):
            total_blocks += len(os.listdir(data_dir+"/node"+str(i)))
        block_size = block_size * 10**6 #converting given data from Mega Bytes to Bytes
        #spliting data
        fs.split(file=local_path, split_size=block_size, output_dir=temp_dir+"temp/", newline = True) 
        lst = [i for i in os.listdir(temp_dir+"temp")]
        lst.remove("fs_manifest.csv")
        # checking for maximum replication
        replication_factor = min(((datanode_size*num_nodes)-total_blocks)//len(lst),replication_factor)
        if not replication_factor:
            print("Cannot store. No space available.")
            for i in lst:
                os.remove(temp_dir+"temp/"+i)
            os.remove(temp_dir+"temp/fs_manifest.csv")
            return
        #creating virtual path of file in name node.
        new_path = ""
        if name_of_new_dir=="/":    
            data[fs_path+file_name]={}
            new_path = fs_path+file_name
            name_of_new_dir = ""
        else:
            data[fs_path+name_of_new_dir+file_name]={}
            new_path = fs_path+name_of_new_dir+file_name

        #adding files in datanode
        for i in lst:
            j = i.split('_')
            hash = int(j[-1].split('.')[0])%num_nodes #hash value
            destination = data_dir+"node"
            k=0
            r=0
            data[new_path][i]=[]


            while k<num_nodes and r<replication_factor:
                files_count = len(os.listdir(destination+str(hash)))
                #checking if directory is not full and adding block of file.
                if files_count <datanode_size:
                    var = i
                    while True:
                        if not Path(destination+str(hash)+"/",var).exists():
                            shutil.copyfile(temp_dir+"temp/"+i,destination+str(hash)+"/"+var)
                            break
                        else:
                            var = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 10)) + '.' + i.split('.')[-1]
                            var = str(var)


                    #adding to namenode
                    data[new_path][i].append(destination+str(hash)+"/"+var)
                    #writing in log file.
                    with open(os.path.join(datanode_log_path,"node"+str(hash)+".txt"), 'a') as fp:
                        fp.write(i+" added to "+"node"+str(hash)+"\n")
                    r+=1
                else:
                    k+=1
                hash = (hash+1)%num_nodes
            
            with open(os.path.join(path_to_namenodes,"namenode.json"), 'w') as fp:
                json.dump(data,fp)
            os.remove(temp_dir+"temp/"+i)
        with open(os.path.join(namenode_log_path,"namenode_log.txt"), 'a') as fp:
            fp.write(file_name+" was added to "+fs_path+name_of_new_dir+"\n")    
            fp.close()
            #removing files from temp
        os.remove(temp_dir+"temp/fs_manifest.csv")
    except Exception as e:
        print(e)
        print("Unable to add File.")    

# put(block_size,path_to_datanodes,path_to_namenodes,replication_factor,number_of_datanodes,datanode_size,datanode_log_path,namenode_log_path,temp_dir,name_of_new_dir,fs_path,local_path)

def remove(virtual_path,path_to_namenodes,datanode_log_path,namenode_log_path):
    try:
        with open(os.path.join(path_to_namenodes,"namenode.json"), 'r') as fp:
            data=json.load(fp)
        for i in data[virtual_path].values():
            for j in i:
                try:
                    os.remove(j)
                except:
                    pass
                node_name,file_name = j.split('/')[-2],j.split('/')[-1]
                with open(os.path.join(datanode_log_path,node_name+".txt"), 'a') as fp:
                        fp.write(file_name+" removed from "+node_name+"\n")
                        fp.close()
        with open(os.path.join(namenode_log_path,"namenode_log.txt"), 'a') as fp:
            fp.write(virtual_path+" was removed."+"\n")    
            fp.close()
        data.pop(virtual_path)
        with open(os.path.join(path_to_namenodes,"namenode.json"), 'w') as fp:
            json.dump(data,fp)
    except:
        print("Removing could not be performed.")

def rmdir(fs_path,virtual_path,path_to_namenodes,datanode_log_path,namenode_log_path):
    virtual_path = fs_path+virtual_path
    try:
        with open(os.path.join(path_to_namenodes,"namenode.json"), 'r') as fp:
            data=json.load(fp)
        lst = [i for i in data.keys()]
        for i in lst:
            if virtual_path in i:
                if i[-1]=='/':
                    data.pop(i)
                    with open(os.path.join(namenode_log_path,"namenode_log.txt"), 'a') as fp:
                        fp.write(i+" was removed."+"\n")    
                        fp.close()
                else:
                    remove(i,path_to_namenodes,datanode_log_path,namenode_log_path)
                    data.pop(i)
        with open(os.path.join(path_to_namenodes,"namenode.json"), 'w') as fp:
            json.dump(data,fp)
    except:
        print("Removing could not be performed.")

def ls(fs_path,virtual_path,path_to_namenodes):
    if virtual_path != '/':
        virtual_path = fs_path+virtual_path
    else:
        virtual_path = fs_path
    try:
        with open(os.path.join(path_to_namenodes,"namenode.json"), 'r') as fp:
            data=json.load(fp)
        if virtual_path not in data:
            print("Directory Not Found.") 
            return

        count = len(virtual_path.split('/'))
        l_count = 0
        # print(virtual_path,count)
        for i in data.keys():
            # print(i)
            if virtual_path in i:
                files=i.split('/')
                if(i[-1]=='/'):
                    if count+1==len(files):            
                        print(files[-2]+'/')
                elif(count==len(files) and i!=virtual_path):
                    print(files[-1])
                l_count=1
        if l_count==0:
            print("No such Directory Exist")
    except:
        print("Error while loading Namenode.")
            
    

def cat(file_path,path_to_namenodes,fs_path):
    try:
        with open(os.path.join(path_to_namenodes,'namenode.json'),'r') as f:
            data = json.load(f)
        splits=data[fs_path+file_path]
        # print(len(splits))
        filename=file_path.split('/')
        filen=filename[-1].split('.')
        for i in range(1,len(splits)+1):
            a=filen[0]+'_'+str(i)+'.'+filen[1]

            f = open(splits[a][0], "r")
            text = f.read()
            print(text)
            f.close()
    except:
        print("Error while loading Namenode.")


