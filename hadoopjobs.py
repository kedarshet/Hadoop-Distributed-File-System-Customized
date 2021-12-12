import os,json,pathlib
import threading

def mapping(filen,i,splits,local_path_mapper,output,arguements):
    a=filen[0]+'_'+str(i)+'.'+filen[1]
    rep = len(splits[a])
    inp = '-1'
    for i in range(rep):
        if not pathlib.Path(splits[a][i]).exists:
            i+=1
        else:
            inp = splits[a][i]
            break
    if inp == "-1":
        print("File is corrupted. Please re-upload input files.")
        return
    try:
        os.system('cat '  + inp +' | python3 '+local_path_mapper+ " "+ arguements+ ' >'+output)
        with open(output,'r') as f1,open('moutput.txt','a') as f2:
            f2.writelines(f1.readlines())
    except:
        print("Mapper Path not Found.")        
    f1.close()
    f2.close()


def mapreduce(path_to_dataset,local_path_to_output,local_path_mapper,local_path_reducer,args,path_to_namenodes,fs_path):
    arguements=""
    for i in args:
        arguements+=str(i)+" "
    with open(os.path.join(path_to_namenodes,'namenode.json'),'r') as f:
        data = json.load(f)
    splits=data[fs_path+path_to_dataset]
    # print(len(splits))
    filename=path_to_dataset.split('/')
    filen=filename[-1].split('.')
    with open('moutput.txt','w') as f1:
        f1.write('')
    for i in range(1,len(splits)+1,2):
        t1 = threading.Thread(target=mapping, args=(filen,i,splits,local_path_mapper,"output.txt",arguements))
        t1.start()
        if(i+1<len(splits)+1):
            t2 = threading.Thread(target=mapping, args=(filen,i+1,splits,local_path_mapper,"output2.txt",arguements))
            t2.start()
        t1.join()
        t2.join()
    try:
        os.system("cat moutput.txt | sort -k 1,1 | python3 "+local_path_reducer+' >'+local_path_to_output)
        os.remove("output.txt")
        os.remove("output2.txt")
    except:
        print("Reducer Path not Found.") 
        