import os
import sys

try:
    config_file = sys.argv[1]
except:
    lst = [i for i in os.listdir("./configs")]
    if(len(lst)==1):
        config_file = "./configs/DEFAULT.json"
    else:
        config_file  = "./configs/"+str(len(lst)-1)+".json"
with open("exit.txt","w+") as fp:
    fp.writelines("0")
fp.close()
    
try:
    os.system("/bin/python3 sync.py " + config_file +"& /bin/python3 checkpoints.py "+ config_file+"& /bin/python3 main.py "+config_file)
except:
    print("Please reinstall all files.")
    exit()