# Customized Hadoop Distributed File System

Hadoop DFS is a mini-HDFS setup on your system, complete with the architectural
structure consisting of Data Nodes and Name Nodes and replication of data across nodes and
capable of performing some of the important tasks a distributed file system performs, running
HDFS commands as well as scheduling Hadoop jobs.

## QuickStart

### Installation
```bash
git clone https://github.com/kedarshet/Hadoop-Distributed-File-System-Customized.git HDFS
cd HDFS
pip3 install -r requirements.txt
```

### To Start Program
```
python3 start.py #config_file (optional)
```
## Edit config file for customization.


|Attribute|Type|Description|
|:--------|----|-----------|
|``Block Size            ``|Int|Indicates block size to split files in MegaBytes.</br>
|``Path To Datanodes      ``|Str|Absolute path to create Data Nodes.</br>
|``Path To Namenodes      ``|Str|Absolute path to create Name Node and secondary Name Node.</br>
|``Replication Factor     ``|Int|Replication factor of each block of a file.</br>
|``Num Datanodes           ``|Int|Number of Data Nodes to create.</br>
|``Datanode Size           ``|Int|Number of blocks a single Data Node may contains.</br>
|``Sync Period             ``|Int|Interval in seconds for the secondary Name Node to sync with the primary Name Node.</br>
|``Datanode Log Path       ``|Str|Absolute path to a directory create a log file to store information about Data Nodes.</br>
|``Namenode Log Path     ``  |Str|Absolute path to create a log file to store information about Data Nodes.</br>
|``Namenode Checkpoints``    |Str|Absolute path to a directory to store Name Node checkpoints.</br>
|``FS Path           ``      |Str|Absolute path to file system where a user may create files.</br>
|``DFS Setup Config``        |Str|Absolute path to DFS setup information.</br>

## Using the Command-Line Interface (CLI)
To create virtual directory use following command.
```
mkdir path-to-new-directory 
```
To add data to DFS  
```
put absolute-local-file-path absolute-path-in-dfs
```
To check contents of a particular directory
```
ls path-to-directory
```
To remove file from DFS
```
rm path-to-file
```
To remove directory and its contents
```
rmdir path-to-directory
```
To display the contents of the file.
```
cat path-to-file
```
To run mapreduce use following command.
```
mapreduce --input path-to-input --output absolute-path-to-output --mapper absolute-path-to-mapper --reducer absolute-path-to-reducer arguements(optional)
```
## LICENSE
This project is licensed under the Apache License 2.0 - see the [LICENSE.md](https://github.com/kedarshet/Hadoop-Distributed-File-System-Customized/blob/main/LICENSE) file for details

## Conclusion

**This was currently tested for `Ubuntu Linux`**
