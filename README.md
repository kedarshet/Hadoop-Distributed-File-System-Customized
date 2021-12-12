# Yet Another Hadoop

Completed DataNode

Namenode in progress 
      - remaining : namenode log file
                    (points 3,4,5,6,7)
      - Done : syn_period
      
CLI Commands completed - put, mkdir , rmdir , rm ,ls, cat


Running Hadoop Jobs Completed - Tested.
Fixed - Split files to run hadoop jobs'
To do (optional) - Create multi process threads to run mapper

To do (optional) - take care of replication factor when data node is full. 

File contents:
1. namenodes.py: create_namenodes, mkdir, sync(heartbeat)
2. datanodes.py: create_data_nodes, put, remove(rm), rmdir, ls, cat
3. hadoopjobs.py: mapreduce
4. main.py: CLI implementation and  running thread funcitons for heartbeat(secondary name node)
                    
