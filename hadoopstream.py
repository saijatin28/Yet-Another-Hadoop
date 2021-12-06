import sys
import json
import namenode
import os

path = sys.argv[1]
output = sys.argv[2]
config = sys.argv[3]
mapper = sys.argv[4]
reducer = sys.argv[5]

def executeCommand(command):
    output = os.popen(command).read().strip()
    return output


try:
    f = open(config)
    folder = int(config.split('dfs_setup_config')[1].split('.json')[0])
    folderName = 'GlobalFS/'+str(folder)
    configFile = json.load(open(config))
    logFile = folderName+'/NAMENODE/log.json'
    blockSize = configFile['block_size']
    numDatanodes = configFile['num_datanodes']
    replication_factor = configFile['replication_factor']
    secondary = configFile['namenode_checkpoints']+'/Secondary.txt'
    
    namenode.cat(logFile,path,replication_factor,folderName,secondary)
    print(executeCommand("cat file.txt | python3 " +mapper +" | sort -k1,1 | python3 " +reducer + " > output.txt"))
    
    namenode.mkdir(logFile,output,secondary)
    
    namenode.put(logFile,"output.txt",output,replication_factor,folderName,secondary,numDatanodes,blockSize)
    
except Exception as e:
    print(e)
    print('Config file does not exist.')

