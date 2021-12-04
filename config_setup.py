# This file allows user to create new configurations for dfs and use the ones they already created

import sys
import json
import glob
import os
import namenode

def getLastDFSConfFile():
    files = glob.glob('dfs_setup_config*.json')
    maxi = 0

    for i in range(len(files)):
        maxi = max(maxi, int(files[i].split('dfs_setup_config')[1].split('.json')[0]))

    return maxi

def createNewDFS():
    fileNo = getLastDFSConfFile()
    newDFSConfigFile = 'dfs_setup_config'+str(fileNo+1)+'.json'
    
    defaultConfig = json.load(open('config_sample.json'))
    defaultConfig['path_to_datanodes'] = 'GlobalFS/'+str(fileNo+1)+'/DATANODE'
    defaultConfig['path_to_namenodes'] = 'GlobalFS/'+str(fileNo+1)+'/NAMENODE'
    defaultConfig['datanode_log_path'] = 'GlobalFS/'+str(fileNo+1)+'/DATANODE/LOGS'
    defaultConfig['namenode_log_path'] = 'GlobalFS/'+str(fileNo+1)+'/NAMENODE/log.json'
    defaultConfig['namenode_checkpoints'] = 'GlobalFS/'+str(fileNo+1)+'/NAMENODE/CHECKPOINTS'
    defaultConfig['fs_path'] = 'GlobalFS/'+str(fileNo+1)+'/FS'
    defaultConfig['dfs_setup_config'] = newDFSConfigFile
    
    with open(newDFSConfigFile, "w") as outfile:
        json.dump(defaultConfig, outfile)
    
    # create the required folders and files
    for attr in defaultConfig:
        if ('path' in attr or 'checkpoints' in attr) and attr != 'namenode_log_path':
            os.makedirs(defaultConfig[attr])   
    
    open(defaultConfig['namenode_log_path'], 'w')
    for i in range(defaultConfig['num_datanodes']):
        open(defaultConfig['path_to_datanodes']+'/DNODE'+str(i+1), 'w')

    print('New DFS created, the config file is named dfs_setup_config'+str(fileNo+1)+'.json')
    print('Pass the above filename as command line argument the next time')

    return 'GlobalFS/'+str(fileNo+1)

def config_setup():
    
    if len(sys.argv) == 2:
        # verify if it exists
        try:
            f = open(sys.argv[1])
            pathToDfs = json.load(f)['path_to_namenodes']

        except:
            print('Config file does not exist. Creating new dfs with default settings')
            pathToDfs = createNewDFS()

    else:
        pathToDfs = createNewDFS()
    
    namenode.run(pathToDfs)

config_setup()