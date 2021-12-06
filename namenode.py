import json 
from os import name
import shutil
import sched
import time
from datetime import datetime

def ls(file, dir,secondary):
    try:
        fs = json.load(open(file))['fs']
    except:
        shutil.copyfile(secondary,file)
        fs = json.load(open(file))['fs']
    
    try:
        curDir = dir.split('/')[1:]
        for dir in curDir:
            if dir:
                fs = fs[dir]
            else:
                break
        for file in fs:
            print(file)
    
    except Exception as e:
        print("Directory does not exist")    

        
def mkdir(file, dir,secondary):
    try:
        obj = json.load(open(file))
    except:
        shutil.copyfile(secondary,file)
        obj = json.load(open(file))
    fs = obj['fs']
    homeFS = fs
    curDir = dir.split('/')[1:]

    for dir in curDir:
        if dir not in fs:
            fs[dir] = {}
        fs = fs[dir]

    obj['fs'] = homeFS

    with open(file, "w") as outfile:
        json.dump(obj, outfile)


def update(files,fileName,start,end,l,d,r):
    temp = open('temp.txt',"r")
    for f in files:
        if f != fileName:
            for i in range(r):
                if d == list(files[f][i].keys())[0]:
                    s = files[f][i][d][0]
                    e = files[f][i][d][1]
                    if s > start:"""checking"""
                        #print(start,end,s-l,e-l)
                        files[f][i][d][0] = s-l
                        files[f][i][d][1] = e-l


def rm(logFile,path,replication_factor,folderName,secondary):
    try:
        fss = json.load(open(logFile))
    except:
        shutil.copyfile(secondary,logFile)
        fss = json.load(open(logFile))
    files = fss['files']
    files_copy = dict(files)
    t = fss['fs']
    datanodeMeta = fss['datanodes']
    try:
        if path in files:
            datanodeMeta = remove_file(logFile,path,replication_factor,folderName,secondary)
            return 
        else:
            for f in files_copy:
                if path in f:
                    datanodeMeta = remove_file(logFile,f,replication_factor,folderName,secondary)
                    del files[f]
                #print(datanodeMeta)
               
        cur = path.split('/')[1:-1]
        if cur != []:
            for d in cur:
                t = t[d]
        del t[path.split('/')[-1]]
        #print(fss['fs'],t)
        with open(logFile, "w") as outfile:
            json.dump({'fs':fss['fs'], 'files':files , 'datanodes':datanodeMeta, 'lastEnteredDataNode':fss['lastEnteredDataNode']}, outfile)
        print("Directory has been removed\n")
               
            
    except Exception as e:
        #print(e)
        print("File/Directory doesn't exist")
 
          
def remove_file(logFile,fileName,replication_factor,folderName,secondary):
    try:
        fs = json.load(open(logFile))
    except:
        shutil.copyfile(secondary,logFile)
        fs = json.load(open(logFile))
    files = fs['files']
    datanodeMeta = fs['datanodes']
    if fileName not in files:
        print('File does not exist.\n')
        return
    fileMeta = files[fileName]
    
    delDNodeLog(fileName, logFile, folderName)

    for i in range(replication_factor):
        temp = open('temp.txt',"a")
        temp.seek(0)
        temp.truncate(0)
        datanode = list(fileMeta[i].keys())[0]
        start = fileMeta[i][datanode][0]
        end = fileMeta[i][datanode][1] 
        
        f = open(folderName+'/DATANODE/DNODE'+str(datanode),"r")
        lines = f.readlines()
        count = 0
        l = 0
        for line in lines:
            if len(line)-1 + count == end :
                l = len(line)
                continue
            else:
                temp.write(line)
                update(files,fileName,start,end,l,datanode,replication_factor)
                count = count + len(line)
        
        f.close()
        temp.close()   
        shutil.copyfile('temp.txt',folderName+'/DATANODE/DNODE'+str(datanode))
        
        datanodeMeta[str(datanode)] += 1
    
    try:    
        f = json.load(open(logFile))
    except:
        shutil.copyfile(secondary,logFile)
        f = json.load(open(logFile))
    
    t = f['fs']
    last = f['lastEnteredDataNode']
    del files[fileName]
    cur = fileName.split('/')[1:-1]
    for d in cur:
        t = t[d]
    del t[fileName.split('/')[-1]]
    with open(logFile, "w") as outfile:
        json.dump({'fs':f['fs'], 'files':files , 'datanodes':datanodeMeta, 'lastEnteredDataNode':last}, outfile)
    print("File has been removed\n")
    return datanodeMeta


def rmdir(logFile,path,secondary):
    try:
        f = json.load(open(logFile))
    except:
        shutil.copyfile(secondary,logFile)
        f = json.load(open(logFile))
        
    t = f['fs']
    cur = path.split('/')[1:-1]
    for d in cur:
        t = t[d]
    if t[path.split('/')[-1]] != {}:
        print("Failed to remove: Directory is not empty\n")
    else:
        del t[path.split('/')[-1]]
        with open(logFile, "w") as outfile:
            json.dump({'fs':f['fs'], 'files':f['files'] , 'datanodes':f['datanodes'], 'lastEnteredDataNode':f['lastEnteredDataNode']}, outfile)
        print("Directory has been deleted\n")

def addDNodeLog(fileName, folderName, dNode, start, end):
    logFile = folderName+'/DATANODE/LOGS/DNODE'+str(dNode)+'LOG.json'
    
    f = json.load(open(logFile))
    
    if fileName in f:
        f[fileName].append((start, end))
    else:
        f[fileName] = [(start, end)]
    
    with open(logFile, 'w') as op:
        json.dump(f, op)   

def delDNodeLog(fileName, logFile, folderName):
    # potentially every dnode may contain a part of this file
    # scan the logs and remove from there
    numDataNodes = len(json.load(open(logFile))['datanodes'])
    for i in range(1, numDataNodes+1):
        dlogFile = folderName+'/DATANODE/LOGS/DNODE'+str(i)+'LOG.json'
        # delete the file from this dnodelog
        obj = json.load(open(dlogFile))
        if fileName in obj:
            del obj[fileName]
            with open(dlogFile, 'w') as outfile:
                json.dump(obj, outfile)

def heartbeat(folderName):
    # validate data in datanodelogs and namenodelogs
    nameNodeLog = folderName+'/NAMENODE/log.json'
    obj = json.load(open(nameNodeLog))
    
    namenodeFiles = obj['files']
    numDataNodes = len(obj['datanodes'])
    report = []
    
    # to be matched with data stored in datanodes
    datanodeMeta = {}

    for file in namenodeFiles:
        for data in namenodeFiles[file]:
            dNode = list(data.keys())[0]
            if dNode not in datanodeMeta:
                datanodeMeta[dNode] = {}
            if file not in datanodeMeta[dNode]:
                datanodeMeta[dNode][file] = [[data[dNode][0], data[dNode][1]]]
            else:
                datanodeMeta[dNode][file].append([data[dNode][0], data[dNode][1]])

    for i in range(1, numDataNodes+1):
        # check if each datanode exists
        try:
            open(folderName+'/DATANODE/DNODE'+str(i))
            obj = json.load(open(folderName+'/DATANODE/LOGS/DNODE'+str(i)+'LOG.json'))
            if str(i) in datanodeMeta:
                if datanodeMeta[str(i)] != obj:
                    report.append('DNODE'+str(i)+' is corrupted')
        except:
            report.append('DNODE'+str(i)+' is unavailable')

    for prob in report:
        print(prob)
        
def put(logFile,src,dst,replication_factor,folderName,path,numDatanodes,blockSize):    
    try:
        nameNodeData = json.load(open(logFile))
    except:
        shutil.copyfile(path,logFile)
        nameNodeData = json.load(open(logFile))
                
    curDataNode = nameNodeData['lastEnteredDataNode'] + 1
    if curDataNode > numDatanodes:
         curDataNode = 1

    files = nameNodeData['files']
    datanodesMeta = nameNodeData['datanodes'] 
    fileName = dst+'/'+src.split('/')[-1]

    # check if file with same name exists already
    if fileName in files:
        print('File with same name already exists.')
        return

    files[fileName] = []
    # breakup the src file into blocks, size(in kb) given in config file
    with open(src) as f:
        chunk = f.read(1024*blockSize)
        placed = True
        while chunk:
            for i in range(replication_factor):
                if datanodesMeta[str(curDataNode)] == 0:
                    print('No space remaining. Deleting the file')                            
                    rm(logFile, fileName, replication_factor, folderName)
                    placed = False
                    break

                datanode = folderName+'/DATANODE/DNODE'+str(curDataNode)
                openDataNode = open(datanode, 'a')
                        
                start = openDataNode.tell()
                openDataNode.write(chunk)
                end = openDataNode.tell()-1
                openDataNode.close()
                        
                # datanode log must contain filename and which block of the file is stored in it 
                addDNodeLog(fileName, folderName, curDataNode, start, end)

                files[fileName].append({str(curDataNode): [start, end]})
                datanodesMeta[str(curDataNode)] -= 1

                curDataNode += 1
                if curDataNode == numDatanodes+1:
                    curDataNode = 1

            if not placed:
                break

            chunk = f.read(1024*blockSize)
            
    if placed:
        mkdir(logFile, fileName, path)                
        print("File added")
        fs = json.load(open(logFile))['fs']
        if curDataNode == 1:
            curDataNode = numDatanodes
        with open(logFile, "w") as outfile:
            json.dump({'fs':fs, 'files':files , 'datanodes':datanodesMeta, 'lastEnteredDataNode':curDataNode-1}, outfile)
 

def cat(logFile,fileName,replication_factor,folderName,path):
    try:
        files = json.load(open(logFile))['files']
    except:
        shutil.copyfile(path,logFile)
        files = json.load(open(logFile))['files']
                
    if fileName not in files:
        print('File does not exist.\n')
        return

    # extract file, but keep in mind the replication factor and the way the file is stored
    fileMeta = files[fileName]
    tf = open("file.txt","a")
    tf.seek(0)
    tf.truncate(0)
            
    for i in range(0, len(fileMeta), replication_factor):
        # open the datanode at idx i and and read in
        datanode = list(fileMeta[i].keys())[0]
        start = fileMeta[i][datanode][0]
        end = fileMeta[i][datanode][1]
                
        f = open(folderName+'/DATANODE/DNODE'+str(datanode))
        f.seek(start, 0)
        line = f.read(end-start+1)
        print(line,end='')
        tf.write(line)
        f.close()
    tf.close()


def run(path):
    folderName = path[0]
    configFile = json.load(open(path[1]))
    logFile = folderName+'/NAMENODE/log.json'
    blockSize = configFile['block_size']
    numDatanodes = configFile['num_datanodes']
    datanodeSize = configFile['datanode_size']
    replication_factor = configFile['replication_factor']
    syncPeriod = configFile['sync_period']
    checkpoints = configFile['namenode_checkpoints']
    path = checkpoints+'/Secondary.txt'
    try:
        json.load(open(logFile))
    except:
            with open(logFile, "w") as outfile:
                datanodes = {}
                for i in range(numDatanodes):
                    datanodes[i+1] = datanodeSize

                json.dump({'fs':{}, 'files':{}, 'datanodes':datanodes, 'lastEnteredDataNode':0}, outfile)
            
            # populate all datanode log files 
            for i in range(1, numDatanodes+1):
                with open(folderName+'/DATANODE/LOGS/DNODE'+str(i)+'LOG.json', 'w') as outfile:
                    json.dump({}, outfile)
            with open(path,"w") as s:
                json.dump(json.load(open(logFile)),s)

    heartbeat(folderName)
    with open(path,"w") as s:
                json.dump(json.load(open(logFile)),s)
    lastHeartBeat = datetime.now()

    while True:
        curTime = datetime.now()
        if (curTime - lastHeartBeat).seconds >= syncPeriod:
            with open(path,"w") as s:
                json.dump(json.load(open(logFile)),s)
            heartbeat(folderName)
            lastHeartBeat = datetime.now()
                    
        action = input()
        action = action.split()
        
        if action[0] == 'ls':
            ls(logFile, action[1],path)
            print()
        
        elif action[0] == 'mkdir':
            mkdir(logFile, action[1],path)
            print("Directory Created\n")

        elif action[0] == 'put':
            put(logFile,action[1],action[2],replication_factor,folderName,path,numDatanodes,blockSize)
            print("File added\n")
            
        elif action[0] == 'cat':
            cat(logFile,action[1],replication_factor,folderName,path)
            print()
        
        elif action[0] == 'rm':
            rm(logFile,action[1],replication_factor,folderName,path)

        elif action[0] == 'rmdir':
            rmdir(logFile,action[1],path)
            
        elif action[0] == 'exit':
            break          
        
        else:
            print("Enter valid command\n")
