import json
import shutil

def ls(file, dir):
    fs = json.load(open(file))['fs']
    curDir = dir.split('/')[1:]
    
    try:
        for dir in curDir:
            if dir:
                fs = fs[dir]
            else:
                break
        for file in fs:
            print(file)
    
    except Exception as e:
        print("Directory does not exist")    

        
def mkdir(file, dir):
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
                    if s > start:
                        #print(start,end,s-l,e-l)
                        files[f][i][d][0] = s-l
                        files[f][i][d][1] = e-l


def rm(logFile,path,replication_factor,folderName):
    fss = json.load(open(logFile))
    files = fss['files']
    files_copy = dict(files)
    t = fss['fs']
    datanodeMeta = fss['datanodes']
    try:
        if path in files:
            datanodeMeta = remove_file(logFile,path,replication_factor,folderName)
            return 
        else:
            for f in files_copy:
                if path in f:
                    datanodeMeta = remove_file(logFile,f,replication_factor,folderName)
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
 
          
def remove_file(logFile,fileName,replication_factor,folderName):
    files = json.load(open(logFile))['files']
    datanodeMeta = json.load(open(logFile))['datanodes']
    if fileName not in files:
        print('File does not exist.\n')
        return
    fileMeta = files[fileName]
    
    for i in range(replication_factor):
        temp = open('temp.txt',"a")
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


def rmdir(logFile,path):
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


def run(path):
    folderName = path[0]
    configFile = json.load(open(path[1]))
    logFile = folderName+'/NAMENODE/log.json'

    blockSize = configFile['block_size']
    numDatanodes = configFile['num_datanodes']
    datanodeSize = configFile['datanode_size']
    replication_factor = configFile['replication_factor']


    # if the logFile was never populated, set it up
    try:
        json.load(open(logFile))
    except:
            with open(logFile, "w") as outfile:
                datanodes = {}
                for i in range(numDatanodes):
                    datanodes[i+1] = datanodeSize

                json.dump({'fs':{}, 'files':{}, 'datanodes':datanodes, 'lastEnteredDataNode':0}, outfile)

    while True:
        action = input()
        action = action.split()
        
        if action[0] == 'ls':
            ls(logFile, action[1])
            print()
        
        elif action[0] == 'mkdir':
            mkdir(logFile, action[1])
            print("Directory Created\n")

        elif action[0] == 'put':
            src = action[1]
            dst = action[2]
            
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
                continue

            files[fileName] = []
            # breakup the src file into blocks, size(in kb) given in config file
            with open(src) as f:
                chunk = f.read(1024*blockSize)
                placed = True
                while chunk:
                    for i in range(replication_factor):
                        if datanodesMeta[str(curDataNode)] == 0:
                            print('No space remaining. Deleting the file')
                            #rm(fileName)
                            placed = False
                            break

                        datanode = folderName+'/DATANODE/DNODE'+str(curDataNode)
                        openDataNode = open(datanode, 'a')
                        
                        start = openDataNode.tell()
                        openDataNode.write(chunk)
                        end = openDataNode.tell()-1
                        openDataNode.close()
                        
                        files[fileName].append({str(curDataNode): [start, end]})
                        datanodesMeta[str(curDataNode)] -= 1

                        curDataNode += 1
                        if curDataNode == numDatanodes+1:
                            curDataNode = 1

                    if not placed:
                        break

                    chunk = f.read(1024*blockSize)
            
            if placed:
                mkdir(logFile, fileName)                
            
            if curDataNode == 1:
                curDataNode = numDatanodes

            fs = json.load(open(logFile))['fs']
            with open(logFile, "w") as outfile:
                json.dump({'fs':fs, 'files':files , 'datanodes':datanodesMeta, 'lastEnteredDataNode':curDataNode-1}, outfile)
            print("File added\n")
            
        elif action[0] == 'cat':
            fileName = action[1]
            files = json.load(open(logFile))['files']

            if fileName not in files:
                print('File does not exist.\n')
                continue

            # extract file, but keep in mind the replication factor and the way the file is stored
            fileMeta = files[fileName]

            for i in range(0, len(fileMeta), replication_factor):
                # open the datanode at idx i and and read in
                datanode = list(fileMeta[i].keys())[0]
                start = fileMeta[i][datanode][0]
                end = fileMeta[i][datanode][1]
                
                f = open(folderName+'/DATANODE/DNODE'+str(datanode))
                f.seek(start, 0)
                print(f.read(end-start+1),end='')
                f.close()

            print()

        elif action[0] == 'rm':
            rm(logFile,action[1],replication_factor,folderName)

        elif action[0] == 'rmdir':
            rmdir(logFile,action[1])
            
        elif action[0] == 'exit':
            break          
        else:
            print("Enter valid command\n")

