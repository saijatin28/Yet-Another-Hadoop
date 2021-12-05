import json

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

def rm():
    pass

def run(path):
    folderName = path[0]
    configFile = json.load(open(path[1]))
    logFile = folderName+'/NAMENODE/log.json'

    blockSize = configFile['block_size']
    numDatanodes = configFile['num_datanodes']
    datanodeSize = configFile['datanode_size']

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
        
        elif action[0] == 'mkdir':
            mkdir(logFile, action[1])

        elif action[0] == 'put':
            src = action[1]
            dst = action[2]
            
            nameNodeData = json.load(open(logFile))
            replication_factor = configFile['replication_factor']
            
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

        elif action[0] == 'cat':
            pass

        elif action[0] == 'rm':
            pass

        elif action[0] == 'rmdir':
            pass
        elif action[0] == 'exit':
            break
