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

def run(path):
    folderName = path[0]
    configFile = path[1]
    logFile = folderName+'/NAMENODE/log.json'

    # if the logFile was never populated, set it up
    try:
        json.load(open(logFile))
    except:
            with open(logFile, "w") as outfile:
                json.dump({'fs':{}}, outfile)

    while True:
        action = input()
        action = action.split()
        
        if action[0] == 'ls':
            ls(logFile, action[1])
        
        elif action[0] == 'mkdir':
            mkdir(logFile, action[1])

        elif action[0] == 'put':
            pass
        elif action[0] == 'cat':
            pass
        elif action[0] == 'rm':
            pass
        elif action[0] == 'rmdir':
            pass
        elif action[0] == 'exit':
            break
