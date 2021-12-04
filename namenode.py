import json

def ls(file, curDir):
    fs = json.load(open(file))['fs']
    curDir = curDir.split('/')[1:]
    
    for dir in curDir:
        if dir:
            fs = fs[dir]
        else:
            break
    
    for file in fs:
        print(file)


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
            pass
        elif action[0] == 'put':
            pass
        elif action[0] == 'cat':
            pass
        elif action[0] == 'rm':
            pass
        elif action[0] == 'rmdir':
            pass