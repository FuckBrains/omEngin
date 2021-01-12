import os
import shutil

def w2tx(dirName,xx):
    fp = open(dirName, "w")
    fp.write(xx)
    fp.close()

def w2t(text):
    nx = datetime.now ()
    file1 = os.getcwd() + "\\" + nx.strftime("%m%d%H%M%S") + ".txt"
    file2 = os.getcwd() + "\\dump\\" + nx.strftime("%m%d%H%M%S") + ".txt"
    try:
        try:
            f = open(file2, 'a+')
        except:
            f = open(file1, 'a+')
        f.write("\n")
        f.write(text)
        f.close()
    except:
        pass
    print(file)
    return ""

def getFiles(dirName):
    listOfFile = os.listdir(dirName)
    completeFileList = list()
    for file in listOfFile:
        completePath = os.path.join(dirName, file)
        if os.path.isdir(completePath):
            completeFileList = completeFileList + getFiles(completePath)
        else:
            completeFileList.append(completePath)
    return completeFileList

def cpyfile(original,target):
    shutil.copyfile(original, target)

dirName = os.getcwd()
listOfFiles = getFiles(dirName)
xx = ""
for i in range(len(listOfFiles)):
    a1 = listOfFiles[i]
    if ".ipynb" in a1 or ".py" in a1:
        if "init" not in a1 and ".pyc" not in a1 and "__" not in a1:
            xx = xx + chr(10) + a1

w2tx(os.getcwd() + "\\B.txt",xx)
#os.system('copy file1.txt file7.txt')
#cpyfile(os.getcwd() + "\\TBOT\\omfn\\Untitled.ipynb","C:\\Users\\kabir.omi\\omom")
    
    
    
    