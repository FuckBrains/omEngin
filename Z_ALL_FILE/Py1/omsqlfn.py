
def prep_update(lscol,lsval):
    hp = ''
    if isinstance(lscol, list) and isinstance(lsval, list):
        if len(lscol) == len(lsval):
            for i in range(len(lscol)):
                x = str(lscol[i]) + "='" + str(lsval[i]) + "'"
                if hp == '':
                    hp = x
                else:
                    hp = hp + ',' + x
        else:
            print('num of col and value are not same')
        return hp
    elif isinstance(lscol, str) and isinstance(lsval, str):
        hp = ""
        comma = lsval.count(',')
        invertcomma = lsval.count("'")
        if invertcomma == (comma+1)*2:
            x1 = lscol.split(',')
            x2 = lsval.split(',')
            print(x1,x2)
            for i in range(len(x1)):
                x = x1[i] + "=" + x2[i]
                if hp == '':
                    hp = x
                else:
                    hp = hp + ',' + x
        if invertcomma <= 2:
            x1 = lscol.split(',')
            x2 = lsval.split(',')
            for i in range(len(x1)):
                x = str(x1[i]) + "='" + str(x2[i]) + "'"
                if hp == '':
                    hp = x
                else:
                    hp = hp + ',' + x
            
        return hp

def prep_insert(lscol,lsval):
    hp = ''
    if isinstance(lscol, list) and isinstance(lsval, list):
        if len(lscol) == len(lsval):
            ls = []
            for i in range(len(lsval)):
                ls.append("'" + str(lsval[i]) + "'")
                hp = '(' + str.join(',', lscol) + ') values (' + str.join(',', ls) + ')'
        else:
            hp = "check list values for double color"
            print('num of col and value are not same')
        return hp
    elif isinstance(lscol, str) and isinstance(lsval, str):
        hp1 = ""
        hp2 = ""
        hp = ""
        cnt = 0
        comma = lsval.count(',')
        invertcomma = lsval.count("'")
        if invertcomma == (comma+1)*2:
            x1 = lscol.split(',')
            x2 = lsval.split(',')
            for i in range(len(x1)):
                if hp1 == '':
                    hp1 = str(x1[i])
                    hp2 = str(x2[i])
                    cnt = cnt + 1
                else:
                    hp1 = hp1 + "," + str(x1[i])
                    hp2 = hp2 + "," + str(x2[i])
                    cnt = cnt + 1
                hp = '(' + hp1 + ') values (' + hp2 + ')'
            return hp
        elif invertcomma <= 2:
            x1 = lscol.split(',')
            x2 = lsval.split(',')
            for i in range(len(x1)):
                if hp1 == '':
                    hp1 = str(x1[i])
                    hp2 = "'" + str(x2[i]) + "'"
                    cnt = cnt + 1
                else:
                    hp1 = hp1 + "," + str(x1[i])
                    hp2 = hp2 + "," + "'" + str(x2[i]) + "'"
                    cnt = cnt + 1
                hp = '(' + hp1 + ') values (' + hp2 + ')'
            return hp

def fetchone_read(rs):
    if isinstance(rs, list):
        print('fetchone readed called \n ')
        ls = []
        cnt = 0
        for r in rs:
            ls1 = list(r)
            cnt = cnt + 1
            print(cnt , '.', ls1)
            ls.append(ls1)
    else:
        print('list type data required but passed data type is ', type(rs))
