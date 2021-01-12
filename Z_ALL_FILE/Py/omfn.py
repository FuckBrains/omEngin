
def sitecode_pick(txt):
    stcnt = 0
    numcnt = 0
    lp = 1
    prelp = 0
    indx = 0
    for i in txt:
        try:
            ix = int(i)
        except:
            ix = i
        if isinstance(ix,str):
            stcnt = stcnt + 1
        elif isinstance(ix,int):
            numcnt = numcnt + 1
            if lp - prelp == 1 and indx ==0:
                indx = lp
            else:
                prelp = lp
        lp = lp + 1
    if indx != 0:
        code = txt[indx-7:indx]
        return code
    else:
        return "NA"