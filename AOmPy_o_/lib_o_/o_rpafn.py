import pandas as pd
import numpy as np
import os
from datetime import *
try:
    from lib_o_.o_fn import *
except:
    from o_fn import *
    

lss = ['BHBRN','BHCFN','BHDLT','BHLMN','BHMNP','BHSDR','BHTMN','BRAMT','BRBMN','BRBTG','BRPTG','BRSDR','BSAGL',
'BSBBG','BSBKG','BSBNP','BSGND','BSHZL','BSMDG','BSMLD','BSSDR','BSWZP','JKKTL','JKNCT','JKRZP','JKSDR',
'MDKLK','MDRJR','MDSDR','MDSHB','PPBND','PPKKL','PPMTB','PPNSB','PPNZP','PPSDR','PPZNG','PTBFL','PTDMK',
'PTDMN','PTGCP','PTKLP','PTMZG','PTSDR','SPBDG','SPDMD','SPGSR','SPNRA','SPSDR','SPZNR','CMBMP','CMBRC',
'CMBRR','CMCND','CMDBD','CMDKN','CMHMN','CMLXM','CMMDN','CMMGN','CMMHG','CMNKT','CMSDD','CMSDR','CMTTS',
'CPFDG','CPHGN','CPHMC','CPKCH','CPMTB','CPSDR','CPSRT','CPUMT','CGBAK','CGBYZ','CGCDG','CGDMG','CGHLS',
'CGKHL','CGKTL','CGPCH','CGPRT','CGPTG','CGPTL','CGFTK','CGHTZ','CGMIR','CGRNG','CGRZN','CGSDP','CGSKD',
'KCDGN','KCLXC','KCMHC','KCMKC','KCMTR','KCPNC','KCRMG','KCSDR','RMBGC','RMBLC','RMBRK','RMJCR','RMKKL',
'RMKPT','RMLND','RMNNC','RMRTL','RMSDR','BBAKD','BBLMA','BBNKC','BBRMA','BBRNC','BBSDR','BBTCI','CGANW',
'CGBLK','CGBSK','CGCND','CGLHG','CGPTA','CGSKN','CXCKR','CXKTD','CXMHK','CXPKA','CXRMU','CXSDR','CXTKN',
'CXUKH','DHADB','DHBDD','DHDHN','DHGUL','DHKHL','DHKLB','DHMDP','DHMJH','DHNMK','DHPTN','DHRMN','DHRMP',
'DHSBB','DHSBG','DHSBN','DHTEJ','DHTIA','DHAPT','DHDKK','DHKKT','DHTRG','DHUTK','DHUTT','GPKLG','GPKLK',
'GPKPS','GPSDR','GPSRP','NGARH','NGRPG','NSBLB','NSMND','NSPLS','NSRPR','NSSBP','NSSDR','DHCNT','DHDHM',
'DHDHR','DHDRS','DHKCH','DHKFR','DHKGN','DHMRP','DHNWG','DHPLB','DHSHA','DHSVR','MGDLP','MGGHR','MGHRM',
'MGSBL','MGSDR','MGSNG','MGSTR','DHBNS','DHCKB','DHDEM','DHGND','DHHZR','DHJTB','DHKDM','DHKTL','DHLLB',
'DHSHM','DHSTR','MNGZR','MNLHG','MNSDR','MNSRK','MNSRN','MNTGB','NGBND','NGSDR','NGSNG','BGCTL','BGFKR',
'BGKCH','BGMLH','BGMNG','BGMRL','BGRMP','BGSDR','BGSRN','GGKOT','GGKSN','GGMKS','GGSDR','GGTNG','JSABH',
'JSBGH','JSCGH','JSJKR','JSKSB','JSMNR','JSSDR','JSSRS','KHBTG','KHDCP','KHDGH','KHDLP','KHDMR','KHKJA',
'KHKLS','KHKOY','KHPHL','KHPKG','KHRPS','KHSDR','KHSND','KHTRK','NRKLA','NRLHG','NRSDR','SKASN','SKDEB','SKKLG',
'SKKOL','SKSDR','SKSYM','SKTLA','CDALM','CDDMH','CDJBN','CDSDR','FPALF','FPBHN',
'FPBLM','FPCBH','FPMDH','FPNGR','FPSDR','FPSLT','FPSPR','JHHRK','JHKLG','JHKOT','JHMHS','JHSDR','JHSLK',
'KUBRM','KUDLP','KUKKS','KUKMK','KUMRP','KUSDR','MAMDP','MASDR','MASLK','MASRP','MHGNG','MHMJB','MHSDR',
'PBATG','PBBER','PBBNG','PBCTM','PBFRD','PBISR','PBSDR','PBSNG','PBSTH','RJBLK','RJGLN','RJKLK','RJPNG',
'RJSDR','JPBKG','JPDWG','JPISL','JPMDG','JPMLN','JPSDR','JPSSB','KGCRJ','KGRMR','KSBJT','KSBRB','KSHSN',
'KSITN','KSKLC','KSKRM','KSKTD','KSMTM','KSNKL','KSOST','KSPKN','KSSDR','KSTRL','MYBLK','MYDBR','MYFLB',
'MYFLP','MYGFG','MYGRP','MYHLG','MYISG','MYMKT','MYNND','MYSDR','MYTRL','NKATP','NKBRH','NKDGP','NKKLK',
'NKKLZ','NKKND','NKMDN','NKMHN','NKPBD','NKSDR','SNDMP','SRJNG','SRNKL','SRNLT','SRSBD','SRSDR','TNBPR',
'TNBSL','TNDBR','TNDDR','TNGPL','TNGTL','TNKLH','TNMDP','TNMZP','TNNGP','TNSDR','TNSKP','CMCDG','FNCGL',
'FNDGN','FNFGZ','FNPRS','FNSDR','FNSNG','LXKMN','LXRGN','LXRGT','LXRPR','LXSDR','NOBGM','NOCMP','NOCTK',
'NOHTA','NOKBH','NOMJD','NOSBC','NOSNB','NOSNM','BOADM','BODNT','BODPC','BOGBT','BOKHL','BONND','BOSBG',
'BOSDR','BOSJP','BOSNT','BOSRK','BOSRP','JYAKL','JYKLI','JYKTL','JYPBB','JYSDR','NAATR','NABGC','NADMR',
'NAMDB','NAMND','NANMT','NAPRS','NAPTN','NARNG','NASDR','NASPR','NTBGT','NTBRG','NTGDS','NTLLP','NTSDR',
'NTSNG','NWBLH','NWGMS','NWNCL','NWSBG','NWSDR','RSBGH','RSBGM','RSBLA','RSCGT','RSDGP','RSGDG','RSMHN',
'RSMHR','RSPBA','RSPTH','RSRJP','RSSMD','RSTNR','SGBKC','SGCHL','SGKMK','SGKZP','SGROY','SGSDR','SGSJP',
'SGTRS','SGULP','DPBCG','DPBRG','DPBRL','DPBRM','DPCRB','DPFLB','DPGRT','DPHKM','DPKHN','DPKHR','DPNWG',
'DPPBT','DPSDR','GBFLC','GBGBD','GBPLS','GBSDL','GBSDR','GBSGT','GBSND','KGBRM','KGCLM','KGFLB','KGNGS',
'KGRJR','KGSDR','KGULP','LMADT','LMHTB','LMKLG','LMPTG','LMSDR','NPDML','NPDMR','NPJLD','NPKSG','NPSDP',
'NPSDR','PGATR','PGBDA','PGDBG','PGSDR','PGTTL','RPBDG','RPGNG','RPKAU','RPMTH','RPPGC','RPPGN','RPSDR',
'RPTRG','TGBLD','TGHRP','TGPGN','TGRSN','TGSDR','BMAKH','BMASG','BMBJN','BMBNC','BMKSB','BMNBG','BMNNG',
'BMSDR','BMSRL','HGAZM','HGBBL','HGBNC','HGCNR','HGLKH','HGMDB','HGNBG','HGSDR','MBBRL','MBJRI','MBKLR',
'MBKML','MBRZN','MBSDR','MBSML','SNBSM','SNCTK','SNDKS','SNDRB','SNDRI','SNJGN','SNJML','SNSDR','SNSLA',
'SNTHP','SYBLG','SYBNB','SYBSW','SYCMP','SYDKS','SYFNC','SYGLP','SYGWN','SYJNT','SYKNG','SYSDR','SYZKG']

def omnm(sx):
    print(chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),
          chr(45),chr(45),chr(45),chr(45),chr(45))
    xx = sx.replace("42","32")
    yy = xx.replace("33","xx")
    ls = yy.split("xx")
    hp = ""
    for i in range(len(ls)):
        xz = ls[i].split(",")
        hp = ""
        for n in range(len(xz)):
            if xz[n] == "":
                print(hp)
                hp = ""
            else:
                hp = hp + chr(int(xz[n]))
    print(chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),chr(45),
          chr(45),chr(45),chr(45),chr(45),chr(45))
        

def o_print(my_dict):
    for key in my_dict.items():
        x = my_dict.get(key)

def getvalue(my_dict, ky):
    if ky is not None:
        for key, value in my_dict.items():
            if key in str (ky):
                return value
        else:
            return 0

TS = lambda x : '2G' if ('2G SITE DOWN' in x) \
                else ('3G' if ('3G SITE DOWN' in x) \
                else ('4G' if ('4G SITE DOWN' in x) \
                else ('MF' if ('MAIN' in x) \
                else ('DL' if ('VOLTAGE' in x) \
                else ('TM' if ('TEMPERATURE' in x) \
                else ('SM' if ('SMOKE' in x) \
                else ('GN' if ('GEN' in x) \
                else ('GN' if ('GENSET' in x) \
                else ('TH' if ('THEFT' in x) \
                else ('C2G' if ('2G CELL DOWN' in x) \
                else ('C3G' if ('3G CELL DOWN' in x) \
                else ('C4G' if ('4G CELL DOWN' in x) \
                else ('DOOR' if ('DOOR' in x) \
                else "NA")))))))))))))



def codecorr(code,akey):
    cd = code
    if 'UNKNOW' in code:
        for i in range(len(lss)):
            vl = akey.find(lss[i])
            if vl > 0 and vl is not None:
                cd = akey[vl:vl+7]
                break
        else:
            return cd
    else:
        return cd

def msgprep_head_znwise(hd = "Periodic Notification"):
    nw = datetime.now()
    dt = nw.strftime("%d-%m-%Y")
    tm = nw.strftime("%H:%M")
    a1 = hd + " at " + tm + " on " + dt
    return a1

def mpdata(df,omdb):
    dfdb = omdb
    df0 = df.rename (columns=str.upper)
    ls = ['RESOURCE','CUSTOMATTR15','SUMMARY','ALERTKEY','LASTOCCURRENCE','CLEARTIMESTAMP']
    df1 = df0[ls]
    df1 = df1.assign(CAT = df1.apply (lambda x: TS (x.SUMMARY), axis=1))
    df1 = df1.assign(CODE = df1.apply (lambda x: codecorr(x.CUSTOMATTR15, x.ALERTKEY), axis=1))
    df2 = df1.assign(sCode = df1.apply (lambda x: x.CODE[0:5] if (x.CODE is not None) else "XXXXXXXX", axis=1))
    df3 = df2.merge (dfdb, on='sCode')
    df3['CODECAT'] = df3['CUSTOMATTR15'].str.cat(df3['CAT'])
    df3['ZNCAT'] = df3['sZone'].str.cat(df3['CAT'])
    df3 = df3.assign(CDLO = df3.apply (lambda x: x['CUSTOMATTR15'] + ": " + x['LASTOCCURRENCE'], axis=1))
    dfp1p2 = pd.DataFrame([])
    try:
        dfp1p2 = pd.read_csv(os.getcwd() + "\\lib_o_\\site_p1p2.csv")
    except:
        dfp1p2 = pd.read_csv(os.getcwd() + "\\site_p1p2.csv")
    finally:
        dfp1p2 = pd.read_csv(os.getcwd() + "\\csv_o_\\site_p1p2.csv")
    df4 = df3.merge (dfp1p2, on='CUSTOMATTR15')
    return df4


def zonewise_parse(df1, whichzn, pickcols=[], colsMain="CAT", colsMain_val=['DL'], cols2=False, ls_val_cols2=False):
    heap = ""
    if df1.shape[0] != 0:
        if len(pickcols) is not None:
            for i in range(len(pickcols)):
                df1 = df1.assign(COLO1 = "0")
                df1['CDLO1'] = df1.apply (lambda x: x['CDLO'] + " - " + x[pickcols[i]], axis=1)
                df1['CDLO'] = df1['CDLO1']
                df1 = df1.drop(['CDLO1'], axis=1)
        if df1.shape[0] != 0:
            for i in range(len(colsMain_val)):
                hp1 = ""
                cri_1 = colsMain_val[i]
                df2 = df1[df1[colsMain].isin([cri_1])]
                if df2.shape[0] !=0 and ls_val_cols2 != False:
                    for j in range(len(ls_val_cols2)):
                        cri_2 = ls_val_cols2[j]
                        dff = df2[df2[cols2].isin([cri_2])]
                        if dff.shape[0] != 0:
                            dfx = dff.reset_index()
                            hp = cri_2 + "| " + str(dff.shape[0]) + chr(10) + dfx['CDLO'].str.cat(sep=chr(10))
                            if hp1 == "":
                                hp1 = hp
                            else:
                                hp1 = hp1 + chr(10) + chr(10) + hp
                    else:
                        heap = heap + chr(10) + chr(10) + cri_1 + " || Count: " + str(df2.shape[0]) + chr(10) + chr(10) + hp1
                else:
                    if df2.shape[0] !=0:
                        df2 = df2.reset_index()
                        hp = cri_1 + ": " + str(df2.shape[0]) + chr(10) + df2['CDLO'].str.cat(sep=chr(10))
                        if heap == "" or heap== chr(10):
                            heap = hp
                        else:
                            heap = heap + chr(10) + chr(10) + hp
                    else:
                        heap = heap + chr(10) + cri_1 + ": " + "NA"
            else:
                finalout = "Region: " + whichzn + chr(10) + heap
                return finalout


def parsing(df, whichzn, pickcols=[], colsMain="CAT", colsMain_val=['DL'], cols2=False, ls_val_cols2=False):
    zn = {'DHK_M':'','DHK_N':'','DHK_O':'','DHK_S':'','CTG_M':'','CTG_N':'','CTG_S':'','COM':'','NOA':'',
          'BAR':'','KHL':'','KUS':'','MYM':'','RAJ':'','RANG':'','SYL':''}
    cnt = 0
    rval = ""
    if whichzn =="ALL":
        for ky in zn.keys():
            cnt = cnt + 1
            whichzn = ky
            df1 = df[df['sZone'].isin([ky])]
            if ls_val_cols2 != False:
                zn[ky] = zonewise_parse(df1,whichzn,pickcols,colsMain, colsMain_val, cols2, ls_val_cols2)
                print(zn.get(ky))
                print("############################",chr(10))
            else:
                zn[ky] = zonewise_parse(df1,whichzn,pickcols,colsMain, colsMain_val)
                print(zn.get(ky))
                print("############################",chr(10))
        else:
            return zn
    elif whichzn =="":
        if ls_val_cols2 != False:
            rval = zonewise_parse(df,whichzn,pickcols,colsMain, colsMain_val, cols2, ls_val_cols2)
        else:
            rval = zonewise_parse(df,whichzn,pickcols,colsMain, colsMain_val)
        return rval
    elif whichzn !="NA" and whichzn!="ALL" and whichzn !="":
        df1 = df[df['sZone'].isin([whichzn])]
        if ls_val_cols2 != False:
            rval = zonewise_parse(df1,whichzn,pickcols,colsMain, colsMain_val, cols2, ls_val_cols2)
        else:
            rval = zonewise_parse(df1,whichzn,pickcols,colsMain, colsMain_val)
        return rval

def rmvdup(df,lscol=[]):
    df1 = df.drop_duplicates(subset=lscol, inplace=False, ignore_index=True)
    return df1

def zonewise_count(df0, oncat):
    zn = ['DHK_M','DHK_N','DHK_O','DHK_S','CTG_M','CTG_N','CTG_S','COM','NOA','BAR','KHL','KUS','MYM','RAJ','RANG','SYL']
    Tcnt = {}
    hd1 = ""
    hd2 = ""
    for j in range(len(oncat)):
        Tcnt[oncat[j]] = 0
        if hd1 == "":
            hd1 = "Region: " + oncat[j]
        else:
            hd1 = hd1 + "/" + oncat[j]
    hp = chr(10)
    ls = []
    for i in range(len(zn)):
        for n in range(len(oncat)):
            zct  = zn[i] + oncat[n]
            cnt = countifs(df0,df0['ZNCAT'],zct)
            ls.append(str(cnt))
            current_val = int(getvalue(Tcnt, oncat[n])) + cnt
            Tcnt[oncat[n]] = current_val
        else:
            hp = hp + chr(10) + zn[i] + ": " + "/".join(list(ls))
            ls = []
    for k in range(len(oncat)):
        hdval = Tcnt.get(oncat[k])
        if hd2 == "":
            hd2 = "National: " + str(hdval)
        else:
            hd2 = hd2 + "/" + str(hdval)
    else:
        trail1 = "This is RPA generated periodic notification." + chr(10) + "For any query, please contact with " + chr(10)
        trail2 = trail1 + "SOC Shift Manager, 01817183680"
        FinalText = msgprep_head_znwise() + chr(10) + chr(10) + hd1 + chr(10) + hd2 + hp + chr(10) + chr(10) + trail2
        return FinalText


def mpx(df,lscol,lsval):
    print(lscol,lsval)
    return ""
        
        
        
        
        
        
        
    
    
    