import pandas as pd
import pyodbc

UserEx = "Driver={SQL Server};Server=10.101.4.193;Database=ROC;Uid=om29861;Pwd=Roc@072$123"
conn = pyodbc.connect(UserEx)

def siteinfo(txtwht):
    bts_info = """\
                EXEC [dbo].[spDetailsBTSInfoReport];
            """
    nodeb_inf = """\
                        EXEC [dbo].[spDetailsNodeBInfoReport];
                        """
    enodeb_inf = """\
                        EXEC [dbo].[spDetails_eNodeBInfoReport];
                    """
    if (txtwht == "All2g") or (txtwht == "all2g") or (txtwht == "All2G") or (txtwht == "2G"):
        dfbts = pd.read_sql(bts_info, conn)
        dfbts0 = dfbts[dfbts['BTSTotal'] != 0]
        btsdif = dfbts.shape[0] - dfbts0.shape[0]
        currbts = dfbts.shape[0] - btsdif
        return "ALL ON AIRED 2G: " + str(currbts)
    elif (txtwht == "All3G") or (txtwht == "all3G") or (txtwht == "All3g") or (txtwht == "3G"):
        nbdf = pd.read_sql(nodeb_inf, conn)
        nb = nbdf.shape[0]
        return "ALL ON AIRED 3G: " + str(nb)
    elif (txtwht == "All4G") or (txtwht == "all4G") or (txtwht == "All4g") or (txtwht == "4G"):
        enb_df = pd.read_sql(enodeb_inf, conn)
        enb = enb_df.shape[0]
        return "ALL ON AIRED 4G: " + str(enb)
    elif (txtwht == "AllCount") or (txtwht == "SC"):
        df2G = pd.read_sql(bts_info, conn)
        allnode = df2G.shape[0]
        df2G1 = df2G[df2G['BTSTotal'] != 0]
        btsdif = df2G.shape[0] - df2G1.shape[0]
        bts = df2G.shape[0] - btsdif
        df_3G = pd.read_sql(nodeb_inf, conn)
        nb = df_3G.shape[0]
        enb_df = pd.read_sql(enodeb_inf, conn)
        enb = enb_df.shape[0]
        xstr = "ALL ONAIR" + "\n" + "Radio Node: " + str(allnode) + "\n" + "2G: " + str(bts) + "\n" + "3G: " + str(nb) + "\n" + "4G: " + str(enb)
        return xstr
    else:
        return "#"
    

