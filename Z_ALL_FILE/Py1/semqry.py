{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 4,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "11.2.0.4.0\n",
      "<class 'datetime.datetime'>\n",
      "select * FROM SEMHEDB.ALERTS_STATUS_V_FULL WHERE  AGENT LIKE 'U2000 IP' and LASTOCCURRENCE BETWEEN TO_DATE('26-12-2020 00:08:00','DD-MM-YYYY HH24:MI:SS') AND TO_DATE('26-12-2020 23:50:00','DD-MM-YYYY HH24:MI:SS')\n",
      "Stat Time:  27-12-2020 21:26:12\n",
      "End Time:  27-12-2020 21:26:41\n",
      "Time Consumed:  218.3  mins\n"
     ]
    }
   ],
   "source": [
    "import pandas as pd\n",
    "import cx_Oracle\n",
    "import os\n",
    "from datetime import *\n",
    "from dateutil.parser import *\n",
    "from dateutil.tz import *\n",
    "from dateutil.relativedelta import *\n",
    "\n",
    "nw = datetime.now()\n",
    "dtst = nw.strftime (\"%d%m%Y%H%M%S\")\n",
    "fl = os.getcwd() + \"\\\\dw\\\\\" + dtst + \".csv\"\n",
    "#print(fl)\n",
    "\n",
    "def sem_view_filter_cols():\n",
    "    df = pd.read_csv(os.getcwd() + \"\\\\col_filter_semdb_view_non_macro.csv\")\n",
    "    ls = df.iloc[:,0].to_list()\n",
    "    x = \",\".join(list(ls))\n",
    "    return x\n",
    "\n",
    "def timedelt(diff):\n",
    "    x = datetime.now ()\n",
    "    d = x + timedelta (hours=diff)\n",
    "    str_d = d.strftime (\"%d-%m-%Y %H:%M:%S\")\n",
    "    return str_d\n",
    "\n",
    "def tmx(t1=False):\n",
    "    nw = datetime.now()\n",
    "    dtst = nw.strftime(\"%d-%m-%Y %H:%M:%S\")\n",
    "    if t1 == False:\n",
    "        print(\"Stat Time: \", dtst)\n",
    "        return nw\n",
    "    else:\n",
    "        x = (parse(\"22-12-2020 01:05\") - datetime.now()).seconds / 60\n",
    "        print(\"End Time: \", dtst)\n",
    "        print(\"Time Consumed: \", x, \" mins\")\n",
    "        \n",
    "conn = cx_Oracle.connect ('SOC_READ','soc_read', 'ossam-cluster-scan.robi.com.bd:1721/RBPB.robi.com.bd')\n",
    "print (conn.version)\n",
    "    \n",
    "def qryex(qr = False, flname = fl):\n",
    "    q = \"\"\n",
    "    if qr == False:\n",
    "        q1 = \"select \" + sem_view_filter_cols() + \" FROM SEMHEDB.ALERTS_STATUS_V_FULL  Where SEVERITY>0\"\n",
    "    else:\n",
    "        q1 = \"select \" + \"*\" + \" FROM SEMHEDB.ALERTS_STATUS_V_FULL WHERE \" + str(qr)\n",
    "    print(q1)\n",
    "    st = tmx()\n",
    "    df = pd.read_sql(q1, con = conn)\n",
    "    et = tmx(st)\n",
    "    df.to_csv(os.getcwd() + \"\\\\dw\\\\\" + flname)\n",
    "    return df\n",
    "    \n",
    "def timebetween(t1,t2):\n",
    "    d1 = parse(t1)\n",
    "    d2 = parse(t2)\n",
    "    print(type(d1))\n",
    "    dd = \"LASTOCCURRENCE BETWEEN TO_DATE('\" + d1.strftime(\"%d-%m-%Y %H:%M:%S\") + \"','DD-MM-YYYY HH24:MI:SS') AND TO_DATE('\" +  d2.strftime(\"%d-%m-%Y %H:%M:%S\") + \"','DD-MM-YYYY HH24:MI:SS')\"\n",
    "    return dd\n",
    "\n",
    "#######################################################################################\n",
    "def qr1():\n",
    "    x21 = timebetween('24-12-2020 12:08','24-12-2020 12:18')\n",
    "    Y21= qryex(x21,'EFDSDFSDFS.csv')\n",
    "\n",
    "def qr2():\n",
    "    x21 = timebetween('26-12-2020 00:08','26-12-2020 23:50')\n",
    "    x22 = \" CUSTOMATTR3 LIKE 'PHYSICAL PORT DOWN' and \" + x21 \n",
    "    df= qryex(x22,'all_oneday_ip.csv')\n",
    "\n",
    "qr2()\n",
    "\n",
    "#xx = (parse(\"22-12-2020 01:05\") - datetime.now()).seconds / 60\n",
    "#print(xx)\n",
    "#x = relativedelta(\n",
    "    #print(datetime.strptime(\"22-12-2020 01:05\",\"%d-%m-%Y %H:%M:%S\"))- datetime.strptime(datetime.now(),\"%d-%m-%Y %H:%M:%S\").seconds / 60\n",
    "#print(x)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.1"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
