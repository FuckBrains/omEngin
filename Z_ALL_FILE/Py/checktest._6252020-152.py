zn = "ABC"
p1p2 ="P1"
pg = "ACT"
owner = "ulka"
smsid = "123"
thn ="TH"
pwaut="REB"
qryupd = "UPDATE [dbo].[omidb] SET (REGION=" + zn + ", P1P2=" + p1p2 + ", PG=" + pg + ", OWNER=" + owner + ") WHERE SMSID=" + smsid
print(qryupd)

qryupd2 = "UPDATE [dbo].[pglog4] SET REGION='" + zn +"', PRIORITY='" + p1p2 + "',SITETYPE_PG='" + pg + \
          "', POWER_AUTH='" + pwaut + "', THANA='" + thn + "', OWNER='" + owner + "' WHERE SMSID='" + smsid +"'"
print(qryupd2)