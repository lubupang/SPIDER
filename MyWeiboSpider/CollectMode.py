from Spider import weibospider
import sys
import json

configfile=sys.argv[1]
configjob=json.loads(open(configfile).read())
host=configjob['host']
gsid=configjob['gsid']
s=configjob['s']
android_id=configjob['android_id']
port=configjob['port']
user=configjob['user']
password=configjob['psd']
datebase=configjob['database']
ctm=weibospider.Collect(s=s,android_id=android_id,gsid=gsid,host=host,port=port,user=user,password=password,datebase=datebase)
ctm.start()