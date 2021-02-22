import sys
from Spider import weibospider
import pandas
import json
import hashlib
configfile=sys.argv[1]
configjob=json.loads(open(configfile).read())
searchWords=configjob['searchWords']
gsid=configjob['gsid']
s=configjob['s']
android_id=configjob['android_id']

spider=weibospider.SearchSpider(searchWords=searchWords,gsid=gsid,s=s,android_id=android_id)

spider.getallcontents()






