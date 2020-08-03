import sys
from Spider import weibospider
import pandas
import json
configfile=sys.argv[1]
configjob=json.loads(open(configfile).read())
uids=configjob['uids']
gsid=configjob['gsid']

spider=weibospider.Spider(uids=uids,gsid=gsid)
spider.getallcontents()
spider.getrepostusers()
spider.getflowusers()
df=pandas.read_json(json.dumps(spider.users))
df.to_csv(r'111.csv',index= False)
print("用户数据已写入 111.csv")


