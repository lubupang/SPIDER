import sys
from Spider import weibospider
import pandas
import json
import hashlib
configfile=sys.argv[1]
configjob=json.loads(open(configfile).read())
uids=configjob['uids']
gsid=configjob['gsid']

spider=weibospider.UserSpider(uids=uids,gsid=gsid)

spider.getallcontents()

spider.getrepostusers()
spider.getflowusers()

print("用户数据已写入 111.csv")
df=pandas.read_csv('111.csv')
df=df[df.id!='id']
df=df.groupby(by=['id','nick']).count()
df=df.reset_index()
df=df[['id','nick']]
df.to_csv('222.csv',index=False)
df['id']=[hashlib.sha256(bytes(str(x),'utf8')).hexdigest() for x in df.id]
df['nick']=[hashlib.sha256(bytes(str(x),'utf8')).hexdigest() for x in df.nick]
df.to_csv('333.csv',index=False)



