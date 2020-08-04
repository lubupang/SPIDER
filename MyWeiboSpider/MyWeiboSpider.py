import sys
from Spider import weibospider
import pandas
import json
configfile=sys.argv[1]
configjob=json.loads(open(configfile).read())
uids=configjob['uids']
gsid=configjob['gsid']

spider=weibospider.Spider(uids=uids,gsid=gsid)
#没内容ID用这个
#spider.getallcontents()
#有美容ID用这个
spider.contentlist=json.loads(open('contents.json').read())
spider.getrepostusers()
spider.getflowusers()

print("用户数据已写入 111.csv")


