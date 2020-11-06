import pandas
import pymysql
import sys
import hashlib
level =sys.argv[1]
cnn=pymysql.Connect(host='192.168.1.188',user='rrfsj',password='rrfAdmin123!@#')
sql="select convert(t3.userid ,char) as uid from (select  contentid from appconfigs.spider_status_contentsspider where `level`='{}') t1 left join (select rootid,relatedid from dds.data_weibo_relations) t2 on t1.contentid=t2.rootid left join (select contentid,userid from dds.data_weibo_contents) t3 on t2.relatedid=t3.contentid".format(str(level))
res=pandas.read_sql(sql,cnn)
print('爬到的评论转发总数{}'.format(str(len(res.uid))))
res=res.groupby('uid').count().reset_index()
print('去重后的uid总数{}'.format(str(len(res.uid))))
res['uid']=[hashlib.sha256(bytes(str(x),'utf8')).hexdigest()  for x in res['uid']]
res=res.reset_index()
res['file']=[int(x/1500000) for x in res['index']]
for k,v in res.groupby(by='file'):
    v['uid'].to_csv(r'{}.csv'.format(str(k)),index=False)

print('ok')