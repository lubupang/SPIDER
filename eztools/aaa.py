import sys
import pandas
import json
import hashlib
import glob
file=sys.argv[1]
a=glob.glob(file+r'\*.csv')
df=pandas.DataFrame()
for x in a:
	df=df.append(pandas.read_csv(x,header=None))
df=df.rename(columns={0:'cid',1:'id',2:'nick'})
df=df[df.id!='id']
print('爬到的评论转发总数{}'.format(str(len(df.id))))
df=df.groupby(by=['id','nick']).count()
df=df.reset_index()
df=df[['id','nick']]
print('去重后的uid总数{}'.format(str(len(df.id))))
df['id']=[hashlib.sha256(bytes(str(x),'utf8')).hexdigest() for x in df.id]
df['nick']=[hashlib.sha256(bytes(str(x),'utf8')).hexdigest() for x in df.nick]
res=df.reset_index()
res['file']=[int(x/1500000) for x in res['index']]
for k,v in res.groupby(by='file'):
    v['id'].to_csv(file+r'{}.csv'.format(str(k)),index=False)

print('ok')


