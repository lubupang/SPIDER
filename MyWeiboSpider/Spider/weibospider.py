import requests
import json
import pandas
import time
import os.path
import threading 
import pymysql
import datetime
contentbaseurl='https://api.weibo.cn/2/cardlist?count=200&c=android&s={}&from=1098495010&ua=Netease-MuMu__weibo__9.8.4__android__android6.0.1&android_id={}&gsid={}'
#这个抓得 感觉不怎么好用啊
newcontentbaseurl='https://api.weibo.cn/2/statuses/user_timeline?count=200&c=android&s={}&from=1098495010&ua=Netease-MuMu__weibo__9.8.4__android__android6.0.1&android_id={}&gsid={}'
#别提了又是猜得

repostbaseurl='https://api.weibo.cn/2/statuses/repost_timeline?count=200&c=android&s={}&from=1098495010&ua=Netease-MuMu__weibo__9.8.4__android__android6.0.1&android_id={}&gsid={}'
flowbaseurl='https://api.weibo.cn/2/comments/show?count=200&is_show_bulletin=2&c=android&s={}&from=1098495010&ua=Netease-MuMu__weibo__9.8.4__android__android6.0.1&android_id={}&gsid={}'
topicbaseurl='https://api.weibo.cn/2/page?count=200&is_show_bulletin=2&c=android&s={}&from=1098495010&ua=Netease-MuMu__weibo__9.8.4__android__android6.0.1&android_id={}&gsid={}'
searchurl='https://api.weibo.cn/2/searchall?count=200&c=android&s={}&from=1098495010&ua=Netease-MuMu__weibo__9.8.4__android__android6.0.1&android_id={}&gsid={}'
topstarurl='https://topstar.h5.weibo.cn/rank/list?sort=rank&limit=50'

config={
    "publish":{"sqltemplate":"insert into appconfigs.spider_status_userspider(`userid`,`maxid`,`minid`,`maxpagenum`,`isbottom`,`lastupdatetime`) values('{}','{}','{}','{}','{}','{}') on duplicate key update maxid=values(maxid),minid=values(minid),maxpagenum=values(maxpagenum),isbottom=values(isbottom),lastupdatetime=values(lastupdatetime)","key":"statuses","url":newcontentbaseurl},
    "repost":{"sqltemplate":"insert into appconfigs.spider_status_contentsspider(`contentid`,`repost_maxid`,`repost_minid`,`repost_maxpagenum`,`repost_isbottom`,`repost_lastupdatetime`) values('{}','{}','{}','{}','{}','{}') on duplicate key update repost_maxid=values(repost_maxid),repost_minid=values(repost_minid),repost_maxpagenum=values(repost_maxpagenum),repost_isbottom=values(repost_isbottom),repost_lastupdatetime=values(repost_lastupdatetime)","key":"reposts","url":repostbaseurl},
    "comment":{"sqltemplate":"insert into appconfigs.spider_status_contentsspider(`contentid`,`comment_maxid`,`comment_minid`,`comment_maxpagenum`,`comment_isbottom`,`comment_lastupdatetime`) values('{}','{}','{}','{}','{}','{}') on duplicate key update comment_maxid=values(comment_maxid),comment_minid=values(comment_minid),comment_maxpagenum=values(comment_maxpagenum),comment_lastupdatetime=values(comment_lastupdatetime),comment_isbottom=values(comment_isbottom)","key":"comments","url":flowbaseurl}
    }
topstar_types={"1":"亚太榜","3":"港澳台榜","4":"欧美榜","5":"内地榜","6":"新星榜","8":"组合榜","9":"练习生榜","20":"韩流势力榜"}
class Config():
    '''
    订阅式爬取需要的配置
    '''
    def __init__(self,id, maxid,minid,isbottom,maxpagenum,mytype):
        self.maxid=maxid
        self.minid=minid
        self.isbottom=isbottom
        self.maxpagenum=maxpagenum
        self.id=id
        self.mytype=mytype
class Connection():
    '''
    pymysql持久连接简易解决方案。。。。
    '''
    def __init__(self,host,port,user,password,dbname):
        self.host= host
        self.port= port
        self.user= user
        self.password= password
        self.dbname=dbname
    def cnn(self):
        try:
            tempcnn=pymysql.Connect(host=self.host,port=self.port,user=self.user,password=self.password,database=self.dbname)
            return tempcnn
        except Exception as e:
            print('cnn faild')
            return False
    def execute(self,sql):
        mycnn=self.cnn()
        while not mycnn:
            print('aaaa')
            time.sleep(10)
            mycnn=self.cnn()
        with mycnn.cursor() as cursor:            
            try:
                cursor.execute(sql)            
                mycnn.commit()
            except Exception as e:
                print(e)
        mycnn.close()
    def getpddata(self,sql):
        mycnn=self.cnn()
        while not mycnn:
            print('aaaa')
            time.sleep(10)
            mycnn=self.cnn()
        df=pandas.read_sql(sql,mycnn)
        mycnn.close()
        return df
class Base():
    '''

    '''
    @staticmethod
    def getByUrlDetail(url):
        '''
        根据具体URL获取JSON OBJECT
        '''
        print(url)
        response=None
        while response==None:
            try:
                response=requests.get(url, verify=True)
            except Exception as e:
                time.sleep(20)
                print(e)
            if response!=None:
                if response.status_code!=200:
                   response=None                   
                   time.sleep(20)
        responsejob=json.loads(response.text)
        time.sleep(1.1)
        return responsejob
    @staticmethod
    def commitLog(cnn,url,responsejob,method):
        '''
        把爬的数据简易的写入数据库
        '''
        tmpsql="insert into ods.spiderlogs(`url`,`name`,`response`) values('{}','{}','{}')".format(url,method,json.dumps(responsejob,ensure_ascii=False).replace("'","''").replace("\\","\\\\"))    
        try:
            cnn.execute(tmpsql)
        except Exception as  e:
            print(tmpsql)
            print(e)
        
    @staticmethod
    def updates(cnn,baseurl,myconf,method):
        '''
        需要翻页数据的订阅办法
        '''
        page=1
        actmaxid=int(myconf.maxid)
        actminid=int(myconf.minid)
        actmaxpagenum=int(myconf.maxpagenum)
        ids=[int(myconf.maxid)+1]
        #print('{}:{}'.format(method,ids))
        while((int(myconf.maxid) < (int(myconf.maxid) if ids==[] else min(ids)) )  and ids!=[]):
            url=baseurl+'&page='+str(page)
            responsejob=Base.getByUrlDetail(url)            
            
            ids=[] if config[myconf.mytype]['key'] not in responsejob.keys() else [] if responsejob[config[myconf.mytype]['key']]==None else [x['id'] for x in responsejob[config[myconf.mytype]['key']]] 
 
            
            if ids!=[]:
                Base.commitLog(cnn,url,responsejob,method)
                actmaxid=actmaxid=max(max(ids),actmaxid)
                actminid=min(min(ids),actminid) if actminid!=-1 else min(ids)
                actmaxpagenum=max(myconf.maxpagenum,page)                
                try:
                    cnn.execute(config[myconf.mytype]['sqltemplate'].format(myconf.id,actmaxid,actminid,actmaxpagenum,myconf.isbottom,str(datetime.datetime.now())))          
                except Exception as e:
                    print(config[myconf.mytype]['sqltemplate'].format(myconf.id,actmaxid,actminid,actmaxpagenum,myconf.isbottom,str(datetime.datetime.now())))
                    print(e)
            else:
                try:
                    cnn.execute(config[myconf.mytype]['sqltemplate'].format(myconf.id,actmaxid,actminid,actmaxpagenum,'true',str(datetime.datetime.now())))          
                except Exception as e:
                    print(config[myconf.mytype]['sqltemplate'].format(myconf.id,actmaxid,actminid,actmaxpagenum,'true',str(datetime.datetime.now())))
                    print(e)
            page=page+1
            time.sleep(1.1)
        #往前爬爬最新的
        myconf.isbottom='true' if actminid==-1 else myconf.isbottom
        
        try:
            cnn.execute(config[myconf.mytype]['sqltemplate'].format(myconf.id,actmaxid,actminid,actmaxpagenum,myconf.isbottom,str(datetime.datetime.now())))            
        except Exception as e:
            print(config[myconf.mytype]['sqltemplate'].format(myconf.id,actmaxid,actminid,actmaxpagenum,myconf.isbottom,str(datetime.datetime.now())))
            print(e)
        
        if myconf.isbottom=='false':
            page=actmaxpagenum
            url=baseurl+'&page='+str(page)
            responsejob=Base.getByUrlDetail(url)
            ids=[] if config[myconf.mytype]['key'] not in responsejob.keys() else [] if responsejob[config[myconf.mytype]['key']]==None else [x['id'] for x in responsejob[config[myconf.mytype]['key']]] 
            while((actminid <=(actminid if ids==[] else min( ids))) and ids!=[]):
                page=page+1
                url=baseurl+'&page='+str(page)
                responsejob=Base.getByUrlDetail(url)
                ids=[] if config[myconf.mytype]['key'] not in responsejob.keys() else [] if responsejob[config[myconf.mytype]['key']]==None else [x['id'] for x in responsejob[config[myconf.mytype]['key']]] 
                time.sleep(1.1)
            while(ids!=[]):
                Base.commitLog(cnn,url,responsejob,method)
                page=page+1
                url=baseurl+'&page='+str(page)
                responsejob=Base.getByUrlDetail(url)
                ids=[] if config[myconf.mytype]['key'] not in responsejob.keys() else [] if responsejob[config[myconf.mytype]['key']]==None else [x['id'] for x in responsejob[config[myconf.mytype]['key']]] 
                if ids!=[]:
                    actmaxid=max(max(ids),actmaxid)
                    actminid=min(min(ids),actminid) if actminid!=-1 else min(ids)
                    actmaxpagenum=max(actmaxpagenum,page)                    
                    try:
                        cnn.execute(config[myconf.mytype]['sqltemplate'].format(myconf.id,actmaxid,actminid,actmaxpagenum,myconf.isbottom,str(datetime.datetime.now())))
                            
                    except Exception as e:
                        print(config[myconf.mytype]['sqltemplate'].format(myconf.id,actmaxid,actminid,actmaxpagenum,myconf.isbottom,str(datetime.datetime.now())))
                        print(e)
                time.sleep(1.1)
            myconf.isbottom='true'            
            try:
                cnn.execute(config[myconf.mytype]['sqltemplate'].format(myconf.id,actmaxid,actminid,actmaxpagenum,myconf.isbottom,str(datetime.datetime.now())))
               
            except Exception as e:
                print(config[myconf.mytype]['sqltemplate'].format(myconf.id,actmaxid,actminid,actmaxpagenum,myconf.isbottom,str(datetime.datetime.now())))
                print(e)
        #如果没爬到底就往后爬
        time.sleep(1.1)

    def updatesFullback(cnn,s,aid,gsid,method):



        
        lastmaxidfield=method+'_fullback_lastmaxid'
        idsfield=method+'_ids'
        isbottomfield=method+'_isbottom'
        maxpagenumfield=method+'_maxpagenum'
        limits={
            'comment':50,
            'repost':500
            }
        sql="select contentid,{},{} from appconfigs.spider_status_contentsspider where  {}='true' and {}>={}".format(lastmaxidfield,idsfield,isbottomfield,maxpagenumfield,limits[method])
        
        df=cnn.getpddata(sql)
        
        df=df.set_index('contentid')
        myconfigs=json.loads(df.to_json(orient='index'))

        for x in myconfigs:           
            baseurl=config[method]['url'].format(s,aid,gsid)
            idsjson=json.loads(myconfigs[x][idsfield])
            tempdf=pandas.DataFrame({'ids':idsjson})
            tempdf=tempdf.sort_values(by=['ids'],ascending=[False])
            tpids=tempdf[tempdf['ids']<int(myconfigs[x][lastmaxidfield])]['ids']
            
            for y in tpids:
                myurl=baseurl+'&id='+str(x)+'&max_id='+str(y)+'&page=50'
                responsejob=Base.getByUrlDetail(myurl)
                ids=[] if config[method]['key'] not in responsejob.keys() else [x['id'] for x in responsejob[config[method]['key']]]
                ids=[tpids.min()] if ids==[] else ids      
                if min(ids)<tpids.min():
                    Base.commitLog(cnn,myurl,responsejob,'{}_fullback'.format(method))
                    for z in range(1,limits[method]+1):   
                        myurl=baseurl+'&id='+str(x)+'&max_id='+str(y)+'&page='+str(z)
                        responsejob=Base.getByUrlDetail(myurl)
                        Base.commitLog(cnn,myurl,responsejob,'{}_fullback'.format(method))
                        time.sleep(1.2)
                cnn.execute("update appconfigs.spider_status_contentsspider set {}={} where {}={}".format(lastmaxidfield,y,'contentid',str(x)))

    @staticmethod
    def getdatas(cnn,s,aid,gsid,configsql,indexfield,mytype):
        '''
        实现updates
        '''
        
        maxidfield='maxid'
        minidfield='minid'
        isbottomfield='isbottom'
        maxpagefield='maxpagenum'
        idfield='uid'
        if mytype!='publish':
            idfield='id'
            maxidfield=mytype+'_'+maxidfield
            minidfield=mytype+'_'+minidfield
            isbottomfield=mytype+'_'+isbottomfield
            maxpagefield=mytype+'_'+maxpagefield
        
        status=cnn.getpddata(configsql)
        
        
        status=status.set_index(indexfield)
        status_job=json.loads(status.to_json(orient='index'))
        
        baseurl=config[mytype]['url'].format(s,aid,gsid)
        for x in status_job:
            myconf=Config(x,status_job[x][maxidfield],status_job[x][minidfield],status_job[x][isbottomfield],status_job[x][maxpagefield],mytype)
            myurl=baseurl+'&{}={}'.format(idfield,x)
            
            Base.updates(cnn,myurl,myconf,mytype)

    @staticmethod
    def getContentsByUsers(cnn,s,aid,gsid):
        '''
        爬取用户发布的微博
        '''
        sql0="select min(level) as l from appconfigs.spider_status_userspider"
        df0=pandas.read_sql(sql0,cnn.cnn())
        l=df0['l'][0]
        while l!=99:
            sql="select `userid`,`maxid`,`minid`,`isbottom`,`maxpagenum` from appconfigs.spider_status_userspider where level="+str(l)+";"
            mytype='publish'
            indexfield='userid'
            Base.getdatas(cnn,s,aid,gsid,sql,indexfield,mytype)
            df0=pandas.read_sql(sql0,cnn.cnn())
            l=df0['l'][0]
            time.sleep(1.1) 

    @staticmethod
    def getRepostsByContents(cnn,s,aid,gsid,create='1900-01-01',maxnum=0):
        '''
        爬取用户的转发

        '''
        l=0
        sql0="select min(level) as l from appconfigs.spider_status_contentsspider where `level`>'{}'".format(str(l))
        df0=pandas.read_sql(sql0,cnn.cnn())
        l=df0['l'][0]
        while l!=99:        
            sql="select `contentid`,`repost_maxid`,`repost_minid`,`repost_isbottom`,`repost_maxpagenum` from appconfigs.spider_status_contentsspider  where (`create`>='"+create+"' or repost_maxpagenum>'"+str(maxnum)+"')  and repost_isbottom='false' and `level`="+str(l)+";"
            mytype='repost'
            indexfield='contentid'
            Base.getdatas(cnn,s,aid,gsid,sql,indexfield,mytype)
            sql0="select min(level) as l from appconfigs.spider_status_contentsspider where `level`>'{}'".format(str(l))
            df0=pandas.read_sql(sql0,cnn.cnn())
            l=df0['l'][0]
            time.sleep(1.1) 
    
    @staticmethod
    def getCommentsByContents(cnn,s,aid,gsid,create='1900-01-01',maxnum=0):
        '''
        爬取用户的评论
        '''
        l=0
        sql0="select min(level) as l from appconfigs.spider_status_contentsspider where `level`>'{}'".format(str(l))
        df0=pandas.read_sql(sql0,cnn.cnn())
        l=df0['l'][0]
        while l!=99:        
            sql="select `contentid`,`comment_maxid`,`comment_minid`,`comment_isbottom`,`comment_maxpagenum` from appconfigs.spider_status_contentsspider   where (`create`>='"+create+"' or comment_maxpagenum>'"+str(maxnum)+"') and comment_isbottom='false' and `level`="+str(l)+";"
            mytype='comment'
            indexfield='contentid'
            Base.getdatas(cnn,s,aid,gsid,sql,indexfield,mytype)
            sql0="select min(level) as l from appconfigs.spider_status_contentsspider where `level`>'{}'".format(str(l))
            df0=pandas.read_sql(sql0,cnn.cnn())
            l=df0['l'][0]
            time.sleep(1.1) 

    @staticmethod
    def getStarsByType(cnn,typeid):
        '''
        就是TOP明星呗
        {"1":"亚太榜","3":"港澳台榜","4":"欧美榜","5":"内地榜","6":"新星榜","8":"组合榜","9":"练习生榜","20":"韩流势力榜"}
        '''
        for x in range(1,3):
            url=topstarurl+'&rType='+str(typeid)+'&dType=month&page='+str(x)

            Base.getByUrlDetail(url)
            responsejob=Base.getByUrlDetail(url)
            Base.commitLog(cnn,url,responsejob,'gettopstar')
            sql1="insert into  appconfigs.spider_status_topstar(`typeid`,`startype`,`lastupdatetime`) values('{}','{}','{}') on duplicate key update lastupdatetime=values(lastupdatetime) ".format(typeid,topstar_types[str(typeid)],str(datetime.datetime.now()))
            try:
                cnn.execute(sql1)
            except Exception as e:
                print(sql1)                    
                print(e)
    @staticmethod
    def getTopStars(cnn):
        '''
        根据状态爬取全类型明星榜单
        '''
        
        config=cnn.getpddata("select typeid,max(lastupdatetime) as updatetime from appconfigs.spider_status_topstar group by typeid")
        
        status=config.set_index('typeid')
        status_json=json.loads(status.to_json(orient='index'))
        if len(status_json.keys())!=0:
            for x in status_json:
                dt=datetime.datetime.utcfromtimestamp( (status_json[str(x)]['updatetime']/1000)+8*3600)                
                if dt.month<datetime.datetime.now().month:
                    Base.getStarsByType(cnn,int(x))
        else:
            for x in topstar_types:
                Base.getStarsByType(cnn,int(x))

class Collect():
    '''
    订阅式的爬虫...就是指定用户天天盯着他们的发的内容及 每条内容的评论转发,然后更新
    '''
    def __init__(self,**kwargs):
        self.gsid= kwargs['gsid'] if 'gsid' in kwargs.keys() else ''
        self.s= kwargs['s'] if 's' in kwargs.keys() else ''
        self.android_id= kwargs['android_id'] if 'android_id' in kwargs.keys() else ''
        self.host= kwargs['host'] if 'host' in kwargs.keys() else '127.0.0.1'
        self.port= kwargs['port'] if 'port' in kwargs.keys() else 3306
        self.user= kwargs['user'] if 'user' in kwargs.keys() else 'root'
        self.password= kwargs['password'] if 'password' in kwargs.keys() else ''
        self.dbname=kwargs['database'] if 'database' in kwargs.keys() else ''
        self.cnn=Connection(self.host,self.port,self.user,self.password,self.dbname)
        t0=threading.Thread(name='gettopstar',target=Base.getTopStars,args=(self.cnn,) )
        t1=threading.Thread(name='getpublishes',target=Base.getContentsByUsers,args=(self.cnn,self.s,self.android_id,self.gsid) )
        t2=threading.Thread(name='getreposts',target=Base.getRepostsByContents,args=(self.cnn,self.s,self.android_id,self.gsid) )
        t3=threading.Thread(name='getcomments',target=Base.getCommentsByContents,args=(self.cnn,self.s,self.android_id,self.gsid) )
        t4=threading.Thread(name='getcomments_fullback',target=Base.updatesFullback,args=(self.cnn,self.s,self.android_id,self.gsid,'comment') )
        t5=threading.Thread(name='getcomments_fullback',target=Base.updatesFullback,args=(self.cnn,self.s,self.android_id,self.gsid,'repost') )

        self.threads={
            'gettopstar':t0,
            'publish':t1,
            'repost':t2,
            'comment':t3,
            'comment_fullback':t4,
            'repost_fullback':t5
            }

    def start(self):
        self.threads[ 'gettopstar'].start()
        time.sleep(5)
        self.threads[ 'publish'].start()
        time.sleep(5)
        self.threads[ 'repost'].start()
        time.sleep(5)
        self.threads[ 'comment'].start()
        time.sleep(5)
        #self.threads[ 'comment_fullback'].start()
        #time.sleep(5)
        #self.threads[ 'repost_fullback'].start()
        t=datetime.datetime.now()
        times={
            
            'publish':t,
            'repost':t,
            'comment':t,
            'comment_fullback':t,
            'repost_fullback':t
            }
        dts={
            
            'publish':10,
            'repost':10,
            'comment':10 ,
            'comment_fullback':10,
            'repost_fullback':10
            }

        while(True):
            self.threads[ 'gettopstar']=threading.Thread(name='gettopstar',target=Base.getTopStars,args=(self.cnn,) )
            self.threads[ 'publish']=threading.Thread(name='getpublishes',target=Base.getContentsByUsers,args=(self.cnn,self.s,self.android_id,self.gsid) )
            self.threads[ 'repost']=threading.Thread(name='getreposts',target=Base.getRepostsByContents,args=(self.cnn,self.s,self.android_id,self.gsid,datetime.datetime.today()-datetime.timedelta(days=40),40) )
            self.threads[ 'comment']=threading.Thread(name='getcomments',target=Base.getCommentsByContents,args=(self.cnn,self.s,self.android_id,self.gsid,datetime.datetime.today()-datetime.timedelta(days=40),40) )
            #self.threads[ 'comment_fullback']=threading.Thread(name='getcomments_fullback',target=Base.updatesFullback,args=(self.cnn,self.gsid,'comment') )
            #self.threads[ 'repost_fullback']=threading.Thread(name='getcomments_fullback',target=Base.updatesFullback,args=(self.cnn,self.gsid,'repost') )
            if datetime.datetime.now().day==5 and not self.threads ['gettopstar'].is_alive():
                self.threads ['gettopstar'].start()
                time.sleep(5)
            for x in times:
                if (datetime.datetime.now()-times[x]).seconds>dts[x] and not self.threads[x].is_alive():
                    self.threads[x].start()
                    time.sleep(5)
                times[x]=datetime.datetime.now()

class UserSpider():
    '''a
    仅仅是根据用户ID爬取ID下的全微博全评论全转发
    现已废弃。改成了数据库交互,并且改订阅式了
    '''
    def __init__(self, **kwargs):
        self.uids= kwargs['uids'] if 'uids' in kwargs.keys() else ''
        self.gsid= kwargs['gsid'] if 'gsid' in kwargs.keys() else ''
        self.s= kwargs['s'] if 's' in kwargs.keys() else ''
        self.android_id= kwargs['android_id'] if 'android_id' in kwargs.keys() else ''
        self.status=json.loads(open('status.config').read())
        print(self.status)
        self.users=[]        
    def getallcontents(self):
        if os.path.isfile('contents.json'):
            self.contentlist=json.loads(open('contents.json').read())
        else:
            list1=[self.getonebyuid(x) for x in self.uids]
            self.contentlist=[y for x in list1 for y in x ]
        print(self.contentlist)
        df=pandas.DataFrame(self.contentlist)        
        df=df.sort_values(by='id')
        df=df.reset_index().drop(columns=['index'])
        df.to_json('contents.json',orient='records')
    def getonebyuid(self,uid):
        res=[]
        containerid='230413'+str(uid)+'_-_WEIBO_SECOND_PROFILE_WEIBO'
        page=1
     
        url=contentbaseurl.format(str(self.s),str(self.android_id),str(self.gsid))+'&containerid='+str(containerid)+'&page='+str(page)

        responsejob=Base.getByUrlDetail(url)
        time.sleep(1.1)
        while('page_type' in responsejob['cardlistInfo'].keys()):
            
            cards=responsejob['cards']            
            cardsdf=pandas.read_json(json.dumps(cards,ensure_ascii=False))
            contentcardsdf=cardsdf[cardsdf.card_type==9]
            page=page+1
            for x in contentcardsdf.mblog:
                res.append({'id':x['id'],'mid':x['mid']})
            
            url=contentbaseurl.format(str(self.s),str(self.android_id),str(self.gsid))+'&containerid='+str(containerid)+'&page='+str(page)

            responsejob=Base.getByUrlDetail(url)
            time.sleep(1.1)
        return res
    def getrepostusers(self):
        df=pandas.DataFrame(self.contentlist)        
        df=df.sort_values(by='id')
        df=df.reset_index().drop(columns=['index'])
        if self.status['lastrepostcontentid']!=0:
            indexnum=df[df['id']==self.status['lastrepostcontentid']].index[0]
            print(indexnum)
            df=df.reset_index()
            df=df[df['index']>indexnum]
            df=df.drop(columns=['index'])
        job=json.loads(df.to_json(orient='records'))
        time.sleep(1.1)
        for x in job:
            users_thiscontent=[]
            page=1   
            actnum=0
            
            url=repostbaseurl.format(str(self.s),str(self.android_id),str(self.gsid))+'&id='+str(x['id'])+'&page='+str(page)
            responsejob=Base.getByUrlDetail(url)          
            time.sleep(1.1)
            check=False if responsejob['reposts']==None else False if len(responsejob['reposts'])==0 else True
            
            while(check):
                for y in responsejob['reposts']:
                    
                    users_thiscontent.append({'cid':x['id'],'id':y['user']['id'],'nick':y['user']['name']})
                page=page+1
               
                
                url=repostbaseurl.format(str(self.s),str(self.android_id),str(self.gsid))+'&id='+str(x['id'])+'&page='+str(page)
                responsejob=Base.getByUrlDetail(url)
                check=False if responsejob['reposts']==None else False if len(responsejob['reposts'])==0 else True
                time.sleep(1.1)
            self.status['lastrepostcontentid']=x['id']
            open('status.config','w').write(json.dumps(self.status,ensure_ascii=False))
            df=pandas.read_json(json.dumps(users_thiscontent,ensure_ascii=False))
            df.to_csv(r'111.csv',index= False,mode='a',header=False)
            time.sleep(1.1) 
    def getflowusers(self):
        df=pandas.DataFrame(self.contentlist)        
        df=df.sort_values(by='id')
        df=df.reset_index().drop(columns=['index'])
        if self.status['lastcommentcontentid']!=0:
            indexnum=df[df['id']==self.status['lastcommentcontentid']].index[0]
            print(indexnum)
            df=df.reset_index()
            df=df[df['index']>indexnum]
            df=df.drop(columns=['index'])
        job=json.loads(df.to_json(orient='records'))
        time.sleep(1.1)
        for x in job:
            users_thiscontent=[]
            page=1   
            actnum=0
            
            url=flowbaseurl+str(self.gsid)+'&id='+str(x['id'])+'&page='+str(page)
            responsejob=Base.getByUrlDetail(url)          
            time.sleep(1.1)
            check=False if 'comments' not in responsejob.keys() else False if responsejob['comments']==None else False if len(responsejob['comments'])==0 else True
            
            while(check):
                for y in responsejob['comments']:
                    
                    users_thiscontent.append({'cid':x['id'],'id':y['user']['id'],'nick':y['user']['name']})
                page=page+1
               
                
                url=flowbaseurl+str(self.gsid)+'&id='+str(x['id'])+'&page='+str(page)
                responsejob=Base.getByUrlDetail(url)
                check=False if 'comments' not in responsejob.keys() else False if responsejob['comments']==None else False if len(responsejob['comments'])==0 else True
                time.sleep(1.1)
            self.status['lastcommentcontentid']=x['id']
            open('status.config','w').write(json.dumps(self.status,ensure_ascii=False))
            df=pandas.read_json(json.dumps(users_thiscontent,ensure_ascii=False))
            df.to_csv(r'111.csv',index= False,mode='a',header=False)
            time.sleep(1.1) 











