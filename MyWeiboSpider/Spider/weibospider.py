import requests
import json
import pandas
import time
import os.path
import threading 
import pymysql
import datetime
contentbaseurl=r'https://api.weibo.cn/2/cardlist?count=200&c=android&s=8915076d&from=1098495010&ua=Netease-MuMu__weibo__9.8.4__android__android6.0.1&android_id=4d0e05a6668dbdaa&gsid='
#这个抓得 感觉不怎么好用啊
newcontentbaseurl=r'https://api.weibo.cn/2/statuses/user_timeline?count=200&c=android&s=8915076d&from=1098495010&ua=Netease-MuMu__weibo__9.8.4__android__android6.0.1&android_id=4d0e05a6668dbdaa&gsid='
#别提了又是猜得

repostbaseurl=r'https://api.weibo.cn/2/statuses/repost_timeline?count=200&c=android&s=8915076d&from=1098495010&ua=Netease-MuMu__weibo__9.8.4__android__android6.0.1&android_id=4d0e05a6668dbdaa&gsid='
flowbaseurl=r'https://api.weibo.cn/2/comments/show?count=200&is_show_bulletin=2&c=android&s=8915076d&from=1098495010&ua=Netease-MuMu__weibo__9.8.4__android__android6.0.1&android_id=4d0e05a6668dbdaa&gsid='
topicbaseurl='https://api.weibo.cn/2/page?count=200&is_show_bulletin=2&c=android&s=8915076d&from=1098495010&ua=Netease-MuMu__weibo__9.8.4__android__android6.0.1&android_id=4d0e05a6668dbdaa&gsid='
searchurl='https://api.weibo.cn/2/searchall?count=200&c=android&s=8915076d&from=1098495010&ua=Netease-MuMu__weibo__9.8.4__android__android6.0.1&android_id=4d0e05a6668dbdaa&gsid='
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
    def __init__(self,id, maxid,minid,isbottom,maxpagenum,type):
        self.maxid=maxid
        self.minid=minid
        self.isbottom=isbottom
        self.maxpagenum=maxpagenum
        self.id=id
        self.type=type
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
            return pymysql.Connect(host=self.host,port=self.port,user=self.user,password=self.password,database=self.dbname)
        except Exception as e:
            print(e)
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
class Base():
    '''

    '''
    @staticmethod
    def getByUrlDetail(url):
        '''
        根据具体URL获取JSON OBJECT
        '''
        print(url)
        response=requests.get(url)
        while(response.status_code !=200):
            time.sleep(10)
            response=requests.get(url)
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
        ids=[0]
        while((int(myconf.maxid) not in ids)  and ids!=[]):
            url=baseurl+'&page='+str(page)
            responsejob=Base.getByUrlDetail(url)            
            
            ids=[x['id'] for x in responsejob[config[myconf.type]['key']]] if config[myconf.type]['key'] in responsejob.keys() else [] 
 
            
            if ids!=[]:
                Base.commitLog(cnn,url,responsejob,method)
                actmaxid=actmaxid=max(max(ids),actmaxid)
                actminid=min(min(ids),actminid) if actminid!=-1 else min(ids)
                actmaxpagenum=max(myconf.maxpagenum,page)                
                try:
                    cnn.execute(config[myconf.type]['sqltemplate'].format(myconf.id,actmaxid,actminid,actmaxpagenum,myconf.isbottom,str(datetime.datetime.now())))          
                except Exception as e:
                    print(config[myconf.type]['sqltemplate'].format(myconf.id,actmaxid,actminid,actmaxpagenum,myconf.isbottom,str(datetime.datetime.now())))
                    print(e) 
            page=page+1
            time.sleep(1.1)
        #往前爬爬最新的
        
        try:
            cnn.execute(config[myconf.type]['sqltemplate'].format(myconf.id,actmaxid,actminid,actmaxpagenum,myconf.isbottom,str(datetime.datetime.now())))            
        except Exception as e:
            print(config[myconf.type]['sqltemplate'].format(myconf.id,actmaxid,actminid,actmaxpagenum,myconf.isbottom,str(datetime.datetime.now())))
            print(e)
        myconf.isbottom='true' if actminid==-1 else myconf.isbottom
        if myconf.isbottom=='false':
            page=actmaxpagenum
            url=baseurl+'&page='+str(page)
            responsejob=Base.getByUrlDetail(url)
            ids=[x['id'] for x in responsejob[config[myconf.type]['key']]] if config[myconf.type]['key'] in responsejob.keys() else []
            while(actminid not in ids and ids!=[]):
                page=page+1
                url=baseurl+'&page='+str(page)
                responsejob=Base.getByUrlDetail(url)
                ids=[x['id'] for x in responsejob[config[myconf.type]['key']]] if config[myconf.type]['key'] in responsejob.keys() else []
                time.sleep(1.1)
            while(ids!=[]):
                Base.commitLog(cnn,url,responsejob,method)
                page=page+1
                url=baseurl+'&page='+str(page)
                responsejob=Base.getByUrlDetail(url)
                ids=[x['id'] for x in responsejob[config[myconf.type]['key']]] if config[myconf.type]['key'] in responsejob.keys() else []
                if ids!=[]:
                    actmaxid=max(max(ids),actmaxid)
                    actminid=min(min(ids),actminid) if actminid!=-1 else min(ids)
                    actmaxpagenum=max(actmaxpagenum,page)                    
                    try:
                        cnn.execute(config[myconf.type]['sqltemplate'].format(myconf.id,actmaxid,actminid,actmaxpagenum,myconf.isbottom,str(datetime.datetime.now())))
                            
                    except Exception as e:
                        print(config[myconf.type]['sqltemplate'].format(myconf.id,actmaxid,actminid,actmaxpagenum,myconf.isbottom,str(datetime.datetime.now())))
                        print(e)
                time.sleep(1.1)
            myconf.isbottom='true'            
            try:
                cnn.execute(config[myconf.type]['sqltemplate'].format(myconf.id,actmaxid,actminid,actmaxpagenum,myconf.isbottom,str(datetime.datetime.now())))
               
            except Exception as e:
                print(config[myconf.type]['sqltemplate'].format(myconf.id,actmaxid,actminid,actmaxpagenum,myconf.isbottom,str(datetime.datetime.now())))
                print(e)
        #如果没爬到底就往后爬
        time.sleep(1.1)
        
    @staticmethod
    def getdatas(cnn,gsid,configsql,indexfield,type):
        '''
        实现updates
        '''
        maxidfield='maxid'
        minidfield='minid'
        isbottomfield='isbottom'
        maxpagefield='maxpagenum'
        idfield='uid'
        if type!='publish':
            idfield='id'
            maxidfield=type+'_'+maxidfield
            maxidfield=type+'_'+minidfield
            maxidfield=type+'_'+isbottomfield
            maxidfield=type+'_'+maxpagefield
        status=pandas.read_sql(configsql,cnn.cnn())
        status=status.set_index(indexfield)
        status_job=json.loads(status.to_json(orient='index'))
        baseurl=config[type]['url']+gsid
        for x in status_job:
            myconf=Config(x,status_job[x][maxidfield],status_job[x][minidfield],status_job[x][isbottomfield],status_job[x][maxpagefield],type)
            baseurl=baseurl+'&{}={}'.format(idfield,x)
            Base.updates(cnn,baseurl,myconf,type)

    @staticmethod
    def getContentsByUsers(cnn,gsid):
        '''
        爬取用户发布的微博
        '''
        sql="select `userid`,`maxid`,`minid`,`isbottom`,`maxpagenum` from appconfigs.spider_status_userspider"
        type='publish'
        indexfield='userid'
        Base.getdatas(cnn,gsid,sql,indexfield,type)
        time.sleep(1.1) 

    @staticmethod
    def getRepostsByContents(cnn,gsid):
        '''
        爬取用户的转发
        '''
        sql="select `contentid`,`repost_maxid`,`repost_minid`,`repost_isbottom`,`repost_maxpagenum` from appconfigs.spider_status_contentsspider"
        type='repost'
        indexfield='contentid'
        Base.getdatas(cnn,gsid,sql,indexfield,type)
        time.sleep(1.1) 
    @staticmethod
    def getCommentsByContents(cnn,gsid):
        '''
        爬取用户的评论
        '''
        sql="select `contentid`,`comment_maxid`,`comment_minid`,`comment_isbottom`,`comment_maxpagenum` from appconfigs.spider_status_contentsspider"
        type='comment'
        indexfield='contentid'
        Base.getdatas(cnn,gsid,sql,indexfield,type)
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
            sql1="insert into  appconfigs.spider_status_topstar(`typeid`,`startype`) values('{}','{}') on duplicate key update startype=values(startype)".format(typeid,topstar_types[str(typeid)])
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
        config=pandas.read_sql("select typeid,max(lastupdatetime) as updatetime from appconfigs.spider_status_topstar group by typeid",cnn.cnn())
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
        self.host= kwargs['host'] if 'host' in kwargs.keys() else '127.0.0.1'
        self.port= kwargs['port'] if 'port' in kwargs.keys() else 3306
        self.user= kwargs['user'] if 'user' in kwargs.keys() else 'root'
        self.password= kwargs['password'] if 'password' in kwargs.keys() else ''
        self.dbname=kwargs['database'] if 'database' in kwargs.keys() else ''
        self.cnn=Connection(self.host,self.port,self.user,self.password,self.dbname)
        t0=threading.Thread(target=Base.getTopStars,args=(self.cnn,) )
        t1=threading.Thread(target=Base.getContentsByUsers,args=(self.cnn,self.gsid) )
        t2=threading.Thread(target=Base.getRepostsByContents,args=(self.cnn,self.gsid) )
        t3=threading.Thread(target=Base.getCommentsByContents,args=(self.cnn,self.gsid) )
        self.threads={
            'gettopstar':t0,
            'publish':t1,
            'repost':t2,
            'comment':t3
            }

    def start(self):
        self.threads['gettopstar'].start()
        self.threads['publish'].start()
        self.threads['repost'].start()
        self.threads['comment'].start()
        t=datetime.datetime.now()
        times={
            
            'publish':t,
            'repost':t,
            'comment':t
            }
        dts={
            
            'publish':60*60*4,
            'repost':60*60,
            'comment':60*60
            }

        while(True):
            if datetime.datetime.now().day==5 and not self.threads ['gettopstar'].is_alive():
                self.threads ['gettopstar'].start()
            for x in times:
                if (datetime.datetime.now()-times[x]).seconds>dts[x] and not self.threads[x].is_alive():
                    self.threads[x].start()
                times[x]=datetime.datetime.now()

class UserSpider():
    '''
    仅仅是根据用户ID爬取ID下的全微博全评论全转发
    现已废弃。改成了数据库交互,并且改订阅式了
    '''
    def __init__(self, **kwargs):
        self.uids= kwargs['uids'] if 'uids' in kwargs.keys() else ''
        self.gsid= kwargs['gsid'] if 'gsid' in kwargs.keys() else ''
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
        print(contentbaseurl+str(self.gsid)+'&containerid='+str(containerid)+'&page='+str(page))
        url=contentbaseurl+str(self.gsid)+'&containerid='+str(containerid)+'&page='+str(page)

        responsejob=Base.getone(url)
        time.sleep(1.1)
        while('page_type' in responsejob['cardlistInfo'].keys()):
            
            cards=responsejob['cards']            
            cardsdf=pandas.read_json(json.dumps(cards,ensure_ascii=False))
            contentcardsdf=cardsdf[cardsdf.card_type==9]
            page=page+1
            for x in contentcardsdf.mblog:
                res.append({'id':x['id'],'mid':x['mid']})
            print(contentbaseurl+str(self.gsid)+'&containerid='+str(containerid)+'&page='+str(page))
            url=contentbaseurl+str(self.gsid)+'&containerid='+str(containerid)+'&page='+str(page)

            responsejob=Base.getone(url)
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
            print(repostbaseurl+str(self.gsid)+'&id='+str(x['id'])+'&page='+str(page))
            url=repostbaseurl+str(self.gsid)+'&id='+str(x['id'])+'&page='+str(page)
            responsejob=Base.getone(url)          
            time.sleep(1.1)
            check=False if responsejob['reposts']==None else False if len(responsejob['reposts'])==0 else True
            
            while(check):
                for y in responsejob['reposts']:
                    
                    users_thiscontent.append({'cid':x['id'],'id':y['user']['id'],'nick':y['user']['name']})
                page=page+1
               
                print(repostbaseurl+str(self.gsid)+'&id='+str(x['id'])+'&page='+str(page))
                url=repostbaseurl+str(self.gsid)+'&id='+str(x['id'])+'&page='+str(page)
                responsejob=Base.getone(url)
                check=False if responsejob['reposts']==None else False if len(responsejob['reposts'])==0 else True
                time.sleep(1.1)
            self.status['lastrepostcontentid']=x['id']
            open('status.config','w').write(json.dumps(self.status,ensure_ascii=False))
            df=pandas.read_json(json.dumps(users_thiscontent,ensure_ascii=False))
            df.to_csv(r'111.csv',index= False,mode='a')
            time.sleep(1.1) 
    def getflowusers(self):
        df=pandas.DataFrame(self.contentlist)
        df=df.sort_values(by='id')
        df=df.reset_index().drop(columns=['index'])
        if self.status['lastcommentcontentid']!=0:
            indexnum=df[df['id']==self.status['lastcommentcontentid']].index[0]
            df=df.reset_index()
            df=df[df['index']>indexnum] 
            df=df.drop(columns=['index'])
        job=json.loads(df.to_json(orient='records'))
        time.sleep(1.1)
        for x in job:    
            users_thiscontent=[]
            print(flowbaseurl+str(self.gsid)+'&id='+str(x['id'])+'&max_id=0')
            url=flowbaseurl+str(self.gsid)+'&id='+str(x['id'])+'&max_id=0'
            response=requests.get(flowbaseurl+str(self.gsid)+'&id='+str(x['id'])+'&max_id=0')
            responsejob=json.loads(response.text if response.status_code==200 else '{}')
            while(response.status_code !=200 or 'comments' not in responsejob.keys()):
                time.sleep(10)                
                response=requests.get(flowbaseurl+str(self.gsid)+'&id='+str(x['id'])+'&max_id=0')
                responsejob=json.loads(response.text if response.status_code==200 else '{}')          
           
            self.pushone(responsejob,users_thiscontent,x['id'])
            if 'max_id' in responsejob.keys():
                while(responsejob['max_id']!=0):
                    maxid=responsejob['max_id']
                    url=flowbaseurl+str(self.gsid)+'&id='+str(x['id'])+'&max_id='+str(maxid)
                    print(url)
                    response=requests.get(url)
                    responsejob=json.loads(response.text if response.status_code==200 else '{}')
                    while(response.status_code !=200 or ('max_id' not in responsejob.keys())):
                        time.sleep(10)
                        response=requests.get(url)
                        responsejob=json.loads(response.text if response.status_code==200 else '{}')
                
                    self.pushone(responsejob,users_thiscontent,x['id'])
                    time.sleep(1.1)
                self.status['lastcommentcontentid']=x['id']
                open('status.config','w').write(json.dumps(self.status,ensure_ascii=False))                
                df=pandas.read_json(json.dumps(users_thiscontent,ensure_ascii=False))
                df.to_csv(r'111.csv',index= False,mode='a')
            time.sleep(1.1)
    def pushone(self,jsonobject,users_this,cid):
        for y in jsonobject['comments']:
            users_this.append({'cid':cid,'id':y['user']['id'],'nick':y['user']['name']})




class HotsSpider():
    '''
    https://api.weibo.cn/2/searchall?count=200&c=android&s=8915076d&from=1098495010&ua=Netease-MuMu__weibo__9.8.4__android__android6.0.1&android_id=4d0e05a6668dbdaa&gsid=_2A25yI-1YDeRxGeFK7lEV9ynLzT2IHXVveWeQrDV6PUJbkdAKLWz3kWpNQ15OQ3_sB5fh5nrmfuomrYv0H3WhoNpo&containerid=100303type%3d1%26q%3d%23%E5%9B%9B%E5%B7%9D%E4%BA%91%E7%80%91%23%26t%3d3&page=10&count=200
    搜索话题的链接DEMO
    暂时不写吧感觉实时热搜没啥意义....话题数据量下 适合现爬,不适合获取实时热话题爬
    '''
    containerid={'231648_-_3':'实时要闻'
                 ,'106003type=25&t=3&disable_hot=1&filter_type=realtimehot':'实时热搜'
                 ,'231648_-_4':'热议'
                 ,'231648_-_1':'榜单'
                 ,'106003type=25&t=3&disable_hot=1&filter_type=moderngoods&category=master':'潮物榜'
                 ,'106003type=25&t=3&disable_hot=1&filter_type=moderngoods&category=fashion':'时尚美妆榜'
                 ,'106003type=25&t=3&disable_hot=1&filter_type=moderngoods&category=digital':'数码榜'
                 ,'106003type=25&t=3&disable_hot=1&filter_type=moderngoods&category=food':'美味榜单'
                 }
    card_type=[25,101]





