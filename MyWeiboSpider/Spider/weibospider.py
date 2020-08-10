import requests
import json
import pandas
import time
import os.path
contentbaseurl=r'https://api.weibo.cn/2/cardlist?count=200&c=android&s=8915076d&from=1098495010&ua=Netease-MuMu__weibo__9.8.4__android__android6.0.1&android_id=4d0e05a6668dbdaa&gsid='
repostbaseurl=r'https://api.weibo.cn/2/statuses/repost_timeline?count=200&c=android&s=8915076d&from=1098495010&ua=Netease-MuMu__weibo__9.8.4__android__android6.0.1&android_id=4d0e05a6668dbdaa&gsid='
flowbaseurl=r'https://api.weibo.cn/2/comments/show?count=200&is_show_bulletin=2&c=android&s=8915076d&from=1098495010&ua=Netease-MuMu__weibo__9.8.4__android__android6.0.1&android_id=4d0e05a6668dbdaa&gsid='

class Spider():
    '''
    
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
        response=requests.get(contentbaseurl+str(self.gsid)+'&containerid='+str(containerid)+'&page='+str(page))
        while(response.status_code !=200):
            time.sleep(10)
            response=requests.get(contentbaseurl+str(self.gsid)+'&containerid='+str(containerid)+'&page='+str(page))
        reponsejob=json.loads(response.text)
        time.sleep(1.1)
        while('page_type' in reponsejob['cardlistInfo'].keys()):
            
            cards=reponsejob['cards']            
            cardsdf=pandas.read_json(json.dumps(cards))
            contentcardsdf=cardsdf[cardsdf.card_type==9]
            page=page+1
            for x in contentcardsdf.mblog:
                res.append({'id':x['id'],'mid':x['mid']})
            print(contentbaseurl+str(self.gsid)+'&containerid='+str(containerid)+'&page='+str(page))
            response=requests.get(contentbaseurl+str(self.gsid)+'&containerid='+str(containerid)+'&page='+str(page))
            while(response.status_code !=200):
                time.sleep(10)
                response=requests.get(contentbaseurl+str(self.gsid)+'&containerid='+str(containerid)+'&page='+str(page))
            reponsejob=json.loads(response.text)
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
            response=requests.get(repostbaseurl+str(self.gsid)+'&id='+str(x['id'])+'&page='+str(page))
            while(response.status_code !=200):
                time.sleep(10)
                response=requests.get(repostbaseurl+str(self.gsid)+'&id='+str(x['id'])+'&page='+str(page))

            responsejob=json.loads(response.text)            
            time.sleep(1.1)
            check=False if responsejob['reposts']==None else False if len(responsejob['reposts'])==0 else True
            
            while(check):
                for y in responsejob['reposts']:
                    
                    users_thiscontent.append({'cid':x['id'],'id':y['user']['id'],'nick':y['user']['name']})
                page=page+1
               
                print(repostbaseurl+str(self.gsid)+'&id='+str(x['id'])+'&page='+str(page))
                response=requests.get(repostbaseurl+str(self.gsid)+'&id='+str(x['id'])+'&page='+str(page))
                while(response.status_code !=200):
                    time.sleep(10)
                    response=requests.get(repostbaseurl+str(self.gsid)+'&id='+str(x['id'])+'&page='+str(page))

                responsejob=json.loads(response.text)
                check=False if responsejob['reposts']==None else False if len(responsejob['reposts'])==0 else True
                time.sleep(1.1)
            self.status['lastrepostcontentid']=x['id']
            open('status.config','w').write(json.dumps(self.status))
            df=pandas.read_json(json.dumps(users_thiscontent))
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
                open('status.config','w').write(json.dumps(self.status))                
                df=pandas.read_json(json.dumps(users_thiscontent))
                df.to_csv(r'111.csv',index= False,mode='a')
            time.sleep(1.1)
    def pushone(self,jsonobject,users_this,cid):
        for y in jsonobject['comments']:
            users_this.append({'cid':cid,'id':y['user']['id'],'nick':y['user']['name']})


            
        
        