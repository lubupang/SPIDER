import requests
import json
import pandas
import time
contentbaseurl=r'https://api.weibo.cn/2/cardlist?c=android&s=8915076d&from=1098495010&ua=Netease-MuMu__weibo__9.8.4__android__android6.0.1&android_id=4d0e05a6668dbdaa&gsid='
repostbaseurl=r'https://api.weibo.cn/2/statuses/repost_timeline?c=android&s=8915076d&from=1098495010&ua=Netease-MuMu__weibo__9.8.4__android__android6.0.1&android_id=4d0e05a6668dbdaa&gsid='
flowbaseurl=r'https://api.weibo.cn/2/comments/build_comments?is_show_bulletin=2&c=android&s=8915076d&from=1098495010&ua=Netease-MuMu__weibo__9.8.4__android__android6.0.1&android_id=4d0e05a6668dbdaa&gsid='
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
        list1=[self.getonebyuid(x) for x in self.uids]
        self.contentlist=[y for x in list1 for y in x ]
        print(self.contentlist)
    def getonebyuid(self,uid):
        res=[]
        containerid='230413'+str(uid)+'_-_WEIBO_SECOND_PROFILE_WEIBO'
        page=1
        print(contentbaseurl+str(self.gsid)+'&containerid='+str(containerid)+'&page='+str(page))
        response=requests.get(contentbaseurl+str(self.gsid)+'&containerid='+str(containerid)+'&page='+str(page))
        while(response.status_code !=200):
            time.sleep(30)
            response=requests.get(contentbaseurl+str(self.gsid)+'&containerid='+str(containerid)+'&page='+str(page))
        reponsejob=json.loads(response.text)
        t=0
        while('page_type' in reponsejob['cardlistInfo'].keys() and t<1):
            t=t+1
            cards=reponsejob['cards']            
            cardsdf=pandas.read_json(json.dumps(cards))
            contentcardsdf=cardsdf[cardsdf.card_type==9]
            page=page+1
            for x in contentcardsdf.mblog:
                res.append({'id':x['id'],'mid':x['mid']})
            print(contentbaseurl+str(self.gsid)+'&containerid='+str(containerid)+'&page='+str(page))
            response=requests.get(contentbaseurl+str(self.gsid)+'&containerid='+str(containerid)+'&page='+str(page))
            while(response.status_code !=200):
                time.sleep(30)
                response=requests.get(contentbaseurl+str(self.gsid)+'&containerid='+str(containerid)+'&page='+str(page))
            reponsejob=json.loads(response.text)
            time.sleep(1.5)
        return res
    def getrepostusers(self):
        df=pandas.DataFrame(self.contentlist)        
        df=df.sort_values(by='id')
        
        if self.status['lastrepostcontentid']!=0:
            indexnum=df[df['id']==self.status['lastrepostcontentid']]
            df=df.reset_index()
            df=df[df['index']>indexnum]
            df=df.drop(columns=['index'])
        job=json.loads(df.to_json(orient='records'))        
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
         
            check=False if responsejob['reposts']==None else False if len(responsejob['reposts'])==0 else True
            
            while(check):
                for y in responsejob['reposts']:
                    
                    users_thiscontent.append({'id':y['user']['id'],'nick':y['user']['name']})
                page=page+1
               
                print(repostbaseurl+str(self.gsid)+'&id='+str(x['id'])+'&page='+str(page))
                response=requests.get(repostbaseurl+str(self.gsid)+'&id='+str(x['id'])+'&page='+str(page))
                while(response.status_code !=200):
                    time.sleep(10)
                    response=requests.get(repostbaseurl+str(self.gsid)+'&id='+str(x['id'])+'&page='+str(page))

                responsejob=json.loads(response.text)
                check=False if responsejob['reposts']==None else False if len(responsejob['reposts'])==0 else True
                time.sleep(0.5)
            self.status['lastrepostcontentid']=x['id']
            open('status.config','w').write(json.dumps(self.status))
            df=pandas.read_json(json.dumps(users_thiscontent))
            df.to_csv(r'111.csv',index= False,mode='a')
            
 
    def getflowusers(self):
        df=pandas.DataFrame(self.contentlist)
        df=df.sort_values(by='id')
        if self.status['lastcommentcontentid']!=0:
            indexnum=df[df['id']==self.status['lastcommentcontentid']]
            df=df.reset_index()
            df=df[df['index']>indexnum] 
            df=df.drop(columns=['index'])
        job=json.loads(df.to_json(orient='records'))
        for x in job:    
            users_thiscontent=[]
            print(flowbaseurl+str(self.gsid)+'&id='+str(x['id'])+'&max_id=0')
            
            response=requests.get(flowbaseurl+str(self.gsid)+'&id='+str(x['id'])+'&max_id=0')
            while(response.status_code !=200):
                time.sleep(10)
                response=requests.get(flowbaseurl+str(self.gsid)+'&id='+str(x['id'])+'&max_id=0')
            responsejob=json.loads(response.text)
           
            self.pushone(responsejob,users_thiscontent)
            while(responsejob['max_id']!=0):
                maxid=responsejob['max_id']
                url=flowbaseurl+str(self.gsid)+'&id='+str(x['id'])+'&max_id='+str(maxid)
                print(url)
                response=requests.get(url)
                while(response.status_code !=200):
                    time.sleep(10)
                    response=requests.get(url)
                responsejob=json.loads(response.text)
                
                self.pushone(responsejob,users_thiscontent)
                time.sleep(0.5)
            self.status['lastcommentcontentid']=x['id']
            open('status.config','w').write(json.dumps(self.status))                
            df=pandas.read_json(json.dumps(users_thiscontent))
            df.to_csv(r'111.csv',index= False,mode='a')
    def pushone(self,jsonobject,users_this):
        for y in jsonobject['root_comments']:
            users_this.append({'id':y['user']['id'],'nick':y['user']['name']})
            root_comment_id=y['id']
            if 'more_info' in y.keys():
                print(flowbaseurl+str(self.gsid)+'&id='+str(root_comment_id)+'&max_id=0&fetch_level=1')
                response_recomment=requests.get(flowbaseurl+str(self.gsid)+'&id='+str(root_comment_id)+'&max_id=0&fetch_level=1')
                while(response_recomment.status_code !=200):
                    time.sleep(10)
                    response_recomment=requests.get(flowbaseurl+str(self.gsid)+'&id='+str(root_comment_id)+'&max_id=0&fetch_level=1')
                response_recomment_job=json.loads(response_recomment.text)
                for z in response_recomment_job['comments']:
                    users_this.append({'id':z['user']['id'],'nick':z['user']['name']})
                
                while(response_recomment_job['max_id']!=0):
                    recomment_maxid=response_recomment_job['max_id']
                    print('回复评论:'+flowbaseurl+str(self.gsid)+'&id='+str(root_comment_id)+'&max_id='+str(recomment_maxid)+'&fetch_level=1')
                    response_recomment=requests.get(flowbaseurl+str(self.gsid)+'&id='+str(root_comment_id)+'&max_id='+str(recomment_maxid)+'&fetch_level=1')
                    while(response_recomment.status_code !=200):
                        time.sleep(10)
                        response_recomment=requests.get(flowbaseurl+str(self.gsid)+'&id='+str(root_comment_id)+'&max_id='+str(recomment_maxid)+'&fetch_level=1')
                    time.sleep(0.5)
                    response_recomment_job=json.loads(response_recomment.text)
                    for z in response_recomment_job['comments']:
                        users_this.append({'id':z['user']['id'],'nick':z['user']['name']})
            else:
                for z in y['comments']:
                    users_this.append({'id':z['user']['id'],'nick':z['user']['name']})

            
        
        