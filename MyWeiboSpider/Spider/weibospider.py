import requests
import json
import pandas
import time
contentbaseurl=r'https://api.weibo.cn/2/cardlist?c=android&s=8915076d&from=1098495010&gsid='
repostbaseurl=r'https://api.weibo.cn/2/statuses/repost_timeline?c=android&s=8915076d&from=1098495010&gsid='
flowbaseurl=r'https://api.weibo.cn/2/comments/build_comments?is_show_bulletin=2&c=android&s=8915076d&from=1098495010&gsid='
class Spider():
    '''
    
    '''
    def __init__(self, **kwargs):
        self.uids= kwargs['uids'] if 'uids' in kwargs.keys() else ''
        self.gsid= kwargs['gsid'] if 'gsid' in kwargs.keys() else ''
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
                time.sleep(30)
                response=requests.get(contentbaseurl+str(self.gsid)+'&containerid='+str(containerid)+'&page='+str(page))
            reponsejob=json.loads(response.text)
            
        return res
    def getrepostusers(self):
        for x in self.contentlist:
            page=1   
            actnum=0
            print(repostbaseurl+str(self.gsid)+'&id='+str(id)+'&page='+str(page))
            response=requests.get(repostbaseurl+str(self.gsid)+'&id='+str(id)+'&page='+str(page))
            while(response.status_code !=200):
                time.sleep(30)
                response=requests.get(repostbaseurl+str(self.gsid)+'&id='+str(id)+'&page='+str(page))

            responsejob=json.loads(response.text)
            rawtotalnum=responsejob['total_number']
            actnum=0
            
            while(actnum<rawtotalnum):
                for y in responsejob['reposts']:
                    
                    self.users.append({'id':y['user']['id'],'nick':y['user']['name']})
                page=page+1
                actnum=actnum+len(responsejob['reposts'])
                print(repostbaseurl+str(self.gsid)+'&id='+str(id)+'&page='+str(page))
                response=requests.get(repostbaseurl+str(self.gsid)+'&id='+str(id)+'&page='+str(page))
                while(response.status_code !=200):
                    time.sleep(30)
                    response=requests.get(repostbaseurl+str(self.gsid)+'&id='+str(id)+'&page='+str(page))

                responsejob=json.loads(response.text)
                
 
    def getflowusers(self):
        for x in self.contentlist:        
            print(flowbaseurl+str(self.gsid)+'id='+str(x['id'])+'&max_id=0')
            response=requests.get(flowbaseurl+str(self.gsid)+'id='+str(x['id'])+'&max_id=0')
            while(response.status_code !=200):
                time.sleep(30)
                response=requests.get(flowbaseurl+str(self.gsid)+'id='+str(x['id'])+'&max_id=0')
            responsejob=json.loads(response.text)
            
            pushone(self,responsejob)
            while(responsejob['max_id']!=0):
                maxid=responsejob['max_id']
                url=flowbaseurl+str(self.gsid)+'id='+str(x['id'])+'&max_id='+str(maxid)
                print(url)
                response=requests.get(url)
                while(response.status_code !=200):
                    time.sleep(30)
                    response=requests.get(url)
                responsejob=json.loads(response.text)
                
                pushone(responsejob)
    def pushone(self,json):
        for y in json['root_comments']:
            self.users.append({'id':y['user']['id'],'nick':y['user']['name']})
            root_comment_id=y['id']
            if 'more_info' in y.keys():
                print(flowbaseurl+str(self.gsid)+'id='+str(root_comment_id)+'&max_id=0&fetch_level=1')
                response_recomment=requests.get(flowbaseurl+str(self.gsid)+'id='+str(root_comment_id)+'&max_id=0&fetch_level=1')
                while(response_recomment.status_code !=200):
                    time.sleep(30)
                    response_recomment=requests.get(flowbaseurl+str(self.gsid)+'id='+str(root_comment_id)+'&max_id=0&fetch_level=1')
                for z in response_recomment['comments']:
                    self.users.append({'id':z['user']['id'],'nick':z['user']['name']})
                
                while(response_recomment['max_id']!=0):
                    recomment_maxid=response_recomment['max_id']
                    print('回复评论:'+flowbaseurl+str(self.gsid)+'id='+str(root_comment_id)+'&max_id='+str(recomment_maxid)+'&fetch_level=1')
                    response_recomment=requests.get(flowbaseurl+str(self.gsid)+'id='+str(root_comment_id)+'&max_id='+str(recomment_maxid)+'&fetch_level=1')
                    while(response_recomment.status_code !=200):
                        time.sleep(30)
                        response_recomment=requests.get(flowbaseurl+str(self.gsid)+'id='+str(root_comment_id)+'&max_id='+str(recomment_maxid)+'&fetch_level=1')
                    
                    for z in response_recomment['comments']:
                        self.users.append({'id':z['user']['id'],'nick':z['user']['name']})
            else:
                for z in y['comments']:
                    self.users.append({'id':z['user']['id'],'nick':z['user']['name']})

            
        
        