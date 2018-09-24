#!/usr/bin/env python
import datetime
import hashlib
import base64
import random
import string
import requests
import json
import pandas as pd
from datetime import date, timedelta

def get_headers():
    # Get the current Created timestamp in ISO8601 format.
    current = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    # Get a randomly generated 16 byte Nonce formatted as 32 hexadecimal characters.
    rand_str = lambda n: ''.join([random.choice(string.lowercase) for i in range(n)])
    # Now to generate a random string of length 10
    s = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(32))

    nonce = s

    #rand = os.urandom(16)
    #nonce = ''.join('{:02x}'.format(x) for x in rand)

    #Concatenate the following three values in this order: nonce, timestamp, secret.
    conc = nonce + current + secret

    # Calculate the SHA1 hash value of the concatenated string, and make sure this value is in hexadecimal format!
    sha_1 = hashlib.sha1((conc).encode("utf-8")).hexdigest()
    
    # Apply a BASE64 encoding to the resulted hash to get the final PasswordDigest value.
    passwordDigest = base64.b64encode(sha_1.encode()).decode()
  
    # X-WSSE
    get_xwsse = "UsernameToken Username=\"{0}\",PasswordDigest=\"{1}\",Nonce=\"{2}\",Created=\"{3}\"".format(username,passwordDigest,nonce,current)
    headers = {"X-WSSE": get_xwsse}
    return headers

def get_emailCategory():
    # Email Category
    payload = {}
    res = requests.get("https://api.emarsys.net/api/v2/emailcategory", json=payload, headers=get_headers())
    
    if res.status_code != 200:
          resData = json.loads(res.text)
          print('Email Category, status_code: ' + str(res.status_code) + ', replyCode: ' + str(resData['replyCode']) + ', replyText: ' + str(resData['replyText']))
    
    if res.status_code == 200:
        resData = json.loads(res.text)
        campaignCategory = pd.DataFrame.from_dict(resData['data'])
    return campaignCategory

def get_listEmailCampaign(dateStart):
    # List Email Campaigns  
    payload = {}
    res = requests.get("https://api.emarsys.net/api/v2/email/?status=3&launched=1&showdeleted=0&fromdate=" + dateStart, json=payload, headers=get_headers())
    
    if res.status_code != 200:
      resData = json.loads(res.text)
      print('List Email Canmpaign, status_code: ' + str(res.status_code) + ', replyCode: ' + str(resData['replyCode']) + ', replyText: ' + str(resData['replyText']))
      
    
    if res.status_code == 200:
      resData = json.loads(res.text)
      listEmailCampaign = pd.DataFrame.from_dict(resData['data'])
      listEmailCampaign = listEmailCampaign[['created', 'email_category', 'id', 'name', 'subject']]
  
    #список рассылок по которым будем собирать статистику
    filter_list = listEmailCampaign[listEmailCampaign['created'] > dateStart]
    return filter_list

def get_emailStat(filter_list):
    # Summary Stat
    payload = {}
  
    appendedStat = []
    for emailId in filter_list['id']:
        res = requests.get("https://api.emarsys.net/api/v2/email/" + str(emailId) + "/responsesummary", json=payload, headers=get_headers())
        print(str(emailId) + ' ' + str(res.status_code))
  
        if res.status_code != 200:
          resData = json.loads(res.text)
          print('emailId: ' + str(emailId) + ', status_code: ' + str(res.status_code) + ', replyCode: ' + str(resData['replyCode']) + ', replyText: ' + str(resData['replyText']))
          
            
        if res.status_code == 200:
            resData = json.loads(res.text)
            statData = pd.DataFrame(resData['data'], index=[0])
            statData.insert(0, 'emailId', emailId)
            appendedStat.append(statData)
            emailStat = pd.concat(appendedStat)
  
    emailStat = emailStat.merge(filter_list, left_on='emailId', right_on='id', how='left')
    emailStat['created'] = emailStat['created'].astype('datetime64[ns]')
    emailStat['date'] = [d.date() for d in emailStat['created']]
    emailStat['time'] = [d.time() for d in emailStat['created']]
    emailStat = emailStat.drop(columns=['created'])
    return emailStat

def campaignCategory_toBQ(campaignCategory,project_id,dataset):
    # Выгружаем в BQ
    campaignCategory.to_gbq(
        destination_table=dataset + '.category',
        project_id=project_id,
        if_exists='replace'
    )

def emailStat_toBQ(emailStat,project_id,dataset):
    days = emailStat['date'].unique()
    for d in days:
      emailStatTemp = emailStat[emailStat['date'] == d]
      emailStatTemp.to_gbq(
          destination_table=dataset + '.emailStat_'+str(d).replace('-', ''),
          project_id=project_id,
          if_exists='replace'
        )
      print(d, 'uploaded')

def main():
  emailCategory = get_emailCategory()
  print('category downloaded')
  listEmailCampaign = get_listEmailCampaign(dateStart)
  print('listEmailCampaign downloaded')
  emailStat = get_emailStat(listEmailCampaign)
  print('emailStat downloaded')
  campaignCategory_toBQ(emailCategory,project_id,dataset)
  print('campaignCategory_toBQ uploaded to BQ')
  emailStat_toBQ(emailStat,project_id,dataset)
  print('emailStat uploaded to BQ')
  


username = '*****' #emarsys account
secret = '****' #emarsys secret key
project_id='*****' # big query project
dataset='email' #big query dataset

#дата с которой начинаем забирать данные
dateStart = date.today() - timedelta(30)
dateStart = dateStart.strftime('%Y-%m-%d')
#dateStart = "2016-03-29"
  
try:
  main()
except  Exception as e:
  print('error  ' + str(e), chat)
    
