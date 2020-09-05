__author__ = 'yon'
import pandas
import requests
import json,random

from multiprocessing import Queue,Process
from collections import defaultdict
from BeautifulSoup import BeautifulSoup,Comment
import time
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def test_jsoner(theurl,thePage):
    foo=thePage
    json_result=defaultdict(dict)
    #Global Rank

    try:
        foo1d=foo.find('section',id="traffic-rank-content").find("span",{"class":"span-col last"}).find("span",{"class":"col-pad","data-cat":"globalRank"})
        j= foo1d.find("div").find("strong")#.text.strip()
        comments = j.findAll(text=lambda text:isinstance(text, Comment))
        [comment.extract() for comment in comments]
        j= foo1d.find("div").find("strong").text.strip()
        json_result[theurl]["globalRank"]=j
    except:
        pass
        #.............................................
    try:
        #Rank in United States
        #localrank=foo.find('section',id="traffic-rank-content").find('span',{"data-cat":"countryRank","class":"col-pad"}).find('div').find('strong',{"class":"metrics-data align-vmiddle"})
        #Country_name=foo.find('section',id="traffic-rank-content").find('span',{"data-cat":"countryRank","class":"col-pad"}).find('h4',{"class","metrics-title"}).find('a').text
        Country_name=foo.find('section',id="traffic-rank-content").find('span',{"data-cat":"countryRank","class":"col-pad"}).find('a').text

        localRank=foo.find('section',id="traffic-rank-content").find('span',{'data-cat':"countryRank","class":"col-pad"}).find('div').find("strong",{"class":"metrics-data align-vmiddle"}).text.strip()

        json_result[theurl]['local_rank']={'country':Country_name,'local_rank':localRank}
    except:
        pass
        #.............................................
        #Country	Percent of Visitors	Rank in Country
    try:
        percent_visitors=[]

        demographic=thePage.find('table',id="demographics_div_country_table").find('tbody')
        for jj in demographic.findAll('tr'):
            if jj.findAll('a'):
                c=jj.findAll('td')
                country=c[0].find('a').text.split(';')[1]
                perc=c[1].find('span').text
                ranks=c[2].find('span').text
                percent_visitors.append({"country":country,"percent_of_visitors":perc,"rank_in_country":ranks})

        json_result[theurl]['visitos_per_country']=percent_visitors
    except:
        pass
        #.............................................
        #How engaged are visitors to facebook.com?
        #id="engagement-content"
    try:
        engage=thePage.find("section",id="engagement-content")
        bounce_percent=engage.find("span",{"data-cat":"bounce_percent"}).find("strong",{"class":"metrics-data align-vmiddle"}).text
        pageviews_per_visitor=engage.find("span",{"data-cat":"pageviews_per_visitor"}).find("strong",{"class":"metrics-data align-vmiddle"}).text

        time_on_site=engage.find("span",{"data-cat":"time_on_site"}).find("strong",{"class":"metrics-data align-vmiddle"}).text
        json_result[theurl]['engagement-content']={"bounce_percent":bounce_percent,"pageviews_per_visitor":pageviews_per_visitor,"time_on_site":time_on_site}
    except:
        pass
        #.............................................
        #Top Keywords from Search Engines
        id="keywords_top_keywords_table"
    try:
        keywords_table=thePage.find('table',id="keywords_top_keywords_table").find('tbody')

        keywords_top_keywords_table=[{m.find('td',{"class":"topkeywordellipsis"}).findAll('span')[1].text:m.find('td',{"class":"text-right"}).text} for m in keywords_table.findAll('tr')]
        json_result[theurl]['top_keywords_from_search_engline']=keywords_top_keywords_table
    except:
        pass
        #.............................................
    try:
        #Search traffic
        #id="keyword-content"
        json_result[theurl]["keyword_content"]=foo.find('span',{"class":"sitemetrics-col"}).find("strong",{"class":"metrics-data align-vmiddle"}).text

    except:
        pass
        #.............................................
        #Upstream Sites, sites visited before coming
    try:

        upstreams=thePage.find('table',id="keywords_upstream_site_table").find('tbody')
        keywords_upstream_site_table=[]
        for m in upstreams.findAll('tr'):
            keywords_upstream_site_table.append({m.find('a').text:m.findAll('td',{"class":"text-right"})[0].find('span').text})
        json_result[theurl]['upstream_sites']=keywords_upstream_site_table
    except:
        pass
        #.......................
        #Total Sites Linking In
    try:

        json_result[theurl]["total_linking_sites"]=thePage.find('section',id="linksin-panel-content").find('span').find('span').text
    except:
        pass
        #..........................................
    try:
        linksin_tables=[]
        linksin_table=thePage.find('table',id='linksin_table')
        for m in linksin_table.find('tbody').findAll('tr'):
            linksin_tables.append(m.findAll('td')[1].find('a').text)

        json_result[theurl]["in_linking_websites"]=linksin_tables
    except:
        pass
        #..........................................
    try:
        #Related Links
        #--------
        related_link_table=[]

        relatedlink=thePage.find('table',id="related_link_table").find('tbody')
        for m in relatedlink.findAll('tr'):
            for q in m.find('td').find('a'):
                related_link_table.append(q)


        json_result[theurl]['related_links']=related_link_table
    except:
        pass
        #..........................................
    try:
        #Other Sites Owned
        oll=[]
        owned_table=thePage.find('table',id="owned_link_table")
        for m in owned_table.findAll('tr'):
            for q in  m.findAll('td'):
                oll.append(q.find('a').text)
        json_result[theurl]["other_sites_owned"]=oll
    except:
        pass
        #..........................................
        #where do visitors go on this site
        #--------
    try:
        subdomain_dic={}
        subdomainRows=thePage.findAll('table',id="subdomain_table")[0].find('tbody')
        for m in subdomainRows.findAll('tr'):
            subdomain_dic[ m.findAll('td')[0].find('span').text]= m.findAll('td')[1].find('span').text
        json_result[theurl]['sub_domains']=subdomain_dic
    except:
        pass
        #............................................
    try:
        #How fast does diretube.com load?
        #Very Slow (5.461 Seconds), 95% of sites are faster.
        #--------
        loadspeed_dic={}
        loadspeed=thePage.find(id="loadspeed-panel-content")
        loadspeed_dic["loadspeed_category"]=loadspeed.find('span').text
        loadspeed_dic["loadspeed_measure"]=loadspeed.find('p').text
        json_result[theurl]['load_speed']=loadspeed_dic
    except Exception:
        pass
    return json_result



def requestAlexa(topURL,proxyQ,pNO,my_fake_ua):
    fake_uas=[]
    """
    try:
        fake_ua_tables = open('real_userAgents.txt')
    except:
        print 'user agent not found'
        return
    for ua in fake_ua_tables:
        ua_dict={}
        ua_dict['User-Agent']=ua.strip()
        fake_uas.append(ua_dict)
    """
    alexa_url = "http://www.alexa.com/siteinfo/"+topURL
    response_status = -1
    maxretries = 10
    myretry=0
    ALEXAresponse=-1
    while response_status != requests.codes.ok and myretry < maxretries:
        time.sleep(2)
        #get one from proxy queue, if ti works put it back to the queue else just throw it
        proxynode = proxyQ.get()
        proxydic={}
        proxydic['http'] = proxynode # "http://"+proxynode
        #print topURL, proxynode
        try:
            ALEXAresponse = requests.get(alexa_url, headers=my_fake_ua, proxies=proxydic)
            response_status=ALEXAresponse.status_code
            #print "!! hi yonas the (url,proxynode) valid :",topURL, proxynode
        except requests.ConnectionError as err:
            #print err,"hi yonas (url,proxynode) NOT valid :",topURL, proxynode
            #print "failed connection",proxynode
            continue
        except Exception:
            #print Exception,  "hi yonas (url,proxynode) NOT valid : (",topURL,',', proxynode,")"
            continue
        if response_status == requests.codes.ok:
            #print "perfecto!"
            proxyQ.put(proxynode)
        myretry+=1
    if ALEXAresponse!=-1:
        alexaSOUP=BeautifulSoup(ALEXAresponse.text)
        prettyjson= test_jsoner(topURL,alexaSOUP)
        alexaFILE=open(topURL.replace(".","_")+'.json','w')
        json.dump(prettyjson,alexaFILE,encoding='utf-8',indent=4)
        alexaFILE.flush()
        alexaFILE.close()
        #print "correctly written",topURL
    return 0





def alexas_reader(queue,proxyQ,pNO):
    ## Read from the queue
    #print 'reader process',pNO
    pages = 0
    #open fake ua and create a user agent list that you can randomly select from
    fake_uas=[]

    try:
        fake_ua_tables = open('real_userAgents.txt')
    except:
        print 'user agent not found'
        return
    for ua in fake_ua_tables:
        ua_dict={}
        ua_dict['User-Agent']=ua.strip()
        fake_uas.append(ua_dict)
    while True:
        msg = queue.get()       # Read from the queue and do nothing
        pages+=1
        # print "going to request for ",msg##time.sleep(2)
        if msg == 'STOP!!':
            print 'i broke';break
        time.sleep(random.randint(1,3))#sleep for five second between requests
        #fetch msg from alexa, and save it as json
        my_fake_ua=fake_uas[random.randint(0,len(fake_uas)-1)]
        flag=requestAlexa(msg.strip(), proxyQ, pNO,my_fake_ua)
        #print 'done request'
def alexa_url_putter(queuei,processes):
    ## Write to the queue
    #open the top_1M.csv file and put it in the db
    from Queue import Queue as localQueue
    samplebreak=0
    tempQ=localQueue()
    print 'initiating url queue'#startflag=0
    with open('top-1m_saafi.csv') as alexa1m:#for a in alexa1m:ss=1
        for ii in alexa1m:
            #if 'xxxbunker' in ii:startflag=1
            #if startflag==0:continue
            tempQ.put(ii.split(",")[1])#;print 'i put',ii
    while not tempQ.empty() :#samplebreak<20 and not tempQ.empty() :
        if not queuei.full():
            queuei.put(tempQ.get())        # Write 'count' numbers into the queue
            #samplebreak+=1
    for j in range(processes+1):queuei.put("STOP!!")
 
def proxyQueue_filler(proxyQueue):
    with open('uniqproxies.txt') as openproxies: # 'openproxies.txt'
        for aline in openproxies:
            proxyQueue.put(aline.strip())
    return

if __name__=='__main__':
    queue = Queue(10000)   # reader() reads from queue
    proxyQueue=Queue(10000)
    # Launch reader() as a separate python process
    _start = time.time()
    print "initiating scrappers ..."
    #worker_queue=[]
    #writers(10, queue)
    scrap_processes=70
    proxyQueue_filler(proxyQueue)
    alexa_url_feeder=Process(target=alexa_url_putter,args=(queue,scrap_processes))
    #alexa_url_feeder.daemon=True

    alexa_url_feeder.start()
    print 'initiating readers ...'
    alexa_readers=[Process(target=alexas_reader, args=(queue, proxyQueue, i)) for i in range(scrap_processes)]
        #worker_queue.append(reader_p)
    for reader_p in alexa_readers:
        #reader_p.daemon=True
        reader_p.start()
    for m in alexa_readers:m.join()         # Wait for the reader to finish
    alexa_url_feeder.join()
    print "Sending %s numbers to Queue() took %s seconds" % (len(alexa_readers), (time.time() - _start))
