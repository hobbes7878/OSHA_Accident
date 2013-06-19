import psycopg2
import mechanize
import cookielib
import urllib2
import csv
import re
from BeautifulSoup import BeautifulSoup
import time
from datetime import date, timedelta
import math
import random
import sys
import string
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta


#####################################################
## Database Connection ##
#####################################################
#conn=psycopg2.connect("dbname=osha user=postgres password=dmndata")
#cur = conn.cursor()



#####################################################
## Mechanize Presets ## Nevermine these..
#####################################################

br = mechanize.Browser()

###Cookie Jar###
cj = cookielib.LWPCookieJar()
br.set_cookiejar(cj)

###Browser options###
br.set_handle_equiv(True)
br.set_handle_gzip(True)
br.set_handle_redirect(True)
br.set_handle_referer(True)
br.set_handle_robots(False)

##Follows refresh 0 but not hangs on refresh > 0##
br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1)

###Mechanize Debugging###
#~ br.set_debug_http(True)
#~ br.set_debug_redirects(True)
#~ br.set_debug_responses(True)

###User-Agent###
br.addheaders = [('User-agent', 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')]

###################################################
## Scrape Summary Nos and Abstracts
###################################################

srch_string1="http://www.osha.gov/pls/imis/accidentsearch.search?sic=&sicgroup=&acc_description="
srch_string2 = "&acc_abstract=&acc_keyword=&inspnr=&officetype=All&office=All&startmonth=10&startday=30&startyear=2011&endmonth=01&endday=01&endyear=1990&keyword_list=&p_start=&p_finish=0&p_sort=&p_desc=DESC&p_direction=Next&p_show=100000000000000000000000000000000000000000000000000000000000"

#List of report types to scrape
search_terms_list=["employee","worker","explosion","chemical","laborer","killed","dies","died","injured","injure","hospital"]
mined_terms=[]

write_list=[] #list to write csv of abstracts

summary_no_list=[] #list just for summary nos


def scrape(search_term):
    print "Query: "+search_term
    try: 
        search_url=srch_string1+search_term+srch_string2
        #open search page, select search form
        page = br.open(search_url)
        soup = BeautifulSoup(page)
        table=soup.find('table',{'cellpadding':'1'})
        header_tds = table.findAll('td',{'class':'blueBoldTen'})
        header_list=[]
        for htd in header_tds:
            header_list.append(str(htd.text))
        trs=table.findAll('tr')
        terms=[]
        for tr in trs:
            tds = tr.findAll('td',{'class':'blueTen'})
            td_dict={}
            if len(tds)>0:
                for td in tds:
                    td_dict[header_list[tds.index(td)]]=str(td.text)
                terms += td_dict["Event Description"].split()
                write_list.append(td_dict)
    except:
        terms=[]
    return terms

def nopunc(a_list):
    a_list = [''.join(char for char in stringy if char not in string.punctuation) for stringy in a_list if ''.join(char for char in stringy if char not in string.punctuation) not in search_terms_list]
    return a_list


for search_term in search_terms_list:
    mined_terms+=scrape(search_term)

no_punc_list = nopunc(mined_terms)


print "Mined Dups: "+str(len(no_punc_list))
mined_search=list(set(no_punc_list))
print "Mined: "+str(len(mined_search))
i=1
for search_term in mined_search:
    print "   "+str(i)+" : "+str(len(mined_search))
    i+=1
    scrape(search_term)



print "Dups: " + str(len(write_list))
#list of unique dicts
csv_list={v['Summary Nr']:v for v in write_list if len(v)>0}.values()
print "Unique: " + str(len(csv_list))

###write to CSV
try:
    with open("data\\"+date.today().strftime('%Y-%m-%d')+"_Numbers.csv", 'ab') as writefile:
        all_keys=[k.keys() for k in csv_list]
        writer=csv.DictWriter(writefile, set([key for sub_key_list in all_keys for key in sub_key_list]))
        writer.writeheader()
        writer.writerows(csv_list)
except Exception, e:
    print "       *Error@ "+search_term+": "+str(e)



for list_dict in csv_list:
    summary_no_list.append(list_dict["Summary Nr"])

print "No List: " + str(len(summary_no_list))


##############################################################
##Scrape accident narratives
##############################################################
csv_list=[]

i=1
for no in summary_no_list:
    search_url="http://www.osha.gov/pls/imis/accidentsearch.accident_detail?id="+no
    print ">> "+str(i)+" : "+str(len(summary_no_list))
    i+=1

    page = br.open(search_url)
    soup = BeautifulSoup(page)

    write_dict={}

    tables = soup.findAll ('table',{ 'width':'99%','cellpadding':'3','border':'0'})
    for table in tables:

        headers = table.findAll('td',{'class':'blueBoldTen'})
        header_list = [str(h.text) for h in headers]
        if len(header_list)>0:
            data = table.findAll('td',{'class':'blueTen'})
            inspection_dict={}
            data_list = [str(d.text) for d in data]
            inspection_dict=dict(zip(header_list, data_list))
        else: #narrative has no heading
            data = table.findAll('td',{'class':'blueTen'})
            inspection_dict={}
            inspection_dict['Narrative']=[str(d.text) for d in data][0]


        write_dict.update(inspection_dict)

    tables = soup.findAll ('table',{ 'width':'99%','cellpadding':'1','border':'0'})
    for table in tables:

        headers = table.findAll('td',{'class':'blueBoldTen'})
        header_list = [str(h.text) for h in headers]
        data = table.findAll('td',{'class':'blueTen'})
        data_list = [str(d.text) for d in data]
        inspection_dict={}
        inspection_dict=dict(zip(header_list, data_list))

        write_dict.update(inspection_dict)

    csv_list.append(write_dict)

    time.sleep(random.randint(0,1))

###write to CSV
try:
    with open("data\\"+date.today().strftime('%Y-%m-%d')+"_Narratives.csv", 'ab') as writefile:
        all_keys=[k.keys() for k in csv_list]
        writer=csv.DictWriter(writefile, set([key for sub_key_list in all_keys for key in sub_key_list]))
        writer.writeheader()
        writer.writerows(csv_list)
except Exception, e:
    print "       *Error@ "+search_term+": "+str(e)

