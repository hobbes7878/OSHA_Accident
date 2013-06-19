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
conn=psycopg2.connect("dbname=OSHA user=postgres password=tomcat")
cur=conn.cursor()

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



##############################################################
##Functionals
##############################################################
def puncless_string(a_string):
    an_string = ''.join(char for char in a_string if char not in string.punctuation)
    return an_string


def pg_express_writer(table , diction):
    #Get column names from database
    SQL= 'SELECT * FROM \"'+table+'\";'
    cur.execute(SQL)
    col_names = [desc[0] for desc in cur.description]
    #Get column names from dict
    dict_keys=diction.keys()
    ### Create any new columns in database ###
    for dk in dict_keys:
        if dk in col_names:
            pass
        else:
            if len(dk.strip())>0:
                SQL='ALTER TABLE \"'+table+'\" ADD \"'+str(dk)+'\" varchar;'
                cur.execute(SQL)
                conn.commit()
            else:
                pass

    ### Write dict to database ###
    SQL='INSERT INTO \"'+table+'\" (' + str(diction.keys()).strip("[").strip("]").replace("'","\"") + ') VALUES ( ' +str(diction.values()).strip("[").strip("]") + ');' 
    cur.execute(SQL)
    conn.commit()


##############################################################
##To Dos
##############################################################

cur.execute('SELECT \"Summary Nr\" FROM acc_abstracts;')
narratives_todo=cur.fetchall()

cur.execute('SELECT \"accid\" FROM acc_narratives;')
narratives_scraped=cur.fetchall()

summary_no_list = [num for num in narratives_todo if num not in narratives_scraped]

##############################################################
##Scrape accident narratives
##############################################################
i=1
stage_stamp=time.clock()
times=[]
for num in summary_no_list:
    

    no = str(num[0]) #convert from tuple
    search_url="http://www.osha.gov/pls/imis/accidentsearch.accident_detail?id="+no

    #chatter
    times.append(time.clock()-stage_stamp)
    stamp="Run: "+str(round(time.clock()-stage_stamp,1))+" secs. AVG: "+str(round(sum(times)/len(times),2))
    sys.stdout.write(stamp)
    stage_stamp=time.clock()
    sys.stdout.write(" > "+no)
    
    print " >> "+str(i)+" : "+str(len(summary_no_list))
    i+=1

    while True:
        try:
            page = br.open(search_url)
        except Exception, e:
            sys.stdout.write("#")
            print "Error:",e
            continue
        break
            




    soup = BeautifulSoup(page)

    write_dict={'accid' : no}

    tables = soup.findAll ('table',{ 'width':'99%','cellpadding':'3','border':'0'})
    for table in tables:

        headers = table.findAll('td',{'class':'blueBoldTen'})
        header_list = [puncless_string(str(h.text)).replace(" ","_").lower() for h in headers]
        if len(header_list)>0:
            data = table.findAll('td',{'class':'blueTen'})
            inspection_dict={}
            data_list = [str(d.text).replace("'"," ") for d in data]
            inspection_dict=dict(zip(header_list, data_list))
        else: #narrative has no heading
            data = table.findAll('td',{'class':'blueTen'})
            inspection_dict={}
            inspection_dict['narrative']=[str(d.text).replace("'"," ") for d in data][0]


        write_dict.update(inspection_dict)

    tables = soup.findAll ('table',{ 'width':'99%','cellpadding':'1','border':'0'})
    for table in tables:

        headers = table.findAll('td',{'class':'blueBoldTen'})
        header_list = [puncless_string(str(h.text)).replace(" ","_").lower() for h in headers]
        data = table.findAll('td',{'class':'blueTen'})
        data_list = [str(d.text).replace("'"," ") for d in data]
        inspection_dict={}
        inspection_dict=dict(zip(header_list, data_list))

        write_dict.update(inspection_dict)

        
    try: #kill null header
        write_dict['null']=write_dict.pop('') 
    except:
        continue
    
    #kill null keys
    db_dict=write_dict.copy()
    for key in write_dict: 
        if write_dict[key]=="":
            db_dict.pop(key)

    try:
        pg_express_writer(diction=db_dict, table="acc_narratives")
    except Exception, e:
        print "       *Error@ "+no+": "+str(e)


