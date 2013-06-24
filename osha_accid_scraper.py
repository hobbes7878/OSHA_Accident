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
conn=psycopg2.connect("dbname=osha user=postgres password=dmndata")
cur=conn.cursor()

#####################################################
## Mechanize Presets ## Nevermine these..
#####################################################_

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

cur.execute('SELECT \"activityno\" FROM \"activity_nos_from_1990\";')
narratives_td_temp=cur.fetchall()
narratives_td=[tup[0] for tup in narratives_td_temp]
print len(narratives_td)

cur.execute('SELECT \"insp_num\" FROM \"acc_id_nums\";')
narratives_scraped_temp=cur.fetchall()
narratives_scraped=[tup[0] for tup in narratives_scraped_temp]
print len(narratives_scraped)




narratives_todo=list(set(narratives_td)^set(narratives_scraped))
print "To Do: "+ str(len(narratives_todo))


##############################################################
##Scrape accident narratives
##############################################################


i=1
stage_stamp=time.clock()
times=[]


for num in narratives_todo[40000:]:
    

    no = str(num) #convert from decimal

    search_url="http://www.osha.gov/pls/imis/establishment.inspection_detail?id="+no


    #chatter
    times.append(time.clock()-stage_stamp)
    stamp="Run: "+str(round(time.clock()-stage_stamp,1))+" secs. AVG: "+str(round(sum(times)/len(times),2))
    sys.stdout.write(stamp)
    stage_stamp=time.clock()
    sys.stdout.write(" > "+no)
    print " >> "+str(i)+" : "+str(len(narratives_todo))
    i+=1

    #open page
    while True:
        try:
            page = br.open(search_url)
        except Exception, e:
            sys.stdout.write("#")
            print "Error:",e
            continue
        break
            

    soup = BeautifulSoup(page)

    write_dict={"insp_num":no}

    # tables = soup.findAll ('table',{ 'width':'99%','cellpadding':'2','cellspacing':'2','border':'0'})
    # for table in tables:
    tds=soup.findAll('td',{'class':'blueTen'})
    for td in tds:
        
        if re.search(r'Summary Nr: (\d{4,})',td.text):
            match=re.search(r'Summary Nr: (\d{4,})',td.text)
            write_dict["acc_id"]=str(match.group(1))

        if re.search(r'Event: (\d{2}/\d{2}/\d{4})',td.text):
            match=re.search(r'Event: (\d{2}/\d{2}/\d{4})',td.text)
            write_dict["event_date"]=str(match.group(1))
    
    if len(write_dict) ==1:
        print "   Matchless"
        error_file=open('error_file.txt','a')
        error_file.write(no)
        error_file.write("\r\n")
    else:
        try:
            pg_express_writer(diction=write_dict, table="acc_id_nums")
        except Exception, e:
            print "       *Error@ "+no+": "+str(e)


