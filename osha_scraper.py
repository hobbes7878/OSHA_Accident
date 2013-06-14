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


srch_string1="http://www.osha.gov/pls/imis/accidentsearch.search?sic=&sicgroup=&acc_description="
srch_string2 = "&acc_abstract=&acc_keyword=&inspnr=&fatal=fatal&officetype=All&office=All&startmonth=10&startday=30&startyear=2011&endmonth=01&endday=01&endyear=1990&keyword_list=&p_start=&p_finish=0&p_sort=&p_desc=DESC&p_direction=Next&p_show=10000000000000000000000000"

#List of report types to scrape
search_terms_list=["employee","worker","explosion","chemical"]

for search_term in search_terms_list:
    
    print "Query: "+search_term

    search_url=srch_string1+search_term+srch_string2

    writelist=[]


    #open search page, select search form
    page = br.open(search_url)
    soup = BeautifulSoup(page)

    table=soup.find('table',{'cellpadding':'1'})


    header_tds = table.findAll('td',{'class':'blueBoldTen'})
    header_list=[]
    for htd in header_tds:
        header_list.append(str(htd.text))

    write_list=[]

    trs=table.findAll('tr')
    for tr in trs:
        tds = tr.findAll('td',{'class':'blueTen'})
        td_dict={}
        if len(tds)>0:
            for td in tds:
                td_dict[header_list[tds.index(td)]]=str(td.text)


        write_list.append(td_dict)
    try:
        with open("data\\"+date.today().strftime('%Y-%m-%d')+".csv", 'a') as writefile:
            all_keys=[k.keys() for k in write_list]
            writer=csv.DictWriter(writefile, set([key for sub_key_list in all_keys for key in sub_key_list]))
            writer.writeheader()
            writer.writerows(write_list)
    except Exception, e:
        print "       *Error@ "+search_term+": "+str(e)