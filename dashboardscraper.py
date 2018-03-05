import boto3
import os
from selenium import webdriver
import selenium
import time
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select



def find_all_date_files():
    #finds all filenames but need to transformed to lookup key
    datebox=browser.find_element_by_css_selector('#addon_reports_builder_pay_period_select')
    optiontags=datebox.find_elements_by_css_selector('option')
    return optiontags

def find_origdate(num, optiontags):
    origdate=optiontags[num].get_attribute('value')
    return origdate

def make_lookup_key(origdate):
    #convert origdate to actual lookup key to select
    startdate=origdate[5:7]+'/'+origdate[8:10]
    enddate=' '+'-'+' '+origdate[16:18]+'/'+origdate[19:21]
    year=origdate[0:4]
    lookupkey=startdate+enddate+','+' '+ year
    return lookupkey

def select_file(lookupkey):
    mySelect = Select(browser.find_element_by_id("addon_reports_builder_pay_period_select"))
    mySelect.select_by_visible_text(lookupkey)

def click_download():
    dl_button=browser.find_element_by_css_selector('button#addon_reports_builder_formsubmit_download_sql')
    dl_button.click()


def dl_all_files():
    #currently 106 timesheet logs
    optiontags=find_all_date_files
    for i in range(len(optiontags)):
        origdate=find_origdate(i, optiontags)
        lookupkey=make_lookup_key(origdate)
        select_file(lookupkey)
        click_download


def uploadfile_tobucket(filename):
    #need to make directory and organize the files in one place

    bucket_name='capstone-timesheet-data'
    foldername='timesheetdata'

    s3=boto3.client("s3")
    bucketloc='s3://{}/{}'.format(bucket_name, foldername)
    aws_base_command='aws s3 sync {}/{}'.format(foldername,filename)

    os.system(aws_base_command+" {}".format(bucketloc))
