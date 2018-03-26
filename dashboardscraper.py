import boto3
import os
from selenium import webdriver
import selenium
import time
from selenium.webdriver import Firefox,Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
import random

def login(browser,url,email,password):
    browser.get(url)
    time.sleep(4)
    emailElem = browser.find_element_by_css_selector('input#username')
    emailElem.send_keys(email)
    time.sleep(1)
    pwElem = browser.find_element_by_css_selector('input#password')
    pwElem.send_keys(password)
    time.sleep(1)
    signin_button=browser.find_element_by_css_selector("button.flat.primary.action")
    signin_button.click()
    time.sleep(4)

def navigate_to_timesheet(browser,url,email,password):
    login(browser,url,email,password)
    report_more=browser.find_element_by_xpath("//div[@id='TT_reports_shortcut']\
                                          /span[@class='flyout_text more_arrow_label']")
    report_more.click()
    time.sleep(1)
    logging_auditing=browser.find_element_by_xpath("//div[@id='wwTT_reports']/div[@id='report_menu_container']/div/div[@id='section_header_logs']")
    logging_auditing.click()
    time.sleep(1)
    timesheet_log=browser.find_element_by_xpath("//div[@id='wwTT_reports']/div[@id='report_menu_container']/div/div[@id='section_links_logs']\
        /ul/li[@id='main_menu_time_log']")
    timesheet_log.click()


def refresh_page(browser):
    browser.refresh()
    report_more=browser.find_element_by_xpath("//div[@id='TT_reports_shortcut']\
                                      /span[@class='flyout_text more_arrow_label']")
    report_more.click()
    time.sleep(2)
    logging_auditing=browser.find_element_by_xpath("//div[@id='wwTT_reports']/div[@id='report_menu_container']/div/div[@id='section_header_logs']")
    logging_auditing.click()
    time.sleep(1)
    timesheet_log=browser.find_element_by_xpath("//div[@id='wwTT_reports']/div[@id='report_menu_container']/div/div[@id='section_links_logs']\
        /ul/li[@id='main_menu_time_log']")
    timesheet_log.click()


def find_all_date_files(browser):
    #finds all filenames but need to transformed to lookup key
    attempts=0
    while attempts<5:
        try:
            time.sleep(random.randint(2,7))
            datebox=browser.find_element_by_css_selector('#addon_reports_builder_pay_period_select')
            optiontags=datebox.find_elements_by_css_selector('option')
            break
        except selenium.common.exceptions.NoSuchElementException as error:
            attempt+=1
            refresh_page(browser)
            #try again to get the optiontags
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
    year=origdate[11:15]
    lookupkey=startdate+enddate+','+' '+ year
    return lookupkey

def find_selector(browser):
    attempts=0
    while attempts<5:
        try:
            time.sleep(random.randint(2,7))
            mySelect = Select(browser.find_element_by_id("addon_reports_builder_pay_period_select"))
            break
        except selenium.common.exceptions.NoSuchElementException as error:
            attempt+=1
            refresh_page(browser)
            mySelect = Select(browser.find_element_by_id("addon_reports_builder_pay_period_select"))
    return mySelect

def select_file(lookupkey, mySelect):
    mySelect.select_by_visible_text(lookupkey)

def click_download(browser):
    dl_button=browser.find_element_by_css_selector('button#addon_reports_builder_formsubmit_download_sql')
    dl_button.click()

def dl_files(optiontags, browser):
    #inserted to allow many tries for the find_element_by_id
    mySelect=find_selector(browser)
    #currently 108 timesheet logs
    for i in range(len(optiontags)):
        origdate=find_origdate(i, optiontags)
        lookupkey=make_lookup_key(origdate)
        select_file(lookupkey, mySelect)
        click_download(browser)
        print('downloading {}'.format(i))
        time.sleep(random.randint(5,10))


def uploadfile_tobucket(filename):
    #need to make directory and organize the files in one place
    bucket_name='capstone-timesheet-data'
    foldername='timesheetdata'

    s3=boto3.client("s3")
    bucketloc='s3://{}/{}'.format(bucket_name, foldername)
    aws_base_command='aws s3 sync {}/{}'.format(foldername,filename)
    os.system(aws_base_command+" {}".format(bucketloc))

if __name__ == '__main__':
    #log in and select to get the editlog box open
    url='https://capstonesolutions.tsheets.com/page/login'
    email=os.environ['CAPSTONE_EMAIL']
    password=os.environ ['CAPSTONE_PASS']
    options = webdriver.FirefoxOptions()
    options.add_argument("download.default_directory=/home/ubuntu/logdownloads")
    options.add_argument("--headless")

    profile = webdriver.FirefoxProfile()
    profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/csv")

    browser = Firefox(firefox_options=options, firefox_profile=profile)
    navigate_to_timesheet(browser,url,email,password)

    #once log box is open
    optiontags=find_all_date_files(browser)

    #enter in pages
    dl_files(optiontags[82:], browser)
