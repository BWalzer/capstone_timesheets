from selenium.webdriver import Firefox,Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import time
import boto3
import os

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




if __name__ == '__main__':
    url='https://capstonesolutions.tsheets.com/page/login'
    email=os.environ['CAPSTONE_EMAIL']
    password=os.environ ['CAPSTONE_PASS']
    browser = Firefox()
    navigate_to_timesheet(browser,url,email,password)
