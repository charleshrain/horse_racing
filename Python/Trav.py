from selenium import webdriverfrom selenium.webdriver.chrome.options import Optionsimport pandas as pdoptions = Options()options.headless = Trueoptions.add_argument("--window-size=1920,1200")driver = webdriver.Chrome('/Users/charlesrain/IdeaProjects/Trav/chromedriver-2')driver.get('https://www.atg.se/spel/2020-08-01/V75/rattvik')driver.find_element_by_xpath('//*[@id="onetrust-accept-btn-handler"]').click()driver.find_element_by_xpath('//*[@id="main"]/div[3]/div[2]/div/div/div/div/div/div[7]/div[1]/div/div[1]/div[1]/div[2]/div/div/button[2]').click()for i in [1,2,3,4,8,9,11,12]:    link = '/html/body/div[5]/div/div/div/div/div/div[2]/div/div[1]/ul/li['+str(i)+']/div/span[2]'    driver.find_element_by_xpath(link).click()for i in [4,5,6,8]:    link = '/html/body/div[5]/div/div/div/div/div/div[2]/div/div[2]/ul/li['+str(i)+']/div/span[2]'    driver.find_element_by_xpath(link).click()driver.find_element_by_xpath('/html/body/div[5]/div/div/div/div/div/div[2]/div/div[3]/button[2]').click()driver.find_element_by_xpath('//*[@id="main"]/div[3]/div[2]/div/div/div/div/div/div[7]/div[1]/div/table/thead/tr/th[1]/span[2]').click()driver.find_element_by_xpath('//*[@id="main"]/div[3]/div[2]/div/div/div/div/div/div[7]/div[1]/div/table/thead/tr/th[1]/span[2]').click()tabs = pd.read_html(driver.page_source)driver.quit()# TO DO# Remove/add columns# Add row numbers# Delete rows containing strike thtough text# split 2020: if leftmost number is larger then two, calculate Seger% for year# import kuskrank table# split rekordtid, add field for if rcord was on current distance# scrape "oversikt"# add metrics for comparing position to position of favorite# SQL class# Machine Learning class# prediction class ??# graphics ?