
# The code acquires a csv file from an informational postings site for a natural gas pipeline.
# Then it performs very basic data manipulation and writes the final output to a csv file.
# The pipeline posts five data updates (cycles) per day. The data is posted at specific times per day and is time sensitive.
# For this reason the script is designed to be run at intervals through the windows task scheduler.
# If the script fails an error is written to a log file and the script makes another attempt in thirty minuets.
#


from selenium import webdriver
import pandas as pd
import time
import os
import logging

# directory where download is stored
downloads_location = r'path to downloads'
error_log_location = r'location for error log'
chrome_drive_location = r'location of chrome driver'

#  URL to be scraped
url = 'https://transmission.wbienergy.com/informational_postings/capacity/operational_capacity_locations.aspx'

# download and retrieve button x-paths
download_button = '//*[@id="btnDownload"]'
retreive_button = '//*[@id="btnRetrieve"]'

# if the acquirer fails, it will wait this long before attempting to acquire another file.
wait_attempt = 1800 # 1800 seconds is 30 minuets

#  ================================================================================================================================================================================
# ============================================================================= WORKING CODE ======================================================================================
#  ================================================================================================================================================================================

# function to acquire the file
def get_website(url, download_button, retrieve_button):

    #  go to website
    chromedriver = chrome_drive_location
    driver = webdriver.Chrome(chromedriver)
    driver.get(url)

    # wait while website loades
    time.sleep(15)

    #  click retrieve button
    driver.find_element_by_xpath(retrieve_button).click()

    # wait for data to load
    time.sleep(5)

    # click download button
    driver.find_element_by_xpath(download_button).click()

    # wait for download to complete
    time.sleep(5)

    # close the webpage
    driver.close()

def main():

    #  x is a flag. Acquire the file while x is false and attempts at acquiring a file are less than 3
    # if there is an error then x remains false and the acquirer makes another attempt in 30 minuets.
    # after file is acquired flip x to true and break the loop.
    # if an error is encountered write a log file, increment attempts by 1 until 3 attempts are made

    x = False
    attemps = 0

    while x == False and attemps in range(0,3):

        try:
            #  run method to acquire the file
            get_website(url, download_button, retreive_button)

            #  flip flag to true and set attempts to 3 to break the loop
            x = True
            attemps = 3

            # read in raw file
            raw = pd.read_csv(downloads_location + '\\' + 'WBI_Tran_Operational_Capacity_Locations.txt', sep='\t')

            # format dates
            raw['Eff Gas Day'] = pd.to_datetime(raw['Eff Gas Day'].astype(str), format='%Y/%m/%d')
            raw['Post Date'] = pd.to_datetime(raw['Post Date'].astype(str), format='%Y/%m/%d')

            # add new column
            raw['TSP Name'] = 'WBI Energy Transmission, Inc.'

            # return cycle and post date for csv name
            post_date = str(raw['Post Date'][0])
            post_date = post_date[:10]
            cycle = str(raw['Cycle'][0])

            # write to csv output
            raw.to_csv(downloads_location + '\\' + 'WBI_Energy_Transmission-' + cycle + '-' + post_date + '.csv', index=False)

            # remove raw file from downloads location
            os.remove(downloads_location + '\\' + 'WBI_Tran_Operational_Capacity_Locations.txt')

        except Exception as error:
            # if an error is encountered:
            # write the log file
            logging.basicConfig(filename=error_log_location + '\\' + 'Error_Log' + '-' + str(time.strftime('%Y-%m-%d-%H%M%S', time.localtime())) + '.txt')
            logger = logging.getLogger(__name__)
            logger.error(error)

            # then sleep for 30 mins before trying again for a file
            # keep x = false and increment attempts by 1. We want to limit the attempts to acquire the file.
            time.sleep(wait_attempt)
            x = False
            attemps += 1

if __name__ == '__main__':
    main()


