import datetime
import os
import random
import time
from urllib.parse import urlparse

import dateparser
import pandas as pd
import requests
import utils.utils as u
from bs4 import BeautifulSoup
from loguru import logger
from selenium import webdriver
from selenium.webdriver.support.ui import Select

# this is the wrapper filefor the scraper project

class Scrape:
    
    def __init__(self, config):
        self.gl = container()  # global
        self.sc = container()  # scraping
        self.pr = container()  # processing
        self.an = container()  # analysis

        self.gl___root = config['global']['root']
        self.gl___urls = config['global']['urls']
        self.gl___datapath = config['global']['datapath']


        self.sc___driverpath = config['scrape']['driverpath']
        self.sc___driver = webdriver.Firefox(executable_path=self.sc___driverpath)
        self.sc___lib_out = self.gl___datapath / config['scrape']['lib_out']
        self.sc___specific = config['scrape']['specific_configs']

        self.scrapes = {}



    def scrape_wrap(self):
        # run scraping jobs

        for url in self.gl___urls:
            scr = Scraper(self, url)
            scr.scrape()

            if len(scr.result) > 0:
                scr.results_df = pd.concat(scr.result)
                scr.vault = pd.concat([scr.vault, scr.results_df])
                scr.vault.to_csv(scr.vaultpath, index=False)
            logger.info(f'scraped {len(scr.result)} new ads from {scr.domain}, saved in {scr.vaultpath}')

        self.sc___driver.quit()
        logger.info('scraping completed')
        
class container:
    # dummy class for storing attributes
    pass