import datetime
import os
import pickle
from pathlib import Path

from bs4 import BeautifulSoup
from loguru import logger
from selenium import webdriver

from utils.utils import *


def scrape():
    home_url = "https://www.ronorp.net/zuerich/immobilien/wg-zuerich.1220?s=1"
    logger.info('starting scraping')
    driver = webdriver.Firefox(
        executable_path=Path(r'C:\Users\Bo-user\Downloads\geckodriver-v0.27.0-win64') / 'geckodriver.exe')
    driver.get(home_url)
    previews = []
    content = driver.page_source  # this is one big string of webpage html
    soup = BeautifulSoup(content)
    logger.info('soup made')

    elementlist = soup.findAll(attrs={'class': 'ronorp-inserate'})


class Scraper:
    """
    my scraper class object
    """

    def __init__(self, config):
        self.start_url = config['urls'][0]
        self.driverpath = config['driverpath']
        self.domain = url_to_domain(self.start_url)
        self.results = {}
        self.root = config['root']
        self.datapath = self.root / 'data'

    def scrape(self):
        # for domain in self.domains:
        if self.domain == 'ronorp':
            self.scrape_ronorp()
        else:
            print(f'There is no scrape() for domain {self.domain}.')

    def scrape_ronorp(self):
        self.soupfile = self.datapath / 'soup_ronorp.pk'
        soup = self.make_or_get_soup()
        results = {}
        logger.info('getting 1st lvl items')
        for element in soup.find_all(attrs={'class': 'title_comment colored'}):
            try:
                results.update({element.a['alt']: element.a.attrs})
            except KeyError:
                print('ad')
        self.results = results


    def make_or_get_soup(self):
        # get the soup once per 24h. if ran again, load from disk
        if Scraper.time_since_modified(self.soupfile) < 24:
            with open(self.soupfile, 'rb') as f_in:
                soup_str = pickle.load(f_in)
                soup = BeautifulSoup(soup_str, 'lxml')
            logger.info(f'loaded soup from {self.soupfile}')
        else:
            logger.info('starting scraping')
            driver = webdriver.Firefox(executable_path=self.driverpath)
            driver.get(self.start_url)
            content = driver.page_source  # this is one big string of webpage html
            soup = BeautifulSoup(content)
            logger.info('soup made')
            with open(self.soupfile, 'wb') as f_out:
                pickle.dump(str(soup), f_out)
        return soup

    def time_since_modified(fname):
        if os.path.isfile(fname):
            ts = datetime.datetime.fromtimestamp(os.stat(fname).st_mtime)
            return (datetime.datetime.now() - ts).seconds / 3600.0
        else:
            return 100.0
