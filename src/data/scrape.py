import datetime
import os
import pickle
from pathlib import Path
import re

import dateparser
import pandas as pd
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
        self.driver = webdriver.Firefox(executable_path=self.driverpath)

    def scrape(self):
        # for domain in self.domains:
        if self.domain == 'ronorp':
            self.scrape_ronorp()
        else:
            print(f'There is no scrape() for domain {self.domain}.')

    def scrape_ronorp(self):
        # the scraper specific for ronorp.net
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

        for key, val in self.results.items():
            self.scrape_ronorp_ad(key)

        self.extract_details()
        self.save_to_csv()

    def extract_details(self):
        # the details are as of yet only a bunch of string. convert them to meaningful content
        for key, val in self.results.items():
            regex = {r'category1': r'Biete \/ Suche \/ Tausche: (.+?):',
                     r'bid_ask': r'Biete \/ Suche \/ Tausche: .+?: (.+?) ',
                     r'rent_buy': r'Biete \/ Suche \/ Tausche: (.+?):',
                     r'rooms': r'Zimmer: ([^a-zA-Z ]+)',
                     r'cost': r'Kosten: ([^a-zA-Z ]+)',
                     r'address': r'Adresse: (.*?)Kontakt',
                     r'duration': r'Vertragsart: (.*?) ',
                     }
            d = {}
            for k, v in regex.items():
                prop = re.search(v, self.results[key]['details'])
                if prop is not None:
                    prop = prop.groups(0)[0]
                else:
                    prop = 'unknown'
                d[k] = prop
            self.results[key].update(d)

    def save_to_csv(self):
        df_file = self.datapath / 'scraper_df.csv'
        pd.DataFrame(self.results).transpose().to_csv(df_file)

    def scrape_ronorp_ad(self, key):
        # the scraper for an individual ad, given its url
        logger.info(f'scraping ad: {key}')
        self.driver.get(self.results[key]['href'])
        content = self.driver.page_source  # this is one big string of webpage html
        soup = BeautifulSoup(content)

        d = dict()
        # d['owner'] = soup.find(attrs={'class': 'avatar'}).img['title']
        d['owner'] = soup.find(attrs={'class': 'user_info'}).find('div', attrs={'class': 'name'})['title']
        d['timestamp'] = dateparser.parse(soup.find('div', attrs={'class': 'pull-left'}).string)
        d['text'] = soup.find('p', attrs={'class': 'text_comment'}).get_text()

        details = soup.find('div', attrs={'class': 'detail_block'})
        d['details'] = html_clean_1(details.get_text())

        self.results[key].update({k: html_clean_1(v) for k, v in d.items()})

    def make_or_get_soup(self):
        # get the soup once per 24h. if ran again, load from disk
        if Scraper.time_since_modified(self.soupfile) < 24:
            with open(self.soupfile, 'rb') as f_in:
                soup_str = pickle.load(f_in)
                soup = BeautifulSoup(soup_str)
            logger.info(f'loaded soup from: {self.soupfile}')
        else:
            logger.info('starting scraping')

            self.driver.get(self.start_url)
            content = self.driver.page_source  # this is one big string of webpage html
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
