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


class Scrapewrapper:
    # a wrapper around individual scrape instances

    def __init__(self, config):
        self.driverpath = config['driverpath']
        self.driver = webdriver.Firefox(executable_path=self.driverpath)
        self.root = config['root']
        self.datapath = config['datapath']
        self.vaultpath = self.datapath / config['vault']
        self.scrapes = {}
        self.urls = config['urls']
        self.specific = config['specific_configs']

    def scrape_wrap(self):
        # run scraping jobs

        for url in self.urls:
            scr = Scraper(self, url)
            scr.scrape()

            if len(scr.result) > 0:
                scr.results_df = pd.concat(scr.result)
                scr.vault = pd.concat([scr.vault, scr.results_df])
                scr.vault.to_csv(scr.vaultpath, index=False)
            logger.info(f'scraped {len(scr.result)} new ads from {scr.domain}, saved in {scr.vaultpath}')

        self.driver.quit()
        logger.info('scraping completed')


class Scraper(Scrapewrapper):
    """
    my scraper class object
    """

    def __init__(self, wrapper, url):
        self.start_url = url
        self.base_url = urlparse(url).netloc
        self.domain = u.url_to_domain(self.start_url)
        self.savedir = wrapper.gl___datapath / self.domain
        if not os.path.isdir(self.savedir):
            os.makedirs(self.savedir)

        self.driverpath = wrapper.sc___driverpath
        self.driver = wrapper.sc___driver
        self.vaultpath = wrapper.vaultpath.parent / (f'{wrapper.vaultpath.name}_{self.domain}.csv')
        self.vault = self.get_or_make_vault()
        self.indexfile = self.savedir / 'indexfile.scrape' # todo: remove indexfile
        self.specific = wrapper.sc___specific
        self.result = []

    def get_or_make_vault(self):
        # load the vault file. if not existing, make one.
        if os.path.isfile(self.vaultpath):
            vault = pd.read_csv(self.vaultpath)
            logger.info(f'found {len(vault)} pre-existing {self.domain} entries in {self.vaultpath}')
        else:
            vault = pd.DataFrame()
        return vault

    def scrape(self):
        # coordinate scraping of a specific website
        if self.domain == 'ronorp':
            self.scrape_ronorp()

        elif self.domain == 'wgzimmer':
            self.scrape_wgzimmer()
        # elif self.domain in [implemented]:
        #     do stuff
        else:
            print(f'There is no scrape_{self.domain}() implemented yet.')


    def scrape_ronorp(self):
        # the scraper specific for ronorp.net
        id_col = 'title' # the column to identify existing entries
        logger.info(f'starting scraping {self.domain}')
        results_df = []
        url_next = self.start_url

        # go through all indexpages
        max_indexpages = 20
        for i in range(max_indexpages):
            # start with the first indexpage
            if not url_next:
                break
            # get the head of the indexpage
            self.driver.get(url_next)
            # go through index page, make everything load, and find link to next index page
            maxiter = 10
            last = 0
            for k in range(maxiter):  # scroll down up to maxiter times
                last = u.wait_minimum(abs(random.gauss(1, 1)) + 3, last)
                self.driver.execute_script("window. scrollTo(0,document.body.scrollHeight)")
                content = self.driver.page_source  # this is one big string of webpage html
                soup = BeautifulSoup(content, features="html.parser")
                goal = soup.find_all('a', attrs={'class': 'pages_links_href pages_arrow'})
                if goal:
                    arrows = [element for element in goal if element['title'] == 'Weiter']
                    if arrows:
                        logger.info(f'number of arrows: {len(arrows)}')
                        try:
                            url_next = 'https://' + self.base_url + arrows[0]['href']
                            logger.info(f'found next indexpage: {url_next}')
                        except:
                            logger.info('could not extract link for next page')
                            url_next = None

                        break
                if k == maxiter - 1:
                    logger.info(f'indexpage {i} {k}: no next page found')
                    url_next = None

            # now the indexpage is fully loaded. go through the links of the indexpage
            elements = soup.find_all(attrs={'class': 'title_comment colored'})
            n_elements = len(elements)
            n_scrapes = 0
            n_known = 0
            for j, element in enumerate(elements):
                # if j > 4:  # debug
                #     break
                # if len(results_df)>2: # debug
                #     break
                try:
                    link = element.a['href']
                except KeyError:
                    logger.info("could not fetch link from element.a['href']")
                    continue

                # 'alt' was the characteristic element name that contained the link & values
                if 'alt' in element.a.attrs:
                    if 'title' in self.vault.columns:
                        if element.a['alt'] in self.vault['title'].values:
                            logger.info(f'already scraped: {link}')
                            n_known += 1
                            continue

                    last = u.wait_minimum(abs(random.gauss(1, 1)) + 3, last)
                    single_result = self.scrape_individual_adpage(link)
                    single_result['title'] = element.a.attrs['alt']
                    results_df.append(single_result)
                    n_scrapes += 1
                else:
                    logger.info(f'ad: {link}')
                    n_elements -= 1
            if n_elements == n_known:
                logger.info('no new ads on this indexpage. not proceeding to next indexpage')
                url_next = None

        self.result = results_df

    def scrape_individual_adpage(self, url):
        # the scraper for an individual ad, given its url
        logger.info(f'scraping adpage: {url}')
        now = datetime.datetime.now()
        # self.driver.get(url)
        # content = self.driver.page_source  # this is one big string of webpage html
        content = requests.get(url)
        if not content.status_code == 200:
            for i in range(3):
                time.sleep(5)
                content = requests.get(url)
                if content.status_code == 200:
                    continue
            logger.info(f'url returned status code {content.status_code}: {url}')
            fpath = None
        else:
            fpath = self.save_content(content.text, 'adpage', ts=now)
        result = pd.DataFrame(index = [0],
                              data = {'href':url,
                                      'domain': self.domain,
                                      'fname': fpath,
                                      'scrape_ts': now.replace(microsecond=0),
                              })
        # todo: add failure case
        return result


    def save_content(self, content, prefix, ts=None):
        if not ts:
            ts = datetime.datetime.now()

        fname = prefix + ts.strftime("_%Y-%m-%d_%H-%M-%S_%f") + '.scrape'
        fpath = self.savedir / fname
        with open(fpath, mode='w', encoding='UTF-8', errors='strict', buffering=1) as f_out:
            f_out.write(content)
        logger.info(f'saving {fpath}')
        return fpath

    def scrape_wgzimmer(self):
        logger.info('scraping wgzimmer.ch')
        base_sleeptime = 0.5
        regions = self.specific['wgzimmer']['regions']
        results_df = []
        for region in regions:
            logger.info(f'wgzimmer {region} scraping...')
            # go to landing page
            self.driver.get(self.start_url)
            time.sleep(base_sleeptime)

            # pick region & setting
            select = Select(self.driver.find_element_by_id('selector-state'))
            time.sleep(base_sleeptime)
            select.select_by_value(region)
            time.sleep(base_sleeptime)
            self.driver.find_element_by_id('permanentall').click()

            # open next page (here the first index page)
            next_page = "button-wrapper.button-etapper"
            a = self.driver.find_element_by_class_name(next_page)
            a.find_element_by_xpath('input').click()
            # get html text
            html_from_page = self.driver.page_source

            # go through all indexpages
            max_indexpages = 1
            for i in range(max_indexpages):
                if not html_from_page:
                    break

                # go through the next page and save the links
                soup = BeautifulSoup(html_from_page, 'html.parser')
                adlist = soup.find('ul', attrs={'id': 'search-result-list'})
                list_entries = adlist.find_all('li', attrs={'class': 'search-result-entry search-mate-entry'})
                n_elements = len(list_entries)
                n_scrapes = 0
                n_known = 0
                last = 0
                for i, entry in enumerate(list_entries):

                    hrefobj = entry.find(
                        lambda tag: (tag.name == "a") and
                                    ({'href'} == set(tag.attrs.keys())))

                    link = 'https://www.wgzimmer.ch' + hrefobj['href']
                    if link in self.vault['href'].values:
                        logger.info(f'already scraped: {link}')
                        n_known += 1
                        continue

                    # update single-page-result
                    timestampstr = hrefobj.find('span', attrs={'class': 'create-date left-image-result'}).text
                    timestamp = dateparser.parse(u.html_clean_1(timestampstr), date_formats=['%d/%m/%Y'])
                    if not timestamp:
                        pass
                    last = u.wait_minimum(abs(random.gauss(1, 1)) + 3, last)
                    single_result = self.scrape_individual_adpage(link)

                    single_result['timestamp'] = timestamp
                    results_df.append(single_result)
                    n_scrapes += 1

                if n_elements == n_known:
                    logger.info('no new ads on this indexpage. not proceeding to next indexpage')
                    next_page =[]
                else:

                    # check for next indexpage
                    next_page = self.driver.find_elements_by_id("gtagSearchresultNextPage")

                if  (len(next_page) > 0):
                    next_page[0].click()
                    html_from_page = self.driver.page_source
                    logger.info('going to next indexpage')
                else:
                    # or abort
                    html_from_page = None
                    logger.info('no more button to next indexpage. prabaly last page')

        self.result = results_df # list of 1-row-dfs


def time_since_modified(fname):
    if os.path.isfile(fname):
        ts = datetime.datetime.fromtimestamp(os.stat(fname).st_mtime)
        return (datetime.datetime.now() - ts).seconds / 3600.0
    else:
        return 100.0


def main(config):
    """
    landing page' for the scraper. from here, we coordinate "
    :param config: a dict with config properties
    :return: the scraper wrapper instance
    """
    logger.info(f'starting scraping')
    sw = Scrapewrapper(config)
    sw.scrape_wrap()
    return sw
