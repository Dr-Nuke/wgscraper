import datetime
import os
import random
from urllib.parse import urlparse

import pandas as pd
from bs4 import BeautifulSoup
from loguru import logger
from selenium import webdriver
import src.utils.utils as u


class Scrapewrapper:
    # a wrapper around individual scrape instances

    def __init__(self, config):
        self.driverpath = config['driverpath']
        self.driver = webdriver.Firefox(executable_path=self.driverpath)
        self.root = config['root']
        self.datapath = self.root / 'data'
        self.vaultpath = self.datapath / config['vault']
        self.get_or_make_vault()
        self.scrapes = {}
        self.urls = config['urls']

    def get_or_make_vault(self):
        # load the vault file. if not existing, make one.
        if os.path.isfile(self.vaultpath):
            self.vault = pd.read_csv(self.vaultpath, index_col='index_url')
        else:
            self.vault = pd.DataFrame()
            self.vault.to_csv(self.vaultpath,index=False)

    def scrape_wrap(self):
        # run scraping jobs

        for url in self.urls:
            scr = Scraper(self, url)
            scr.scrape()
            self.scrapes.update({scr.domain: scr.results})

            self.vault = pd.concat([self.vault, scr.df], sort=False)
            scr.df.to_csv(self.vaultpath, mode='a', header=True,index=False)
            loger.info(f'scraped {len(scr.df)} new ads from {scr.domain}')

        self.vault.to_csv(self.vaultpath,index=False)


class Scraper(Scrapewrapper):
    """
    my scraper class object
    """

    def __init__(self, wrapper, url):
        self.start_url = url
        self.base_url = urlparse(url).netloc
        self.domain = u.url_to_domain(self.start_url)
        self.savedir = wrapper.datapath / self.domain
        if not os.path.isdir(self.savedir):
            os.makedirs(self.savedir)

        self.driverpath = wrapper.driverpath
        self.driver = wrapper.driver
        self.vault = wrapper.vault
        self.indexfile = self.savedir / 'indexfile.scrape'

    def scrape(self):
        # coordinate scraping of a specific website
        self.results = {}
        if self.domain == 'ronorp':
            self.scrape_ronorp()
        # elif self.domain in [implemented]:
        #     do stuff
        else:
            print(f'There is no scrape_{self.domain}().')
        self.df = pd.DataFrame(self.results).transpose()
        self.df.index.name = 'index_url'

    def scrape_ronorp(self):
        # the scraper specific for ronorp.net

        logger.info(f'starting scraping {self.domain}')
        results = {}
        url_next = self.start_url
        max_indexpages = 20
        for i in range(max_indexpages):
            # start with the first indexpage
            if not url_next:
                break

            self.driver.get(url_next)
            maxiter = 10
            last = 0
            for k in range(maxiter):
                last = u.wait_minimum(abs(random.gauss(1, 1)) + 3, last)
                self.driver.execute_script("window. scrollTo(0,document.body.scrollHeight)")
                content = self.driver.page_source  # this is one big string of webpage html
                soup = BeautifulSoup(content,'features="html.parser"')
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

            # Todo: make index df for indexpages. or if not, remove
            # fpath = self.save_content(content, 'indexpage')

            # go through the links of the indexpage
            elements = soup.find_all(attrs={'class': 'title_comment colored'})
            n_elements = len(elements)
            n_scrapes = 0
            n_known = 0
            for j, element in enumerate(elements):
                # if j > 1:  # debug
                #     break
                try:
                    link = element.a['href']
                except KeyError:
                    logger.info("could not fetch link from element.a['href']")
                    continue

                if 'alt' in element.a.attrs:
                    if 'alt' in self.vault.columns:
                        if element.a['alt'] in self.vault['alt'].values:
                            logger.info(f'already scraped: {link}')
                            n_known += 1
                            continue
                    self.results.update({link: element.a.attrs})
                    self.scrape_individual_adpage(link)
                    n_scrapes += 1
                else:
                    logger.info(f'ad: {link}')
                    n_elements -= 1
            if n_elements == n_known:
                logger.info('no new ads on this indexpage. not proceeding to next indexpage')
                url_next = None

        self.driver.quit()

    def scrape_individual_adpage(self, url):
        # the scraper for an individual ad, given its url
        logger.info(f'scraping adpage: {url}')
        now = datetime.datetime.now().replace(microsecond=0)
        self.driver.get(url)
        content = self.driver.page_source  # this is one big string of webpage html
        fpath = self.save_content(content, 'adpage', ts=now)
        self.results[url].update({'domain': self.domain,
                                  'fname': fpath,
                                  'scrape_ts': now,
                                  'processed': False})

    def save_content(self, content, prefix, ts=None):
        if not ts:
            ts = datetime.datetime.now()

        fname = prefix + ts.strftime("_%Y-%m-%d_%H-%M-%S_%f") + '.scrape'
        fpath = self.savedir / fname
        with open(fpath, mode='w', encoding='UTF-8', errors='strict', buffering=1) as f_out:
            f_out.write(content)
        logger.info(f'saving {fpath}')
        return fpath

    def save_to_csv(self,index=False):
        df_file = self.datapath / 'scraper_df.csv'
        pd.DataFrame(self.results).transpose().to_csv(df_file,index=False)


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
