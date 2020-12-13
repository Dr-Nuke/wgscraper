import datetime
import os
import random
import re
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

        # global attributes

        self.d = {}
        self.d['gl'] = {}
        self.d['gl']['root'] = config['global']['root']
        self.d['gl']['urls'] = config['global']['urls']
        self.d['gl']['domains'] = [u.url_to_domain(url) for url in self.d['gl']['urls']]
        self.d['gl']['domains'] = ['ronorp', 'wgzimmer'] # debug
        self.d['gl']['datapath'] = config['global']['datapath']
        self.d['gl']['current_stage'] = ''
        self.d['gl']['current_dom'] = ''
        self.d['gl']['current_url'] = ''
        self.d['gl']['stages'] = {key: {} for key in config.keys() if key != 'global'}
        self.d['gl']['force_reprocessing'] = config['global']['force_reprocessing']
        self.d['gl']['forced_cutoff'] = config['global']['forced_cutoff']

        # attributes for scraping
        self.d['sc'] = {}
        self.d['sc']['name'] = config['scrape']['stage_name']
        self.d['sc']['driverpath'] = config['scrape']['driverpath']
        self.d['sc']['driver'] = None
        self.d['sc']['specific'] = config['scrape']['specific_configs']
        self.d['sc']['lib_out'] = config['scrape']['lib_out']
        self.d['sc']['results'] = []

        self.d['pr'] = {}
        self.d['pr']['name'] = config['process']['stage_name']
        self.d['pr']['lib_out'] = config['process']['lib_out']

        # generate out_file_paths
        for stage in ['sc', 'pr']:
            for dom in ['ronorp','wgzimmer']:
                self.d[stage][dom] = {}
                self.d[stage][dom]['lib_out_path'] = u.lib_path_maker(folder=self.d['gl']['datapath'],
                                                                      name=self.d[stage]['lib_out'],
                                                                      dom=dom)
        # generate in_file_paths
        for stage in ['pr']:
            for dom in ['ronorp','wgzimmer']:
                self.d[stage][dom]['lib_in_path'] = self.d['sc'][dom]['lib_out_path']

        print('')

    def get_or_make_lib(self, stage, dom=None):
        # load the lib file. if not existing, make one.
        if os.path.isfile(self.vaultpath):
            vault = pd.read_csv(self.vaultpath)
            logger.info(f'found {len(vault)} pre-existing {self.domain} entries in {self.vaultpath}')
        else:
            vault = pd.DataFrame()
        return vault

    def scrape(self):
        # go through all domains and execute according scraper, if available
        self.d['sc']['driver'] = webdriver.Firefox(executable_path=self.d['sc']['driverpath'])
        self.d['gl']['current_stage'] = self.d['sc']['name']
        for i, ud in enumerate(zip(self.d['gl']['urls'],
                                   self.d['gl']['domains'])):
            self.d['gl']['current_url'] = ud[0]
            self.d['gl']['current_dom'] = ud[1]
            if ud[1] == 'ronorp':
                self.scrape_ronorp()

            elif ud[1] == 'wgzimmer':
                self.scrape_wgzimmer()

            else:
                print(f'There is no scraper for {ud[1]} implemented yet.')
        self.d['sc']['driver'].quit()
        logger.info('scraping completed')

    def scrape_ronorp(self):
        # the scraper specific for ronorp.net
        dom = self.d["gl"]["current_dom"]
        url = self.d['gl']['current_url']
        logger.info(f'starting scraping {dom}')

        # get existing data
        lib_out_path = self.d['sc'][self.d['gl']['current_dom']]['lib_out_path']
        lib_out = u.load_df_safely(lib_out_path)
        logger.info(f'found {len(lib_out)} pre-existing {dom} entries in {lib_out_path}')

        id_col = 'title'  # the column to identify existing entries
        results_df = []
        url_next = self.d['gl']['current_url']
        base_url = urlparse(url_next).netloc

        # go through all indexpages
        max_indexpages = 20
        for i in range(max_indexpages):
            # start with the first indexpage
            if not url_next:
                break
            # get the head of the indexpage
            self.d['sc']['driver'].get(url_next)
            # go through index page, make everything load, and find link to next index page
            maxiter = 10
            last = 0
            for k in range(maxiter):  # scroll down up to maxiter times
                last = u.wait_minimum(abs(random.gauss(1, 1)) + 3, last)
                self.d['sc']['driver'].execute_script("window. scrollTo(0,document.body.scrollHeight)")
                content = self.d['sc']['driver'].page_source  # this is one big string of webpage html
                soup = BeautifulSoup(content, features="html.parser")
                goal = soup.find_all('a', attrs={'class': 'pages_links_href pages_arrow'})
                if goal:
                    arrows = [element for element in goal if element['title'] == 'Weiter']
                    if arrows:
                        logger.info(f'number of arrows: {len(arrows)}')
                        try:
                            url_next = 'https://' + base_url + arrows[0]['href']
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
                    if 'title' in lib_out.columns:
                        if element.a['alt'] in lib_out['title'].values:
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

        if len(results_df) > 0:
            results_df = pd.concat(results_df)
            self.d['sc'][dom]['results_df'] = results_df
            lib_out = pd.concat([lib_out, results_df])
            lib_out.drop_duplicates(subset='href', inplace=True, keep='last')
            lib_out.to_csv(self.d['sc'][dom]['lib_out_path'], index=False)
        logger.info(
            f'scraped {len(results_df)} new ads from {dom}, saved in {self.d["sc"][dom]["lib_out_path"]}')

    def scrape_individual_adpage(self, url):
        # the scraper for an individual ad, given its url
        logger.info(f'scraping adpage: {url}')
        now = datetime.datetime.now()
        # self.d['sc']['driver'].get(url)
        # content = self.d['sc']['driver'].page_source  # this is one big string of webpage html
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
        result = pd.DataFrame(index=[0],
                              data={'href': url,
                                    'domain': self.d['gl']['current_dom'],
                                    'fname': fpath,
                                    'scrape_ts': now.replace(microsecond=0),
                                    })
        # todo: add failure case
        return result

    def save_content(self, content, prefix, ts=None):
        if not ts:
            ts = datetime.datetime.now()

        fname = prefix + ts.strftime("_%Y-%m-%d_%H-%M-%S_%f") + '.scrape'
        fpath = self.d['gl']['datapath'] / self.d['gl']['current_dom'] / fname
        with open(fpath, mode='w', encoding='UTF-8', errors='strict', buffering=1) as f_out:
            f_out.write(content)
        logger.info(f'saving {fpath}')
        return fpath

    def scrape_wgzimmer(self):
        logger.info('scraping wgzimmer.ch')
        dom = self.d["gl"]["current_dom"]
        url = self.d['gl']['current_url']

        # get existing data
        lib_out_path = self.d['sc'][dom]['lib_out_path']
        lib_out = u.load_df_safely(lib_out_path)
        logger.info(f'found {len(lib_out)} pre-existing {dom} entries in {lib_out_path}')

        base_sleeptime = 0.5
        regions = self.d['sc']['specific'][dom]['regions']
        results_df = []
        for region in regions:
            logger.info(f'{dom} {region} scraping...')
            # go to landing page
            self.d['sc']['driver'].get(url)
            time.sleep(base_sleeptime)

            # pick region & setting
            select = Select(self.d['sc']['driver'].find_element_by_id('selector-state'))
            time.sleep(base_sleeptime)
            select.select_by_value(region)
            time.sleep(base_sleeptime)
            self.d['sc']['driver'].find_element_by_id('permanentall').click()

            # open next page (here the first index page)
            next_page = "button-wrapper.button-etapper"
            a = self.d['sc']['driver'].find_element_by_class_name(next_page)
            a.find_element_by_xpath('input').click()
            # get html text
            html_from_page = self.d['sc']['driver'].page_source

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
                    if link in lib_out['href'].values:
                        logger.info(f'already scraped: {link}')
                        n_known += 1
                        continue

                    # update single-page-result
                    # todo: dateparser seems to mix day and month (2020-06-12) in decembrer)
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
                    next_page = []
                else:

                    # check for next indexpage
                    next_page = self.d['sc']['driver'].find_elements_by_id("gtagSearchresultNextPage")

                if (len(next_page) > 0):
                    next_page[0].click()
                    html_from_page = self.d['sc']['driver'].page_source
                    logger.info('going to next indexpage')
                else:
                    # or abort
                    html_from_page = None
                    logger.info('no more button to next indexpage. prabaly last page')

        if len(results_df) > 0:
            results_df = pd.concat(results_df)
            self.d['sc'][dom]['results_df'] = results_df
            lib_out = pd.concat([lib_out, results_df])
            lib_out.drop_duplicates(subset='href', inplace=True, keep='last')
            lib_out.to_csv(self.d['sc'][dom]['lib_out_path'], index=False)
        logger.info(
            f'scraped {len(results_df)} new ads from {dom}, saved in {self.d["sc"][dom]["lib_out_path"]}')

    def time_since_modified(fname):
        if os.path.isfile(fname):
            ts = datetime.datetime.fromtimestamp(os.stat(fname).st_mtime)
            return (datetime.datetime.now() - ts).seconds / 3600.0
        else:
            return 100.0

    def process(self):
        # go through all domains and execute according process, if available
        stage = 'pr'
        self.d['gl']['current_stage'] = self.d[stage]['name']
        for i, dom in enumerate(self.d['gl']['domains']):

            self.d['gl']['current_dom'] = dom

            # first, let's get all the new stuff
            lib_in = u.load_df_safely(self.d[stage][dom]['lib_in_path'])
            self.d[stage][dom]['lib_in'] = lib_in

            lib_out = u.load_df_safely(self.d[stage][dom]['lib_out_path'])
            self.d[stage][dom]['lib_out'] = lib_out

            logger.info(f'found {len(lib_in)} existing {dom} entries in {self.d[stage][dom]["lib_in_path"]}')
            logger.info(f'found {len(lib_out)} existing {dom} entries in {self.d[stage][dom]["lib_out_path"]}')

            if not self.d['gl']['force_reprocessing']:
                # Either Identify what is new in the input
                key_diff = set(lib_in.index).difference(lib_out.index)
                df = lib_in[lib_in.index.isin(key_diff)]
                n = len(lib_in)
                n_attempt = len(key_diff)
                ignored = len(lib_in) - n_attempt
                logger.info(
                    f'attempting to process {n_attempt} of {n} entries, ignoring {ignored} previously processed existing entries')
            else:
                # or take what came after the cutoff
                df = self.lib_in[self.lib_in['scrape_ts'] > self.d['gl']['forced_cutoff']]
                logger.info(f'processing all {dom} entries')
            self.d[stage][dom]['df_attempt'] = df

            # process the pending data for the ronorp domain
            if len(df) == 0:
                logger.info(f'no entries for {dom} to process')
                continue

            results_df = []
            if dom == 'ronorp':
                results_df = self.process_ronorp()
            elif dom == 'wgzimmer':
                results_df = self.process_wgzimmer()
            else:
                print(f'There is no scraper for {dom} implemented yet.')

            if len(results_df) > 0:
                results_df = pd.concat(results_df)
                self.d[stage][dom]['results_df'] = results_df
                lib_out = pd.concat([lib_out, results_df])
                lib_out.to_csv(self.d[stage][dom]['lib_out_path'], index=False)
                logger.info(
                    f'processed {len(results_df)} new ads from {dom}, saved in {self.d[stage][dom]["lib_out_path"]}')
            else:
                logger.info(f'no entries were processed for {dom}')

        logger.info('processing completed')

    def process_ronorp(self):
        # run processing jobs
        dom = self.d["gl"]["current_dom"]
        stage = 'pr'

        fails = []
        processed = []
        df = self.d[stage][dom]['df_attempt']

        regex = {r'category1': r'Biete \/ Suche \/ Tausche: (.+?):',
                 r'bid_ask': r'Biete \/ Suche \/ Tausche: .+?: (.+?) ',
                 r'rent_buy': r'Mieten \& Kaufen: (.+?) ',
                 r'rooms': r'Zimmer: ([^a-zA-Z ]+)',
                 r'rent': r'Kosten: ([^a-zA-Z ]+)',
                 r'address': r'Adresse: (.*?)Kontakt',
                 r'duration': r'Vertragsart: (.*?) ',
                 }
        dfs = []
        n_iter = len(df)
        l = u.Looplogger(n_iter, f'processing {n_iter} entries')
        for i, row in df.iterrows():
            # if i > 10:
            #     break # debug
            l.log(i)
            try:
                with open(row['fname'], 'r', encoding='utf-8') as f_in:
                    soup = BeautifulSoup(f_in.read(), features="html.parser")
            except:
                logger.info(f"could not ingest {row['fname']}")

            details = u.html_clean_1(soup.find('div', attrs={'class': 'detail_block'}).get_text())
            details = details.split('Die vollen', 1)[0]

            extracted_details = extract_regex_details(details, regex)
            extracted_details['details'] = details
            extracted_details['text'] = soup.find('p', attrs={'class': 'text_comment'}).get_text()
            extracted_details['timestamp'] = dateparser.parse(
                soup.find_all('div', attrs={'class': 'pull-left'})[0].get_text())
            extracted_details['title'] = row['title']
            extracted_details['url'] = row['href']
            extracted_details['domain'] = row['domain']
            extracted_details['scrape_ts'] = row['scrape_ts']
            timestampstr = soup.find_all('div', attrs={'class': 'pull-left'})[0].text
            extracted_details['timestamp'] = dateparser.parse(u.html_clean_1(timestampstr),
                                                              date_formats=['%d.%m.%Y %h:%M'])
            dfs.append(pd.DataFrame(index=[i], data=extracted_details))

        return dfs

    def process_wgzimmer(self):
        # run processing jobs

        dom = self.d["gl"]["current_dom"]
        stage = 'pr'

        fails = []
        processed = []
        df = self.d[stage][dom]['df_attempt']

        dfs = []
        n_iter = len(df)
        l = u.Looplogger(n_iter, f'processing {n_iter} entries')
        for i, row in df.iterrows():
            # if i > 10:
            #     break  # debug
            l.log(i)
            try:
                with open(row['fname'], 'r', encoding='utf-8') as f_in:
                    soup = BeautifulSoup(f_in.read(), features="html.parser")
            except:
                logger.info(f"could not ingest {row['fname']}")

            extracted_details = {}
            ad_page_body = soup.find('div', attrs={'class': 'text result nbt'})
            block1 = ad_page_body.find('div', attrs={'wrap col-wrap date-cost'})
            block1_items = block1.find_all('p')

            # get block 1 items: start, end, rent
            for item in block1_items:
                itemstring = item.text.strip()
                if itemstring.startswith('Ab dem'):
                    fromstring = itemstring.replace('Ab dem', '').strip()
                    startdate = dateparser.parse(fromstring, date_formats=['%d/%m/%Y'])
                    extracted_details['from'] = startdate.date()

                if itemstring.startswith('Bis'):
                    if 'unbefristet' in itemstring.lower():
                        duration = 'unbefristet'
                    else:
                        duration = 'befristet'
                    extracted_details['duration'] = duration

                if itemstring.startswith('Miete / Monat'):
                    coststring = itemstring.replace('Miete / Monat sFr.', '').strip()
                    coststring = coststring.replace('.–', '').strip()
                    extracted_details['rent'] = coststring

            # get block2 items: address
            block2 = ad_page_body.find('div', attrs={'class': 'wrap col-wrap adress-region'})
            block2_items = block2.find_all('p')
            for item in block2_items:
                itemstring = item.text.strip()
                if ' Adresse ' in item.text:
                    extracted_details['Street'] = itemstring.replace('Adresse', '').strip()
                elif ' Ort ' in item.text:
                    extracted_details['zip'] = itemstring.replace('Ort', '').strip()

            extracted_details['address'] = extracted_details['Street'] + ' ' + extracted_details['zip']

            # block3: "the room is..."
            block3 = ad_page_body.find('div', attrs={'class': 'wrap col-wrap mate-content nbb'})
            block3_items = block3.find_all('p')
            extracted_details['room_is'] = u.html_clean_1(block3_items[0].text)

            # block 4: "Wir suchen..."
            block4 = ad_page_body.find('div', attrs={'class': 'wrap col-wrap room-content'})
            block4_items = block4.find_all('p')
            extracted_details['we_want'] = u.html_clean_1(block4_items[0].text)

            # block3 5: "we are..."
            block5 = ad_page_body.find('div', attrs={'class': 'wrap col-wrap person-content'})
            block5_items = block5.find_all('p')
            extracted_details['we_are'] = u.html_clean_1(block5_items[0].text)

            extracted_details['text'] = ''.join([extracted_details[key] for key in ['room_is', 'we_want', 'we_are']])

            # get remaining properties that we already know
            extracted_details['url'] = row['href']
            extracted_details['domain'] = row['domain']
            extracted_details['scrape_ts'] = row['scrape_ts']
            extracted_details['timestamp'] = row['timestamp']

            dfs.append(pd.DataFrame(index=[i], data=extracted_details))

        return dfs


def extract_regex_details(text, regex):
    # extract a specific set of ronorp

    # the details are as of yet only a bunch of string. convert them to meaningful content
    d = {}

    for k, v in regex.items():
        prop = re.search(v, text)
        if prop is not None:
            prop = prop.groups(0)[0]
        else:
            prop = 'unknown'
        d[k] = prop
    return d


class container:
    # dummy class for storing attributes
    pass
