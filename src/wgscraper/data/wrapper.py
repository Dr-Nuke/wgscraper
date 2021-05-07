import datetime
import os
import random
import re
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from urllib.parse import urlparse

import dateparser
import numpy as np
import pandas as pd
import requests
import utils.utils as u
from bs4 import BeautifulSoup
from loguru import logger
from selenium import webdriver
from selenium.webdriver import DesiredCapabilities
from selenium.webdriver.firefox.options import Options
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

        self.d['gl']['datapath'] = config['global']['datapath']
        self.d['gl']['current_stage'] = ''
        self.d['gl']['current_dom'] = ''
        self.d['gl']['current_url'] = ''
        self.d['gl']['stages'] = {key: {} for key in config.keys() if key != 'global'}
        self.d['gl']['force_reprocessing'] = config['global']['force_reprocessing']
        self.d['gl']['forced_cutoff'] = config['global']['forced_cutoff']
        self.d['gl']['active_domains'] = config['global']['active_domains']

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

        self.d['un'] = {}
        self.d['un']['name'] = config['unify']['stage_name']
        self.d['un']['lib_out'] = config['unify']['lib_out']
        self.d['un']['lib_out_path'] = u.lib_path_maker(folder=self.d['gl']['datapath'],
                                                        name=self.d['un']['lib_out'])

        self.d['an'] = {}
        self.d['an']['name'] = config['analyze']['stage_name']
        self.d['an']['lib_out'] = config['analyze']['lib_out']
        self.d['an']['results_archive_path'] = u.lib_path_maker(folder=self.d['gl']['datapath'],
                                                                name=config['analyze']['results_archive_path'])
        self.d['an']['lib_in_path'] = self.d['un']['lib_out_path']
        self.d['an']['lib_out_path'] = u.lib_path_maker(folder=self.d['gl']['datapath'],
                                                        name=self.d['an']['lib_out'])
        self.d['an']['searchdict'] = config['analyze']['searchdict']
        self.d['an']['email'] = config['analyze']['email']

        # generate out_file_paths
        for stage in ['sc', 'pr', 'un']:
            for dom in self.d['gl']['domains']:
                self.d[stage][dom] = {}
                self.d[stage][dom]['lib_out_path'] = u.lib_path_maker(folder=self.d['gl']['datapath'],
                                                                      name=self.d[stage]['lib_out'],
                                                                      dom=dom)
        # generate in_file_paths
        prev = ['sc', 'pr', 'un']
        for i, stage in enumerate(['pr', 'un']):
            for dom in self.d['gl']['domains']:
                self.d[stage][dom]['lib_in_path'] = self.d[prev[i]][dom]['lib_out_path']

    def scrape(self):
        # go through all domains and execute according scraper, if available

        options = Options()
        options.headless = False

        options.add_argument('--window-size=1920,1080')
        profile = webdriver.FirefoxProfile(
            str(Path(r'C:\Users\Bo-user\AppData\Local\Mozilla\Firefox\Profiles\ewyg9dd9.Hans')))

        profile.set_preference("dom.webdriver.enabled", False)
        profile.set_preference('useAutomationExtension', False)

        profile.update_preferences()

        desired = DesiredCapabilities.FIREFOX
        self.d['sc']['driver'] = webdriver.Firefox(firefox_profile=profile,
                                                   desired_capabilities=desired,
                                                   options=options,
                                                   executable_path=self.d['sc']['driverpath'])
        self.d['sc']['driver'].execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        self.d['gl']['current_stage'] = self.d['sc']['name']
        for i, ud in enumerate(zip(self.d['gl']['urls'],
                                   self.d['gl']['domains'])):
            self.d['gl']['current_url'] = ud[0]
            self.d['gl']['current_dom'] = ud[1]
            if ud[1] == 'ronorp':
                self.scrape_ronorp()

            elif ud[1] == 'wgzimmer':
                self.scrape_wgzimmer()

            elif ud[1] == 'flatfox':
                self.scrape_flatfox()


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
                            # logger.info(f'already scraped: {link}')
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
        # this is universal for all domains
        logger.info(f'scraping adpage: {url}')
        now = datetime.datetime.now()
        content = requests.get(url)
        if not content.status_code == 200:  # if fist time fails, try again ew times
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
        # todo: add a hash for later hashing
        # result['hash'] = result.apply(lambda x: hash(tuple(x)), axis = 1)

        # todo: add failure case
        return result

    def save_content(self, content, prefix, ts=None):
        if not ts:
            ts = datetime.datetime.now()

        fname = prefix + ts.strftime("_%Y-%m-%d_%H-%M-%S_%f") + '.scrape'
        fpath = self.d['gl']['datapath'] / self.d['gl']['current_dom'] / fname
        # todo: add if not fpath.parent exists, then mkdir
        with open(fpath, mode='w', encoding='UTF-8', errors='strict', buffering=1) as f_out:
            f_out.write(content)
        logger.info(f'saving {fpath}')
        return fpath

    def scrape_wgzimmer(self):
        dom = self.d["gl"]["current_dom"]
        url = self.d['gl']['current_url']
        logger.info(f'starting scraping {dom}')

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
                        # logger.info(f'already scraped: {link}')
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
                    logger.info('no more button to next indexpage. probaly last page or no new ads on this one')

        if len(results_df) > 0:
            results_df = pd.concat(results_df)
            self.d['sc'][dom]['results_df'] = results_df
            lib_out = pd.concat([lib_out, results_df])
            lib_out.drop_duplicates(subset='href', inplace=True, keep='last')
            lib_out.to_csv(self.d['sc'][dom]['lib_out_path'], index=False)
        logger.info(
            f'scraped {len(results_df)} new ads from {dom}, saved in {self.d["sc"][dom]["lib_out_path"]}')

    def scrape_flatfox(self):

        dom = self.d["gl"]["current_dom"]
        url = self.d['gl']['current_url']
        logger.info(f'starting scraping {dom}')

        url = self.make_random_flatfox_url()

        # get existing data
        lib_out_path = self.d['sc'][dom]['lib_out_path']
        lib_out = u.load_df_safely(lib_out_path)
        logger.info(f'found {len(lib_out)} pre-existing {dom} entries in {lib_out_path}')

        results_df = []
        # get initial page
        self.d['sc']['driver'].get(url)
        # go through "mehr anzeigen", i.e. 48 ads per clicked button
        max_extend_list = 5
        button_clicks = 0
        last = 0

        # make set of scraped links
        if 'href' in lib_out.columns:
            previous_links = set(lib_out['href'])
        else:
            previous_links = set()
        scraped_links = set()

        while button_clicks < max_extend_list:
            new_links_found = False  # initialize the flag as false

            # click the button
            button_clicks += 1
            # last = u.wait_minimum(abs(random.gauss(1, 1)) + 5, last)
            logger.info(f'moving to {button_clicks} extensions of results page')
            time.sleep(5)  # let the webpage load, otherwise we won't find the buttons
            buttons = self.d['sc']['driver'].find_elements_by_css_selector('.button.button--primary')
            logger.info(f'found {len(buttons)} buttons')
            buttons[1].click()
            time.sleep(5)  # more loading time, otherwise we miss out links

            # find all inks on the page
            found_links = flatfox_get_links(self.d['sc']['driver'])

            # compare found links with known links (previous + scraped)
            to_do_links = set(found_links).difference(previous_links.union(scraped_links))

            # if no new links, quit the scraping
            if len(to_do_links) == 0:
                logger.info('no new links. aborting')
                break

            # scrape new links
            for i, link in enumerate(to_do_links):
                last = u.wait_minimum(abs(random.gauss(1, 1)) + 5, last)
                single_result = self.scrape_individual_adpage(link)
                single_result['timestamp'] = None  # havnt found timestamp on the ads yet
                results_df.append(single_result)
                # if i > 0:  # debug
                #     break
            scraped_links.update(to_do_links)

        if len(results_df) > 0:
            results_df = pd.concat(results_df)
            self.d['sc'][dom]['results_df'] = results_df
            lib_out = pd.concat([lib_out, results_df])
            lib_out.drop_duplicates(subset='href', inplace=True, keep='last')
            lib_out.to_csv(self.d['sc'][dom]['lib_out_path'], index=False)
        logger.info(
            f'scraped {len(results_df)} new ads from {dom}, saved in {self.d["sc"][dom]["lib_out_path"]}')

    def make_random_flatfox_url(self):
        config = {key: val for key, val in self.d['sc']['specific']['flatfox'].items()}

        url_config = config['url_config']
        url_randomization = config['url_randomization']
        #     'east': 8.577960,
        #     'max_price': 1500,
        #     'min_price': 250,
        #     'north': 47.423220,
        #     'ordering': '-insertion',
        #     'south': 47.352394,
        #     'west': 8.477002,
        # }
        url_config['west'], url_config['east'] = randomize_map(url_config['west'],
                                                               url_config['east'],
                                                               variation=url_randomization['variation'],
                                                               precision=url_randomization['precision'])
        url_config['north'], url_config['south'] = randomize_map(url_config['north'],
                                                                 url_config['south'],
                                                                 variation=url_randomization['variation'],
                                                                 precision=url_randomization['precision'])
        url_config['min_price'] = randomize_price(url_config['min_price'], url_randomization['min_price_variation'])
        url_config['max_price'] = randomize_price(url_config['max_price'], url_randomization['max_price_variation'])

        url = config['base_url'] + '&'.join([key + config['sep'] + str(val) for key, val in url_config.items()])
        logger.info(f'usinf flatfox start url {url}')
        return url

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
                df = lib_in[self.lib_in['scrape_ts'] > self.d['gl']['forced_cutoff']]
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

            elif dom == 'flatfox':
                results_df = self.process_flatfox()
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

            # safety: preallocate street & address
            extracted_details['Street'] = ''
            extracted_details['zip'] = ''

            for item in block2_items:
                itemstring = item.text.strip()
                if 'Adresse' in itemstring:
                    extracted_details['Street'] = itemstring.replace('Adresse', '').strip()
                elif 'Ort' in itemstring:
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

    def process_flatfox(self):
        # run processing jobs

        dom = self.d["gl"]["current_dom"]
        stage = 'pr'

        fails = []
        processed = []
        df = self.d[stage][dom]['df_attempt']

        dfs = []
        n_iter = len(df)
        l = u.Looplogger(n_iter, f'processing {n_iter} entries')
        intcols = ['brutto', 'netto', 'utilities']
        dfs = []
        for i, irow in df.iterrows():
            try:
                with open(irow['fname'], 'r', encoding='utf-8') as f_in:
                    soup = BeautifulSoup(f_in.read(), features="html.parser")
            except:
                print(f"could not ingest {irow['fname']}")

            result = {}
            copycols = ['href', 'domain', 'scrape_ts', 'fname']
            targetcols = ['url', 'domain', 'scrape_ts', 'fname']
            for ccol, tcol in zip(copycols, targetcols):
                result[tcol] = irow[ccol]

            titlefield = soup.find('div', attrs={'class': 'widget-listing-title'})
            result['address'] = u.html_clean_1(titlefield.h2.text).split('-')[0]
            result['title'] = u.html_clean_1(titlefield.h1.text)
            tables = soup.find_all('table', attrs={'class': 'table table--rows table--fluid table--fixed table--flush'})
            if len(tables) != 2:
                logger.info(f'expected 2 tables, found {len(tables)} for {irow["href"]}, {irow["fname"]}. ignoring')
                continue

            data = []
            rows = tables[0].find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                cols = [ele.text.strip() for ele in cols]
                data.append([ele for ele in cols if ele])  # Get rid of empty values
            for ele in data:
                if 'bruttomiete' in ele[0].lower():
                    result['brutto'] = ele[1]
                elif 'preiseinheit' in ele[0].lower():
                    result['price_detail'] = ele[1]
                elif 'nettomiete' in ele[0].lower():
                    result['netto'] = ele[1]
                elif 'nebenkosten' in ele[0].lower():
                    result['utilities'] = ele[1]

                else:
                    logger.info(f'unreckognized field {ele[0]} with value {ele[1]} on ad {irow["href"]}')

            data = []
            rows = tables[1].find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                cols = [ele.text.strip() for ele in cols]
                data.append([ele for ele in cols if ele])  # Get rid of empty values
            data

            for ele in data:
                if 'anzahl zimmer' in ele[0].lower():
                    result['rooms'] = ele[1]
                elif 'besonderes' in ele[0].lower():
                    result['special'] = ele[1]
                elif 'wohnfläche' in ele[0].lower():
                    result['area'] = ele[1]
                elif 'ausstattung' in ele[0].lower():
                    result['particulars'] = ele[1]
                elif 'bezugstermin' in ele[0].lower():
                    result['from'] = ele[1]

                elif 'referenz' in ele[0].lower():
                    result['reference'] = ele[1]
                elif 'etage' in ele[0].lower():
                    result['floor'] = ele[1]
                elif 'nutzfläche' in ele[0].lower():
                    result['area_usage'] = ele[1]
                elif 'baujahr' in ele[0].lower():
                    result['constructed'] = ele[1]
                elif 'webseite' in ele[0].lower():
                    result['website'] = ele[1]
                elif 'dokumente' in ele[0].lower():
                    pass
                elif 'kubatur' in ele[0].lower():
                    pass

                else:
                    logger.info(f'unreckognized field {ele[0]} with value {ele[1]} on ad {irow["href"]}')

            for col in intcols:
                if col in result:
                    result[col] = u.make_float(result[col])

            if 'brutto' in result:
                result['rent'] = result['brutto']
            elif 'netto' in result:
                result['rent'] = result['netto']

                if 'utilities' in result:
                    result['rent'] += result['utilities']
            else:
                logger.info(f'no rent info can be extracted from {irow["href"]}, {irow["fname"]}')

            a = soup.find_all('div', attrs={'class': 'fui-stack'})[1]
            result['text'] = re.sub(r"^Beschreibung", "", u.html_clean_1(a.find_all('div')[-2].text))
            dfs.append(pd.DataFrame(index=[i], data=result))

        return dfs

    def unify(self):
        # unifies all the per-domain results into a single archive
        stage = 'un'
        self.d['gl']['current_stage'] = self.d[stage]['name']

        lib_ins = []

        for i, dom in enumerate(self.d['gl']['domains']):
            self.d['gl']['current_dom'] = dom

            # first, let's get all the new stuff
            lib_in = u.load_df_safely(self.d[stage][dom]['lib_in_path'])
            lib_ins.append(lib_in)

        lib_out = pd.concat(lib_ins).reset_index(drop=True)

        lib_out.to_csv(self.d[stage]['lib_out_path'], index=False)
        logger.info(f'unified {len(lib_out)} ads ')
        logger.info('unifying completed')

    def analyze(self):
        # todo: move email part into sparate member function
        # analyze the ads & make results
        stage = 'an'
        self.d['gl']['current_stage'] = self.d[stage]['name']

        # first, let's get all the new stuff
        lib_in = u.load_df_safely(self.d[stage]['lib_in_path'])
        self.d[stage]['lib_in'] = lib_in

        lib_out = u.load_df_safely(self.d[stage]['lib_out_path'])
        self.d[stage]['lib_out'] = lib_out

        former_results = u.load_df_safely(self.d[stage]['results_archive_path'])
        former_results['scrape_ts'] = pd.to_datetime(former_results['scrape_ts'])
        self.d[stage]['former_results'] = former_results

        logger.info(f'found {len(lib_in)} existing entries in {self.d[stage]["lib_in_path"]}')
        logger.info(f'found {len(lib_out)} existing entries in {self.d[stage]["lib_out_path"]}')

        if len(lib_out) > 0:
            if not self.d['gl']['force_reprocessing']:
                # Either Identify what is new in the input
                key_diff = set(lib_in['url']).difference(lib_out['url'])
                df = lib_in[lib_in['url'].isin(key_diff)]
                n = len(lib_in)
                n_attempt = len(key_diff)
                ignored = len(lib_in) - n_attempt
                logger.info(
                    f'attempting to analyze {n_attempt} of {n} entries, ignoring {ignored} previously processed existing entries')
            else:
                # or take what came after the cutoff
                df = lib_in[self.lib_in['scrape_ts'] > self.d['gl']['forced_cutoff']]
                logger.info(f'analyzing all entries')
        else:
            df = lib_in
        self.d[stage]['df_attempt'] = df

        # make a copy for later
        df_bak = df

        if len(df) == 0:
            logger.info(f'no entries for to analyze')
            return

        df = df[df['duration'] != 'befristet']
        df = df[df['bid_ask'] != 'Suche']

        keepcols = ['rent', 'address', 'scrape_ts', 'timestamp',
                    'rooms', 'details', 'title', 'url', 'text', 'domain']
        df = df[keepcols]
        df = df.fillna('')

        results = []
        for search in self.d['an']['searchdict']:
            col = search['column']
            pat = search['pattern']
            if search['flag'] == 'IGNORECASE':
                result = df[df[col].str.contains(pat, flags=re.IGNORECASE, regex=True)]
            else:
                result = df[df[col].str.contains(pat, regex=True)]

            if len(result) > 0:
                pd.options.mode.chained_assignment = None
                result['searchterm'] = pat
                result['searchfield'] = col
                pd.options.mode.chained_assignment = 'warn'
            results.append(result)

        result = pd.concat(results).drop_duplicates(subset=['title'])

        # write results
        now = datetime.datetime.now()
        cutoff = now - datetime.timedelta(days=60)
        if len(result) > 0:
            all_results = pd.concat([former_results, result])
            self.d[stage]['results_df'] = all_results
            all_results.to_csv(self.d['an']['results_archive_path'], index=False)

            # make email
            result = result[
                ['rent', 'url', 'address', 'rooms', 'title', 'domain', 'searchterm', 'searchfield', 'scrape_ts']]
            result_formatted = result.sort_values(by='scrape_ts', ascending=False).style.format({'url': make_clickable})

            if len(former_results) > 0:
                old_results = former_results[
                    ['rent', 'url', 'address', 'rooms', 'title', 'domain', 'searchterm', 'searchfield', 'scrape_ts']]
                old_results = old_results[old_results['scrape_ts'] > cutoff]
                old_results = old_results.sort_values(by='scrape_ts', ascending=False).style.format(
                    {'url': make_clickable})

            else:
                old_results = pd.DataFrame().style.format({'url': make_clickable})

            fromaddr = "Mayerhansmichael@gmail.com"
            toaddr = "chris@boles.ch"
            msg = MIMEMultipart()
            msg['From'] = self.d['an']['email']['fromaddr']
            msg['To'] = self.d['an']['email']['toaddr']
            msg['Subject'] = self.d['an']['email']['subject']

            intro = self.d['an']['email']['intro']

            body = result_formatted.hide_index().render()
            second_table = old_results.hide_index().render()

            msg.attach(MIMEText(intro, 'text'))
            msg.attach(MIMEText(body, 'html'))
            msg.attach(MIMEText(second_table, 'html'))

            server = smtplib.SMTP(self.d['an']['email']['smtp_host'],
                                  self.d['an']['email']['smtp_port'])
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(self.d['an']['email']['fromaddr'],
                         self.d['an']['email']['pwd'])
            text = msg.as_string()
            server.sendmail(fromaddr, toaddr, text)
            logger.info('email sent')

        else:
            logger.info(f'no results from analyzing.')
        lib_out = pd.concat([lib_out, df_bak])
        lib_out.to_csv(self.d[stage]['lib_out_path'], index=False)
        logger.info(
            f'analyzed {len(df_bak)} new ads from, found {len(result)} results. saved in {self.d[stage]["lib_out_path"]}')

        logger.info('analyzing completed')


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


def make_clickable(val):
    # allows to cast a pandas url column into a clickable link
    return '<a target="_blank" href="{}">{}</a>'.format(val, 'link')


class container:
    # dummy class for storing attributes
    pass


def flatfox_get_links(driver):
    # gets the links from a given flatfox results page
    html_from_page = driver.page_source
    soup = BeautifulSoup(html_from_page, 'html.parser')
    adlist = soup.find('div', attrs={'class': 'search-result'})
    ads = adlist.find_all('div', attrs={'class': 'listing-thumb'})
    logger.info(f'found {len(ads)} ads')
    base = 'https://flatfox.ch'
    links = [base + element.a['href'] for element in ads]
    return links


def randomize_map(east, west, variation=0.01, precision=6):
    # modifies the flatfox geo-tags by a little bit
    variation = (east - west) / east * variation
    new_east = np.round(east * random.uniform(1 + variation, 1 - variation), precision)
    new_west = np.round(west * random.uniform(1 + variation, 1 - variation), precision)
    return new_east, new_west


def randomize_price(price, variations):
    'mdifies the flatfox prices abit'
    return price + random.choice(variations)
