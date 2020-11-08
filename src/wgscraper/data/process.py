import os
import re

import dateparser
import pandas as pd
import utils.utils as u
from bs4 import BeautifulSoup
from loguru import logger


def process(config):
    # coordinate scraping of a specific website
    pros = []
    for url in config['urls']:
        pro = Processor(config, url)
        logger.info(f'starting processing {pro.domain}')

        if pro.domain == 'ronorp':
            pro.process_ronorp()

        elif pro.domain == 'wgzimmer':
            pro.process_wgzimmer()
        # elif self.domain in [implemented]:
        #     do stuff
        else:
            print(f'There is no process_{pro.domain}() implemented yet.')
        pros.append(pro)

    return pros


class Processor:

    def __init__(self, config, url):
        self.start_url = url
        self.domain = u.url_to_domain(self.start_url)
        self.datapath = config['datapath']
        self.root = config['root']
        self.result = []
        self.vaultpath = self.datapath / (f'{self.datapath.name}_{self.domain}.csv')

        self.input_vault_path = self.datapath / (f"{config['vault']}_{self.domain}.csv")
        self.output_vault_path = self.datapath / (f"{config['processed']}_{self.domain}.csv")
        self.urls = config['urls']
        self.specific = config['specific_configs']

        # get input
        if os.path.isfile(self.input_vault_path):
            self.df_in = pd.read_csv(self.input_vault_path)
            logger.info(f'found {len(self.df_in)} entries from scraping {self.domain} in {self.input_vault_path}')
        else:
            logger.info(f'no input data for {self.domain} in {self.input_vault_path}')
            self.df_in = pd.DataFrame()
        # get pre-existing processed list
        if os.path.isfile(self.output_vault_path):
            self.df_out = pd.read_csv(self.output_vault_path)
            logger.info(f'found {len(self.df_out)} pre-existing {self.domain} entries in {self.output_vault_path}')
        else:
            self.df_out = pd.DataFrame()

        self.force_reprocessing = config['force_reprocessing']
        self.forced_cutoff = config['forced_cutoff']

    def process_ronorp(self):
        # run processing jobs
        # first, let's get all the new stuff
        if not self.force_reprocessing:
            # Either Identify what is new in the input
            key_diff = set(self.df_in.index).difference(self.df_out.index)
            df = self.df_in[self.df_in.index.isin(key_diff)]
            n = len(self.df_in)
            n_attempt = len(key_diff)
            ignored = len(self.df_in) - n_attempt
            logger.info(
                f'attempting to process {n_attempt} of {n} entries, ignoring {ignored} previously processed existing entries')
        else:
            # or take what came after the cutoff
            df = self.df_in[self.df_in['scrape_ts'] > self.forced_cutoff]
            logger.info(f'post processing all {self.domain} entries')

        fails = []
        processed = []

        # process the pending data for the ronorp domain
        if len(df) == 0:
            logger.info('no entries for ronorp to process')
            return []
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

        if len(dfs) > 0:
            self.result = pd.concat(dfs)
            self.df_out = pd.concat([self.df_out, self.result])
            self.df_out.to_csv(self.output_vault_path, index=False)
            logger.info(f'processed {len(self.result)} enries from ronorp')
        else:
            logger.info('no entries were processed')

        return self


    def process_wgzimmer(self):
        # run processing jobs
        # first, let's get all the new stuff
        if not self.force_reprocessing:
            # Either Identify what is new in the input
            key_diff = set(self.df_in.index).difference(self.df_out.index)
            df = self.df_in[self.df_in.index.isin(key_diff)]
            n = len(self.df_in)
            n_attempt = len(key_diff)
            ignored = len(self.df_in) - n_attempt
            logger.info(
                f'attempting to process {n_attempt} of {n} entries, ignoring {ignored} previously processed existing entries')
        else:
            # or take what came after the cutoff
            df = self.df_in[self.df_in['scrape_ts'] > self.forced_cutoff]
            logger.info(f'post processing all {self.domain} entries')

        fails = []
        processed = []

        # process the pending data for the ronorp domain
        if len(df) == 0:
            logger.info('no entries for ronorp to process')
            return []
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

        if len(dfs) > 0:
            self.result = pd.concat(dfs)
            self.df_out = pd.concat([self.df_out, self.result])
            self.df_out.to_csv(self.output_vault_path, index=False)
            logger.info(f'processed {len(self.result)} enries from ronorp')
        else:
            logger.info('no entries were processed')

        return self

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


def main(config):
    logger.info(f'starting processing')

    proc = process(config)
    return proc
