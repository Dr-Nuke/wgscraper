import os
import re

import dateparser
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from loguru import logger
from utils.utils import *


class Processwrapper:
    # a wrapper class for the individual processing stage
    def __init__(self, config):
        self.root = config['root']
        self.datapath = self.root / 'data'
        self.input = self.datapath / config['vault']
        self.output = self.datapath / config['processed']

        self.urls = config['urls']
        if os.path.isfile(self.input):
            self.df_in = pd.read_csv(self.input)
        else:
            logger.info('no input data')
            return None

        if os.path.isfile(self.output):
            self.df_out = pd.read_csv(self.output)
        else:
            self.df_out = pd.DataFrame()
        self.force_reprocessing = config['force_reprocessing']
        self.forced_cutoff = config['forced_cutoff']

    def process(self):
        # run processing jobs
        logging.info(f'starting processing {self.input}')

        # first, let's get all the new stuff
        if not self.force_reprocessing:
            # Either Identify what is new in the input
            key_diff = set(self.df_in.index).difference(self.df_out.index)
            df_to_do = self.df_in[self.df_in.index.isin(key_diff)]
        else:
            # or take what came after the cutoff
            df_to_do = self.df_in[self.df_in['scrape_ts'] > self.forced_cutoff]

        processors = [self.process_ronorp,
                      ]
        fails = []
        processed = []
        for func in processors:
            df_processed, df_to_do, df_fail = func(df_to_do)
            fails.append(df_fail)
            processed.append(df_processed)

        df_failed = pd.concat(fails)
        logger.info(f'failed to process {len(df_failed)} entries')
        logger.info(f'for {len(df_to_do)} entries there is no prcoessor')

        self.df_out = pd.concat([self.df_out, df_processed])
        self.df_out.to_csv(self.output)
        logger.info(f'added {len(df_processed)} entries to processing output')

    def process_ronorp(self, df):
        # process the pending data for the ronorp domain
        if len(df) == 0:
            logger.info('no entries for ronorp to process')
            return df, df, df
        regex = {r'category1': r'Biete \/ Suche \/ Tausche: (.+?):',
                 r'bid_ask': r'Biete \/ Suche \/ Tausche: .+?: (.+?) ',
                 r'rent_buy': r'Mieten \& Kaufen: (.+?) ',
                 r'rooms': r'Zimmer: ([^a-zA-Z ]+)',
                 r'cost': r'Kosten: ([^a-zA-Z ]+)',
                 r'address': r'Adresse: (.*?)Kontakt',
                 r'duration': r'Vertragsart: (.*?) ',
                 }

        ind_ronorp = df['domain'] == 'ronorp'
        df_processed = df[ind_ronorp]
        df_to_do = df[~ind_ronorp]
        dfs = []
        failed = [False] * len(df)
        n_iter = len(df)
        l = Looplogger(n_iter, f'processing {n_iter} entries')
        for i, row in df.iterrows():
            l.log(i)
            try:
                with open(row['fname'], 'r', encoding='utf-8') as f_in:
                    soup = BeautifulSoup(f_in.read())
            except:
                logger.info(f"could not ingest {row['fname']}")
                failed[i] = True
                continue

            details = html_clean_1(soup.find('div', attrs={'class': 'detail_block'}).get_text())
            details = details.split('Die vollen', 1)[0]

            extracted_details = extract_regex_details(details, regex)
            extracted_details['details'] = details
            extracted_details['text'] = soup.find('p', attrs={'class': 'text_comment'}).get_text()
            extracted_details['timestamp'] = dateparser.parse(
                soup.find_all('div', attrs={'class': 'pull-left'})[0].get_text())

            dfs.append(pd.DataFrame(index=[i], data=extracted_details))

        failed = np.array(failed)
        df_fail = df[failed]
        df_processed = df[~failed].join(pd.concat(dfs), how='left')

        return df_processed, df_to_do, df_fail


# class process(Processwrapper):
#     """
#     my data processing class
#     """
#
#     def __init__(self, config):
#         self.input =  # input file
#
#         self.savedir = wrapper.datapath / self.domain
#         if not os.path.isdir(self.savedir):
#             os.makedirs(self.savedir)
#
#         self.driverpath = wrapper.driverpath
#         self.driver = wrapper.driver
#         self.vault = wrapper.vault
#         self.indexfile = self.savedir / 'indexfile.scrape'
#
#     def process(self):
#
#         if self.domain == 'ronorp':
#             self.process_ronorp()
#         else:
#             logger.info(f'There is no process_{self.domain}().')


def extract_details(self):
    # the details are as of yet only a bunch of string. convert them to meaningful content
    for key, val in self.results.items():
        regex = {r'category1': r'Biete \/ Suche \/ Tausche: (.+?):',
                 r'bid_ask': r'Biete \/ Suche \/ Tausche: .+?: (.+?) ',
                 r'rent_buy': r'Mieten \& Kaufen: (.+?) ',
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

        # d = dict()
        # # d['owner'] = soup.find(attrs={'class': 'avatar'}).img['title']
        # d['owner'] = soup.find(attrs={'class': 'user_info'}).find('div', attrs={'class': 'name'})['title']
        # d['timestamp'] = dateparser.parse(soup.find('div', attrs={'class': 'pull-left'}).string)
        # d['text'] = soup.find('p', attrs={'class': 'text_comment'}).get_text()
        #
        # details = soup.find('div', attrs={'class': 'detail_block'})
        # d['details'] = html_clean_1(details.get_text())
        #
        # self.results[key].update({k: html_clean_1(v) for k, v in d.items()})


# seudocode
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
    pw = Processwrapper(config)
    pw.process()
