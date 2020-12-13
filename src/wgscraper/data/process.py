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




def main(config):
    logger.info(f'starting processing')

    proc = process(config)
    return proc
