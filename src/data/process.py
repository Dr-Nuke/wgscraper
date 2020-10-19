import os

import pandas as pd
from bs4 import BeautifulSoup
from loguru import logger
from patlib import Path
from utils.utils import *


class Processwrapper:
    # a wrapper class for the individual processing stage
    pass


cwd = Path(os.getcwd())

config = {'df_file': 'room_database.csv'}


def process_ronorp(config):
    df_file = config['df_file']
    df = pd.read_csv(df)
    for i, row in df.iterrows():
        if row['processed']:
            continue

        try:
            with open(row['fname'], 'r') as f_in:
                soup = BeautifulSoup(f_in.read())
        except:
            logger.info(f"could not read {row['fname']}")
            continue




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
