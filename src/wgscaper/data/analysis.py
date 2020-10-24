import os
import re

import pandas as pd
from loguru import logger


class Analysis:
    # a simple class to host the analysis & search
    def __init__(self, config):
        self.root = config['root']
        self.datapath = self.root / 'data'
        self.input = self.datapath / config['post_processed']
        self.output = self.datapath / config['results_archive']
        self.resultspath = self.datapath / 'searchresults'
        self.searchdict = config['searchdict']
        self.now = config['now']
        if not os.path.isdir(self.resultspath):
            os.makedirs(self.resultspath)

        if os.path.isfile(self.input):
            self.df_in = pd.read_csv(self.input)
        else:
            logger.info(f'no input data on {self.input}')

        if os.path.isfile(self.output):
            self.df_out = pd.read_csv(self.output)
        else:
            self.df_out = pd.DataFrame()

    def analyse(self):
        logger.info(f'starting analysing {self.input}')
        # first, let's get all the new stuff
        # if not self.force_reprocessing:
        # Either Identify what is new in the input
        if 'title' in self.df_out.columns:
            key_diff = set(self.df_in['title']).difference(self.df_out['title'])
            df_to_do = self.df_in[self.df_in['title'].isin(key_diff)]
            n = len(self.df_in)
            n_attempt = len(key_diff)
            ignored = len(self.df_in) - n_attempt
            logger.info(f'attempting to analyse {n_attempt} of {n} entries, ignoring {ignored} existing entries')
        else:
            df_to_do = self.df_in
            logger.info(f'{self.output} not found. post processing all entries')
        # else:
        #     # or take what came after the cutoff
        #     df_to_do = self.df_in[self.df_in['scrape_ts'] > self.forced_cutoff]

        if len(df_to_do) == 0:
            logger.info(f'no new entries for analysis in {self.input}')
            return None

        results = []
        for search in self.searchdict:
            col = search['column']
            pat = search['pattern']
            if search['flag'] == 'IGNORECASE':
                results.append(df_to_do[df_to_do[col].str.contains(pat, flags=re.IGNORECASE, regex=True)])
            else:
                results.append(df_to_do[df_to_do[col].str.contains(pat, regex=True)])

        result = pd.concat(results).drop_duplicates(subset=['title'])
        result = result[result['bid_ask'] == 'Biete']
        self.result = result

        fname = self.resultspath / ('results_' + self.now.strftime('%Y_%m_%d__%H_%M_%S') + '.csv')
        self.result_file_name = fname
        self.df_out = pd.concat([self.df_out, result], axis=0)

        logger.info(f'added {len(result)} entries to {self.output}')

    def save_to_csv(self):
        self.result.to_csv(self.result_file_name, index=False)
        self.df_out.to_csv(self.output, index=False)


def main(config):
    a = Analysis(config)
    a.analyse()
    a.save_to_csv()
