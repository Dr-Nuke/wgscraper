import os
import re

import pandas as pd
from loguru import logger


class Analysis:
    # a simple class to host the analysis & search
    def __init__(self, config):
        self.urls = config['urls']
        self.specific = config['specific_configs']
        self.root = config['root']

        # separate results
        self.datapath = config['datapath']
        self.input_path = self.datapath / config['post_processed']
        self.output_path = self.datapath / config['results_archive']
        self.analyzed_path = self.datapath / config['analyzed']
        self.results_path = self.datapath / 'searchresults'
        self.searchdict = config['searchdict']
        self.now = config['now']
        self.result = {}
        if not os.path.isdir(self.results_path):
            os.makedirs(self.resultspath)

        if os.path.isfile(self.input_path):
            self.input_df = pd.read_csv(self.input_path)
        else:
            logger.info(f'no input data on {self.input_path}')

        if os.path.isfile(self.analyzed_path):
            self.output_df = pd.read_csv(self.analyzed_path)
        else:
            self.output_df = pd.DataFrame()

    def analyse(self):
        logger.info(f'starting analysing {self.input_path}')
        # first, let's get all the new stuff
        # if not self.force_reprocessing:
        # Either Identify what is new in the input
        if 'title' in self.output_df.columns:
            key_diff = set(self.input_df['title']).difference(self.output_df['title'])
            df_to_do = self.input_df[self.input_df['title'].isin(key_diff)]
            n = len(self.input_df)
            n_attempt = len(key_diff)
            ignored = len(self.input_df) - n_attempt
            logger.info(f'attempting to analyse {n_attempt} of {n} entries, ignoring {ignored} existing entries')
        else:
            df_to_do = self.input_df
            logger.info(f'{self.output_path} not found. post processing all entries')

        if len(df_to_do) == 0:
            logger.info(f'no new entries for analysis in {self.input_path}')


        results = []
        for search in self.searchdict:
            col = search['column']
            pat = search['pattern']
            if search['flag'] == 'IGNORECASE':
                result = df_to_do[df_to_do[col].str.contains(pat, flags=re.IGNORECASE, regex=True)]

            else:
                result = df_to_do[df_to_do[col].str.contains(pat, regex=True)]

            if len(result) > 0:
                pd.options.mode.chained_assignment = None
                result['searchterm'] = pat
                result['searchfield'] = col
                pd.options.mode.chained_assignment = 'warn'
            results.append(result)

        result = pd.concat(results).drop_duplicates(subset=['title'])
        result = result[result['bid_ask'] == 'Biete']
        result = result[result['duration'] == 'unbefristet']
        self.result = result
        if len(result) == 0:
            logger.info(f'no new results fo analysis in {self.input_path}')


        fname = self.results_path / ('results_' + self.now.strftime('%Y_%m_%d__%H_%M_%S') + '.csv')
        self.result_file_name = fname

        self.output_df = pd.concat([self.output_df, df_to_do],axis=0)
        logger.info(f'added {len(result)} search results to {self.output_path}')


    def save_to_csv(self):

        self.output_df.to_csv(self.analyzed_path, index=False)
        self.result.to_csv(self.result_file_name, index=False)
        if len(self.result) != 0:
            self.result.to_csv(self.output_path, index=False)


def main(config):
    a = Analysis(config)
    a.analyse()
    a.save_to_csv()
