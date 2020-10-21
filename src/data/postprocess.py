import os

import pandas as pd
from loguru import logger
from utils.utils import *


class Postprocessor:
    # a simple class to host the postprocessing
    def __init__(self, config):
        self.root = config['root']
        self.datapath = self.root / 'data'
        self.input = self.datapath / config['processed']
        self.output = self.datapath / config['post_processed']

        if os.path.isfile(self.input):
            self.df_in = pd.read_csv(self.input)
        else:
            logger.info('no input data')
            return None

        if os.path.isfile(self.output):
            self.df_out = pd.read_csv(self.output)
        else:
            self.df_out = pd.DataFrame()
        # self.force_reprocessing = config['force_reprocessing']
        # self.forced_cutoff = config['forced_cutoff']

    def postprocess(self):

        # first, let's get all the new stuff
        # if not self.force_reprocessing:
        # Either Identify what is new in the input
        if 'title' in self.df_out.columns:
            key_diff = set(self.df_in.alt).difference(self.df_out.title)
            df_to_do = self.df_in[self.df_in.index.isin(key_diff)]
        else:
            df_to_do = self.df_in
        # else:
        #     # or take what came after the cutoff
        #     df_to_do = self.df_in[self.df_in['scrape_ts'] > self.forced_cutoff]

        post_processors = [self.postprocess_ronorp,
                           ]
        fails = []
        processed = []
        for func in post_processors:
            df_processed, df_to_do, df_fail = func(df_to_do)
            fails.append(df_fail)
            processed.append(df_processed)

        df_failed = pd.concat(fails)
        logger.info(f'failed to post-process {len(df_failed)} entries')
        logger.info(f'for {len(df_to_do)} entries there is no post-prcoessor')

        self.df_out = pd.concat([self.df_out, df_processed])
        self.df_out.to_csv(self.output, index=False)
        logger.info(f'added {len(df_processed)} entries to post-processing output')

    def postprocess_ronorp(self, df):
        if len(df) == 0:
            logger.info('no entries for ronorp to process')
            return df, df, df
        ind_ronorp = df['domain'] == 'ronorp'
        df_processed = df[ind_ronorp]
        df_to_do = df[~ind_ronorp]

        # removecrap
        dropcols = ['index_url',
                    'class',
                    'data-attr-href',
                    'data-trace',
                    'data-category',
                    'data-label',
                    'data-trace',
                    'fname',

                    ]
        df_processed = safe_drop(df_processed, dropcols)

        # renaming
        renames = {'alt': 'title',
                   }
        df_processed.rename(renames, inplace=True)

        # make numericals
        df_processed['cost'] = pd.to_numeric(df_processed['cost'].str.replace("'", ''), errors='coerce')

        return df_processed, df_to_do, pd.DataFrame()


def main(config):
    P = Postprocessor(config)
    P.postprocess()
